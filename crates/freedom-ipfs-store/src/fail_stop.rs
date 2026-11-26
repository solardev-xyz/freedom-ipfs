//! Out of memory as a fail-stop (freedom-core's design 0010, "The memory
//! cap", and 0007's question 9): under a service's memory cap, an
//! allocation SQLite can't make ends the process with exit 71, and never
//! becomes a request's error.
//!
//! The store's only FFI is here: the two `sqlite3_config` calls, the
//! wrappers around SQLite's allocator, and `_exit`, each with its `SAFETY`
//! comment. [`fail_stop_on_oom`] is an `unsafe fn`: SQLite's configuration
//! is process-global and not thread-safe, which no safe function can
//! guarantee on its caller's behalf.

use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use rusqlite::ffi;

/// The exit code an out-of-memory ends the process with: 71 (`EX_OSERR`),
/// `out_of_memory` in freedom-core's designs 0007 and 0010, and fc-service's
/// `Exit::OutOfMemory`.
pub const OUT_OF_MEMORY_EXIT: i32 = 71;

/// The line the hook writes to standard error before it exits: static, so
/// nothing is formatted or allocated on the way out.
pub const OUT_OF_MEMORY_LINE: &[u8] = b"freedom-ipfs-store: SQLite is out of memory; exiting 71\n";

/// Why [`fail_stop_on_oom`] installed nothing. SQLite's allocator is then
/// unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FailStopError {
    /// SQLite was already in use: a store had opened, or other code had
    /// initialized SQLite (`sqlite3_config` answered `SQLITE_MISUSE`).
    #[error("SQLite is already in use: its allocator is wrapped only before its first use")]
    TooLate,
    /// SQLite refused the configuration with another code.
    #[error("SQLite refused its allocator's wrappers (code {0})")]
    Refused(i32),
    /// SQLite's allocator has no `xMalloc` or no `xRealloc` to wrap.
    #[error("SQLite's allocator has no xMalloc or xRealloc to wrap")]
    Incomplete,
}

/// Makes SQLite's out-of-memory end the process: an allocation SQLite can't
/// make exits with [`OUT_OF_MEMORY_EXIT`] (71) at once, before SQLite or the
/// store sees it, so no statement fails with `SQLITE_NOMEM` and no request
/// is answered with it. For a service under freedom-core's memory cap
/// (design 0010, "The memory cap").
///
/// **What it detects:** a call too late to install anything returns
/// [`FailStopError::TooLate`] and changes nothing, in the two cases it can
/// tell: a store of this crate has opened (the call and the opens take one
/// lock), or SQLite answers that it has initialized (`SQLITE_MISUSE`). A
/// call after one that succeeded does nothing and returns `Ok`. The
/// wrappers stay for the process's life.
///
/// **What it does:** it reads SQLite's allocator (`SQLITE_CONFIG_GETMALLOC`)
/// and installs it back (`SQLITE_CONFIG_MALLOC`) with its `xMalloc` and
/// `xRealloc` wrapped: each calls the default and, if that answers NULL for
/// a nonzero size, calls `_exit(71)`. No destructor, `atexit` handler or
/// buffer flush runs, since any could allocate; the exit code alone says
/// why. Its other methods are SQLite's own. An allocation SQLite would have
/// survived the failure of, such as a hash table it couldn't grow, ends the
/// process too: under the cap, the next allocation would fail anyway.
///
/// **What it leaves:** an `SQLITE_NOMEM` that SQLite reports without its
/// allocator failing (a size past its allocation limit, or a heap limit set
/// on it) is still an error,
/// [`StoreError::OutOfMemory`](crate::StoreError::OutOfMemory), which the
/// host ends the process for.
///
/// # Safety
///
/// Call it once, in `main`, before any thread starts and before any SQLite
/// use in the process: before the first
/// [`SqliteBlockStore`](crate::SqliteBlockStore) opens, and before any other
/// code calls into SQLite, through rusqlite or otherwise. `sqlite3_config`
/// isn't thread-safe: SQLite requires that no other thread call any SQLite
/// interface while it runs, and SQLite takes an allocator only before it
/// initializes, which its first use does. The cases above are the only ones
/// it detects; another thread using SQLite during the call is a data race
/// in SQLite's global configuration.
pub unsafe fn fail_stop_on_oom() -> Result<(), FailStopError> {
    let mut state = state();
    match *state {
        State::FailStop => return Ok(()),
        State::InUse => return Err(FailStopError::TooLate),
        State::Untouched => {}
    }
    // Whatever happens next, it happens once: SQLite is either wrapped or
    // left as it is, for the process's life.
    *state = State::InUse;
    let mut methods = ffi::sqlite3_mem_methods {
        xMalloc: None,
        xFree: None,
        xRealloc: None,
        xSize: None,
        xRoundup: None,
        xInit: None,
        xShutdown: None,
        pAppData: std::ptr::null_mut(),
    };
    // SAFETY: SQLITE_CONFIG_GETMALLOC takes one argument, a pointer to a
    // `sqlite3_mem_methods` it fills in; `methods` is one, live and writable
    // for the call. `sqlite3_config` isn't thread-safe: the caller's
    // contract (# Safety) keeps every other SQLite use out while it runs,
    // and the state's lock keeps this crate's opens out besides. Once SQLite
    // has initialized, it changes nothing and answers SQLITE_MISUSE.
    let code = unsafe {
        ffi::sqlite3_config(
            ffi::SQLITE_CONFIG_GETMALLOC,
            &mut methods as *mut ffi::sqlite3_mem_methods,
        )
    };
    refused(code)?;
    let (Some(malloc), Some(realloc)) = (methods.xMalloc, methods.xRealloc) else {
        return Err(FailStopError::Incomplete);
    };
    // Set once: only the one call that finds the state untouched gets here.
    let _ = DEFAULTS.set(Defaults { malloc, realloc });
    let wrapped = ffi::sqlite3_mem_methods {
        xMalloc: Some(fail_stop_malloc),
        xRealloc: Some(fail_stop_realloc),
        ..methods
    };
    // SAFETY: SQLITE_CONFIG_MALLOC takes one argument, a pointer to a
    // `sqlite3_mem_methods`, which SQLite copies before it returns; `wrapped`
    // is live for the call. Its methods are the ones SQLite just gave but
    // for the two wrappers, which keep their contracts: each returns what
    // the default returns, or doesn't return. As for GETMALLOC above, the
    // caller's contract and the lock keep every other SQLite use out.
    let code = unsafe {
        ffi::sqlite3_config(
            ffi::SQLITE_CONFIG_MALLOC,
            &wrapped as *const ffi::sqlite3_mem_methods,
        )
    };
    refused(code)?;
    *state = State::FailStop;
    FAIL_STOP.store(true, Ordering::Release);
    Ok(())
}

/// Called before a store opens its connection: from then on SQLite is in
/// use, and [`fail_stop_on_oom`] installs nothing unless it already has.
/// The two take one lock, so a store opening on one thread never races the
/// configuration on another.
pub(crate) fn opening() {
    let mut state = state();
    if *state == State::Untouched {
        *state = State::InUse;
    }
}

/// Under fail-stop, ends the process as the wrappers do: for an
/// out-of-memory error a store method can't pass up. Otherwise returns.
pub(crate) fn exit_if_fail_stop() {
    if FAIL_STOP.load(Ordering::Acquire) {
        out_of_memory();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Nothing has used SQLite through this crate yet.
    Untouched,
    /// A store has opened, or a call to [`fail_stop_on_oom`] installed
    /// nothing.
    InUse,
    /// The wrappers are installed.
    FailStop,
}

static STATE: Mutex<State> = Mutex::new(State::Untouched);

/// Whether the wrappers are installed, read without the lock.
static FAIL_STOP: AtomicBool = AtomicBool::new(false);

/// SQLite's own `xMalloc` and `xRealloc`, which the wrappers call.
struct Defaults {
    malloc: unsafe extern "C" fn(c_int) -> *mut c_void,
    realloc: unsafe extern "C" fn(*mut c_void, c_int) -> *mut c_void,
}

static DEFAULTS: OnceLock<Defaults> = OnceLock::new();

fn state() -> MutexGuard<'static, State> {
    // Nothing panics under the lock, and the state is a plain value.
    STATE.lock().unwrap_or_else(PoisonError::into_inner)
}

fn refused(code: c_int) -> Result<(), FailStopError> {
    match code {
        ffi::SQLITE_OK => Ok(()),
        ffi::SQLITE_MISUSE => Err(FailStopError::TooLate),
        code => Err(FailStopError::Refused(code)),
    }
}

/// SQLite's `xMalloc`, wrapped: the default's allocation, or the process's
/// end.
unsafe extern "C" fn fail_stop_malloc(bytes: c_int) -> *mut c_void {
    // Installed only once `DEFAULTS` is set.
    let Some(defaults) = DEFAULTS.get() else {
        std::process::abort();
    };
    // SAFETY: SQLite calls this as its `xMalloc`, under that method's
    // contract, which the default it wraps has too.
    let allocation = unsafe { (defaults.malloc)(bytes) };
    if allocation.is_null() && bytes > 0 {
        out_of_memory();
    }
    allocation
}

/// SQLite's `xRealloc`, wrapped: the default's reallocation, or the
/// process's end.
unsafe extern "C" fn fail_stop_realloc(prior: *mut c_void, bytes: c_int) -> *mut c_void {
    // Installed only once `DEFAULTS` is set.
    let Some(defaults) = DEFAULTS.get() else {
        std::process::abort();
    };
    // SAFETY: SQLite calls this as its `xRealloc`, under that method's
    // contract (`prior` came from this allocator, and is live), which the
    // default it wraps has too.
    let allocation = unsafe { (defaults.realloc)(prior, bytes) };
    if allocation.is_null() && bytes > 0 {
        out_of_memory();
    }
    allocation
}

/// Ends the process with [`OUT_OF_MEMORY_EXIT`] at once, after writing
/// [`OUT_OF_MEMORY_LINE`] to standard error.
///
/// Only async-signal-safe calls: a `write` of a static buffer and `_exit`,
/// with nothing formatted, allocated or unwound.
fn out_of_memory() -> ! {
    #[cfg(unix)]
    let len = OUT_OF_MEMORY_LINE.len();
    #[cfg(windows)]
    let len = OUT_OF_MEMORY_LINE.len() as std::ffi::c_uint;
    // SAFETY: `write` reads `len` bytes from `OUT_OF_MEMORY_LINE`, a static
    // buffer of that length, and touches no other memory. A short or failed
    // write is ignored: the exit code is what the host reads.
    let _ = unsafe { libc::write(2, OUT_OF_MEMORY_LINE.as_ptr().cast(), len) };
    // SAFETY: `_exit` takes no pointer and reads no Rust state: it ends the
    // process with the code, running no destructor, `atexit` handler or
    // stdio flush (POSIX `_exit`, and the C runtime's on Windows).
    unsafe { libc::_exit(OUT_OF_MEMORY_EXIT) }
}
