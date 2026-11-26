//! `fail_stop_on_oom` (freedom-core's design 0010, "The memory cap"): with
//! it installed, an allocation SQLite can't make ends the process with exit
//! 71, and no store method returns an error for it.
//!
//! Each case runs in a child of its own (this binary again, with
//! `FREEDOM_IPFS_STORE_FAIL_STOP_CASE` set): the hook is process-wide, and
//! a lowered memory limit must never reach the test runner.
//!
//! - `hooked`: installed first, it changes nothing else: the store works,
//!   and a second call is `Ok`.
//! - `late-store` and `late-sqlite`: after a store has opened, or other
//!   code has initialized SQLite, it installs nothing and says so.
//! - `fill` (Linux): the child lowers its `RLIMIT_DATA` to a few MiB above
//!   what it uses, installs the hook, and fills a memory store, its page
//!   cap lifted and its hot cache off, until SQLite can't allocate. It
//!   exits 71, and no put returns.
//! - `fill-unhooked` (Linux), the control: the same without the hook. The
//!   put that meets the limit returns [`StoreError::OutOfMemory`].

use std::process::{Command, Output};
use std::time::Duration;

use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_store::{
    fail_stop_on_oom, FailStopError, SqliteBlockStore, StoreError, StoreOptions, OUT_OF_MEMORY_EXIT,
};

const CASE: &str = "FREEDOM_IPFS_STORE_FAIL_STOP_CASE";

/// A fill's exit when a put returned out of memory.
const RETURNED_OUT_OF_MEMORY: i32 = 10;
/// A fill's exit when a put returned another error.
const RETURNED_ERROR: i32 = 11;
/// A fill's exit when the store never ran out.
const NEVER_RAN_OUT: i32 = 12;

/// What a fill's limit leaves above the child's data in use.
#[cfg(target_os = "linux")]
const HEADROOM: u64 = 32 * 1024 * 1024;
/// A fill's blocks, each 64 KiB, at most 1 GiB of them.
#[cfg(target_os = "linux")]
const BLOCK: usize = 64 * 1024;
#[cfg(target_os = "linux")]
const MAX_BLOCKS: u64 = 16 * 1024;

fn main() {
    if let Ok(case) = std::env::var(CASE) {
        child(&case);
        return;
    }
    parent();
}

fn child(case: &str) {
    match case {
        "hooked" => hooked(),
        "late-store" => {
            let store = SqliteBlockStore::in_memory(1 << 20).unwrap();
            assert_eq!(fail_stop_on_oom(), Err(FailStopError::TooLate));
            assert_eq!(fail_stop_on_oom(), Err(FailStopError::TooLate));
            round_trip(&store);
            println!("LATE refused");
        }
        "late-sqlite" => {
            // Other code's SQLite, initialized before the call.
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t(x);").unwrap();
            assert_eq!(fail_stop_on_oom(), Err(FailStopError::TooLate));
            round_trip(&SqliteBlockStore::in_memory(1 << 20).unwrap());
            println!("LATE refused");
        }
        #[cfg(target_os = "linux")]
        "fill" => {
            lower_data_limit();
            fail_stop_on_oom().unwrap();
            fill();
        }
        #[cfg(target_os = "linux")]
        "fill-unhooked" => {
            lower_data_limit();
            fill();
        }
        other => panic!("unknown case {other}"),
    }
}

/// Installed first, the hook leaves the store as it was.
fn hooked() {
    assert_eq!(fail_stop_on_oom(), Ok(()));
    assert_eq!(fail_stop_on_oom(), Ok(()), "a second call");
    let store = SqliteBlockStore::in_memory(4 << 20).unwrap();
    round_trip(&store);
    let mut blocks = Vec::new();
    for i in 0..64u8 {
        let data = vec![i; 16 * 1024 + usize::from(i)];
        let cid = cid_from_data(CODEC_RAW, &data);
        store.put_block(&cid, &data).unwrap();
        blocks.push((cid, data));
    }
    for (cid, data) in &blocks {
        assert_eq!(store.get(cid).unwrap().unwrap().data(), &data[..]);
    }
    let car = store.export_car().unwrap();
    let copy = SqliteBlockStore::in_memory(4 << 20).unwrap();
    assert_eq!(copy.import_car(&car).unwrap().len(), 65);
    store.trim_blocks_to(0).unwrap();
    assert_eq!(store.block_count().unwrap(), 0);
    assert!(store.close(Duration::from_secs(1)));
    println!("HOOKED works");
}

fn round_trip(store: &SqliteBlockStore) {
    let data = b"fail-stop".to_vec();
    let cid = cid_from_data(CODEC_RAW, &data);
    store.put_block(&cid, &data).unwrap();
    assert_eq!(store.get(&cid).unwrap().unwrap().data(), &data[..]);
}

/// Lowers this process's `RLIMIT_DATA`, soft and hard, to [`HEADROOM`]
/// above the private writable memory it uses now (`VmData`, what the limit
/// counts).
#[cfg(target_os = "linux")]
fn lower_data_limit() {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let used_kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmData:"))
        .and_then(|value| value.trim().strip_suffix("kB"))
        .and_then(|value| value.trim().parse().ok())
        .expect("VmData in /proc/self/status");
    let limit = used_kib * 1024 + HEADROOM;
    let rlimit = libc::rlimit {
        rlim_cur: limit,
        rlim_max: limit,
    };
    // SAFETY: `setrlimit` reads one `rlimit`, live for the call.
    let set = unsafe { libc::setrlimit(libc::RLIMIT_DATA, &rlimit) };
    assert_eq!(set, 0, "setrlimit: {}", std::io::Error::last_os_error());
    println!("LIMIT {limit}");
}

/// Puts distinct 64 KiB blocks into a memory store with no page cap until
/// SQLite runs out, and exits with what the put that met it returned.
#[cfg(target_os = "linux")]
fn fill() -> ! {
    let store = SqliteBlockStore::in_memory_with(StoreOptions {
        max_bytes: u64::MAX,
        // The block's only copy is SQLite's: the store's own allocations
        // are a few small ones per put, so SQLite's meet the limit first.
        hot_cache_bytes: Some(0),
        ..StoreOptions::default()
    })
    .unwrap();
    // One buffer for every block: its first bytes make each distinct.
    let mut data = vec![0u8; BLOCK];
    for i in 0..MAX_BLOCKS {
        data[..8].copy_from_slice(&i.to_be_bytes());
        let cid = cid_from_data(CODEC_RAW, &data);
        match store.put_block(&cid, &data) {
            Ok(()) => {}
            Err(StoreError::OutOfMemory(_)) => {
                // Text alone: the process has no memory to spare.
                eprintln!("RETURNED out of memory");
                std::process::exit(RETURNED_OUT_OF_MEMORY);
            }
            Err(error) => {
                eprintln!("RETURNED {error}");
                std::process::exit(RETURNED_ERROR);
            }
        }
        if i % 32 == 31 {
            println!("STORED {}", i + 1);
        }
    }
    std::process::exit(NEVER_RAN_OUT);
}

fn run(case: &str) -> Output {
    Command::new(std::env::current_exe().unwrap())
        .env(CASE, case)
        .output()
        .unwrap()
}

/// Runs `case`, and checks it ended with `code` and printed `marker`.
fn expect(case: &str, code: i32, marker: &str) -> String {
    let output = run(case);
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(
        output.status.code(),
        Some(code),
        "{case}: {:?}\nstdout: {stdout}\nstderr: {stderr}",
        output.status
    );
    assert!(
        stdout.contains(marker),
        "{case}: {marker:?} missing\nstdout: {stdout}\nstderr: {stderr}"
    );
    println!("fail_stop: {case}: exit {code}, {}", marker.trim_end());
    stdout + &stderr
}

fn parent() {
    expect("hooked", 0, "HOOKED works");
    expect("late-store", 0, "LATE refused");
    expect("late-sqlite", 0, "LATE refused");
    #[cfg(target_os = "linux")]
    {
        // The hook: exit 71, after some puts, and no put returned.
        let fill = expect("fill", OUT_OF_MEMORY_EXIT, "STORED 32\n");
        assert!(!fill.contains("RETURNED"), "fill: {fill}");
        // The control: the put returns out of memory, its own kind.
        let control = expect("fill-unhooked", RETURNED_OUT_OF_MEMORY, "STORED 32\n");
        assert!(control.contains("RETURNED out of memory"), "{control}");
    }
    #[cfg(not(target_os = "linux"))]
    println!("fail_stop: the fills lower RLIMIT_DATA, on Linux only");
}
