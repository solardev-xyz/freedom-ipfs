//! How retrieval reserves memory (Freedom design 0007, "Budgets").
//!
//! A node built with a [`MemoryBudget`] reserves every receive buffer from
//! it before the buffer is allocated, and never lets the buffer grow past
//! what it reserved. Each reservation is taken before the request it serves
//! goes out, so before any network timer starts: waiting for memory is never
//! mistaken for a slow provider or peer.
//!
//! - **an HTTP provider's body:** the maximum block size (2 MiB), shrunk to
//!   the body's declared `Content-Length` once the head says it. The buffer
//!   grows into it, never past it, and is then trued down to the block's
//!   exact size: shrunk in place, and only then the reservation.
//! - **a Bitswap read:** the message cap (the maximum block size and 4 KiB
//!   of framing). Each message takes its exact length from it, read from
//!   its prefix, for an allocation of exactly that length; a message without
//!   blocks gives it back for the next. The blocks are views into the
//!   message, decoded without copying, so each keeps the whole message
//!   charged until the last of them is dropped. Incoming streams, which
//!   answer no one request, reserve within their own read timeout.
//!
//! One fetch can have several receive buffers at once (racing and hedged
//! HTTP providers, several Bitswap peers), so each reserves its own rather
//! than the fetch reserving once. A budget must hold at least one Bitswap
//! message cap.
//!
//! The bytes come back as [`ChargedBytes`] inside the [`freedom_ipfs_core::Block`],
//! and every clone of the block keeps the charge until the last is dropped.
//!
//! **Waiting.** A reservation waits as the work it serves says
//! ([`MemoryWait`]): request work waits until it is cancelled (its future
//! dropped), or until a deadline its host gives; background work never
//! waits, and fails at once when the budget is full. A fetch that a
//! refusal made fail fails with [`crate::RetrievalError::MemoryBudget`]
//! (through the block provider, [`freedom_ipfs_core::CoreError::MemoryBudget`]),
//! which a host maps to busy.
//!
//! A node built without a budget reserves nothing, as before.

use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

pub use freedom_ipfs_core::{BudgetError, ChargedBytes, MemoryBudget, Reservation, ReservedVec};

/// How a reservation waits when the budget is full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryWait {
    /// Waits until the work is cancelled, which drops the wait, or until
    /// `deadline`, if there is one: request work.
    Wait {
        deadline: Option<tokio::time::Instant>,
    },
    /// Never waits: background work, which fails at once.
    Never,
}

impl MemoryWait {
    /// Waits until the work is cancelled.
    pub const UNTIL_CANCELLED: MemoryWait = MemoryWait::Wait { deadline: None };

    /// Waits until `deadline` at most.
    pub fn until(deadline: tokio::time::Instant) -> Self {
        Self::Wait {
            deadline: Some(deadline),
        }
    }

    /// Reserves `bytes` from `budget`, waiting as this says.
    pub async fn reserve(
        self,
        budget: &MemoryBudget,
        bytes: usize,
    ) -> Result<Reservation, BudgetError> {
        match self {
            Self::Wait { deadline: None } => budget.reserve(bytes).await,
            Self::Wait {
                deadline: Some(deadline),
            } => budget.reserve_until(bytes, deadline).await,
            Self::Never => budget.try_reserve(bytes),
        }
    }
}

tokio::task_local! {
    static MEMORY_WAIT: MemoryWait;
}

thread_local! {
    static THREAD_MEMORY_WAIT: Cell<Option<MemoryWait>> = const { Cell::new(None) };
}

/// Runs `future` as work whose reservations wait as `wait` says.
pub async fn with_memory_wait<F: Future>(wait: MemoryWait, future: F) -> F::Output {
    MEMORY_WAIT.scope(wait, future).await
}

/// How the current work's reservations wait: as [`with_memory_wait`] set,
/// or [`enter_memory_wait`] on this thread, or else until cancelled, as
/// request work does.
pub fn current_memory_wait() -> MemoryWait {
    MEMORY_WAIT
        .try_with(|wait| *wait)
        .ok()
        .or_else(|| THREAD_MEMORY_WAIT.with(Cell::get))
        .unwrap_or(MemoryWait::UNTIL_CANCELLED)
}

/// Makes the synchronous work on this thread, until the guard is dropped,
/// reserve as `wait` says: for a blocking job that reads through the
/// synchronous block provider, which carries it into its fetches.
pub fn enter_memory_wait(wait: MemoryWait) -> MemoryWaitGuard {
    let previous = THREAD_MEMORY_WAIT.with(|current| current.replace(Some(wait)));
    MemoryWaitGuard {
        previous,
        _thread: PhantomData,
    }
}

/// Restores the thread's previous [`MemoryWait`] when dropped.
#[must_use = "the thread reserves as asked only while the guard lives"]
pub struct MemoryWaitGuard {
    previous: Option<MemoryWait>,
    /// It belongs to its thread.
    _thread: PhantomData<*const ()>,
}

impl Drop for MemoryWaitGuard {
    fn drop(&mut self) {
        let previous = self.previous;
        THREAD_MEMORY_WAIT.with(|current| current.set(previous));
    }
}

/// A fetch's memory: the budget its receive buffers reserve from, and what
/// memory pressure did to it, so the fetch can tell local pressure from a
/// provider's fault:
///
/// - **refusals:** a reservation that failed (it couldn't wait, or its
///   deadline passed), or one whose wait was cut short (an enclosing timer
///   fired, or its future was dropped) before it was served;
/// - **the waits now outstanding**, which a coalesced follower watches;
/// - **waits begun**, served or not.
///
/// Pressure is attributed to an operation, not to the fetch for good: a
/// [`PressureWindow`] opened when the operation starts sees only what
/// happened since, so a wait served before (or during) it doesn't excuse a
/// later timeout. Clones share all of it: the request side and the Bitswap
/// swarm's side of one fetch see the same.
#[derive(Clone)]
pub(crate) struct FetchMemory {
    budget: MemoryBudget,
    state: Arc<FetchMemoryState>,
}

struct FetchMemoryState {
    /// The first refusal, which the fetch fails as if it fails.
    refusal: OnceLock<BudgetError>,
    /// The latest refusal, and how many there were.
    latest_refusal: Mutex<Option<BudgetError>>,
    refusals: AtomicU64,
    /// Waits begun, served or not.
    waits: AtomicU64,
    /// Bytes the fetch's reservations wait for now (0: none waits).
    waiting: tokio::sync::watch::Sender<usize>,
}

impl FetchMemoryState {
    fn refuse(&self, refusal: BudgetError) {
        let _ = self.refusal.set(refusal);
        *self
            .latest_refusal
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(refusal);
        self.refusals.fetch_add(1, Ordering::AcqRel);
    }
}

tokio::task_local! {
    static FETCH_MEMORY: FetchMemory;
}

/// A wait in progress: counted until it ends, and recorded as a refusal if
/// it ends unserved.
struct WaitGuard<'a> {
    state: &'a FetchMemoryState,
    bytes: usize,
    served: bool,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        if !self.served {
            self.state
                .refuse(BudgetError::Exhausted { bytes: self.bytes });
        }
        let bytes = self.bytes;
        self.state
            .waiting
            .send_modify(|waiting| *waiting = waiting.saturating_sub(bytes));
    }
}

impl FetchMemory {
    pub(crate) fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            state: Arc::new(FetchMemoryState {
                refusal: OnceLock::new(),
                latest_refusal: Mutex::new(None),
                refusals: AtomicU64::new(0),
                waits: AtomicU64::new(0),
                waiting: tokio::sync::watch::Sender::new(0),
            }),
        }
    }

    /// The memory of the fetch running now, if its node has a budget.
    pub(crate) fn current() -> Option<Self> {
        FETCH_MEMORY.try_with(Clone::clone).ok()
    }

    /// Runs `future` as a fetch with `memory` (nothing new without one).
    pub(crate) async fn scope<F: Future>(memory: Option<Self>, future: F) -> F::Output {
        match memory {
            Some(memory) => FETCH_MEMORY.scope(memory, future).await,
            None => future.await,
        }
    }

    /// Reserves `bytes` for a receive buffer, waiting as the current work
    /// does. A refusal is recorded, and so is a wait, and a wait cut short.
    pub(crate) async fn reserve(&self, bytes: usize) -> Result<Reservation, BudgetError> {
        let wait = current_memory_wait();
        match self.budget.try_reserve(bytes) {
            Ok(reservation) => return Ok(reservation),
            Err(BudgetError::Exhausted { .. }) if wait != MemoryWait::Never => {}
            Err(refusal) => {
                self.state.refuse(refusal);
                return Err(refusal);
            }
        }
        self.state.waits.fetch_add(1, Ordering::AcqRel);
        self.state
            .waiting
            .send_modify(|waiting| *waiting = waiting.saturating_add(bytes));
        let mut guard = WaitGuard {
            state: &self.state,
            bytes,
            served: false,
        };
        let reserved = wait.reserve(&self.budget, bytes).await;
        guard.served = reserved.is_ok();
        drop(guard);
        reserved
    }

    /// `bytes` if they are free now, and `None` (recording nothing) if not:
    /// for a reservation that would rather give up what it holds than wait.
    pub(crate) fn reserve_now(&self, bytes: usize) -> Option<Reservation> {
        self.budget.try_reserve(bytes).ok()
    }

    /// The first refusal the fetch met, if any (a wait cut short counts).
    pub(crate) fn refusal(&self) -> Option<BudgetError> {
        self.state.refusal.get().copied()
    }

    /// The bytes the fetch's reservations wait for now.
    pub(crate) fn waiting(&self) -> usize {
        *self.state.waiting.borrow()
    }

    /// The fetch's refusal, counting a wait still outstanding as one: what a
    /// fetch that ends now while a reservation of it still waits was cut
    /// short by (the waiting side may not have seen it yet).
    pub(crate) fn refusal_or_outstanding(&self) -> Option<BudgetError> {
        self.refusal().or_else(|| {
            let waiting = self.waiting();
            (waiting > 0).then_some(BudgetError::Exhausted { bytes: waiting })
        })
    }

    /// Returns, with the bytes waited for, once one of the fetch's
    /// reservations waits for memory (at once if one does now).
    pub(crate) async fn waiting_for_memory(&self) -> usize {
        let mut waiting = self.state.waiting.subscribe();
        let bytes = waiting
            .wait_for(|bytes| *bytes > 0)
            .await
            .map(|bytes| *bytes);
        match bytes {
            Ok(bytes) => bytes,
            // The sender lives as long as this handle: never reached.
            Err(_) => std::future::pending().await,
        }
    }
}

/// Memory pressure on one operation of a fetch (a timed read, a request, a
/// provider attempt): opened when the operation starts, it sees only what
/// happened since. Nothing, for a fetch without a budget.
pub(crate) struct PressureWindow {
    opened: Option<(FetchMemory, u64, u64)>,
}

impl PressureWindow {
    /// A window on the current fetch's memory, from now.
    pub(crate) fn open() -> Self {
        Self {
            opened: FetchMemory::current().map(|memory| {
                let refusals = memory.state.refusals.load(Ordering::Acquire);
                let waits = memory.state.waits.load(Ordering::Acquire);
                (memory, refusals, waits)
            }),
        }
    }

    /// A window that sees nothing.
    #[cfg(test)]
    pub(crate) fn none() -> Self {
        Self { opened: None }
    }

    /// What makes the operation's failure the memory budget's: a wait still
    /// outstanding, or a refusal since the window opened. A wait served
    /// before or during it doesn't: the failure is then the network's.
    pub(crate) fn pressure(&self) -> Option<BudgetError> {
        let (memory, refusals, _) = self.opened.as_ref()?;
        let waiting = memory.waiting();
        if waiting > 0 {
            return Some(BudgetError::Exhausted { bytes: waiting });
        }
        if memory.state.refusals.load(Ordering::Acquire) > *refusals {
            return *memory
                .state
                .latest_refusal
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
        }
        None
    }

    /// Whether the operation's failure is the memory budget's.
    pub(crate) fn pressured(&self) -> bool {
        self.pressure().is_some()
    }

    /// Whether a timer around the operation that also runs while its
    /// reservations wait (rather than one started after them) ran out for
    /// memory, so no peer is to blame for it: pressure now, or any wait
    /// during the operation, even one since served, as the wait spent the
    /// timer's time. Waits before the window don't count.
    pub(crate) fn excuses_timer(&self) -> bool {
        self.pressured() || self.waited()
    }

    /// Whether any reservation waited during the operation, served or not:
    /// then the time it took is partly memory's, not a peer's slowness.
    pub(crate) fn waited(&self) -> bool {
        self.opened.as_ref().is_some_and(|(memory, _, waits)| {
            memory.state.waits.load(Ordering::Acquire) > *waits || memory.waiting() > 0
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn work_waits_as_its_scope_or_thread_says() {
        assert_eq!(current_memory_wait(), MemoryWait::UNTIL_CANCELLED);
        let guard = enter_memory_wait(MemoryWait::Never);
        assert_eq!(current_memory_wait(), MemoryWait::Never);
        {
            let _inner = enter_memory_wait(MemoryWait::UNTIL_CANCELLED);
            assert_eq!(current_memory_wait(), MemoryWait::UNTIL_CANCELLED);
        }
        assert_eq!(current_memory_wait(), MemoryWait::Never);
        drop(guard);
        assert_eq!(current_memory_wait(), MemoryWait::UNTIL_CANCELLED);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let budget = MemoryBudget::new(100);
        let _full = budget.try_reserve(100).unwrap();
        runtime.block_on(async {
            // Background work fails at once; request work waits.
            let memory = FetchMemory::new(budget.clone());
            let refused = with_memory_wait(MemoryWait::Never, memory.reserve(10)).await;
            assert_eq!(refused.unwrap_err(), BudgetError::Exhausted { bytes: 10 });
            assert_eq!(memory.refusal(), Some(BudgetError::Exhausted { bytes: 10 }));
            let waiting = tokio::time::timeout(
                Duration::from_millis(30),
                FetchMemory::new(budget.clone()).reserve(10),
            )
            .await;
            assert!(waiting.is_err(), "request work waits");
            let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
            let late = with_memory_wait(
                MemoryWait::until(deadline),
                FetchMemory::new(budget.clone()).reserve(10),
            )
            .await;
            assert_eq!(late.unwrap_err(), BudgetError::Exhausted { bytes: 10 });
        });
    }
}
