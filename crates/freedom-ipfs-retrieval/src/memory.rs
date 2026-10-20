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
use std::sync::{Arc, OnceLock};

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

/// A fetch's memory: the budget its receive buffers reserve from, and the
/// first refusal one met, so the fetch fails as the budget's when it does.
#[derive(Clone)]
pub(crate) struct FetchMemory {
    budget: MemoryBudget,
    refusal: Arc<OnceLock<BudgetError>>,
}

tokio::task_local! {
    static FETCH_MEMORY: FetchMemory;
}

impl FetchMemory {
    pub(crate) fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            refusal: Arc::new(OnceLock::new()),
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
    /// does; a refusal is recorded.
    pub(crate) async fn reserve(&self, bytes: usize) -> Result<Reservation, BudgetError> {
        let reserved = current_memory_wait().reserve(&self.budget, bytes).await;
        if let Err(refusal) = &reserved {
            let _ = self.refusal.set(*refusal);
        }
        reserved
    }

    /// The first refusal the fetch met, if any.
    pub(crate) fn refusal(&self) -> Option<BudgetError> {
        self.refusal.get().copied()
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
