//! Owned instances: what a host needs to stop a node and prove it stopped.
//!
//! Aborting a task doesn't stop work that is blocked in `block_in_place`,
//! and work a node spawns outlives its spawner. So a host that must tear a
//! node down (Freedom's `freedom-ipfsd`, design 0007) runs it as an owned
//! group, and this is the group's handle:
//!
//! - **a root cancellation token**, with a child per request. Every blocking
//!   region observes the token of the request it runs for, or the root's
//!   outside a request: the future it runs with `block_on` ends when the
//!   token is cancelled, so cancellation reaches a fetch that is blocked
//!   inside a synchronous block-provider call. Everything else a request
//!   awaits is cancelled by dropping the request's future;
//! - **a task set**: every task the node spawns for itself (the shared
//!   Bitswap swarm, background preconnects) and every request the host runs
//!   through it, each ending when the root token is cancelled;
//! - **a blocking-region count**, raised around every `block_in_place` and
//!   every blocking-pool job the node starts, and readable from outside the
//!   runtime, so a host can wait for it to reach zero;
//! - **the runtime handle** the group runs on. Tasks that libraries start
//!   for the node (hyper's connections, libp2p's) are spawned there too, so
//!   a host that owns that runtime can end them all by shutting it down.
//!
//! A node built without an `InstanceContext` behaves exactly as before.

use std::future::Future;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;
pub use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

tokio::task_local! {
    static CANCELLATION: CancellationToken;
}

/// Runs `future` as work that `token` cancels: blocking regions inside it
/// observe `token` (a request's own token, a child of its instance's root).
pub async fn with_cancellation<F: Future>(token: CancellationToken, future: F) -> F::Output {
    CANCELLATION.scope(token, future).await
}

/// The token the current request runs under, if any.
pub(crate) fn current_cancellation() -> Option<CancellationToken> {
    CANCELLATION.try_with(CancellationToken::clone).ok()
}

/// An owned group's handle: its runtime, its root cancellation token, its
/// task set and its blocking-region count. Clones share them.
#[derive(Clone)]
pub struct InstanceContext {
    inner: Arc<Inner>,
}

struct Inner {
    handle: Handle,
    root: CancellationToken,
    tasks: TaskTracker,
    blocking: Mutex<usize>,
    blocking_done: Condvar,
}

impl std::fmt::Debug for InstanceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceContext")
            .field("cancelled", &self.is_cancelled())
            .field("tasks", &self.tasks())
            .field("blocking_regions", &self.blocking_regions())
            .finish()
    }
}

impl InstanceContext {
    /// A group whose work runs on `handle`, a multi-thread runtime's.
    pub fn new(handle: Handle) -> Self {
        Self {
            inner: Arc::new(Inner {
                handle,
                root: CancellationToken::new(),
                tasks: TaskTracker::new(),
                blocking: Mutex::new(0),
                blocking_done: Condvar::new(),
            }),
        }
    }

    /// The runtime the group runs on.
    pub fn handle(&self) -> &Handle {
        &self.inner.handle
    }

    /// The root token: cancelling it cancels every request's token and every
    /// task of the group.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.inner.root
    }

    /// A new request's token: a child of the root.
    pub fn request_token(&self) -> CancellationToken {
        self.inner.root.child_token()
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.root.is_cancelled()
    }

    /// Spawns a task of the group on its runtime. It ends, dropped at its
    /// next await, when the root token is cancelled, and it counts in the
    /// task set until it has. After [`InstanceContext::cancel`], the future
    /// is dropped at once and never runs. The output is `None` whenever the
    /// token was cancelled before the task ended.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_with(self.inner.root.clone(), future)
    }

    /// Spawns a request of the group on its runtime, under `token` (from
    /// [`InstanceContext::request_token`]): it ends when `token` or the root
    /// is cancelled, and blocking regions inside it observe `token`.
    pub fn spawn_request<F>(
        &self,
        token: CancellationToken,
        future: F,
    ) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let scoped = with_cancellation(token.clone(), future);
        self.spawn_with(token, scoped)
    }

    fn spawn_with<F>(&self, token: CancellationToken, future: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let root = self.inner.root.clone();
        self.inner.tasks.spawn_on(
            async move {
                if root.is_cancelled() {
                    return None;
                }
                let output = tokio::select! {
                    biased;
                    _ = token.cancelled() => None,
                    _ = root.cancelled() => None,
                    output = future => Some(output),
                };
                // Cancel wins a race: a future that finished because its
                // blocking region was cancelled (with an error it made of
                // that) is reported as cancelled too.
                output.filter(|_| !token.is_cancelled() && !root.is_cancelled())
            },
            &self.inner.handle,
        )
    }

    /// Runs `job` on the runtime's blocking pool, counted as a blocking
    /// region of the group until it returns.
    pub fn spawn_blocking<F, R>(&self, job: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let region = self.blocking_region();
        self.inner.handle.spawn_blocking(move || {
            let _region = region;
            job()
        })
    }

    /// Counts a blocking region until the guard is dropped.
    pub fn blocking_region(&self) -> BlockingRegion {
        *lock(&self.inner.blocking) += 1;
        BlockingRegion {
            context: self.clone(),
        }
    }

    /// Runs `future` to completion from inside a task of the group, in a
    /// blocking region: `block_in_place`, then `block_on` on the runtime. It
    /// returns `None`, dropping `future`, as soon as the current request's
    /// token (or, outside a request, the root token) is cancelled.
    pub fn block_in_place<F: Future>(&self, handle: &Handle, future: F) -> Option<F::Output> {
        let token = current_cancellation().unwrap_or_else(|| self.inner.root.clone());
        let root = self.inner.root.clone();
        let _region = self.blocking_region();
        tokio::task::block_in_place(|| {
            handle.block_on(async move {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => None,
                    _ = root.cancelled() => None,
                    output = future => Some(output),
                }
            })
        })
    }

    /// Cancels the group: the root token, and so every request's, and closes
    /// the task set to new tasks.
    pub fn cancel(&self) {
        self.inner.root.cancel();
        self.inner.tasks.close();
    }

    /// Tasks of the group still running.
    pub fn tasks(&self) -> usize {
        self.inner.tasks.len()
    }

    /// Blocking regions of the group still running.
    pub fn blocking_regions(&self) -> usize {
        *lock(&self.inner.blocking)
    }

    /// Waits, from a thread that may block, until no blocking region of the
    /// group runs or `deadline` passes; returns how many still run.
    pub fn wait_blocking_regions(&self, deadline: Instant) -> usize {
        let mut count = lock(&self.inner.blocking);
        while *count > 0 {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            count = match self.inner.blocking_done.wait_timeout(count, left) {
                Ok((count, _)) => count,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        *count
    }

    /// Waits until every task of the group has ended. Only meaningful once
    /// [`InstanceContext::cancel`] has closed the task set.
    pub async fn wait_tasks(&self) {
        self.inner.tasks.wait().await;
    }
}

/// A blocking region of a group, counted until dropped.
#[must_use = "the region is counted only while the guard lives"]
pub struct BlockingRegion {
    context: InstanceContext,
}

impl Drop for BlockingRegion {
    fn drop(&mut self) {
        let mut count = lock(&self.context.inner.blocking);
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.context.inner.blocking_done.notify_all();
        }
    }
}

fn lock(count: &Mutex<usize>) -> MutexGuard<'_, usize> {
    count
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn cancelling_the_root_ends_tasks_and_requests() {
        let runtime = runtime();
        let context = InstanceContext::new(runtime.handle().clone());
        let background = context.spawn(std::future::pending::<()>());
        let request = context.spawn_request(context.request_token(), std::future::pending::<()>());
        let done = context.spawn(async { 7 });
        assert_eq!(runtime.block_on(done).unwrap(), Some(7));
        assert_eq!(context.tasks(), 2);
        context.cancel();
        assert_eq!(runtime.block_on(background).unwrap(), None);
        assert_eq!(runtime.block_on(request).unwrap(), None);
        runtime.block_on(context.wait_tasks());
        assert_eq!(context.tasks(), 0);
        // A closed set refuses nothing, but nothing runs: the future is
        // dropped at once.
        let late = context.spawn(async { panic!("must not run") });
        assert_eq!(runtime.block_on(late).unwrap(), None);
    }

    #[test]
    fn a_request_token_cancels_its_request_only() {
        let runtime = runtime();
        let context = InstanceContext::new(runtime.handle().clone());
        let token = context.request_token();
        let request = context.spawn_request(token.clone(), std::future::pending::<()>());
        let other = context.spawn_request(context.request_token(), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            "done"
        });
        token.cancel();
        assert_eq!(runtime.block_on(request).unwrap(), None);
        assert_eq!(runtime.block_on(other).unwrap(), Some("done"));
        assert!(!context.is_cancelled());
    }

    #[test]
    fn a_blocking_region_ends_when_its_request_is_cancelled() {
        let runtime = runtime();
        let context = InstanceContext::new(runtime.handle().clone());
        let token = context.request_token();
        let entered = Arc::new(AtomicBool::new(false));
        let task = {
            let context = context.clone();
            let entered = entered.clone();
            context.clone().spawn_request(token.clone(), async move {
                // As the synchronous block provider does: block in place,
                // then block on a future that never finishes by itself.
                let handle = Handle::current();
                entered.store(true, Ordering::SeqCst);
                context.block_in_place(&handle, std::future::pending::<()>())
            })
        };
        while !entered.load(Ordering::SeqCst) || context.blocking_regions() == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        token.cancel();
        let deadline = Instant::now() + Duration::from_secs(5);
        assert_eq!(context.wait_blocking_regions(deadline), 0);
        // The region returned `None`, and the request ended with it.
        assert_eq!(runtime.block_on(task).unwrap(), None);
    }

    #[test]
    fn blocking_jobs_count_until_they_return() {
        let runtime = runtime();
        let context = InstanceContext::new(runtime.handle().clone());
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let job = context.spawn_blocking(move || wait.recv().is_ok());
        assert_eq!(context.blocking_regions(), 1);
        let soon = Instant::now() + Duration::from_millis(20);
        assert_eq!(context.wait_blocking_regions(soon), 1, "still running");
        release.send(()).unwrap();
        assert!(runtime.block_on(job).unwrap());
        assert_eq!(
            context.wait_blocking_regions(Instant::now() + Duration::from_secs(5)),
            0
        );
    }

    #[test]
    fn outside_a_request_a_region_observes_the_root() {
        let runtime = runtime();
        let context = InstanceContext::new(runtime.handle().clone());
        let task = {
            let inner = context.clone();
            runtime.spawn(async move {
                let handle = Handle::current();
                inner.block_in_place(&handle, std::future::pending::<()>())
            })
        };
        while context.blocking_regions() == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        context.cancel();
        assert_eq!(runtime.block_on(task).unwrap(), None);
        assert_eq!(context.blocking_regions(), 0);
    }
}
