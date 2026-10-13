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
//! - **the runtime handle** the group runs on. Tasks and blocking jobs that
//!   libraries start for the node (hyper's connections, libp2p's, a
//!   resolver's lookups) run there too, and only dropping and joining that
//!   runtime proves they have all ended;
//! - **a fence:** cancelling the group admits no new task or blocking
//!   region from then on, under the same lock that admits them, and a
//!   blocking job still queued when it is cancelled never runs.
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
    /// Admission and the fence, under one lock: work is admitted (a task
    /// registered, a blocking region counted) only while unfenced, and
    /// [`InstanceContext::cancel`] fences under the same lock, so nothing is
    /// admitted after it returns.
    admission: Mutex<Admission>,
    blocking_done: Condvar,
}

struct Admission {
    fenced: bool,
    blocking: usize,
}

/// The group was cancelled: it admits no new work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fenced;

impl std::fmt::Display for Fenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the instance is cancelled")
    }
}

impl std::error::Error for Fenced {}

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
                admission: Mutex::new(Admission {
                    fenced: false,
                    blocking: 0,
                }),
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
    /// task set until it has. The output is `None` whenever the token was
    /// cancelled before the task ended. After [`InstanceContext::cancel`],
    /// nothing is spawned: [`Fenced`], and the future is dropped unpolled.
    pub fn spawn<F>(&self, future: F) -> Result<JoinHandle<Option<F::Output>>, Fenced>
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
    ) -> Result<JoinHandle<Option<F::Output>>, Fenced>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let scoped = with_cancellation(token.clone(), future);
        self.spawn_with(token, scoped)
    }

    fn spawn_with<F>(
        &self,
        token: CancellationToken,
        future: F,
    ) -> Result<JoinHandle<Option<F::Output>>, Fenced>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let root = self.inner.root.clone();
        // Registered under the admission lock, so a task is either in the
        // set before the fence or never spawned.
        let admission = self.admission();
        if admission.fenced {
            return Err(Fenced);
        }
        let handle = self.inner.tasks.spawn_on(
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
        );
        drop(admission);
        Ok(handle)
    }

    /// Runs `job` on the runtime's blocking pool, counted as a blocking
    /// region of the group from now until it returns. A job still queued
    /// when the group is cancelled never runs (`None`); after the fence,
    /// nothing is queued ([`Fenced`]).
    pub fn spawn_blocking<F, R>(&self, job: F) -> Result<JoinHandle<Option<R>>, Fenced>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let region = self.blocking_region()?;
        let root = self.inner.root.clone();
        Ok(self.inner.handle.spawn_blocking(move || {
            let _region = region;
            (!root.is_cancelled()).then(job)
        }))
    }

    /// Counts a blocking region until the guard is dropped; after the fence,
    /// none is admitted ([`Fenced`]).
    pub fn blocking_region(&self) -> Result<BlockingRegion, Fenced> {
        let mut admission = self.admission();
        if admission.fenced {
            return Err(Fenced);
        }
        admission.blocking += 1;
        Ok(BlockingRegion {
            context: self.clone(),
        })
    }

    fn admission(&self) -> MutexGuard<'_, Admission> {
        self.inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether the current thread belongs to the group's runtime: one of its
    /// workers, or a thread of its blocking pool.
    pub fn runs_here(&self) -> bool {
        Handle::try_current().is_ok_and(|current| current.id() == self.inner.handle.id())
    }

    /// Runs `future` to completion from synchronous code of the group, in a
    /// counted blocking region, and returns `None`, dropping `future`, as
    /// soon as the current request's token (or, outside a request, the root)
    /// is cancelled, or at once after the fence.
    ///
    /// - **On the group's runtime** (a task of the group, or its blocking
    ///   pool): `block_in_place`, then `block_on` on it.
    /// - **On a thread without a runtime:** `block_on` on the group's
    ///   runtime from that thread, so everything `future` spawns, the
    ///   libraries' connection tasks included, runs on the group's runtime.
    /// - **On another runtime:** refused (`None`): work there would escape
    ///   the group.
    pub fn block_on<F: Future>(&self, future: F) -> Option<F::Output> {
        let region = self.blocking_region().ok()?;
        let token = current_cancellation().unwrap_or_else(|| self.inner.root.clone());
        let root = self.inner.root.clone();
        let guarded = async move {
            tokio::select! {
                biased;
                _ = token.cancelled() => None,
                _ = root.cancelled() => None,
                output = future => Some(output),
            }
        };
        let output = match Handle::try_current() {
            Ok(current) if current.id() == self.inner.handle.id() => {
                tokio::task::block_in_place(|| current.block_on(guarded))
            }
            Ok(_) => None,
            Err(_) => self.inner.handle.block_on(guarded),
        };
        drop(region);
        output
    }

    /// Cancels the group and fences it: the root token, and so every
    /// request's, is cancelled; the task set is closed; and from the moment
    /// this returns no task or blocking region is admitted.
    pub fn cancel(&self) {
        let mut admission = self.admission();
        admission.fenced = true;
        self.inner.root.cancel();
        self.inner.tasks.close();
        drop(admission);
    }

    /// Tasks of the group still running.
    pub fn tasks(&self) -> usize {
        self.inner.tasks.len()
    }

    /// Blocking regions of the group still running, or queued.
    pub fn blocking_regions(&self) -> usize {
        self.admission().blocking
    }

    /// Waits, from a thread that may block, until no blocking region of the
    /// group runs or `deadline` passes; returns how many still run.
    pub fn wait_blocking_regions(&self, deadline: Instant) -> usize {
        let mut admission = self.admission();
        while admission.blocking > 0 {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            admission = match self.inner.blocking_done.wait_timeout(admission, left) {
                Ok((admission, _)) => admission,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        admission.blocking
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
        let mut admission = self.context.admission();
        admission.blocking = admission.blocking.saturating_sub(1);
        if admission.blocking == 0 {
            self.context.inner.blocking_done.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn runtime(workers: usize, blocking: usize) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .max_blocking_threads(blocking)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn cancelling_the_root_ends_tasks_and_requests() {
        let runtime = runtime(2, 8);
        let context = InstanceContext::new(runtime.handle().clone());
        let background = context.spawn(std::future::pending::<()>()).unwrap();
        let request = context
            .spawn_request(context.request_token(), std::future::pending::<()>())
            .unwrap();
        let done = context.spawn(async { 7 }).unwrap();
        assert_eq!(runtime.block_on(done).unwrap(), Some(7));
        assert_eq!(context.tasks(), 2);
        context.cancel();
        assert_eq!(runtime.block_on(background).unwrap(), None);
        assert_eq!(runtime.block_on(request).unwrap(), None);
        runtime.block_on(context.wait_tasks());
        assert_eq!(context.tasks(), 0);
    }

    #[test]
    fn the_fence_admits_nothing_after_cancel() {
        let runtime = runtime(1, 4);
        let context = InstanceContext::new(runtime.handle().clone());
        context.cancel();
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        assert_eq!(
            context
                .spawn(async move { flag.store(true, Ordering::SeqCst) })
                .err(),
            Some(Fenced)
        );
        let flag = ran.clone();
        assert!(context
            .spawn_blocking(move || flag.store(true, Ordering::SeqCst))
            .is_err());
        assert!(context.blocking_region().is_err());
        assert_eq!(context.block_on(async { 1 }), None);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!ran.load(Ordering::SeqCst), "nothing ran after the fence");
        assert_eq!((context.tasks(), context.blocking_regions()), (0, 0));
    }

    #[test]
    fn admission_racing_cancellation_is_all_or_nothing() {
        // Many threads admit tasks and blocking jobs while another fences.
        // Once `cancel` returns, the counts only fall: nothing registers late.
        for _ in 0..20 {
            let runtime = runtime(2, 64);
            let context = InstanceContext::new(runtime.handle().clone());
            let admitted_after = Arc::new(AtomicUsize::new(0));
            let fenced = Arc::new(AtomicBool::new(false));
            let workers: Vec<_> = (0..4)
                .map(|n| {
                    let (context, admitted_after, fenced) =
                        (context.clone(), admitted_after.clone(), fenced.clone());
                    std::thread::spawn(move || {
                        for _ in 0..200 {
                            let was_fenced = fenced.load(Ordering::SeqCst);
                            let admitted = if n % 2 == 0 {
                                context.spawn(std::future::pending::<()>()).is_ok()
                            } else {
                                context
                                    .spawn_blocking(|| std::thread::sleep(Duration::from_millis(1)))
                                    .is_ok()
                            };
                            if admitted && was_fenced {
                                admitted_after.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
                })
                .collect();
            std::thread::sleep(Duration::from_micros(200));
            context.cancel();
            fenced.store(true, Ordering::SeqCst);
            for worker in workers {
                worker.join().unwrap();
            }
            assert_eq!(admitted_after.load(Ordering::SeqCst), 0);
            runtime.block_on(context.wait_tasks());
            let deadline = Instant::now() + Duration::from_secs(10);
            assert_eq!(context.wait_blocking_regions(deadline), 0);
            assert_eq!(context.tasks(), 0);
        }
    }

    #[test]
    fn a_queued_blocking_job_never_runs_after_cancel() {
        // One blocking thread: the second job queues behind the first.
        let runtime = runtime(1, 1);
        let context = InstanceContext::new(runtime.handle().clone());
        let (release, held) = std::sync::mpsc::channel::<()>();
        let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
        let first = context
            .spawn_blocking(move || {
                let _ = entered_tx.send(());
                held.recv().is_ok()
            })
            .unwrap();
        // Counted from admission; wait until it has really started.
        entered.recv().unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        let queued = context
            .spawn_blocking(move || flag.store(true, Ordering::SeqCst))
            .unwrap();
        assert_eq!(context.blocking_regions(), 2);
        context.cancel();
        release.send(()).unwrap();
        assert_eq!(
            runtime.block_on(first).unwrap(),
            Some(true),
            "started before the fence"
        );
        assert_eq!(runtime.block_on(queued).unwrap(), None, "skipped");
        assert!(!ran.load(Ordering::SeqCst));
        assert_eq!(context.blocking_regions(), 0);
    }

    #[test]
    fn a_request_token_cancels_its_request_only() {
        let runtime = runtime(2, 8);
        let context = InstanceContext::new(runtime.handle().clone());
        let token = context.request_token();
        let request = context
            .spawn_request(token.clone(), std::future::pending::<()>())
            .unwrap();
        let other = context
            .spawn_request(context.request_token(), async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                "done"
            })
            .unwrap();
        token.cancel();
        assert_eq!(runtime.block_on(request).unwrap(), None);
        assert_eq!(runtime.block_on(other).unwrap(), Some("done"));
        assert!(!context.is_cancelled());
    }

    #[test]
    fn a_blocking_region_ends_when_its_request_is_cancelled() {
        let runtime = runtime(2, 8);
        let context = InstanceContext::new(runtime.handle().clone());
        let token = context.request_token();
        let entered = Arc::new(AtomicBool::new(false));
        let task = {
            let (inner, entered) = (context.clone(), entered.clone());
            context
                .spawn_request(token.clone(), async move {
                    // As the synchronous block provider does.
                    entered.store(true, Ordering::SeqCst);
                    inner.block_on(std::future::pending::<()>())
                })
                .unwrap()
        };
        while !entered.load(Ordering::SeqCst) || context.blocking_regions() == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        token.cancel();
        let deadline = Instant::now() + Duration::from_secs(5);
        assert_eq!(context.wait_blocking_regions(deadline), 0);
        assert_eq!(runtime.block_on(task).unwrap(), None);
    }

    #[test]
    fn off_the_runtime_work_runs_on_it_and_foreign_runtimes_are_refused() {
        let runtime = runtime(1, 4);
        let context = InstanceContext::new(runtime.handle().clone());
        // A plain thread: the future runs with the group's runtime current.
        let id = runtime.handle().id();
        assert_eq!(
            context.block_on(async { tokio::runtime::Handle::current().id() }),
            Some(id)
        );
        // A plain thread blocked in the group is counted, and cancelled.
        let blocked = {
            let context = context.clone();
            std::thread::spawn(move || context.block_on(std::future::pending::<()>()))
        };
        while context.blocking_regions() == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        // Another runtime's task is refused at once.
        let foreign = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let inner = context.clone();
        let refused = foreign
            .block_on(foreign.spawn(async move { inner.block_on(async { 1 }) }))
            .unwrap();
        assert_eq!(refused, None);
        context.cancel();
        assert_eq!(blocked.join().unwrap(), None);
        assert_eq!(context.blocking_regions(), 0);
    }

    #[test]
    fn outside_a_request_a_region_observes_the_root() {
        let runtime = runtime(2, 8);
        let context = InstanceContext::new(runtime.handle().clone());
        let task = {
            let inner = context.clone();
            runtime.spawn(async move { inner.block_on(std::future::pending::<()>()) })
        };
        while context.blocking_regions() == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        context.cancel();
        assert_eq!(runtime.block_on(task).unwrap(), None);
        assert_eq!(context.blocking_regions(), 0);
    }
}
