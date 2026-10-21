//! The network capability (design 0007, "Instances and their teardown").

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use tokio::sync::watch;

/// An instance's handle on the gate. Every socket, HTTP attempt and
/// redirect hop asks it first, and once it is revoked every later ask
/// fails: a teardown revokes it (step 2) before it cancels anything
/// (step 3), so nothing can connect while the rest of the instance ends.
///
/// **Revocation is linearizable with the network work it admitted.** Work
/// that may open a connection (a resolution and the connect after it) runs
/// under a [`Permit`] ([`NetworkCapability::admit`]), and through
/// [`Permit::run`], which drops the work as soon as the capability is
/// revoked. [`NetworkCapability::quiesced`] resolves once the capability is
/// revoked and no permit is left: from then on, no attempt it admitted can
/// still open a connection. A connection completed before that counts as
/// earlier; connections already open are the teardown's to close (its
/// cancel drops them), not the revocation's.
///
/// Every place a new connection starts takes a permit: the HTTP clients'
/// connector layer here, and the gate's resolver and the libp2p transports
/// as design 0007's later parts bring them.
///
/// Clones share it. Revoking is one-way.
#[derive(Clone, Debug)]
pub struct NetworkCapability {
    state: Arc<watch::Sender<State>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct State {
    revoked: bool,
    /// Permits outstanding.
    permits: usize,
}

impl Default for NetworkCapability {
    fn default() -> Self {
        Self {
            state: Arc::new(watch::Sender::new(State::default())),
        }
    }
}

impl NetworkCapability {
    /// A capability nobody has revoked.
    pub fn new() -> Self {
        Self::default()
    }

    /// Revokes it, for this handle and every clone: every later
    /// [`NetworkCapability::check`] and [`NetworkCapability::admit`] fails,
    /// and every permit's [`Permit::run`] drops its work and fails.
    pub fn revoke(&self) {
        self.state
            .send_if_modified(|state| !std::mem::replace(&mut state.revoked, true));
    }

    pub fn is_revoked(&self) -> bool {
        self.state.borrow().revoked
    }

    /// Asks before an HTTP attempt or a hop: `Err` once revoked. Work that
    /// may open a connection takes a [`Permit`] instead.
    pub fn check(&self) -> Result<(), Revoked> {
        if self.is_revoked() {
            Err(Revoked)
        } else {
            Ok(())
        }
    }

    /// A permit for work that may open a connection: `Err` once revoked.
    /// Checking and counting are one step, so a permit is either refused or
    /// counted before [`NetworkCapability::quiesced`] can see none left.
    pub fn admit(&self) -> Result<Permit, Revoked> {
        let admitted = self.state.send_if_modified(|state| {
            if state.revoked {
                false
            } else {
                state.permits += 1;
                true
            }
        });
        if admitted {
            Ok(Permit {
                capability: self.clone(),
            })
        } else {
            Err(Revoked)
        }
    }

    /// Resolves once the capability is revoked and no permit is left: no
    /// attempt it admitted can open a connection any more. Revoking with no
    /// work in flight makes it resolve at once.
    pub async fn quiesced(&self) {
        let mut state = self.state.subscribe();
        // The sender lives in `self`: the channel can't close while this
        // waits.
        let _ = state
            .wait_for(|state| state.revoked && state.permits == 0)
            .await;
    }

    /// The permits outstanding: tests.
    #[doc(hidden)]
    pub fn permits(&self) -> usize {
        self.state.borrow().permits
    }

    async fn revoked(&self) {
        let mut state = self.state.subscribe();
        let _ = state.wait_for(|state| state.revoked).await;
    }
}

/// Network work the capability admitted: counted until it is dropped, so
/// [`NetworkCapability::quiesced`] waits for it.
#[derive(Debug)]
#[must_use = "a permit admits work only while it is held"]
pub struct Permit {
    capability: NetworkCapability,
}

impl Permit {
    /// Runs `work` under the permit, unless the capability is revoked first:
    /// then `work` is dropped (aborting a resolution or a connect in
    /// progress) and this fails with [`Revoked`]. Revocation is seen before
    /// `work` is polled again, so nothing `work` does after the revoke
    /// happens. The permit is let go of once `work` has settled or been
    /// dropped.
    pub async fn run<F: Future>(self, work: F) -> Result<F::Output, Revoked> {
        let outcome = {
            let mut revoked = pin!(self.capability.revoked());
            let mut work = pin!(work);
            std::future::poll_fn(|cx| {
                if revoked.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(Revoked));
                }
                work.as_mut().poll(cx).map(Ok)
            })
            .await
            // `work` is dropped here, before the permit.
        };
        drop(self);
        outcome
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.capability
            .state
            .send_modify(|state| state.permits -= 1);
    }
}

/// The instance's network capability was revoked: its teardown has begun.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the instance's network capability is revoked")]
pub struct Revoked;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[test]
    fn revoking_reaches_every_clone_and_stays() {
        let capability = NetworkCapability::new();
        let clone = capability.clone();
        assert_eq!(clone.check(), Ok(()));
        capability.revoke();
        assert!(clone.is_revoked());
        assert_eq!(clone.check(), Err(Revoked));
        capability.revoke();
        assert_eq!(capability.check(), Err(Revoked));
        assert!(clone.admit().is_err());
        // Another capability is its own.
        assert_eq!(NetworkCapability::new().check(), Ok(()));
    }

    #[tokio::test]
    async fn with_no_work_in_flight_revocation_is_quiescence_at_once() {
        let capability = NetworkCapability::new();
        drop(capability.admit().unwrap());
        assert_eq!(capability.permits(), 0);
        capability.revoke();
        tokio::time::timeout(Duration::from_millis(100), capability.quiesced())
            .await
            .expect("quiescence without work in flight");
    }

    /// A drop guard: set when the work it lives in is dropped.
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn quiescence_waits_until_the_work_in_flight_is_dropped() {
        let capability = NetworkCapability::new();
        let permit = capability.admit().unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let (_release, held) = tokio::sync::oneshot::channel::<()>();
        let guard = Dropped(dropped.clone());
        let work = tokio::spawn(permit.run(async move {
            let _guard = guard;
            let _ = held.await;
        }));
        tokio::task::yield_now().await;
        assert_eq!(capability.permits(), 1);

        // Not revoked: no quiescence, however long the work runs.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), capability.quiesced())
                .await
                .is_err()
        );
        capability.revoke();
        tokio::time::timeout(Duration::from_secs(2), capability.quiesced())
            .await
            .expect("quiescence once the work is dropped");
        assert!(dropped.load(Ordering::SeqCst), "the work was dropped first");
        assert_eq!(work.await.unwrap(), Err(Revoked));
        assert!(capability.admit().is_err());
    }

    #[tokio::test]
    async fn a_permit_held_outside_run_holds_quiescence_back() {
        let capability = NetworkCapability::new();
        let permit = capability.admit().unwrap();
        capability.revoke();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), capability.quiesced())
                .await
                .is_err()
        );
        drop(permit);
        tokio::time::timeout(Duration::from_secs(2), capability.quiesced())
            .await
            .expect("quiescence once the work is dropped");
    }

    #[tokio::test]
    async fn work_that_settles_first_keeps_its_outcome() {
        let capability = NetworkCapability::new();
        let outcome = capability.admit().unwrap().run(async { 7 }).await;
        assert_eq!(outcome, Ok(7));
        assert_eq!(capability.permits(), 0);
    }
}
