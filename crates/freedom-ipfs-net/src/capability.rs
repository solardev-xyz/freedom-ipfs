//! The network capability (design 0007, "Instances and their teardown").

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// An instance's handle on the gate. Every socket, HTTP attempt and
/// redirect hop asks it first, and once it is revoked every later ask
/// fails: a teardown revokes it (step 2) before it cancels anything
/// (step 3), so nothing can connect while the rest of the instance ends.
///
/// Clones share it. Revoking is one-way.
#[derive(Clone, Debug, Default)]
pub struct NetworkCapability {
    revoked: Arc<AtomicBool>,
}

impl NetworkCapability {
    /// A capability nobody has revoked.
    pub fn new() -> Self {
        Self::default()
    }

    /// Revokes it, for this handle and every clone: every later
    /// [`NetworkCapability::check`] fails.
    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }

    pub fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::SeqCst)
    }

    /// Asks before a socket, an HTTP attempt or a hop: `Err` once revoked.
    pub fn check(&self) -> Result<(), Revoked> {
        if self.is_revoked() {
            Err(Revoked)
        } else {
            Ok(())
        }
    }
}

/// The instance's network capability was revoked: its teardown has begun.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the instance's network capability is revoked")]
pub struct Revoked;

#[cfg(test)]
mod tests {
    use super::*;

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
        // Another capability is its own.
        assert_eq!(NetworkCapability::new().check(), Ok(()));
    }
}
