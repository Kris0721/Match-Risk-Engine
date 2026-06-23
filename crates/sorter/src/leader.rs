//! CAS-based Sorter leader election.
//!
//! Two redundant Sorter instances run (active + standby). Only the one
//! that wins the CAS on `SORTER_LEADER` becomes active. The standby
//! periodically retries in case the active Sorter crashes.
//!
//! This eliminates the need for external coordination (ZooKeeper, etcd).
//! Architecture Doc §5, Operation #3.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Shared leader token for Sorter election.
///
/// Exactly one Sorter can hold leadership at a time. The leader runs
/// the scanning loop; the standby sleeps and retries periodically.
pub struct SorterLeader {
    /// Shared atomic flag. `true` = someone is leader, `false` = no leader.
    token: Arc<AtomicBool>,
    /// Whether THIS instance currently holds leadership.
    is_leader: bool,
}

impl SorterLeader {
    /// Create a new leader election token (shared between instances).
    pub fn new(token: Arc<AtomicBool>) -> Self {
        Self {
            token,
            is_leader: false,
        }
    }

    /// Create a pair of leader election instances sharing the same token.
    ///
    /// Convenience method for setting up active + standby Sorters.
    pub fn pair() -> (Self, Self) {
        let token = Arc::new(AtomicBool::new(false));
        (
            Self::new(Arc::clone(&token)),
            Self::new(Arc::clone(&token)),
        )
    }

    /// Attempt to become the leader via CAS.
    ///
    /// Returns `true` if this instance is now the leader.
    /// Returns `false` if another instance already holds leadership.
    ///
    /// Idempotent: calling repeatedly when already leader returns `true`.
    pub fn try_become_leader(&mut self) -> bool {
        if self.is_leader {
            return true;
        }
        let won = self.token.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ).is_ok();
        if won {
            self.is_leader = true;
        }
        won
    }

    /// Returns whether this instance is currently the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Voluntarily relinquish leadership (e.g., on graceful shutdown).
    pub fn resign(&mut self) {
        if self.is_leader {
            self.token.store(false, Ordering::SeqCst);
            self.is_leader = false;
        }
    }
}

impl Drop for SorterLeader {
    fn drop(&mut self) {
        // Auto-resign on drop so the standby can take over if this
        // instance is destroyed (crash recovery).
        self.resign();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_leader_wins() {
        let (mut a, mut b) = SorterLeader::pair();

        let a_wins = a.try_become_leader();
        let b_wins = b.try_become_leader();

        // Exactly one must win
        assert!(a_wins ^ b_wins);
    }

    #[test]
    fn leader_is_idempotent() {
        let (mut a, _b) = SorterLeader::pair();

        assert!(a.try_become_leader());
        assert!(a.try_become_leader()); // still leader
        assert!(a.is_leader());
    }

    #[test]
    fn resign_allows_other_to_lead() {
        let (mut a, mut b) = SorterLeader::pair();

        assert!(a.try_become_leader());
        assert!(!b.try_become_leader());

        a.resign();
        assert!(!a.is_leader());
        assert!(b.try_become_leader());
        assert!(b.is_leader());
    }

    #[test]
    fn drop_releases_leadership() {
        let token = Arc::new(AtomicBool::new(false));
        {
            let mut a = SorterLeader::new(Arc::clone(&token));
            assert!(a.try_become_leader());
            assert!(token.load(Ordering::SeqCst));
            // a dropped here
        }
        // Token should be released
        assert!(!token.load(Ordering::SeqCst));
    }
}
