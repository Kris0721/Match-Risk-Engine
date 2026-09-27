use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use wal::recovery::{recover, RecoveryError};

/// Polls a WAL file (shared storage, or replicated to local disk by your
/// transport of choice — not implemented here) and tracks the last sequence
/// number observed, so promotion can resume numbering without a gap or
/// collision.
pub struct StandbyReplicator {
    wal_path: PathBuf,
    snapshot_dir: PathBuf,
    shard_id: u32,
    last_seq: u64,
    highest_term: u64,
    poll_interval: Duration,
}

impl StandbyReplicator {
    pub fn new(
        wal_path: impl Into<PathBuf>,
        snapshot_dir: impl Into<PathBuf>,
        shard_id: u32,
    ) -> Self {
        Self {
            wal_path: wal_path.into(),
            snapshot_dir: snapshot_dir.into(),
            shard_id,
            last_seq: 0,
            highest_term: 0,
            poll_interval: Duration::from_millis(20),
        }
    }

    pub fn poll_once(&mut self) -> Result<u64, RecoveryError> {
        let out = recover(&self.wal_path, &self.snapshot_dir, self.shard_id)?;

        if out.highest_term < self.highest_term {
            eprintln!(
                "[standby] WAL shows a stale term ({} < {}) — ignoring this pass",
                out.highest_term, self.highest_term
            );
            return Ok(self.last_seq);
        }
        self.highest_term = out.highest_term;

        if out.last_recovered_seq > self.last_seq {
            self.last_seq = out.last_recovered_seq;
            // for sc in &out.commands { shadow_engine.apply(sc); }  // (still a TODO, unchanged)
        }
        Ok(self.last_seq)
    }

    pub fn run_until_promoted(mut self, role: &crate::failover::RoleHandle) -> u64 {
        loop {
            if role.is_leader() {
                // One last catch-up pass to close any gap between the final
                // poll and the lease flip.
                let _ = self.poll_once();
                return self.last_seq;
            }
            if let Err(e) = self.poll_once() {
                eprintln!("[standby] WAL poll failed: {e}");
            }
            std::thread::sleep(self.poll_interval);
        }
    }
}
