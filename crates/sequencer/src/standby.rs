//! Hot standby: keeps a warm replica of Sequencer state by tailing the WAL,
//! ready to promote into an active `Sequencer` with correct `seq` continuity.

use std::path::PathBuf;
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
            poll_interval: Duration::from_millis(20),
        }
    }

    /// Run one catch-up pass against the current WAL contents. Returns the
    /// highest `seq` observed so far. Call this in a loop (e.g. from a
    /// dedicated standby thread) while `RoleHandle::is_leader()` is false.
    ///
    /// This re-scans the whole WAL file each call, which is fine at typical
    /// WAL sizes (hundreds of MB) polled every ~20ms, but is intentionally
    /// simple rather than an incremental tail — swap in an incremental
    /// `mmap` offset-tracking scan (mirroring `wal::pmem::scan_to_end`) if
    /// profiling shows this matters at your WAL size.
    pub fn poll_once(&mut self) -> Result<u64, RecoveryError> {
        let out = recover(&self.wal_path, &self.snapshot_dir, self.shard_id)?;
        if out.last_recovered_seq > self.last_seq {
            self.last_seq = out.last_recovered_seq;
            // NOTE: `out.commands` is exactly the set of newly-durable
            // commands since the last snapshot point — feed these into your
            // shadow order-book / risk-engine state here so the standby's
            // in-memory state stays warm, not just its `seq` counter:
            //
            //   for sc in &out.commands { shadow_engine.apply(sc); }
        }
        Ok(self.last_seq)
    }

    /// Block, polling at `poll_interval`, until `role.is_leader()` becomes
    /// true, then return the `seq` to resume numbering from. Run this on its
    /// own thread; when it returns, use the result as `initial_seq` in
    /// `Sequencer::new(...)`.
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
