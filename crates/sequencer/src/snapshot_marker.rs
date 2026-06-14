// Sequencing logic for marking snapshot points
//! Snapshot marker injection schedule.
//!
//! The Sequencer periodically injects a `SnapshotMarker` system event into
//! every per-symbol and per-shard SPMC fan-out queue. When a downstream
//! thread (matching engine, risk shard, WAL writer) processes the marker it
//! serialises its own state to disk at that logical sequence point.
//!
//! Because every thread snapshots at the *same* sequence number
//! independently, recovery is trivially parallel — no cross-thread
//! coordination during the snapshot itself.

/// Controls how often the Sequencer injects `SnapshotMarker` events.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotMarkerSchedule {
    /// Inject a marker every `interval_seq` sequenced commands.
    /// Must be > 0. Typical value: 100_000 (roughly every 100ms at 1M ops/s).
    pub interval_seq: u64,
}

impl SnapshotMarkerSchedule {
    pub fn new(interval_seq: u64) -> Self {
        assert!(interval_seq > 0, "snapshot interval must be > 0");
        Self { interval_seq }
    }

    /// Returns `true` if a marker should be injected after sequencing
    /// the command with global sequence number `seq`.
    #[inline]
    pub fn should_fire(&self, seq: u64) -> bool {
        seq % self.interval_seq == 0
    }
}

impl Default for SnapshotMarkerSchedule {
    fn default() -> Self {
        Self::new(100_000)
    }
}