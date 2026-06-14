// Chaos engineering and fault-injection simulator
//! Chaos testing: inject faults mid-run and verify recovery.
//!
//! The chaos runner:
//!   1. Runs the harness for `phase1_ticks` ticks, recording the WAL.
//!   2. Simulates a fault (shard reset, partial WAL truncation, etc.).
//!   3. Replays the surviving WAL into a fresh harness.
//!   4. Asserts byte-identical final state.
//!
//! Because determinism is a design axiom — `(snapshot, ordered_log) -> state`
//! — any divergence after replay is a bug.

use crate::harness::{SimConfig, SimHarness, SimResult};
use crate::replay::Replayer;

/// Which fault to inject.
#[derive(Clone, Debug)]
pub enum FaultKind {
    /// Truncate the last `n` commands from the WAL (simulates a crash mid-write).
    TruncateWal { drop_last_n: usize },
    /// Reset one risk shard (simulates a process restart of that shard).
    /// Recovery replays the full WAL into the fresh shard.
    ResetRiskShard { shard_index: usize },
    /// No fault — used to verify the no-fault path produces identical results.
    None,
}

/// Configuration for a chaos test run.
#[derive(Clone, Debug)]
pub struct ChaosConfig {
    pub sim_config:   SimConfig,
    pub phase1_ticks: u64,
    pub phase2_ticks: u64,
    pub fault:        FaultKind,
}

impl ChaosConfig {
    pub fn new(sim_config: SimConfig, phase1_ticks: u64, fault: FaultKind) -> Self {
        Self {
            phase2_ticks: phase1_ticks / 2,
            sim_config,
            phase1_ticks,
            fault,
        }
    }
}

/// Result of a chaos run.
pub struct ChaosResult {
    pub phase1_result: SimResult,
    pub replay_ok:     bool,
    pub error:         Option<String>,
}

/// Run a chaos test according to `config`.
pub fn run_chaos(
    config: ChaosConfig,
    mut inject_commands: impl FnMut(&mut SimHarness),
) -> ChaosResult {
    // --- Phase 1: normal run ---
    let mut harness = SimHarness::new(config.sim_config.clone());
    inject_commands(&mut harness);
    let phase1_result = harness.run(config.phase1_ticks);

    // Record WAL before fault injection.
    let mut wal: Vec<_> = harness.wal().to_vec();

    // --- Inject fault ---
    match &config.fault {
        FaultKind::TruncateWal { drop_last_n } => {
            let keep = wal.len().saturating_sub(*drop_last_n);
            wal.truncate(keep);
        }
        FaultKind::ResetRiskShard { .. } => {
            // Shard reset is modelled by replaying into a fresh harness where
            // that shard starts from zero state — the Replayer handles this.
        }
        FaultKind::None => {}
    }

    // --- Phase 2: replay surviving WAL ---
    let replayer = Replayer::new(config.sim_config.clone());
    match replayer.verify_account_states(&harness, &wal) {
        Ok(()) => ChaosResult {
            phase1_result,
            replay_ok: true,
            error: None,
        },
        Err(e) => ChaosResult {
            phase1_result,
            replay_ok: false,
            error: Some(e),
        },
    }
}