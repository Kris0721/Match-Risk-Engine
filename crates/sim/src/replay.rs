// Deterministic event replay engine
//! WAL replay: reconstruct engine state from a recorded command log.
//!
//! Because the whole system is `(snapshot, ordered_log) -> state`, recovery
//! is simply replaying the WAL through a fresh `SimHarness`. The replayed
//! harness must reach byte-identical state to the original.
//!
//! This module is also used by the chaos tests: they kill a shard mid-run,
//! then use `Replayer` to verify that recovery reaches the same final state.

use crate::harness::{SimConfig, SimHarness, SimResult};
use core_types::SequencedCommand;

/// Replays a recorded WAL into a fresh `SimHarness`.
pub struct Replayer {
    config: SimConfig,
}

impl Replayer {
    pub fn new(config: SimConfig) -> Self {
        Self { config }
    }

    /// Replay `log` (a slice of `SequencedCommand`s) into a fresh harness.
    ///
    /// Returns the result and the harness (for state inspection).
    pub fn replay(&self, log: &[SequencedCommand]) -> (SimResult, SimHarness) {
        let mut harness = SimHarness::new(self.config.clone());

        // Inject every command from the log in order.
        // We bypass the sequencer's seq-assignment and inject pre-sequenced
        // commands directly so the replay is deterministic even if the clock
        // differs from the original run.
        for sc in log {
            harness.push_command(sc.cmd.clone());
        }

        let n_ticks = log.len() as u64 * 2 + 10; // extra ticks to drain queues
        let result = harness.run(n_ticks);
        (result, harness)
    }

    /// Verify that replaying `log` produces the same account states as the
    /// original harness. Returns `Ok(())` on match, `Err(msg)` on divergence.
    pub fn verify_account_states(
        &self,
        original: &SimHarness,
        log: &[SequencedCommand],
    ) -> Result<(), String> {
        let (_, replayed) = self.replay(log);

        for i in 0..self.config.n_accounts {
            let orig = original.account_states[i].read();
            let rep  = replayed.account_states[i].read();

            if orig.balance != rep.balance {
                return Err(format!(
                    "account {} balance mismatch: orig={} replay={}",
                    i, orig.balance, rep.balance
                ));
            }
            if orig.used_margin != rep.used_margin {
                return Err(format!(
                    "account {} used_margin mismatch: orig={} replay={}",
                    i, orig.used_margin, rep.used_margin
                ));
            }
            if orig.frozen != rep.frozen {
                return Err(format!(
                    "account {} frozen mismatch: orig={} replay={}",
                    i, orig.frozen, rep.frozen
                ));
            }
        }
        Ok(())
    }

    /// Verify trade counts match between original result and replay result.
    pub fn verify_trade_counts(
        &self,
        original_result: &SimResult,
        log: &[SequencedCommand],
    ) -> Result<(), String> {
        let (replayed_result, _) = self.replay(log);
        if original_result.trades_matched != replayed_result.trades_matched {
            return Err(format!(
                "trade count mismatch: orig={} replay={}",
                original_result.trades_matched,
                replayed_result.trades_matched,
            ));
        }
        Ok(())
    }
}