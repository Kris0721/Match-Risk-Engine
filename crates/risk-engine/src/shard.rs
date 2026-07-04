// Sharding accounts across different risk evaluators
//! Risk shard: owns a contiguous range of `AccountId`s.
//!
//! This is the **Tier-1** risk path. It runs on its own pinned thread, consuming
//! `EngineEvent`s from the SPMC fan-out ring buffer emitted by every matching
//! engine. It is the **sole writer** to the `AccountRiskState` seqlocks for its
//! account range — no other thread writes those fields.
//!
//! # Responsibilities
//! 1. Update per-(account, symbol) `Position` on every fill.
//! 2. Recompute margin after each fill; publish updated `(balance, used_margin)`
//!    to the seqlock so Tier-0 sees fresh data on the next order.
//! 3. Trigger liquidation by emitting a privileged `InboundCommand::Liquidate`
//!    back through the sequencer when `used_margin > balance`.

use std::collections::HashMap;
use std::ops::Range;

use core_types::{AccountId, EngineEvent, InboundCommand, Price, Symbol};
use ring_buffer::{SpmcConsumer, SpscProducer};
use seqlock::AccountRiskState;

use crate::config::ShardConfig;
use crate::position::Position;

const FANOUT_CAP: usize = 1 << 14; // 16 384 — must match the producer side
const CMD_CAP:    usize = 1 << 10; // 1 024

/// Mark prices fed to the shard for margin recomputation.
/// In production these come from a market-data feed; here we pass them in
/// as a map for testability.
pub type MarkPrices = HashMap<Symbol, Price>;

/// The complete state owned by one risk shard.
pub struct RiskShard {
    /// The half-open range of `AccountId`s this shard owns.
    pub owned: Range<u64>,
    /// Per-(account, symbol) position. Single-writer: only this shard writes.
    pub positions: HashMap<(AccountId, Symbol), Position>,
    /// Seqlock states for each account this shard owns.
    /// Indexed by `account_id - owned.start`.
    pub states: Vec<AccountRiskState>,
    /// Config / limits.
    pub config: ShardConfig,
}

impl RiskShard {
    pub fn new(owned: Range<u64>, config: ShardConfig) -> Self {
        let n = (owned.end - owned.start) as usize;
        let states = (0..n).map(|_| AccountRiskState::default()).collect();
        Self {
            owned,
            positions: HashMap::new(),
            states,
            config,
        }
    }

    /// Returns `true` if this shard owns `account`.
    #[inline]
    fn owns(&self, account: AccountId) -> bool {
        self.owned.contains(&account.0)
    }

    /// Index into `self.states` for a given `AccountId`.
    #[inline]
    fn state_idx(&self, account: AccountId) -> usize {
        (account.0 - self.owned.start) as usize
    }

    /// Process a single `EngineEvent`. Called from the shard loop.
    ///
    /// Returns `Some(InboundCommand::Liquidate { .. })` if an account has
    /// breached its maintenance margin and must be liquidated.
    pub fn process_event(
        &mut self,
        event: EngineEvent,
        mark_prices: &MarkPrices,
    ) -> Option<InboundCommand> {
        match event {
            EngineEvent::Trade {
                maker_acct,
                taker_acct,
                symbol,
                price,
                qty,
                maker_side,
                ..
            } => {
                let mut liquidate_cmd: Option<InboundCommand> = None;

                for (acct, side) in [
                    (maker_acct, maker_side),
                    (taker_acct, maker_side.opposite()),
                ] {
                    if !self.owns(acct) {
                        continue;
                    }

                    // Update position.
                    let pos = self
                        .positions
                        .entry((acct, symbol))
                        .or_default();
                    pos.apply_fill(side, price, qty);

                    // Recompute margin across all symbols for this account.
                    let (balance, used_margin) =
                        self.recompute_margin(acct, mark_prices);

                    // Publish updated state via seqlock.
                    let idx = self.state_idx(acct);
                    let s = self.states[idx].read();
                    self.states[idx].update(balance, used_margin, s.frozen, s.halted, s.position, s.open_order_count);

                    // Trigger liquidation if maintenance margin breached.
                    if used_margin > balance {
                        liquidate_cmd = Some(InboundCommand::Liquidate {
                            account: acct,
                            symbol,
                        });
                        // Freeze the account immediately so no new orders slip through.
                        let s2 = self.states[idx].read();
                        self.states[idx].update(balance, used_margin, true, s2.halted, s2.position, s2.open_order_count);
                    }
                }

                liquidate_cmd
            }
            
            EngineEvent::OrderCancelled { account_id, .. }
            | EngineEvent::OrderRejected { account_id, .. } => {
                // No position change; nothing to do for the risk shard.
                let _ = account_id;
                None
            }

            EngineEvent::SnapshotMarker { seq } => {
                // Serialize shard state to disk at this logical sequence point.
                // In production this calls into the WAL/snapshot crate.
                // Here we just log the marker.
                eprintln!("[risk-shard {:?}] snapshot at seq={}", self.owned, seq);
                None
            }
            _ => None
        }
    }

    /// Recompute `(balance, used_margin)` for an account across all its positions.
    ///
    /// `balance`     = initial deposit + sum(realised_pnl across all symbols)
    /// `used_margin` = sum(notional * maintenance_margin_fraction across all open positions)
    ///
    /// This is intentionally simplified — a real system would also track
    /// funding payments, fees, deposits/withdrawals, etc.
    fn recompute_margin(
        &self,
        account: AccountId,
        mark_prices: &MarkPrices,
    ) -> (i64, i64) {
        let limits = &self.config.limits;
        // Retrieve the current balance from the seqlock (includes prior deposits).
        let idx = self.state_idx(account);
        let snap = self.states[idx].read();
        let mut balance = snap.balance;
        let mut used_margin: i64 = 0;

        for ((acct, symbol), pos) in &self.positions {
            if *acct != account {
                continue;
            }
            // Add realised PnL to balance.
            balance = balance.saturating_add(pos.realised_pnl);

            if pos.net_qty == 0 {
                continue;
            }

            // Compute notional using mark price if available, else avg_entry.
            let mark = mark_prices
                .get(symbol)
                .copied()
                .unwrap_or(core_types::Price(pos.avg_entry_price));

            let notional = pos.notional(mark);

            let margin_required = notional
                .saturating_mul(limits.maintenance_margin_fraction)
                / 1_000_000;

            used_margin = used_margin.saturating_add(margin_required);
        }

        (balance, used_margin)
    }
}

/// The main shard loop. Run this on a dedicated, pinned OS thread.
///
/// Never returns (returns `!`). Spins on the SPMC consumer, processes events,
/// and emits liquidation commands when needed.
pub fn run_risk_shard(
    mut shard: RiskShard,
    mut inbound: SpmcConsumer<EngineEvent, FANOUT_CAP>,
    mut commands_out: SpscProducer<InboundCommand, CMD_CAP>,
    mark_prices: &MarkPrices,
) -> ! {
    loop {
        let Some(event) = inbound.try_pop() else {
            std::hint::spin_loop();
            continue;
        };

        if let Some(cmd) = shard.process_event(event, mark_prices) {
            // Best-effort: if the command queue is full the market is in
            // distress. We spin here because a missed liquidation is worse
            // than latency. In production add a circuit-breaker timeout.
            loop {
                match commands_out.try_push(cmd.clone()) {
                    Ok(()) => break,
                    Err(_) => std::hint::spin_loop(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{EngineEvent, Price, Qty, Side, Symbol, AccountId};

    fn make_shard(n_accounts: usize) -> RiskShard {
        let config = ShardConfig::new(n_accounts);
        let mut shard = RiskShard::new(0..n_accounts as u64, config);
        // Seed each account with 10,000 USD balance (1e8 scale).
        for state in shard.states.iter_mut() {
            state.update(10_000_00000000, 0, false, false, 0, 0);
        }
        shard
    }

    fn trade_event(
        maker: AccountId,
        taker: AccountId,
        symbol: Symbol,
        price: Price,
        qty: Qty,
    ) -> EngineEvent {
        EngineEvent::Trade {
            seq: 1,
            maker_acct: maker,
            taker_acct: taker,
            maker_side: Side::Buy,
            symbol,
            price,
            qty,
        }
    }

    #[test]
    fn position_updates_on_fill() {
        let mut shard = make_shard(2);
        let marks = HashMap::new();

        let ev = trade_event(
            AccountId(0),
            AccountId(1),
            Symbol(0),
            Price(50_000_00000000),
            Qty(1_00000000),
        );

        let result = shard.process_event(ev, &marks);
        assert!(result.is_none(), "no liquidation expected");

        let pos = shard.positions.get(&(AccountId(0), Symbol(0))).unwrap();
        assert_eq!(pos.net_qty, 1_00000000); // maker bought
        let pos1 = shard.positions.get(&(AccountId(1), Symbol(0))).unwrap();
        assert_eq!(pos1.net_qty, -1_00000000); // taker sold
    }

    #[test]
    fn liquidation_triggered_on_margin_breach() {
        let n = 1;
        let config = ShardConfig::new(n);
        let mut shard = RiskShard::new(0..1, config);
        // Give account 0 a tiny balance: 1 USD (1e8 scale).
        shard.states[0].update(1_00000000, 0, false, false, 0, 0);

        let marks = HashMap::new();

        // Buy a huge position: 100 BTC @ 50,000 USD → notional = 5,000,000 USD
        // maintenance margin = 5% → required = 250,000 USD >> balance of 1 USD
        let ev = trade_event(
            AccountId(0),
            AccountId(0), // self-trade for simplicity in test
            Symbol(0),
            Price(50_000_00000000),
            Qty(100_00000000),
        );

        let result = shard.process_event(ev, &marks);
        assert!(
            matches!(result, Some(InboundCommand::Liquidate { account: AccountId(0), .. })),
            "expected liquidation command"
        );
        // Account should be frozen after breach.
        let snap = shard.states[0].read();
        assert!(snap.frozen);
    }
}