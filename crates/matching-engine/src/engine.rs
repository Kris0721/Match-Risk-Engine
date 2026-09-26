//! The matching engine hot loop: pulls commands off the inbound SPSC
//! ring from the sequencer, applies tier-0 risk checks, matches against
//! the order book, publishes resulting events, and updates the
//! seqlock-published account risk state for the async risk engine.

use std::sync::Arc;
use std::time::Instant;

use core_types::events::Event;
use core_types::ids::SequenceNo;
use core_types::price::Price;

use core_types::{EngineEvent, InboundCommand, RejectReason, SequencedCommand};
use order_book::book::OrderBook;
use ring_buffer::{SpmcProducer, SpscConsumer, SpscProducer};
use seqlock::AccountRiskTable;
use wal::log::WalWriter;

use crate::metrics::EngineMetrics;
use crate::risk_check::{RiskRejectReason, Tier0Limits};

use crate::mapper::map_engine_event;

/// Configuration for a single matching engine shard.
/// /// Must match `risk_engine::shard::FANOUT_CAP` (private to that crate, so
/// this value is kept in sync by hand — same pattern already used between
/// `sequencer::ME_INBOUND_CAP` and this file's queue capacities).
const RISK_FANOUT_CAP: usize = 1 << 14; // 16 384

pub struct EngineConfig {
    pub limits: Tier0Limits,
    /// CPU core to pin this engine's hot thread to, if any.
    pub pin_core: Option<usize>,
}

/// Runtime state for one matching shard.
pub struct MatchingEngine<W: WalWriter> {
    config: EngineConfig,
    book: OrderBook,
    inbound: SpscConsumer<SequencedCommand, 4096>,
    outbound: SpscProducer<Event, 1024>,
    risk_out: SpmcProducer<EngineEvent, RISK_FANOUT_CAP>,
    risk_states: Arc<AccountRiskTable>,
    wal: W,
    metrics: EngineMetrics,
    reference_price: Option<Price>,
    running: bool,
}

impl<W: WalWriter> MatchingEngine<W> {
    pub fn new(
        config: EngineConfig,
        book: OrderBook,
        inbound: SpscConsumer<SequencedCommand, 4096>,
        outbound: SpscProducer<Event, 1024>,
        risk_out: SpmcProducer<EngineEvent, RISK_FANOUT_CAP>,
        risk_states: Arc<AccountRiskTable>,
        wal: W,
    ) -> Self {
        Self {
            config,
            book,
            inbound,
            outbound,
            risk_out,
            risk_states,
            wal,
            metrics: EngineMetrics::new(),
            reference_price: None,
            running: true,
        }
    }

    pub fn metrics(&self) -> &EngineMetrics {
        &self.metrics
    }

    pub fn shutdown(&mut self) {
        self.running = false;
    }

    /// Run the hot loop until `shutdown` is called. Intended to be the
    /// entire body of a pinned OS thread.
    pub fn run(&mut self) {
        if let Some(core) = self.config.pin_core {
            crate::affinity::setup_hot_thread(Some(core), "matching-engine");
        }

        while self.running {
            match self.inbound.try_pop() {
                Some(cmd) => self.handle_command(cmd),
                None => {
                    self.metrics.record_idle_spin();
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Best-effort fan-out of the raw `EngineEvent` to risk shards. Not
    /// WAL-gated like `publish_and_log` (risk shards recompute state from
    /// the seqlock table on recovery, not from this stream), so a full
    /// queue only costs risk-shard visibility, not durability — spin
    /// briefly, then drop + log rather than halt the engine.
    fn publish_to_risk(&mut self, ev: EngineEvent) {
        const MAX_RETRIES: u32 = 64;
        let mut item = ev;
        for _ in 0..MAX_RETRIES {
            match self.risk_out.try_push(item) {
                Ok(()) => return,
                Err(rejected) => {
                    item = rejected;
                    std::hint::spin_loop();
                }
            }
        }
        logger::warn("matching-engine: risk fan-out ring full, dropping event");
    }

    /// Process a single inbound command. Public for use by the
    /// deterministic simulation harness (`sim` crate), which drives the
    /// engine without a real ring buffer thread.
    pub fn handle_command(&mut self, cmd: SequencedCommand) {
        let start = Instant::now();

        match cmd.cmd {
            InboundCommand::NewOrder {
                account,
                client_order_id,
                symbol,
                side,
                price,
                qty,
                order_type,
                time_in_force,
            } => {
                let new_order = core_types::NewOrder {
                    account_id: account,
                    instrument_id: core_types::InstrumentId(symbol.0.into()),
                    client_order_id,
                    side,
                    price: price,
                    order_type,
                    qty,
                    time_in_force,
                };

                let risk_start = Instant::now();
                let risk_result = {
                    match self.risk_states.get(account.get()) {
                        Some(state) => crate::risk_check::check_new_order(
                            &new_order,
                            account,
                            &self.config.limits,
                            state,
                            self.reference_price,
                        ),
                        None => Err(RiskRejectReason::UnknownAccount),
                    }
                };
                self.metrics.risk_check_latency.record(risk_start.elapsed());

                match risk_result {
                    Ok(()) => {
                        let engine_events = self.book.apply(cmd);
                        let mut n_fills: u64 = 0;
                        for ev in engine_events {
                            if let EngineEvent::Trade { price, .. } = &ev {
                                self.reference_price = Some(*price);
                            }
                            self.publish_to_risk(ev.clone());
                            if let Some(out_ev) = map_engine_event(ev) {
                                if matches!(out_ev, Event::Filled { .. }) {
                                    n_fills += 1;
                                }
                                self.publish_and_log(out_ev);
                            }
                        }
                        self.metrics.record_order(n_fills, false);
                    }
                    Err(reason) => {
                        self.metrics.record_order(0, true);
                        let seq_no = SequenceNo::new(cmd.seq).unwrap_or(SequenceNo::FIRST);
                        let ev = Event::Rejected {
                            seq: seq_no,
                            account_id: account,
                            client_order_id,
                            reason: map_risk_reject_reason(reason),
                        };
                        self.publish_and_log(ev);
                    }
                }
            }
            InboundCommand::Cancel {
                account: _,
                order_id: _,
            } => {
                let events = self.book.apply(cmd);
                for ev in events {
                    self.publish_to_risk(ev.clone());
                    if let Some(out_ev) = map_engine_event(ev) {
                        self.publish_and_log(out_ev);
                    }
                }
            }
            InboundCommand::Liquidate { account, .. } => {
                // Cancel every resting order for this account, then let the
                // caller (risk shard) handle the actual position/margin unwind —
                // this engine's job is just to pull all liquidity off the book.
                let order_ids = self.book.open_order_ids_for_account(account);
                for order_id in order_ids {
                    let cancel = SequencedCommand {
                        seq: cmd.seq,
                        ts_ns: cmd.ts_ns,
                        cmd: InboundCommand::Cancel { account, order_id },
                    };
                    let events = self.book.apply(cancel);
                    for ev in events {
                        self.publish_to_risk(ev.clone());
                        if let Some(out_ev) = map_engine_event(ev) {
                            self.publish_and_log(out_ev);
                        }
                    }
                }
            }
            InboundCommand::FreezeAccount { account } => {
                // Administrative freeze, distinct from risk-engine's
                // automatic margin-breach freeze — both write the same
                // `frozen` flag on the shared seqlock state, so either
                // source is picked up by `risk_check::check_new_order`.
                // There is no corresponding "unfreeze" command yet — that's
                // a separate gap (same class as risk-engine's
                // deposit-only, no-withdrawal limitation).
                match self.risk_states.get(account.get()) {
                    Some(state) => state.set_frozen(true),
                    None => logger::warn(&format!(
                        "matching-engine: FreezeAccount for unknown account {account:?}, ignoring"
                    )),
                }
            }
        }

        self.metrics.match_latency.record(start.elapsed());
    }

    fn publish_and_log(&mut self, ev: Event) {
        // WAL write happens before publishing so recovery can never see
        // an event that wasn't durably recorded.
        if let Err(e) = self.wal.append_event(&ev) {
            // WAL failure means durability is broken: halt the engine
            // (stops the hot loop in `run`) and do NOT publish this
            // event downstream, in debug or release.
            self.metrics.record_wal_failure();
            self.running = false;
            eprintln!("FATAL: WAL append failed, halting engine: {:?}", e);
            return;
        }

        // Bounded spin-retry: the outbound consumer (risk shards / gateway
        // forwarders) should normally drain within a handful of spins. If
        // it's still full after that, the consumer is genuinely stuck or
        // overloaded — at that point dropping is a real data-loss event,
        // not routine backpressure, so it must be recorded rather than
        // silently discarded.
        const MAX_PUBLISH_RETRIES: u32 = 64;

        let mut item = ev;
        for _ in 0..MAX_PUBLISH_RETRIES {
            match self.outbound.try_push(item) {
                Ok(()) => return,
                Err(rejected) => {
                    item = rejected;
                    std::hint::spin_loop();
                }
            }
        }

        // Circuit-broken: still full after MAX_PUBLISH_RETRIES spins.
        self.metrics.record_outbound_drop();
        logger::warn(&format!(
            "matching-engine: outbound ring full after {MAX_PUBLISH_RETRIES} retries, \
             dropping event: {item:?}"
        ));
    }
}

fn map_risk_reject_reason(reason: RiskRejectReason) -> RejectReason {
    match reason {
        RiskRejectReason::MaxOrderQtyExceeded => RejectReason::InvalidQuantity,
        RiskRejectReason::MaxOrderNotionalExceeded => RejectReason::RiskLimitBreach,
        RiskRejectReason::AccountHalted => RejectReason::RiskLimitBreach,
        RiskRejectReason::AccountFrozen => RejectReason::RiskLimitBreach,
        RiskRejectReason::PositionLimitExceeded => RejectReason::RiskLimitBreach,
        RiskRejectReason::OpenOrderLimitExceeded => RejectReason::RiskLimitBreach,
        RiskRejectReason::PriceOutOfBand => RejectReason::PriceOutOfRange,
        RiskRejectReason::UnknownAccount => RejectReason::RiskLimitBreach,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::commands::OrderType;
    use core_types::qty::Qty;
    use core_types::side::Side;
    use core_types::{AccountId, Symbol};
    use order_book::book::BookConfig;
    use ring_buffer::spsc_queue;
    use wal::log::NullWal;

    fn mk_engine() -> MatchingEngine<NullWal> {
        let (_in_tx, in_rx) = spsc_queue::<SequencedCommand, 4096>();
        let (out_tx, _out_rx) = spsc_queue::<Event, 1024>();
        let (risk_tx, _risk_rx) = ring_buffer::spmc_queue::<EngineEvent, RISK_FANOUT_CAP>(1);
        let risk_state = Arc::new(AccountRiskTable::new(16));

        let book_cfg = BookConfig {
            symbol: Symbol(1),
            tick_floor: Price::ZERO,
            num_ticks: 1024,
            arena_capacity: 4096,
        };

        MatchingEngine::new(
            EngineConfig {
                limits: Tier0Limits::default(),
                pin_core: None,
            },
            OrderBook::new(book_cfg),
            in_rx,
            out_tx,
            risk_tx,
            risk_state,
            NullWal::default(),
        )
    }

    #[test]
    fn accepts_and_books_resting_order() {
        let mut engine = mk_engine();

        let seq_cmd = SequencedCommand {
            seq: 1,
            ts_ns: 0,
            cmd: InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: core_types::ClientOrderId::new(0),
                symbol: core_types::Symbol(1),
                side: Side::Buy,
                price: Price::from_raw(100),
                qty: Qty::from_raw(10),
                order_type: OrderType::Limit,
                time_in_force: core_types::TimeInForce::Gtc,
            },
        };

        engine.handle_command(seq_cmd);

        let snap = engine.metrics().snapshot();
        assert_eq!(snap.orders_processed, 1);
        assert_eq!(snap.risk_rejects, 0);
    }

    #[test]
    fn matches_crossing_orders() {
        let mut engine = mk_engine();

        let resting = SequencedCommand {
            seq: 1,
            ts_ns: 0,
            cmd: InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: core_types::ClientOrderId::new(0),
                symbol: core_types::Symbol(1),
                side: Side::Sell,
                price: Price::from_raw(100),
                qty: Qty::from_raw(10),
                order_type: OrderType::Limit,
                time_in_force: core_types::TimeInForce::Gtc,
            },
        };
        engine.handle_command(resting);

        let aggressor = SequencedCommand {
            seq: 2,
            ts_ns: 0,
            cmd: InboundCommand::NewOrder {
                account: AccountId(2),
                client_order_id: core_types::ClientOrderId::new(1),
                symbol: core_types::Symbol(1),
                side: Side::Buy,
                price: Price::from_raw(100),
                qty: Qty::from_raw(10),
                order_type: OrderType::Limit,
                time_in_force: core_types::TimeInForce::Gtc,
            },
        };
        engine.handle_command(aggressor);

        let snap = engine.metrics().snapshot();
        assert_eq!(snap.orders_processed, 2);
        assert!(snap.fills_generated >= 1);
    }

    /// Regression test for the bug where the engine held a single shared
    /// risk-state cell for every account instead of looking each account up
    /// in the shared table: halting account 1 must reject account 1's
    /// orders and must NOT reject account 2's orders on the same engine.
    #[test]
    fn risk_state_is_isolated_per_account() {
        let mut engine = mk_engine();

        // Halt account 1 directly through the shared table, the same way a
        // risk shard would in production.
        engine
            .risk_states
            .get(1)
            .expect("test account 1 must be within table capacity")
            .set_halted(true);

        let halted_account_order = SequencedCommand {
            seq: 1,
            ts_ns: 0,
            cmd: InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: core_types::ClientOrderId::new(0),
                symbol: core_types::Symbol(1),
                side: Side::Buy,
                price: Price::from_raw(100),
                qty: Qty::from_raw(10),
                order_type: OrderType::Limit,
                time_in_force: core_types::TimeInForce::Gtc,
            },
        };
        engine.handle_command(halted_account_order);

        let other_account_order = SequencedCommand {
            seq: 2,
            ts_ns: 0,
            cmd: InboundCommand::NewOrder {
                account: AccountId(2),
                client_order_id: core_types::ClientOrderId::new(1),
                symbol: core_types::Symbol(1),
                side: Side::Buy,
                price: Price::from_raw(100),
                qty: Qty::from_raw(10),
                order_type: OrderType::Limit,
                time_in_force: core_types::TimeInForce::Gtc,
            },
        };
        engine.handle_command(other_account_order);

        let snap = engine.metrics().snapshot();
        assert_eq!(
            snap.risk_rejects, 1,
            "only account 1's order should be rejected"
        );
        assert_eq!(snap.orders_processed, 2);
    }
}
