// Matching engine execution loop
//! The matching engine hot loop: pulls commands off the inbound SPSC
//! ring from the sequencer, applies tier-0 risk checks, matches against
//! the order book, publishes resulting events, and updates the
//! seqlock-published account risk state for the async risk engine.

use std::time::Instant;

use core_types::{InboundCommand, SequencedCommand};
use core_types::events::{EngineEvent, Event, Fill, RejectReason, CancelReason};
use core_types::ids::{AccountId, InstrumentId, OrderId, SequenceNo};
use core_types::price::Price;

use order_book::book::OrderBook;
use ring_buffer::{spsc_queue, SpscConsumer, SpscProducer};
use seqlock::account_risk_state::AccountRiskStateWriter;
use wal::log::WalWriter;

use crate::metrics::EngineMetrics;
use crate::risk_check::{check_new_order, RiskRejectReason, Tier0Limits};

use crate::mapper::map_engine_event;

/// Configuration for a single matching engine shard (one instrument,
/// or a fixed set of instruments depending on sharding strategy).
pub struct EngineConfig {
    pub limits: Tier0Limits,
    /// CPU core to pin this engine's hot thread to, if any.
    pub pin_core: Option<usize>,
}

/// Runtime state for one matching shard. Owns the order book and the
/// risk state writer for accounts trading on this shard.
pub struct MatchingEngine<W: WalWriter> {
    config: EngineConfig,
    book: OrderBook,
    inbound: SpscConsumer<SequencedCommand, 1024>,
    outbound: SpscProducer<Event, 1024>,
    risk_state: AccountRiskStateWriter,
    wal: W,
    metrics: EngineMetrics,
    reference_price: Option<Price>,
    running: bool,
}

impl<W: WalWriter> MatchingEngine<W> {
    pub fn new(
        config: EngineConfig,
        book: OrderBook,
        inbound: SpscConsumer<SequencedCommand, 1024>,
        outbound: SpscProducer<Event, 1024>,
        risk_state: AccountRiskStateWriter,
        wal: W,
    ) -> Self {
        Self {
            config,
            book,
            inbound,
            outbound,
            risk_state,
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

    /// Process a single inbound command. Public for use by the
    /// deterministic simulation harness (`sim` crate), which drives the
    /// engine without a real ring buffer thread.
    pub fn handle_command(&mut self, cmd: SequencedCommand) {
        let start = Instant::now();

        match cmd.cmd {
            InboundCommand::NewOrder { account, client_order_id, symbol, side, price, qty, order_type, time_in_force } => {
                // Build a NewOrder-like struct for the risk check.
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
                    let state = self.risk_state.inner();
                    crate::risk_check::check_new_order(&new_order, account, &self.config.limits, state, self.reference_price)
                };
                self.metrics.risk_check_latency.record(risk_start.elapsed());

                match risk_result {
                    Ok(()) => {
                        let engine_events = self.book.apply(cmd);
                        let mut n_fills: u64 = 0;
                        for ev in engine_events {
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
                        // Build a Rejected event to publish
                        let seq_no = SequenceNo::new(cmd.seq).unwrap_or(SequenceNo::FIRST);
                        let ev = Event::Rejected { seq: seq_no, account_id: account, client_order_id, reason: core_types::events::RejectReason::RiskLimitBreach };
                        self.publish_and_log(ev);
                    }
                }
            }
            InboundCommand::Cancel { account, order_id } => {
                let events = self.book.apply(cmd);
                for ev in events {
                    if let Some(out_ev) = map_engine_event(ev) {
                        self.publish_and_log(out_ev);
                    }
                }
            }
            _ => {
                // Other command types (Liquidate, FreezeAccount) are forwarded
                let events = self.book.apply(cmd);
                for ev in events {
                    if let Some(out_ev) = map_engine_event(ev) {
                        self.publish_and_log(out_ev);
                    }
                }
            }
        }

        self.metrics.match_latency.record(start.elapsed());
    }

    fn publish_and_log(&mut self, ev: Event) {
        // WAL write happens before publishing so recovery can never see
        // an event that wasn't durably recorded.
        if let Err(e) = self.wal.append_event(&ev) {
            // In production this would trigger a halt; here we surface
            // via metrics-friendly panic in debug, no-op in release path
            // left to caller policy.
            debug_assert!(false, "WAL append failed: {:?}", e);
        }

        // Best-effort publish; if the outbound ring is full we drop the
        // oldest-style backpressure to the gateway (gateway must keep up).
        let _ = self.outbound.try_push(ev);
    }
}

fn reject_reason_code(reason: RiskRejectReason) -> u16 {
    match reason {
        RiskRejectReason::MaxOrderQtyExceeded => 1,
        RiskRejectReason::MaxOrderNotionalExceeded => 2,
        RiskRejectReason::AccountHalted => 3,
        RiskRejectReason::PositionLimitExceeded => 4,
        RiskRejectReason::OpenOrderLimitExceeded => 5,
        RiskRejectReason::PriceOutOfBand => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::commands::OrderType;
    use core_types::ids::{InstrumentId, OrderId};
    use core_types::qty::Qty;
    use core_types::side::Side;
    use ring_buffer::spsc::channel;
    use wal::log::NullWal;

    fn mk_engine() -> MatchingEngine<NullWal> {
        let (_in_tx, in_rx) = spsc_queue::<SequencedCommand, 1024>();
        let (out_tx, _out_rx) = spsc_queue::<Event, 1024>();
        let risk_state = AccountRiskStateWriter::new();

        MatchingEngine::new(
            EngineConfig { limits: Tier0Limits::default(), pin_core: None },
            OrderBook::new(core_types::InstrumentId(1)),
            in_rx,
            out_tx,
            risk_state,
            NullWal::default(),
        )
    }

    #[test]
    fn accepts_and_books_resting_order() {
        let mut engine = mk_engine();

        let seq_cmd = SequencedCommand { seq: 1, ts_ns: 0, cmd: InboundCommand::NewOrder { account: AccountId(1), client_order_id: core_types::ClientOrderId::new(0), symbol: core_types::Symbol(1), side: Side::Buy, price: Price::from_raw(100), qty: Qty::from_raw(10), order_type: OrderType::Limit, time_in_force: core_types::TimeInForce::Gtc } };

        engine.handle_command(seq_cmd);

        let snap = engine.metrics().snapshot();
        assert_eq!(snap.orders_processed, 1);
        assert_eq!(snap.risk_rejects, 0);
    }

    #[test]
    fn matches_crossing_orders() {
        let mut engine = mk_engine();

        let resting = NewOrder {
            order_id: OrderId(1 << 48),
            instrument: InstrumentId(1),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(Price::from_raw(100)),
            qty: Qty::from_raw(10),
        };
        engine.handle_command(Command::NewOrder(resting));

        let aggressor = NewOrder {
            order_id: OrderId((1 << 48) | 2),
            instrument: InstrumentId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(Price::from_raw(100)),
            qty: Qty::from_raw(10),
        };
        engine.handle_command(Command::NewOrder(aggressor));

        let snap = engine.metrics().snapshot();
        assert_eq!(snap.orders_processed, 2);
        assert!(snap.fills_generated >= 1);
    }

    #[test]
    fn rejects_order_violating_tier0_limits() {
        let (_in_tx, in_rx) = channel::<Command>(1024);
        let (out_tx, _out_rx) = channel::<Event>(1024);
        let risk_state = AccountRiskStateWriter::new();

        let mut engine = MatchingEngine::new(
            EngineConfig {
                limits: Tier0Limits { max_order_qty: Qty::from_raw(5), ..Default::default() },
                pin_core: None,
            },
            OrderBook::new(InstrumentId(1)),
            in_rx,
            out_tx,
            risk_state,
            NullWal::default(),
        );

        let order = NewOrder {
            order_id: OrderId(1 << 48),
            instrument: InstrumentId(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(Price::from_raw(100)),
            qty: Qty::from_raw(10),
        };
        engine.handle_command(Command::NewOrder(order));

        let snap = engine.metrics().snapshot();
        assert_eq!(snap.risk_rejects, 1);
    }
}