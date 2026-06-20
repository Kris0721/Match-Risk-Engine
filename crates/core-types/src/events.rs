// Output events emitted by the matching risk engine
use crate::{AccountId, ClientOrderId, InstrumentId, OrderId, Price, Qty, SequenceNo, Side, Symbol};

/// Reason an order was removed from the book / rejected, used in
/// `Event::Cancel` and `Event::Reject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CancelReason {
    /// Explicit cancel request from the account owner.
    UserRequested,
    /// IOC/FOK order with no remaining quantity after matching.
    TimeInForceExpired,
    /// Risk engine rejected/force-canceled the order (see `risk-engine`).
    RiskLimitBreach,
    /// Instrument was halted while the order was resting.
    InstrumentHalted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RejectReason {
    RiskLimitBreach,
    InstrumentHalted,
    InvalidPrice,
    InvalidQuantity,
    InvalidQty,
    UnknownInstrument,
    UnknownOrder,
    PriceOutOfRange,
    IocNoMatch,
    ArenaFull,
    OrderNotFound,
    WrongAccount,
}

/// Details of a single trade resulting from order matching.
///
/// Emitted once per fill; a single incoming aggressive order may
/// generate multiple `Fill` events if it crosses multiple resting
/// price levels/orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fill {
    pub instrument_id: InstrumentId,
    /// The order that initiated the match (taker).
    pub aggressor_order_id: OrderId,
    pub aggressor_account_id: AccountId,
    pub aggressor_side: Side,
    /// The resting order that was matched against (maker).
    pub resting_order_id: OrderId,
    pub resting_account_id: AccountId,
    /// Execution price â€” always the resting order's price (price-time
    /// priority convention: maker sets the price).
    pub price: Price,
    pub qty: Qty,
    /// Remaining quantity on the resting order after this fill.
    pub resting_remaining_qty: Qty,
    /// Remaining quantity on the aggressor after this fill.
    pub aggressor_remaining_qty: Qty,
}

/// Top-level event enum â€” output of the matching engine, consumed by
/// the risk engine, WAL, gateway (for execution reports), and metrics.
///
/// Every event carries the `SequenceNo` of the command that produced
/// it, providing a total order for replay/recovery (`wal/recovery.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Event {
    /// A new order was accepted and (if any quantity remains) placed
    /// on the book.
    Accepted {
        seq: SequenceNo,
        instrument_id: InstrumentId,
        order_id: OrderId,
        account_id: AccountId,
        client_order_id: ClientOrderId,
        side: Side,
        price: Price,
        qty: Qty,
    },
    /// A trade occurred between an aggressor and a resting order.
    Filled { seq: SequenceNo, fill: Fill },
    /// An order was removed from the book.
    Canceled {
        seq: SequenceNo,
        instrument_id: InstrumentId,
        order_id: OrderId,
        account_id: AccountId,
        reason: CancelReason,
        remaining_qty: Qty,
    },
    /// An order was rejected before ever being placed on the book.
    Rejected {
        seq: SequenceNo,
        account_id: AccountId,
        client_order_id: ClientOrderId,
        reason: RejectReason,
    },
    /// A resting order's quantity and/or price was changed.
    Modified {
        seq: SequenceNo,
        instrument_id: InstrumentId,
        order_id: OrderId,
        account_id: AccountId,
        new_qty: Qty,
        new_price: Option<Price>,
    },
    /// Trading on an instrument was halted.
    InstrumentHalted { seq: SequenceNo, instrument_id: InstrumentId },
    /// Trading on an instrument resumed.
    InstrumentResumed { seq: SequenceNo, instrument_id: InstrumentId },
}

impl Event {
    /// Returns the sequence number associated with this event â€”
    /// every variant carries one, used for ordering during WAL replay.
    #[inline]
    pub const fn seq(&self) -> SequenceNo {
        match self {
            Event::Accepted { seq, .. }
            | Event::Filled { seq, .. }
            | Event::Canceled { seq, .. }
            | Event::Rejected { seq, .. }
            | Event::Modified { seq, .. }
            | Event::InstrumentHalted { seq, .. }
            | Event::InstrumentResumed { seq, .. } => *seq,
        }
    }
}

/// Engine-internal event produced by the order book after processing a
/// `SequencedCommand`.
///
/// Unlike `Event` (the gateway wire-protocol type), `EngineEvent` is
/// used internally between the order book, sequencer, risk engine, and
/// simulation harness. It is designed to be cache-friendly for the hot path.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum EngineEvent {
    // Common trade representation used across the codebase. Different
    // components reference different field names â€” keep a superset so both
    // dialects can construct / pattern-match without changing callers.
    Trade {
        seq: u64,
        ts_ns: u64,
        symbol: Symbol,
        price: Price,
        qty: Qty,
        maker_order: OrderId,
        taker_order: OrderId,
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        maker_acct: AccountId,
        taker_acct: AccountId,
        maker_side: Side,
        maker_remaining_qty: Qty,
        taker_remaining_qty: Qty,
    },

    // Variants matching the order-book's wire of engine-internal events.
    Accepted { seq: u64, order_id: OrderId, ts_ns: u64, symbol: Symbol, account_id: AccountId, client_order_id: ClientOrderId, side: Side, price: Price, qty: Qty },
    Rejected { seq: u64, order_id: OrderId, account_id: AccountId, client_order_id: ClientOrderId, reason: RejectReason },
    Cancelled { seq: u64, order_id: OrderId },
    BookTop { seq: u64, symbol: Symbol, bid: Option<Price>, ask: Option<Price> },

    /// Acknowledgement that an order has been assigned an engine-wide id
    /// and accepted (useful for gateway execution reports / client acks).
    OrderAcknowledged {
        order_id: OrderId,
        account_id: AccountId,
        seq: u64,
    },

    /// Higher-level, documented dialect variants used by some components.
    OrderCancelled {
        order_id: OrderId,
        account_id: AccountId,
        seq: u64,
        symbol: Symbol,
    },
    OrderRejected {
        order_id: OrderId,
        account_id: AccountId,
        reason: RejectReason,
        seq: u64,
        symbol: Symbol,
    },

    SnapshotMarker { seq: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_seq_accessor() {
        let seq = SequenceNo::FIRST;
        let ev = Event::InstrumentHalted { seq, instrument_id: InstrumentId::new(1) };
        assert_eq!(ev.seq(), seq);
    }

    #[test]
    fn fill_event_roundtrip() {
        let seq = SequenceNo::FIRST;
        let fill = Fill {
            instrument_id: InstrumentId::new(1),
            aggressor_order_id: OrderId::new(2),
            aggressor_account_id: AccountId::new(10),
            aggressor_side: Side::Buy,
            resting_order_id: OrderId::new(3),
            resting_account_id: AccountId::new(20),
            price: Price::new(10_000),
            qty: Qty::new(5),
            resting_remaining_qty: Qty::new(0),
            aggressor_remaining_qty: Qty::new(0),
        };
        let ev = Event::Filled { seq, fill };
        assert_eq!(ev.seq(), seq);
        if let Event::Filled { fill: f, .. } = ev {
            assert_eq!(f.price, Price::new(10_000));
        } else {
            panic!("expected Filled variant");
        }
    }
}


