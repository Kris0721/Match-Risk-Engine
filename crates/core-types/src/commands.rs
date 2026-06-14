// Input commands for the matching risk engine
use crate::{AccountId, ClientOrderId, InstrumentId, OrderId, Price, Qty, Side, Symbol};

/// Time-in-force qualifier for new orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimeInForce {
    /// Good-till-cancel — rests on the book until filled or canceled.
    Gtc,
    /// Immediate-or-cancel — fills what it can immediately, cancels remainder.
    Ioc,
    /// Fill-or-kill — fills completely immediately or not at all.
    Fok,
}

/// The order type. `Limit` carries an explicit price; `Market` crosses
/// the book at whatever price is available (subject to TIF).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OrderType {
    Limit { price: Price },
    Market,
}

/// A new order request, as it enters the sequencer from the gateway.
///
/// `order_id` is `None` at this point — it is assigned by the
/// sequencer when the command is admitted and stamped with a
/// `SequenceNo`. See `sequencer/sequencer.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NewOrder {
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub client_order_id: ClientOrderId,
    pub side: Side,
    pub order_type: OrderType,
    pub qty: Qty,
    pub time_in_force: TimeInForce,
}

/// Request to cancel a resting order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CancelOrder {
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub order_id: OrderId,
}

/// Request to reduce or replace the quantity of a resting order
/// without changing its price/time priority position is NOT
/// guaranteed — engines typically treat this as cancel-replace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ModifyOrder {
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub order_id: OrderId,
    pub new_qty: Qty,
    pub new_price: Option<Price>,
}

/// Top-level command enum — the unit of work that flows through the
/// `ring-buffer` SPSC queue from gateway -> sequencer -> matching engine.
///
/// This enum is intentionally flat and `Copy` to avoid allocation on
/// the hot path. Variants that need variable-length data (none today)
/// would require redesigning the ring buffer's element type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    New(NewOrder),
    Cancel(CancelOrder),
    Modify(ModifyOrder),
    /// Administrative: halt trading on an instrument (see `sequencer/halt.rs`).
    Halt { instrument_id: InstrumentId },
    /// Administrative: resume trading on an instrument.
    Resume { instrument_id: InstrumentId },
}

impl Command {
    /// Returns the instrument this command targets, if applicable.
    /// `None` for commands that are not instrument-scoped.
    #[inline]
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        match self {
            Command::New(n) => Some(n.instrument_id),
            Command::Cancel(c) => Some(c.instrument_id),
            Command::Modify(m) => Some(m.instrument_id),
            Command::Halt { instrument_id } => Some(*instrument_id),
            Command::Resume { instrument_id } => Some(*instrument_id),
        }
    }

    /// Returns the account this command originates from, if applicable.
    #[inline]
    pub const fn account_id(&self) -> Option<AccountId> {
        match self {
            Command::New(n) => Some(n.account_id),
            Command::Cancel(c) => Some(c.account_id),
            Command::Modify(m) => Some(m.account_id),
            Command::Halt { .. } | Command::Resume { .. } => None,
        }
    }
}

/// Raw command received from the gateway, before sequencing.
///
/// Unlike `Command` (the wire-protocol type), `InboundCommand` is the
/// engine-internal representation consumed by the sequencer and routed
/// to matching engines. It carries inline fields rather than wrapping
/// request structs, for cache-friendly `Copy`-ability on the hot path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InboundCommand {
    NewOrder {
        account: AccountId,
        symbol: Symbol,
        side: Side,
        price: Price,
        qty: Qty,
        order_type: OrderType,
    },
    Cancel {
        account: AccountId,
        order_id: OrderId,
    },
    /// Privileged command emitted by the risk engine to liquidate an
    /// account's position on a given symbol.
    Liquidate {
        symbol: Symbol,
        account: AccountId,
    },
    /// Privileged command emitted by the risk engine to freeze an
    /// account (halt all new orders).
    FreezeAccount {
        account: AccountId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SequencedCommand {
    pub seq: u64,
    pub ts_ns: u64,
    pub cmd: InboundCommand,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_new_order() -> NewOrder {
        NewOrder {
            account_id: AccountId::new(1),
            instrument_id: InstrumentId::new(7),
            client_order_id: ClientOrderId::new(100),
            side: Side::Buy,
            order_type: OrderType::Limit { price: Price::new(10_050) },
            qty: Qty::new(10),
            time_in_force: TimeInForce::Gtc,
        }
    }

    #[test]
    fn command_accessors() {
        let cmd = Command::New(sample_new_order());
        assert_eq!(cmd.instrument_id(), Some(InstrumentId::new(7)));
        assert_eq!(cmd.account_id(), Some(AccountId::new(1)));

        let halt = Command::Halt { instrument_id: InstrumentId::new(3) };
        assert_eq!(halt.instrument_id(), Some(InstrumentId::new(3)));
        assert_eq!(halt.account_id(), None);
    }

    #[test]
    fn command_is_copy() {
        let cmd = Command::New(sample_new_order());
        let cmd2 = cmd; // Copy, not move
        assert_eq!(cmd, cmd2);
    }
}