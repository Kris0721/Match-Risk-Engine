//! Core domain types shared across the matching/risk engine.
//!
//! Design goals:
//! - Fixed-point arithmetic (no floats) for prices/quantities — deterministic, fast.
//! - `Copy`-able, cache-friendly small types for hot-path use.
//! - Zero allocation on the matching hot path.

pub mod clock;
pub mod commands;
pub mod events;
pub mod ids;
pub mod log_entry;
pub mod order_status;
pub mod price;
pub mod qty;
pub mod side;

pub use commands::{
    CancelOrder, Command, CommandConversionError, InboundCommand, NewOrder, OrderType,
    SequencedCommand, TimeInForce,
};
pub use events::{CancelReason, EngineEvent, Event, RejectReason};
pub use ids::{
    AccountId, ClientOrderId, InstrumentId, OrderId, SequenceNo, Symbol, SymbolRangeError,
};
pub use log_entry::LogEntry;
pub use order_status::OrderStatus;
pub use price::Price;
pub use qty::Qty;
pub use side::Side;
