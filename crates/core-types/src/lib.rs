//! Core domain types shared across the matching/risk engine.
//!
//! Design goals:
//! - Fixed-point arithmetic (no floats) for prices/quantities — deterministic, fast.
//! - `Copy`-able, cache-friendly small types for hot-path use.
//! - Zero allocation on the matching hot path.

pub mod price;
pub mod qty;
pub mod ids;
pub mod side;
pub mod commands;
pub mod events;
pub mod order_status;
pub mod log_entry;

pub use price::Price;
pub use qty::Qty;
pub use ids::{OrderId, AccountId, InstrumentId, SequenceNo, ClientOrderId, Symbol};
pub use side::Side;
pub use commands::{Command, InboundCommand, SequencedCommand, NewOrder, CancelOrder, OrderType, TimeInForce};
pub use events::{Event, EngineEvent, RejectReason, CancelReason};
pub use order_status::OrderStatus;
pub use log_entry::LogEntry;