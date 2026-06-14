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

pub use price::Price;
pub use qty::Qty;
pub use ids::{OrderId, AccountId, InstrumentId, SequenceNo, ClientOrderId, Symbol};
pub use side::Side;
pub use commands::{Command, InboundCommand, SequencedCommand};
pub use events::{Event, EngineEvent};