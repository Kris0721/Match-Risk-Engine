//! Sorter — the intelligence of the dual-engine system.
//!
//! Continuously scans the pending ring buffer, classifying orders and
//! escalating timed-out orders to the Second Engine.

pub mod sorter;
pub mod load_monitor;
pub mod leader;

pub use sorter::{Sorter, SorterConfig};
pub use load_monitor::LoadMonitor;
pub use leader::SorterLeader;
