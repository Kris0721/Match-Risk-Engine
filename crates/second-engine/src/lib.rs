//! Second Engine — backup matching engine with a worker-pool model.
//!
//! Only processes orders that the Sorter escalates (status = Unaddressed).
//! Under normal operation handles ~10–15% of orders. If Primary fails
//! entirely, it absorbs 100% of load.

pub mod engine;

pub use engine::{SecondEngine, SecondEngineConfig};
