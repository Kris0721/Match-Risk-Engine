// Circuit breaker and emergency halt logic
//! Global halt switch.
//!
//! A single `AtomicBool` checked by the Sequencer before every fan-out.
//! One `store(true, Release)` from any thread (ops tool, risk shard, signal
//! handler) halts the entire engine within one sequencer loop iteration —
//! no per-shard coordination, no locks.
//!
//! Readers use `Acquire` so the halt is observed after any prior writes
//! (e.g. a risk event that caused the halt) are also visible.

#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(feature = "loom"))]
use std::sync::Arc;
#[cfg(feature = "loom")]
use loom::sync::Arc;

/// Shared handle to the global halt flag.
///
/// Cheap to clone (`Arc` inside). Pass one copy to the Sequencer and one to
/// every component that may need to trigger an emergency stop.
#[derive(Clone)]
pub struct GlobalHalt(Arc<AtomicBool>);

impl GlobalHalt {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Trigger a halt. Idempotent. Visible to the Sequencer within one loop
    /// iteration (no sleep, no syscall required on the reader side).
    #[inline]
    pub fn trigger(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Check whether a halt has been triggered.
    #[inline]
    pub fn is_set(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl Default for GlobalHalt {
    fn default() -> Self {
        Self::new()
    }
}