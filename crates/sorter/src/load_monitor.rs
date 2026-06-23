//! Load monitor — reads Primary Engine metrics to estimate load level.
//!
//! Used by the Sorter to compute adaptive escalation timeouts:
//! - Low load → short timeout (escalate quickly)
//! - High load → longer timeout (give Primary more time)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared load metrics published by the Primary Engine and read by the Sorter.
///
/// The Primary Engine calls `report_*` methods from its hot loop.
/// The Sorter calls `primary_load_pct()` to read the current load estimate.
///
/// All operations are lock-free via atomics.
pub struct LoadMonitor {
    /// Number of orders the Primary processed in the current window.
    orders_processed: AtomicU64,
    /// Number of idle spins (no work available) in the current window.
    idle_spins: AtomicU64,
    /// Estimated throughput capacity (orders/sec). Set at startup.
    capacity_hint: u64,
}

impl LoadMonitor {
    /// Create a new load monitor.
    ///
    /// `capacity_hint` is the estimated peak throughput of the Primary Engine
    /// in orders per second. Used to compute load percentage.
    pub fn new(capacity_hint: u64) -> Self {
        Self {
            orders_processed: AtomicU64::new(0),
            idle_spins: AtomicU64::new(0),
            capacity_hint: capacity_hint.max(1), // avoid div-by-zero
        }
    }

    /// Called by the Primary Engine after processing an order.
    #[inline]
    pub fn report_order(&self) {
        self.orders_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Called by the Primary Engine on an idle spin (no work available).
    #[inline]
    pub fn report_idle(&self) {
        self.idle_spins.fetch_add(1, Ordering::Relaxed);
    }

    /// Estimated Primary Engine load as a percentage (0–100).
    ///
    /// Uses the ratio of orders processed vs idle spins as a rough
    /// proxy for load. When the engine is fully loaded, idle spins ≈ 0
    /// and load → 100%. When idle, spins dominate and load → 0%.
    ///
    /// This is intentionally approximate — the Sorter's adaptive timeout
    /// is robust to imprecise load estimates.
    pub fn primary_load_pct(&self) -> u8 {
        let orders = self.orders_processed.load(Ordering::Relaxed);
        let idles = self.idle_spins.load(Ordering::Relaxed);
        let total = orders + idles;
        if total == 0 {
            return 0;
        }
        let pct = (orders * 100) / total;
        pct.min(100) as u8
    }

    /// Reset counters for the next measurement window.
    ///
    /// Called periodically (e.g., every 100ms) by the monitoring system.
    pub fn reset_window(&self) {
        self.orders_processed.store(0, Ordering::Relaxed);
        self.idle_spins.store(0, Ordering::Relaxed);
    }
}

impl Default for LoadMonitor {
    fn default() -> Self {
        // Default capacity hint: 1M orders/sec
        Self::new(1_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_activity_returns_zero_load() {
        let mon = LoadMonitor::default();
        assert_eq!(mon.primary_load_pct(), 0);
    }

    #[test]
    fn all_orders_returns_100() {
        let mon = LoadMonitor::default();
        for _ in 0..100 {
            mon.report_order();
        }
        assert_eq!(mon.primary_load_pct(), 100);
    }

    #[test]
    fn half_and_half_returns_50() {
        let mon = LoadMonitor::default();
        for _ in 0..50 {
            mon.report_order();
            mon.report_idle();
        }
        assert_eq!(mon.primary_load_pct(), 50);
    }

    #[test]
    fn reset_clears_counters() {
        let mon = LoadMonitor::default();
        for _ in 0..100 {
            mon.report_order();
        }
        assert_eq!(mon.primary_load_pct(), 100);

        mon.reset_window();
        assert_eq!(mon.primary_load_pct(), 0);
    }
}
