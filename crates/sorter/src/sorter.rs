//! Sorter program — the intelligence of the dual-engine system.
//!
//! # Architecture Doc §4.3
//!
//! The Sorter runs continuously, scanning only the **pending ring buffer**
//! (not the entire log) and classifying every order:
//!
//! ```text
//! Status          Action
//! ──────────────────────────────────────────────────────
//! Addressed     → record metrics, remove from pending ring
//! Pending       → check age against adaptive timeout
//!               → if age < timeout: leave it (Primary may get it)
//!               → if age > timeout: CAS to Unaddressed, push to 2nd Engine
//! Unaddressed   → already escalated, monitor for critical timeout
//! FinallyHandled→ record metrics, remove from pending ring
//! ```
//!
//! **Adaptive timeout:** The Sorter reads Primary Engine load metrics and
//! adjusts the timeout dynamically. Under low load: short timeout (fast
//! escalation). Under heavy load: longer timeout (gives Primary more time).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;

use core_types::log_entry::LogEntry;
use core_types::order_status::OrderStatus;

use dual_log::PendingRing;

use crate::load_monitor::LoadMonitor;

/// Critical timeout in nanoseconds (10ms). If an order exceeds this age
/// regardless of status, the Monitor is alerted.
const CRITICAL_TIMEOUT_NS: u64 = 10_000_000;

/// Configuration for the Sorter.
#[derive(Debug, Clone)]
pub struct SorterConfig {
    /// How often the Sorter scans the pending ring (default: 500μs).
    pub scan_interval: Duration,
    /// CPU core to pin the Sorter thread to, if any.
    pub pin_core: Option<usize>,
}

impl Default for SorterConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_micros(500),
            pin_core: None,
        }
    }
}

/// Metrics tracked by the Sorter.
#[derive(Debug, Default)]
pub struct SorterMetrics {
    /// Orders that were escalated to the Second Engine.
    pub escalated: AtomicU64,
    /// Orders that the Primary Engine handled before timeout.
    pub primary_handled: AtomicU64,
    /// Orders that the Second Engine handled.
    pub secondary_handled: AtomicU64,
    /// Entries removed from the pending ring (GC).
    pub gc_removed: AtomicU64,
    /// Number of scan cycles completed.
    pub scan_cycles: AtomicU64,
    /// Critical timeout alerts raised.
    pub critical_alerts: AtomicU64,
}

/// The Sorter program — scans the pending ring and escalates timed-out orders.
pub struct Sorter {
    /// Shared pending ring buffer (also read by Primary Engine).
    pending_ring: Arc<PendingRing>,
    /// Channel to send escalated orders to the Second Engine.
    second_engine_tx: Sender<Arc<LogEntry>>,
    /// Load monitor for adaptive timeout computation.
    load_monitor: Arc<LoadMonitor>,
    /// Configuration.
    config: SorterConfig,
    /// Clock origin for timestamp calculations.
    clock_origin: Instant,
    /// Metrics.
    metrics: SorterMetrics,
    /// Whether the sorter is running.
    running: bool,
}

impl Sorter {
    /// Create a new Sorter.
    pub fn new(
        pending_ring: Arc<PendingRing>,
        second_engine_tx: Sender<Arc<LogEntry>>,
        load_monitor: Arc<LoadMonitor>,
        config: SorterConfig,
    ) -> Self {
        Self {
            pending_ring,
            second_engine_tx,
            load_monitor,
            config,
            clock_origin: Instant::now(),
            metrics: SorterMetrics::default(),
            running: true,
        }
    }

    /// Run the Sorter loop. Blocks until `shutdown()` is called.
    ///
    /// In production this runs on a dedicated pinned thread. For testing,
    /// use `scan_once()` instead.
    pub fn run(&mut self) {
        while self.running {
            self.scan_once();
            std::thread::sleep(self.config.scan_interval);
        }
    }

    /// Signal the Sorter to stop.
    pub fn shutdown(&mut self) {
        self.running = false;
    }

    /// Perform a single scan of the pending ring.
    ///
    /// Public for use in tests and deterministic simulation.
    pub fn scan_once(&mut self) {
        let now_ns = self.clock_origin.elapsed().as_nanos() as u64;
        let timeout = self.adaptive_timeout();

        self.metrics.scan_cycles.fetch_add(1, Ordering::Relaxed);

        // Take a snapshot of the pending ring to avoid holding the lock
        // while processing entries.
        let entries = self.pending_ring.snapshot();

        for entry in entries {
            let status = entry.load_status();
            let age = entry.age_ns(now_ns);

            match status {
                OrderStatus::Pending if age > timeout => {
                    // Timed out — escalate to Second Engine.
                    if entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed) {
                        // We won the CAS — send to Second Engine.
                        let _ = self.second_engine_tx.send(Arc::clone(&entry));
                        self.metrics.escalated.fetch_add(1, Ordering::Relaxed);
                    }
                    // If try_claim failed: Primary just grabbed it — perfect.
                }
                OrderStatus::Pending => {
                    // Still within timeout — leave it for the Primary Engine.
                    // Check for critical timeout.
                    if age > CRITICAL_TIMEOUT_NS {
                        self.metrics.critical_alerts.fetch_add(1, Ordering::Relaxed);
                    }
                }
                OrderStatus::Addressed => {
                    // Primary handled it — remove from ring and record.
                    self.pending_ring.remove(entry.seq);
                    self.metrics.primary_handled.fetch_add(1, Ordering::Relaxed);
                    self.metrics.gc_removed.fetch_add(1, Ordering::Relaxed);
                }
                OrderStatus::FinallyHandled => {
                    // Second Engine handled it — remove from ring and record.
                    self.pending_ring.remove(entry.seq);
                    self.metrics.secondary_handled.fetch_add(1, Ordering::Relaxed);
                    self.metrics.gc_removed.fetch_add(1, Ordering::Relaxed);
                }
                OrderStatus::Unaddressed => {
                    // Already escalated — monitor for critical timeout.
                    if age > CRITICAL_TIMEOUT_NS {
                        self.metrics.critical_alerts.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Compute the adaptive timeout in nanoseconds based on Primary load.
    ///
    /// Architecture Doc §4.3:
    /// - 0-50% load: 200μs (Primary fast, escalate quickly)
    /// - 51-80%: 500μs (moderate load)
    /// - 81-95%: 1ms (heavy load, give Primary time)
    /// - >95%: 2ms (Primary overloaded, still escalate eventually)
    fn adaptive_timeout(&self) -> u64 {
        let load = self.load_monitor.primary_load_pct();
        match load {
            0..=50  => 200_000,     // 200μs
            51..=80 => 500_000,     // 500μs
            81..=95 => 1_000_000,   // 1ms
            _       => 2_000_000,   // 2ms
        }
    }

    /// Read the Sorter's metrics.
    pub fn metrics(&self) -> &SorterMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{
        AccountId, ClientOrderId, InboundCommand, OrderType, Price, Qty, Side, Symbol, TimeInForce,
    };

    fn sample_entry(seq: u64, age_ns: u64) -> Arc<LogEntry> {
        // Create entry with timestamp_in = 0 so age is always `now_ns`
        Arc::new(LogEntry::new(
            seq,
            0_u64.wrapping_sub(age_ns), // Will produce correct age when now_ns is large
            InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: ClientOrderId::new(seq),
                symbol: Symbol(0),
                side: Side::Buy,
                price: Price(100_00000000),
                qty: Qty(10_00000000),
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        ))
    }

    fn mk_sorter() -> (Sorter, crossbeam_channel::Receiver<Arc<LogEntry>>, Arc<PendingRing>) {
        let ring = Arc::new(PendingRing::new());
        let (tx, rx) = crossbeam_channel::unbounded();
        let load = Arc::new(LoadMonitor::default());
        let sorter = Sorter::new(
            Arc::clone(&ring),
            tx,
            load,
            SorterConfig::default(),
        );
        (sorter, rx, ring)
    }

    #[test]
    fn addressed_entries_are_gc_removed() {
        let (mut sorter, _rx, ring) = mk_sorter();

        let entry = sample_entry(1, 0);
        entry.try_claim(OrderStatus::Pending, OrderStatus::Addressed);
        ring.push(entry);

        sorter.scan_once();

        assert_eq!(ring.len(), 0);
        assert_eq!(
            sorter.metrics.primary_handled.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn finally_handled_entries_are_gc_removed() {
        let (mut sorter, _rx, ring) = mk_sorter();

        let entry = sample_entry(1, 0);
        entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed);
        entry.try_claim(OrderStatus::Unaddressed, OrderStatus::FinallyHandled);
        ring.push(entry);

        sorter.scan_once();

        assert_eq!(ring.len(), 0);
        assert_eq!(
            sorter.metrics.secondary_handled.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn recent_pending_entries_are_left_alone() {
        let (mut sorter, rx, ring) = mk_sorter();

        // Create entry with timestamp_in close to now (very recent)
        let now_ns = sorter.clock_origin.elapsed().as_nanos() as u64;
        let entry = Arc::new(LogEntry::new(
            1,
            now_ns, // Just created, age ≈ 0
            InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: ClientOrderId::new(1),
                symbol: Symbol(0),
                side: Side::Buy,
                price: Price(100_00000000),
                qty: Qty(10_00000000),
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        ));
        ring.push(entry);

        sorter.scan_once();

        // Entry should still be in the ring (not escalated)
        assert_eq!(ring.len(), 1);
        assert!(rx.try_recv().is_err(), "should not have escalated");
    }

    #[test]
    fn timed_out_pending_entries_are_escalated() {
        let (mut sorter, rx, ring) = mk_sorter();

        sorter.clock_origin = std::time::Instant::now() - std::time::Duration::from_secs(1);

        // Create entry with timestamp_in far in the past so it times out
        let entry = Arc::new(LogEntry::new(
            1,
             0, // timestamp_in = 0, age will be huge
            InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: ClientOrderId::new(1),
                symbol: Symbol(0),
                side: Side::Buy,
                price: Price(100_00000000),
                qty: Qty(10_00000000),
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        ));
        ring.push(entry);

        sorter.scan_once();

        // Entry should have been escalated (CAS Pending → Unaddressed)
        let escalated = rx.try_recv().expect("should have escalated entry");
        assert_eq!(escalated.seq, 1);
        assert_eq!(escalated.load_status(), OrderStatus::Unaddressed);
        assert_eq!(
            sorter.metrics.escalated.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn adaptive_timeout_varies_with_load() {
        let ring = Arc::new(PendingRing::new());
        let (tx, _rx) = crossbeam_channel::unbounded();
        let load = Arc::new(LoadMonitor::default());

        let sorter = Sorter::new(
            ring,
            tx,
            Arc::clone(&load),
            SorterConfig::default(),
        );

        // No load → 200μs
        assert_eq!(sorter.adaptive_timeout(), 200_000);

        // Simulate 75% load
        for _ in 0..75 {
            load.report_order();
        }
        for _ in 0..25 {
            load.report_idle();
        }
        assert_eq!(sorter.adaptive_timeout(), 500_000);

        // Simulate 90% load
        load.reset_window();
        for _ in 0..90 {
            load.report_order();
        }
        for _ in 0..10 {
            load.report_idle();
        }
        assert_eq!(sorter.adaptive_timeout(), 1_000_000);
    }
}
