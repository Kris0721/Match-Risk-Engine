//! Second Engine worker pool — processes orders escalated by the Sorter.
//!
//! # Architecture Doc §4.4
//!
//! The Second Engine **only processes orders the Sorter escalates to it**.
//! Under normal operation, it processes ~10–15% of orders. If Primary
//! fails entirely, it absorbs 100% of load.
//!
//! ```text
//! Worker pool model:
//!   Multiple workers read from shared unaddressed queue
//!   Each worker atomically claims via CAS before processing
//!   Workers never block each other — pure lock-free pipeline
//! ```
//!
//! # Thread model
//!
//! Each worker runs on its own OS thread. Workers share nothing except
//! the crossbeam channel (for receiving escalated entries) and the
//! `Arc<LogEntry>` references (for CAS claiming). The order book is
//! per-worker in this initial implementation; a shared lock-free order
//! book is a future optimization.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Receiver;

use core_types::log_entry::LogEntry;
use core_types::order_status::OrderStatus;

use dual_log::PendingRing;

/// Configuration for the Second Engine.
#[derive(Debug, Clone)]
pub struct SecondEngineConfig {
    /// Number of worker threads (default: 8).
    pub worker_count: usize,
}

impl Default for SecondEngineConfig {
    fn default() -> Self {
        Self { worker_count: 8 }
    }
}

/// Metrics for the Second Engine.
#[derive(Debug, Default)]
pub struct SecondEngineMetrics {
    /// Orders successfully claimed and processed by workers.
    pub orders_processed: AtomicU64,
    /// CAS claim failures (another worker or Primary got it first).
    pub cas_failures: AtomicU64,
    /// Orders received from the Sorter.
    pub orders_received: AtomicU64,
}

/// The Second Engine — a pool of worker threads that process escalated orders.
pub struct SecondEngine {
    /// Worker thread handles.
    workers: Vec<JoinHandle<()>>,
    /// Shared metrics across all workers.
    metrics: Arc<SecondEngineMetrics>,
    /// Config.
    config: SecondEngineConfig,
}

impl SecondEngine {
    /// Start the Second Engine with `config.worker_count` worker threads.
    ///
    /// Each worker pulls from `work_rx` (shared channel from Sorter),
    /// performs CAS claiming, and processes the order.
    ///
    /// `pending_ring` is used to remove entries after processing.
    pub fn start(
        config: SecondEngineConfig,
        work_rx: Receiver<Arc<LogEntry>>,
        pending_ring: Arc<PendingRing>,
    ) -> Self {
        let metrics = Arc::new(SecondEngineMetrics::default());
        let mut workers = Vec::with_capacity(config.worker_count);

        for worker_id in 0..config.worker_count {
            let rx = work_rx.clone();
            let ring = Arc::clone(&pending_ring);
            let m = Arc::clone(&metrics);

            let handle = thread::Builder::new()
                .name(format!("second-engine-worker-{}", worker_id))
                .spawn(move || {
                    Self::worker_loop(worker_id, rx, ring, m);
                })
                .expect("Failed to spawn second engine worker thread");

            workers.push(handle);
        }

        Self {
            workers,
            metrics,
            config,
        }
    }

    /// Worker loop — runs until the channel is disconnected.
    fn worker_loop(
        _worker_id: usize,
        work_rx: Receiver<Arc<LogEntry>>,
        pending_ring: Arc<PendingRing>,
        metrics: Arc<SecondEngineMetrics>,
    ) {
        let clock = Instant::now();

        while let Ok(entry) = work_rx.recv() {
            metrics.orders_received.fetch_add(1, Ordering::Relaxed);

            // Step 1: Atomic read — confirm still Unaddressed.
            // (Primary may have grabbed it after Sorter escalated.)
            let status = entry.load_status();
            if status != OrderStatus::Unaddressed {
                metrics.cas_failures.fetch_add(1, Ordering::Relaxed);
                continue; // Primary got it — skip
            }

            // Step 2: Atomic CAS claim.
            if !entry.try_claim(OrderStatus::Unaddressed, OrderStatus::FinallyHandled) {
                metrics.cas_failures.fetch_add(1, Ordering::Relaxed);
                continue; // Another worker got it — skip
            }

            // Step 3: We own this order — process it.
            // In a full implementation, this would match against the
            // Second Engine's order book using Log B data. For now,
            // we record the fill metadata atomically.
            let now_ns = clock.elapsed().as_nanos() as u64;
            entry.record_fill(
                2, // handled_by = secondary
                now_ns,
                0, // fill_price — would be set by matching
                0, // filled_qty — would be set by matching
            );

            // Step 4: Remove from pending ring.
            pending_ring.remove(entry.seq);

            metrics.orders_processed.fetch_add(1, Ordering::Relaxed);
        }
        // Channel disconnected — Sorter has shut down.
    }

    /// Get the shared metrics.
    pub fn metrics(&self) -> &Arc<SecondEngineMetrics> {
        &self.metrics
    }

    /// Number of worker threads.
    pub fn worker_count(&self) -> usize {
        self.config.worker_count
    }

    /// Wait for all worker threads to finish (blocks until channel closes).
    pub fn join(self) {
        for handle in self.workers {
            let _ = handle.join();
        }
    }
}

/// Process a single escalated entry without spawning threads.
///
/// Used for testing and deterministic simulation. Returns `true` if the
/// entry was successfully claimed and processed.
pub fn process_single(
    entry: &LogEntry,
    pending_ring: &PendingRing,
) -> bool {
    let status = entry.load_status();
    if status != OrderStatus::Unaddressed {
        return false;
    }

    if !entry.try_claim(OrderStatus::Unaddressed, OrderStatus::FinallyHandled) {
        return false;
    }

    entry.record_fill(2, 0, 0, 0);
    pending_ring.remove(entry.seq);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{
        AccountId, ClientOrderId, InboundCommand, OrderType, Price, Qty, Side, Symbol, TimeInForce,
    };

    fn sample_entry(seq: u64) -> Arc<LogEntry> {
        Arc::new(LogEntry::new(
            seq,
            0,
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

    #[test]
    fn process_single_claims_unaddressed() {
        let ring = PendingRing::new();
        let entry = sample_entry(1);
        // Pre-escalate to Unaddressed (as Sorter would)
        entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed);
        ring.push(Arc::clone(&entry));

        assert!(process_single(&entry, &ring));
        assert_eq!(entry.load_status(), OrderStatus::FinallyHandled);
        assert_eq!(entry.handled_by.load(Ordering::Acquire), 2);
        assert!(ring.is_empty());
    }

    #[test]
    fn process_single_skips_addressed() {
        let ring = PendingRing::new();
        let entry = sample_entry(1);
        // Primary already claimed it
        entry.try_claim(OrderStatus::Pending, OrderStatus::Addressed);

        assert!(!process_single(&entry, &ring));
    }

    #[test]
    fn process_single_skips_pending() {
        let ring = PendingRing::new();
        let entry = sample_entry(1);
        // Still Pending — Second Engine shouldn't touch it
        assert!(!process_single(&entry, &ring));
    }

    #[test]
    fn worker_pool_processes_escalated_orders() {
        let ring = Arc::new(PendingRing::new());
        let (tx, rx) = crossbeam_channel::unbounded();

        let config = SecondEngineConfig { worker_count: 2 };
        let engine = SecondEngine::start(config, rx, Arc::clone(&ring));

        // Create and escalate 10 entries
        for i in 1..=10 {
            let entry = sample_entry(i);
            entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed);
            ring.push(Arc::clone(&entry));
            tx.send(entry).unwrap();
        }

        // Drop sender to signal workers to exit
        drop(tx);

        // Wait for workers to finish
        engine.join();

        // All entries should have been processed
        assert!(ring.is_empty(), "ring should be empty after processing");
    }
}
