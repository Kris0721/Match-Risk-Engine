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
//! per-worker in this initial implementation — each worker keeps its
//! own `HashMap<Symbol, OrderBook>`, built fresh from the same
//! `BookConfig` list every worker is given; a shared lock-free order
//! book is a future optimization.
//!
//! # Symbol routing
//!
//! `NewOrder` / `Liquidate` carry an explicit `Symbol` and route to that
//! one book. `Cancel` / `FreezeAccount` do not carry a symbol (mirrors
//! the primary engine's per-symbol sharding, where the whole engine only
//! ever sees one symbol) so, like the deterministic sim harness, they are
//! broadcast to every book this worker owns.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Receiver;

use core_types::commands::{InboundCommand, SequencedCommand};
use core_types::events::EngineEvent;
use core_types::ids::{OrderId, Symbol};
use core_types::log_entry::LogEntry;
use core_types::order_status::OrderStatus;

use dual_log::PendingRing;
use order_book::book::BookConfig;
use order_book::OrderBook;

/// Configuration for the Second Engine.
#[derive(Debug, Clone)]
pub struct SecondEngineConfig {
    /// Number of worker threads (default: 8).
    pub worker_count: usize,
    /// Book configuration for every symbol the Second Engine must be able
    /// to back up. Each worker builds its own independent set of books
    /// from this list at startup.
    pub book_configs: Vec<BookConfig>,
}

impl Default for SecondEngineConfig {
    fn default() -> Self {
        Self {
            worker_count: 8,
            book_configs: Vec::new(),
        }
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
    /// Entries claimed for a symbol this worker has no book for.
    pub unknown_symbol: AtomicU64,
}

/// Build a fresh `HashMap<Symbol, OrderBook>` from a list of book configs.
/// Called once per worker at startup so each worker owns an independent
/// set of books (see module docs on the per-worker book model).
fn build_books(configs: &[BookConfig]) -> HashMap<Symbol, OrderBook> {
    configs
        .iter()
        .cloned()
        .map(|cfg| (cfg.symbol, OrderBook::new(cfg)))
        .collect()
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
    /// performs CAS claiming, and processes the order against its own
    /// local order books.
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
            let books = build_books(&config.book_configs);

            let handle = thread::Builder::new()
                .name(format!("second-engine-worker-{}", worker_id))
                .spawn(move || {
                    Self::worker_loop(worker_id, rx, ring, m, books);
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
        mut books: HashMap<Symbol, OrderBook>,
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

            // Step 3: We own this order — match it against our local
            // book(s) using the original inbound command from Log B.
            let now_ns = clock.elapsed().as_nanos() as u64;
            let (fill_price, filled_qty, matched_known_symbol) =
                apply_escalated(&entry, &mut books);

            if !matched_known_symbol {
                metrics.unknown_symbol.fetch_add(1, Ordering::Relaxed);
            }

            entry.record_fill(
                2, // handled_by = secondary
                now_ns, fill_price, filled_qty,
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

/// Apply the entry's original command against the given book set.
///
/// Returns `(fill_price, filled_qty, matched_known_symbol)`:
/// - `fill_price` / `filled_qty` are aggregated from any `Trade` events
///   where this order (`OrderId(entry.seq)`) was the taker — `0`/`0` if
///   there were none.
/// - `matched_known_symbol` is `false` if the command referenced a symbol
///   this worker has no book for (nothing was applied in that case).
fn apply_escalated(entry: &LogEntry, books: &mut HashMap<Symbol, OrderBook>) -> (u64, u64, bool) {
    let seq_cmd = SequencedCommand {
        seq: entry.seq,
        ts_ns: entry.timestamp_in,
        cmd: entry.cmd.clone(),
    };
    let this_order_id = OrderId(entry.seq);

    let mut fill_price: u64 = 0;
    let mut filled_qty: u64 = 0;
    let mut matched_known_symbol = true;

    match &seq_cmd.cmd {
        InboundCommand::NewOrder { symbol, .. } => {
            if let Some(book) = books.get_mut(symbol) {
                let events = book.apply(seq_cmd);
                accumulate_fill(&events, this_order_id, &mut fill_price, &mut filled_qty);
            } else {
                matched_known_symbol = false;
            }
        }

        InboundCommand::Liquidate { symbol, account } => {
            // Mirrors the primary engine: pull every resting order for
            // this account off the book via individual Cancels. No fill
            // data results from a Liquidate, so price/qty stay 0.
            if let Some(book) = books.get_mut(symbol) {
                let order_ids = book.open_order_ids_for_account(*account);
                for order_id in order_ids {
                    let cancel = SequencedCommand {
                        seq: seq_cmd.seq,
                        ts_ns: seq_cmd.ts_ns,
                        cmd: InboundCommand::Cancel {
                            account: *account,
                            order_id,
                        },
                    };
                    let _ = book.apply(cancel);
                }
            } else {
                matched_known_symbol = false;
            }
        }

        InboundCommand::Cancel { .. } => {
            // No symbol on the command itself (same as the primary
            // engine, which only ever sees one symbol per shard) — this
            // worker owns potentially many symbols, so broadcast, same
            // pattern the sim harness uses for routing Cancel/FreezeAccount.
            for book in books.values_mut() {
                let _ = book.apply(seq_cmd.clone());
            }
        }

        InboundCommand::FreezeAccount { .. } => {
            // No book-level effect (matches primary engine's handling —
            // freezing lives in the risk layer, not the book).
        }
    }

    (fill_price, filled_qty, matched_known_symbol)
}

/// Sum quantity and record the last trade price for every `Trade` event
/// where `this_order_id` was the taker (i.e. this escalated order was the
/// aggressor that generated the fill).
fn accumulate_fill(
    events: &[EngineEvent],
    this_order_id: OrderId,
    fill_price: &mut u64,
    filled_qty: &mut u64,
) {
    for ev in events {
        if let EngineEvent::Trade {
            price,
            qty,
            taker_order_id,
            ..
        } = ev
        {
            if *taker_order_id == this_order_id {
                *fill_price = price.raw() as u64;
                *filled_qty += qty.raw();
            }
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
    books: &mut HashMap<Symbol, OrderBook>,
) -> bool {
    let status = entry.load_status();
    if status != OrderStatus::Unaddressed {
        return false;
    }

    if !entry.try_claim(OrderStatus::Unaddressed, OrderStatus::FinallyHandled) {
        return false;
    }

    let (fill_price, filled_qty, _) = apply_escalated(entry, books);
    entry.record_fill(2, 0, fill_price, filled_qty);
    pending_ring.remove(entry.seq);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{AccountId, ClientOrderId, OrderType, Price, Qty, Side, TimeInForce};

    const SYMBOL: Symbol = Symbol(0);

    fn test_book_configs() -> Vec<BookConfig> {
        vec![BookConfig {
            symbol: SYMBOL,
            tick_floor: Price(0),
            num_ticks: 1024,
            arena_capacity: 256,
        }]
    }

    fn sample_entry(seq: u64, side: Side, price: i64) -> Arc<LogEntry> {
        Arc::new(LogEntry::new(
            seq,
            0,
            InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: ClientOrderId::new(seq),
                symbol: SYMBOL,
                side,
                price: Price(price),
                qty: Qty(10),
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        ))
    }

    fn sample_entry_for_account(seq: u64, side: Side, price: i64, account: u64) -> Arc<LogEntry> {
        Arc::new(LogEntry::new(
            seq,
            0,
            InboundCommand::NewOrder {
                account: AccountId(account),
                client_order_id: ClientOrderId::new(seq),
                symbol: SYMBOL,
                side,
                price: Price(price),
                qty: Qty(10),
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        ))
    }

    #[test]
    fn process_single_matches_and_records_fill() {
        let ring = PendingRing::new();
        let mut books = build_books(&test_book_configs());

        // Rest a sell first (account 1).
        let resting = sample_entry_for_account(1, Side::Sell, 100, 1);
        resting.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed);
        ring.push(Arc::clone(&resting));
        assert!(process_single(&resting, &ring, &mut books));

        // Escalated buy from a *different* account crosses it.
        let aggressor = sample_entry_for_account(2, Side::Buy, 100, 2);
        aggressor.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed);
        ring.push(Arc::clone(&aggressor));
        assert!(process_single(&aggressor, &ring, &mut books));

        assert_eq!(aggressor.load_status(), OrderStatus::FinallyHandled);
        assert_eq!(aggressor.fill_price.load(Ordering::Acquire), 100);
        assert_eq!(aggressor.filled_qty.load(Ordering::Acquire), 10);
    }

    #[test]
    fn process_single_skips_addressed() {
        let ring = PendingRing::new();
        let mut books = build_books(&test_book_configs());
        let entry = sample_entry(1, Side::Buy, 100);
        // Primary already claimed it
        entry.try_claim(OrderStatus::Pending, OrderStatus::Addressed);

        assert!(!process_single(&entry, &ring, &mut books));
    }

    #[test]
    fn process_single_skips_pending() {
        let ring = PendingRing::new();
        let mut books = build_books(&test_book_configs());
        let entry = sample_entry(1, Side::Buy, 100);
        // Still Pending — Second Engine shouldn't touch it
        assert!(!process_single(&entry, &ring, &mut books));
    }

    #[test]
    fn worker_pool_processes_escalated_orders() {
        let ring = Arc::new(PendingRing::new());
        let (tx, rx) = crossbeam_channel::unbounded();

        let config = SecondEngineConfig {
            worker_count: 2,
            book_configs: test_book_configs(),
        };
        let engine = SecondEngine::start(config, rx, Arc::clone(&ring));

        // Create and escalate 10 entries
        for i in 1..=10 {
            let entry = sample_entry(i, Side::Buy, 100);
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
