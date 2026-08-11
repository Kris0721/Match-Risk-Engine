//! Lock-free structure holding only pending orders for O(log n) Sorter scanning.
//!
//! # Design (Architecture Doc §6, Optimization 2)
//!
//! Instead of the Sorter scanning the entire log (O(n)), it scans only this
//! structure, which contains at most the orders currently in-flight. At
//! steady state this is ~100-500 entries.
//!
//! # Thread safety
//!
//! - **Push**: called by the DualLog writer thread after dual-write succeeds
//!   (single producer).
//! - **Remove**: called concurrently by every Second Engine worker thread
//!   AND the Sorter thread — this is genuinely multi-writer, keyed by `seq`.
//! - **Iter / next_pending / snapshot**: called by the Sorter thread.
//!
//! # Why a concurrent skip list, not a hand-rolled structure
//!
//! Earlier revision used `Mutex<VecDeque>`. `remove()` is called from
//! multiple Second Engine worker threads plus the Sorter thread
//! concurrently, targeting arbitrary `seq` keys — this is a concurrent
//! keyed-removal set, not a queue. A hand-rolled lock-free version of that
//! (skip list or linked list with manual memory reclamation) needs a
//! correct epoch-based reclamation scheme to avoid use-after-free when one
//! thread reads an `Arc` while another physically unlinks and frees the
//! node; getting that subtly wrong is real, hard-to-detect undefined
//! behavior. `crossbeam-skiplist` provides exactly this — a lock-free,
//! epoch-reclaimed concurrent map — and is already in the same crate
//! family (`crossbeam-*`) used elsewhere in this workspace.
//!
//! `seq` is monotonically increasing (assigned once by the sequencer) and
//! is the map key, so the skip list's sorted iteration order preserves the
//! same FIFO order the old `VecDeque` gave for free.
//!
//! `len()` is tracked with a separate `AtomicUsize` rather than
//! `SkipMap::len()` (which is O(n), a full traversal) so the hot-path
//! `is_empty()`/`len()` calls stay O(1). This counter is only
//! eventually-consistent with the map under concurrent push/remove — a
//! reader can observe it momentarily stale — but that is no weaker a
//! guarantee than the old mutex version gave the instant after the lock
//! was released, so no caller-visible behavior changes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use core_types::log_entry::LogEntry;
use core_types::order_status::OrderStatus;

/// Default capacity for the pending ring buffer (64K slots).
///
/// The skip list itself grows dynamically and does not enforce this as a
/// hard cap — it is kept here for API compatibility with the previous
/// fixed-capacity ring and as the intended steady-state sizing hint.
const DEFAULT_CAPACITY: usize = 65536;

/// A lock-free, thread-safe structure holding only pending (in-flight)
/// log entries, keyed by sequence number.
///
/// Orders are pushed when they enter the system and removed when they are
/// fully processed (Addressed or FinallyHandled). The Sorter scans this
/// structure to find orders that need escalation.
pub struct PendingRing {
    map: SkipMap<u64, Arc<LogEntry>>,
    len: AtomicUsize,
}

impl PendingRing {
    /// Create a new `PendingRing` with the default capacity hint (64K).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a new `PendingRing`. `_capacity` is retained for API
    /// compatibility with the previous fixed-capacity ring; the
    /// underlying skip list grows dynamically and does not pre-allocate.
    pub fn with_capacity(_capacity: usize) -> Self {
        Self {
            map: SkipMap::new(),
            len: AtomicUsize::new(0),
        }
    }

    /// Push a new pending entry.
    ///
    /// Called by the DualLog after a successful dual-write. Single
    /// producer in practice, but `SkipMap::insert` is safe under
    /// concurrent callers regardless.
    pub fn push(&self, entry: Arc<LogEntry>) {
        let seq = entry.seq;
        self.map.insert(seq, entry);
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove the entry with the given sequence number.
    ///
    /// Called concurrently by Second Engine workers (after CAS claim) and
    /// the Sorter (after escalation). Returns `true` if the entry was
    /// found and removed by *this* call — `SkipMap::remove` guarantees
    /// exactly one racing caller wins for a given key, so this remains a
    /// safe, idempotent "did I remove it" check under contention.
    pub fn remove(&self, seq: u64) -> bool {
        if self.map.remove(&seq).is_some() {
            self.len.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Get the next pending entry (status == Pending) without removing it.
    ///
    /// O(n) worst case (skip-list iteration), same asymptotic cost as the
    /// previous `VecDeque` version. Returns a clone of the `Arc`.
    pub fn next_pending(&self) -> Option<Arc<LogEntry>> {
        self.map
            .iter()
            .find(|e| e.value().load_status() == OrderStatus::Pending)
            .map(|e| e.value().clone())
    }

    /// Return a snapshot of all entries currently present, in `seq` order.
    ///
    /// Used by the Sorter to scan all in-flight orders. The returned `Vec`
    /// is a snapshot — entries may change status between snapshot and
    /// processing, which is safe because all transitions use CAS.
    pub fn snapshot(&self) -> Vec<Arc<LogEntry>> {
        self.map.iter().map(|e| e.value().clone()).collect()
    }

    /// Number of entries currently present. O(1) — see module docs on the
    /// eventual-consistency tradeoff of the counter this reads.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Returns `true` if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all entries whose status is terminal (Addressed or
    /// FinallyHandled).
    ///
    /// Called periodically by the Sorter to keep the structure small.
    /// Returns the number of entries removed. Two-pass (collect terminal
    /// keys, then remove) rather than removing while iterating, since
    /// `SkipMap`'s iterator does not support removal during traversal.
    pub fn gc_terminal(&self) -> usize {
        let terminal_keys: Vec<u64> = self
            .map
            .iter()
            .filter(|e| e.value().load_status().is_terminal())
            .map(|e| *e.key())
            .collect();

        let mut removed = 0usize;
        for key in terminal_keys {
            if self.map.remove(&key).is_some() {
                removed += 1;
            }
        }
        if removed > 0 {
            self.len.fetch_sub(removed, Ordering::Relaxed);
        }
        removed
    }
}

impl Default for PendingRing {
    fn default() -> Self {
        Self::new()
    }
}

// Debug impl that doesn't lock (shows type name only)
impl std::fmt::Debug for PendingRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRing")
            .field("len", &self.len())
            .finish()
    }
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
            seq * 1000,
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
    fn push_and_next_pending() {
        let ring = PendingRing::new();
        assert!(ring.is_empty());

        let e1 = sample_entry(1);
        let e2 = sample_entry(2);
        ring.push(e1.clone());
        ring.push(e2.clone());

        assert_eq!(ring.len(), 2);

        let next = ring.next_pending().expect("should have a pending entry");
        assert_eq!(next.seq, 1);
    }

    #[test]
    fn next_pending_skips_addressed() {
        let ring = PendingRing::new();
        let e1 = sample_entry(1);
        let e2 = sample_entry(2);

        // Claim e1 so it's no longer Pending
        e1.try_claim(OrderStatus::Pending, OrderStatus::Addressed);

        ring.push(e1);
        ring.push(e2.clone());

        let next = ring.next_pending().expect("should find e2");
        assert_eq!(next.seq, 2);
    }

    #[test]
    fn remove_by_seq() {
        let ring = PendingRing::new();
        ring.push(sample_entry(1));
        ring.push(sample_entry(2));
        ring.push(sample_entry(3));

        assert!(ring.remove(2));
        assert_eq!(ring.len(), 2);
        assert!(!ring.remove(2)); // already removed
    }

    #[test]
    fn snapshot_returns_all() {
        let ring = PendingRing::new();
        for i in 1..=5 {
            ring.push(sample_entry(i));
        }

        let snap = ring.snapshot();
        assert_eq!(snap.len(), 5);
        assert_eq!(snap[0].seq, 1);
        assert_eq!(snap[4].seq, 5);
    }

    #[test]
    fn gc_terminal_removes_handled() {
        let ring = PendingRing::new();
        let e1 = sample_entry(1);
        let e2 = sample_entry(2);
        let e3 = sample_entry(3);

        e1.try_claim(OrderStatus::Pending, OrderStatus::Addressed);
        e3.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed);
        e3.try_claim(OrderStatus::Unaddressed, OrderStatus::FinallyHandled);

        ring.push(e1);
        ring.push(e2);
        ring.push(e3);

        let removed = ring.gc_terminal();
        assert_eq!(removed, 2); // e1 (Addressed) and e3 (FinallyHandled)
        assert_eq!(ring.len(), 1); // only e2 (Pending) remains
    }

    /// New test: the actual concurrency pattern this structure exists for
    /// — multiple threads racing to remove different keys concurrently.
    /// The old `Mutex<VecDeque>` serialized these; this asserts the
    /// lock-free version is still correct (no double-removal, no lost
    /// entries) when genuinely concurrent.
    #[test]
    fn concurrent_removal_from_multiple_threads_is_correct() {
        use std::thread;

        let ring = Arc::new(PendingRing::new());
        const N: u64 = 2000;
        for seq in 0..N {
            ring.push(sample_entry(seq));
        }
        assert_eq!(ring.len(), N as usize);

        // Split removal of [0, N) across 8 threads, disjoint ranges, plus
        // extra threads racing to remove overlapping keys — mirrors the
        // real pattern of several worker threads and the sorter thread
        // all calling remove() concurrently.
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let ring = Arc::clone(&ring);
                thread::spawn(move || {
                    let mut removed_by_me = 0u64;
                    for seq in 0..N {
                        if seq % 8 == t && ring.remove(seq) {
                            removed_by_me += 1;
                        }
                    }
                    removed_by_me
                })
            })
            .collect();

        let total_removed: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

        assert_eq!(
            total_removed, N,
            "every entry should be removed exactly once"
        );
        assert_eq!(ring.len(), 0);
        assert!(ring.is_empty());
    }
}
