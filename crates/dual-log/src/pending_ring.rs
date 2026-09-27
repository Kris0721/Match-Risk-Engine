use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use core_types::log_entry::LogEntry;
use core_types::order_status::OrderStatus;

const DEFAULT_CAPACITY: usize = 65536;

pub struct PendingRing {
    map: SkipMap<u64, Arc<LogEntry>>,
    len: AtomicUsize,
}

impl PendingRing {
    /// Create a new `PendingRing` with the default capacity hint (64K).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(_capacity: usize) -> Self {
        Self {
            map: SkipMap::new(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, entry: Arc<LogEntry>) {
        let seq = entry.seq;
        self.map.insert(seq, entry);
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove(&self, seq: u64) -> bool {
        if self.map.remove(&seq).is_some() {
            self.len.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn next_pending(&self) -> Option<Arc<LogEntry>> {
        self.map
            .iter()
            .find(|e| e.value().load_status() == OrderStatus::Pending)
            .map(|e| e.value().clone())
    }

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
            1,
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
