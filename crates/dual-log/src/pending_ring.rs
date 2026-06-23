//! Fixed-size ring buffer holding only pending orders for O(1) Sorter scanning.
//!
//! # Design (Architecture Doc §6, Optimization 2)
//!
//! Instead of the Sorter scanning the entire log (O(n)), it scans only this
//! ring buffer which contains at most the orders currently in-flight. At
//! steady state this is ~100–500 entries, giving ~50μs scan time vs ~500μs.
//!
//! # Thread safety
//!
//! - **Push**: called by the DualLog writer thread after dual-write succeeds.
//! - **Remove**: called by Primary Engine (after CAS claim) or Sorter (after escalation).
//! - **Iter / next**: called by Primary Engine and Sorter.
//!
//! We use a `Mutex<VecDeque>` for simplicity. The critical section is very
//! short (push/pop of an `Arc`), and contention is minimal because:
//! - Push happens on the DualLog thread
//! - Remove/iter happen on the Primary Engine thread and Sorter thread
//! - The Sorter runs every 500μs, so lock hold time is negligible
//!
//! For a lock-free version, replace with a concurrent skip-list or
//! lock-free linked list — but the Mutex version is correct and fast enough
//! for the initial implementation.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use core_types::log_entry::LogEntry;
use core_types::order_status::OrderStatus;

/// Default capacity for the pending ring buffer (64K slots).
const DEFAULT_CAPACITY: usize = 65536;

/// A thread-safe ring buffer holding only pending (in-flight) log entries.
///
/// Orders are pushed when they enter the system and removed when they are
/// fully processed (Addressed or FinallyHandled). The Sorter scans this
/// buffer to find orders that need escalation.
pub struct PendingRing {
    inner: Mutex<VecDeque<Arc<LogEntry>>>,
}

impl PendingRing {
    /// Create a new `PendingRing` with the default capacity (64K).
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(DEFAULT_CAPACITY)),
        }
    }

    /// Create a new `PendingRing` with a custom capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    /// Push a new pending entry into the ring.
    ///
    /// Called by the DualLog after a successful dual-write.
    pub fn push(&self, entry: Arc<LogEntry>) {
        let mut ring = self.inner.lock().expect("PendingRing lock poisoned");
        ring.push_back(entry);
    }

    /// Remove the entry with the given sequence number from the ring.
    ///
    /// Called after an entry has been claimed (Addressed or FinallyHandled).
    /// Returns `true` if the entry was found and removed.
    pub fn remove(&self, seq: u64) -> bool {
        let mut ring = self.inner.lock().expect("PendingRing lock poisoned");
        if let Some(pos) = ring.iter().position(|e| e.seq == seq) {
            ring.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get the next pending entry (status == Pending) without removing it.
    ///
    /// Used by the Primary Engine to find work. Returns a clone of the `Arc`.
    pub fn next_pending(&self) -> Option<Arc<LogEntry>> {
        let ring = self.inner.lock().expect("PendingRing lock poisoned");
        ring.iter()
            .find(|e| e.load_status() == OrderStatus::Pending)
            .cloned()
    }

    /// Return a snapshot of all entries currently in the ring.
    ///
    /// Used by the Sorter to scan all in-flight orders. The returned `Vec`
    /// is a snapshot — entries may change status between snapshot and
    /// processing, which is safe because all transitions use CAS.
    pub fn snapshot(&self) -> Vec<Arc<LogEntry>> {
        let ring = self.inner.lock().expect("PendingRing lock poisoned");
        ring.iter().cloned().collect()
    }

    /// Number of entries currently in the ring.
    pub fn len(&self) -> usize {
        let ring = self.inner.lock().expect("PendingRing lock poisoned");
        ring.len()
    }

    /// Returns `true` if the ring has no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all entries whose status is terminal (Addressed or FinallyHandled).
    ///
    /// Called periodically by the Sorter to keep the ring small.
    /// Returns the number of entries removed.
    pub fn gc_terminal(&self) -> usize {
        let mut ring = self.inner.lock().expect("PendingRing lock poisoned");
        let before = ring.len();
        ring.retain(|e| !e.load_status().is_terminal());
        before - ring.len()
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
}
