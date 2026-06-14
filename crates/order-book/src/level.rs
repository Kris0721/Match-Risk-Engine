// Price level in the order book
use core_types::{Price, Qty};
use slotmap::SlotMap;

use crate::order::{OrderKey, RestingOrder};

/// A single price level on the order book.
///
/// Orders at the same price are maintained in a FIFO queue implemented as an
/// intrusive doubly-linked list through `RestingOrder::next` / `::prev`.
/// This avoids any secondary allocation — all storage lives in the arena.
#[derive(Debug)]
pub struct PriceLevel {
    /// The price this level represents.
    pub price: Price,
    /// Head of the FIFO queue (oldest / highest-priority order).
    pub(crate) head: Option<OrderKey>,
    /// Tail of the FIFO queue (most-recently-added order).
    pub(crate) tail: Option<OrderKey>,
    /// Sum of `qty_remaining` for all resting orders at this level.
    /// Updated incrementally on every insert / partial-fill / full-fill.
    pub total_qty: Qty,
}

impl PriceLevel {
    pub fn new(price: Price) -> Self {
        Self {
            price,
            head: None,
            tail: None,
            total_qty: Qty(0),
        }
    }

    /// Append `key` to the tail of the FIFO queue.
    pub fn push_back(&mut self, key: OrderKey, arena: &mut SlotMap<OrderKey, RestingOrder>) {
        let order = &arena[key];
        debug_assert!(order.next.is_none());
        debug_assert!(order.prev.is_none());
        let qty = order.qty_remaining;

        match self.tail {
            None => {
                // Level was empty.
                self.head = Some(key);
                self.tail = Some(key);
            }
            Some(prev_tail) => {
                arena[prev_tail].next = Some(key);
                arena[key].prev = Some(prev_tail);
                self.tail = Some(key);
            }
        }
        self.total_qty = Qty(self.total_qty.0 + qty.0);
    }

    /// Remove an arbitrary order from the linked list by key.
    /// Used for cancels; fills are handled via `pop_head`.
    pub fn remove(
        &mut self,
        key: OrderKey,
        arena: &mut SlotMap<OrderKey, RestingOrder>,
    ) -> Option<RestingOrder> {
        // Check the key is still valid before touching it.
        if !arena.contains_key(key) {
            return None;
        }
        let order = &arena[key];
        let prev = order.prev;
        let next = order.next;
        let qty  = order.qty_remaining;

        // Stitch neighbours together.
        if let Some(p) = prev {
            arena[p].next = next;
        } else {
            // key was the head
            self.head = next;
        }
        if let Some(n) = next {
            arena[n].prev = prev;
        } else {
            // key was the tail
            self.tail = prev;
        }

        self.total_qty = Qty(self.total_qty.0 - qty.0);
        arena.remove(key)
    }

    /// Peek at the head of the FIFO queue without removing it.
    #[inline]
    pub fn peek_head(&self) -> Option<OrderKey> {
        self.head
    }

    /// Reduce `qty_remaining` on the head order.
    ///
    /// Returns `true` if the head order is fully filled and was removed from
    /// both the queue and the arena.
    pub fn fill_head(
        &mut self,
        fill_qty: Qty,
        arena: &mut SlotMap<OrderKey, RestingOrder>,
    ) -> bool {
        let head_key = match self.head {
            Some(k) => k,
            None => return false,
        };

        let order = &mut arena[head_key];
        debug_assert!(fill_qty.0 <= order.qty_remaining.0, "fill exceeds resting qty");
        order.qty_remaining = Qty(order.qty_remaining.0 - fill_qty.0);
        self.total_qty = Qty(self.total_qty.0 - fill_qty.0);

        if order.qty_remaining.0 == 0 {
            // Fully filled — remove from list and arena.
            let next = order.next;
            if let Some(n) = next {
                arena[n].prev = None;
            }
            self.head = next;
            if self.head.is_none() {
                self.tail = None;
            }
            arena.remove(head_key);
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }
}