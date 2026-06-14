// Order struct and representation
use core_types::{OrderId, AccountId, Qty, Side};
use slotmap::new_key_type;

new_key_type! {
    /// Generational handle into the SlotMap arena.
    /// Using a generational key (rather than a raw index or pointer) makes
    /// double-cancel / use-after-free structurally impossible without `unsafe`.
    pub struct OrderKey;
}

/// An order resting on the book.
///
/// Orders are stored inside `OrderBook::arena` and linked together at each
/// price level via an intrusive doubly-linked FIFO queue using `OrderKey`
/// handles rather than raw pointers.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    /// Exchange-assigned order identifier (from the Sequencer).
    pub id: OrderId,
    /// Owning account — used by risk checks and fill events.
    pub account: AccountId,
    /// Quantity still unfilled.
    pub qty_remaining: Qty,
    /// Side of the market.
    pub side: Side,
    /// Price at which this order was placed.
    pub price: core_types::Price,
    /// Global sequence number assigned by the Sequencer — used for FIFO
    /// ordering within a price level (lower seq = higher time priority).
    pub seq: u64,

    // ── Intrusive doubly-linked list links within the price level ──────────
    /// Next order in the FIFO queue at this price level (toward the tail).
    pub(crate) next: Option<OrderKey>,
    /// Previous order in the FIFO queue at this price level (toward the head).
    pub(crate) prev: Option<OrderKey>,
}

impl RestingOrder {
    pub fn new(
        id: OrderId,
        account: AccountId,
        qty: Qty,
        side: Side,
        price: core_types::Price,
        seq: u64,
    ) -> Self {
        Self {
            id,
            account,
            qty_remaining: qty,
            side,
            price,
            seq,
            next: None,
            prev: None,
        }
    }
}