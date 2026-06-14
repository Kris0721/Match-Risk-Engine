// Application of commands (e.g., limit, cancel) to the order book
//! Pure state-transition layer for `OrderBook`.
//!
//! `OrderBook::apply()` is the single entry point called by the matching
//! engine loop.  It is a **pure function** in the architectural sense:
//!
//! ```text
//! (OrderBook, SequencedCommand) → (OrderBook, Events)
//! ```
//!
//! In practice the book is mutated in-place (no clone), and events are
//! collected into a `SmallVec` to avoid heap allocation for the common case
//! (≤ 8 fill events per aggressor order).
//!
//! # Allocations
//! None on the hot path once the arena is warmed up.

use smallvec::SmallVec;

use core_types::{
    commands::{InboundCommand, OrderType, SequencedCommand},
    events::{EngineEvent, RejectReason},
    AccountId, OrderId, Price, Qty, Side, Symbol,
};

use crate::{
    book::OrderBook,
    order::{OrderKey, RestingOrder},
};

/// Errors that can occur during a book operation.
/// These map to `RejectReason` variants in the event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookError {
    /// Price is outside the ladder's configured range.
    PriceOutOfRange(Price),
    /// Quantity is zero or negative.
    InvalidQty(Qty),
    /// Cancel referred to an unknown `OrderId`.
    OrderNotFound(OrderId),
    /// Cancel referred to an order owned by a different account.
    WrongAccount { order_id: OrderId, expected: AccountId },
    /// Arena is at capacity — order cannot be accepted.
    ArenaFull,
}

/// Up to 8 events inline before spilling to the heap.
/// For normal order flow (1 fill or 1 accept) this is always stack-resident.
pub type EventVec = SmallVec<[EngineEvent; 8]>;

impl OrderBook {
    /// Apply a sequenced command and return the resulting events.
    ///
    /// This is the **only** public mutation entry point.  All matching logic
    /// flows through here so the book remains a pure function of its input log.
    pub fn apply(&mut self, cmd: SequencedCommand) -> EventVec {
        match cmd.cmd {
            InboundCommand::NewOrder {
                account,
                symbol,
                side,
                price,
                qty,
                order_type,
            } => self.apply_new_order(cmd.seq, cmd.ts_ns, account, symbol, side, price, qty, order_type),

            InboundCommand::Cancel { account, order_id } => {
                self.apply_cancel(cmd.seq, account, order_id)
            }

            // Liquidate is handled at a higher level (matching-engine crate)
            // and converted into a series of Cancel commands before reaching
            // the book.  We emit nothing here.
            InboundCommand::Liquidate { .. } | InboundCommand::FreezeAccount { .. } => {
                SmallVec::new()
            }
        }
    }

    // ── New Order ─────────────────────────────────────────────────────────

    fn apply_new_order(
        &mut self,
        seq: u64,
        ts_ns: u64,
        account: AccountId,
        symbol: Symbol,
        side: Side,
        price: Price,
        qty: Qty,
        order_type: OrderType,
    ) -> EventVec {
        let mut events: EventVec = SmallVec::new();

        // ── Validate ──────────────────────────────────────────────────────
        if qty.0 <= 0 {
            let order_id = OrderId(seq); // seq is used as a provisional id
            events.push(EngineEvent::Rejected {
                seq,
                order_id,
                reason: RejectReason::InvalidQty,
            });
            return events;
        }

        let Some(price_idx) = self.price_to_idx(price) else {
            let order_id = OrderId(seq);
            events.push(EngineEvent::Rejected {
                seq,
                order_id,
                reason: RejectReason::PriceOutOfRange,
            });
            return events;
        };

        // Assign the order its identity.  The Sequencer guarantees `seq` is
        // globally unique and monotonically increasing.
        let order_id = OrderId(seq);

        // ── Match against the opposite side ───────────────────────────────
        let mut qty_remaining = qty;

        qty_remaining =
            self.match_against_book(seq, ts_ns, symbol, side, price, qty_remaining, order_id, account, &mut events);

        if qty_remaining.0 == 0 {
            // Fully filled — no resting order to add.
            // Emit a BookTop update for the side we just consumed into.
            events.push(self.book_top_event(seq, symbol));
            return events;
        }

        // ── IOC: cancel the unfilled remainder immediately ─────────────────
        if order_type == OrderType::ImmediateOrCancel {
            if qty_remaining < qty {
                // Partially filled — already emitted Trade events above.
            } else {
                // Zero fills — emit a Rejected (unfilled IOC).
                events.push(EngineEvent::Rejected {
                    seq,
                    order_id,
                    reason: RejectReason::IocNoMatch,
                });
            }
            if qty_remaining.0 < qty.0 {
                events.push(EngineEvent::Cancelled { seq, order_id });
            }
            events.push(self.book_top_event(seq, symbol));
            return events;
        }

        // ── Limit: rest the unfilled remainder ────────────────────────────
        if self.arena.len() >= self.arena.capacity() {
            // Arena full — reject rather than allocate.
            events.push(EngineEvent::Rejected {
                seq,
                order_id,
                reason: RejectReason::ArenaFull,
            });
            return events;
        }

        let resting = RestingOrder::new(order_id, account, qty_remaining, side, price, seq);
        let key: OrderKey = self.arena.insert(resting);
        self.id_to_key.insert(order_id, key);

        // Link into the price level FIFO.
        self.level_mut(side, price_idx).push_back(key, &mut self.arena);

        // Update best-bid / best-ask.
        match side {
            Side::Buy  => self.refresh_best_bid_up(price_idx),
            Side::Sell => self.refresh_best_ask_down(price_idx),
        }

        events.push(EngineEvent::Accepted { seq, order_id, ts_ns });
        events.push(self.book_top_event(seq, symbol));
        events
    }

    // ── Matching loop ─────────────────────────────────────────────────────

    /// Match an aggressor order against resting orders on the opposite side.
    ///
    /// Returns the remaining unfilled quantity.
    fn match_against_book(
        &mut self,
        seq: u64,
        ts_ns: u64,
        symbol: Symbol,
        aggressor_side: Side,
        aggressor_price: Price,
        mut qty_remaining: Qty,
        aggressor_order_id: OrderId,
        aggressor_account: AccountId,
        events: &mut EventVec,
    ) -> Qty {
        loop {
            if qty_remaining.0 == 0 {
                break;
            }

            // Find the best resting level on the opposite side.
            let (best_idx, crosses) = match aggressor_side {
                Side::Buy => {
                    let Some(idx) = self.best_ask_idx else { break };
                    let ask_price = self.idx_to_price(idx);
                    // A buy crosses if its limit price ≥ best ask.
                    (idx, aggressor_price.0 >= ask_price.0)
                }
                Side::Sell => {
                    let Some(idx) = self.best_bid_idx else { break };
                    let bid_price = self.idx_to_price(idx);
                    // A sell crosses if its limit price ≤ best bid.
                    (idx, aggressor_price.0 <= bid_price.0)
                }
            };

            if !crosses {
                break;
            }

            let maker_side = aggressor_side.opposite();
            let level = match self.get_level_mut(maker_side, best_idx) {
                Some(l) => l,
                None => break,
            };

            let maker_key = match level.peek_head() {
                Some(k) => k,
                None => {
                    // Level exists but is empty — shouldn't happen but handle gracefully.
                    self.clear_empty_best(maker_side, best_idx);
                    continue;
                }
            };

            // Extract maker info before we mutate the level.
            let (maker_order_id, maker_account, maker_price) = {
                let maker = &self.arena[maker_key];
                (maker.id, maker.account, maker.price)
            };

            let maker_qty = self.arena[maker_key].qty_remaining;
            let fill_qty = if qty_remaining.0 < maker_qty.0 {
                qty_remaining
            } else {
                maker_qty
            };

            // Apply the fill to the resting (maker) side.
            let maker_fully_filled = self.fill_level_head(maker_side, best_idx, fill_qty);

            qty_remaining = Qty(qty_remaining.0 - fill_qty.0);

            events.push(EngineEvent::Trade {
                seq,
                symbol,
                price: maker_price,
                qty: fill_qty,
                ts_ns,
                maker_order: maker_order_id,
                taker_order: aggressor_order_id,
                maker_acct: maker_account,
                taker_acct: aggressor_account,
            });

            if maker_fully_filled {
                // Remove from id→key map.
                self.id_to_key.remove(&maker_order_id);
                // If the level is now empty, advance best pointer.
                if self.is_level_empty(maker_side, best_idx) {
                    self.clear_empty_best(maker_side, best_idx);
                }
            }
        }

        qty_remaining
    }

    // ── Cancel ────────────────────────────────────────────────────────────

    fn apply_cancel(&mut self, seq: u64, account: AccountId, order_id: OrderId) -> EventVec {
        let mut events: EventVec = SmallVec::new();

        let key = match self.id_to_key.remove(&order_id) {
            Some(k) => k,
            None => {
                // Unknown order — could be a duplicate cancel or a race with a fill.
                events.push(EngineEvent::Rejected {
                    seq,
                    order_id,
                    reason: RejectReason::OrderNotFound,
                });
                return events;
            }
        };

        let (side, price) = {
            let order = &self.arena[key];
            if order.account != account {
                // Re-insert the key before returning.
                self.id_to_key.insert(order_id, key);
                events.push(EngineEvent::Rejected {
                    seq,
                    order_id,
                    reason: RejectReason::WrongAccount,
                });
                return events;
            }
            (order.side, order.price)
        };

        let price_idx = match self.price_to_idx(price) {
            Some(i) => i,
            None => {
                // Should never happen — we validated on insert.
                return events;
            }
        };

        let ladder = match side {
            Side::Buy  => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if let Some(level) = &mut ladder[price_idx] {
            level.remove(key, &mut self.arena);
            if level.is_empty() {
                // Don't reallocate — just leave the slot; the best-pointer
                // scan will skip it.
                match side {
                    Side::Buy  => {
                        if self.best_bid_idx == Some(price_idx) {
                            self.scan_best_bid_down();
                        }
                    }
                    Side::Sell => {
                        if self.best_ask_idx == Some(price_idx) {
                            self.scan_best_ask_up();
                        }
                    }
                }
            }
        }

        events.push(EngineEvent::Cancelled { seq, order_id });
        events
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    fn book_top_event(&self, seq: u64, symbol: Symbol) -> EngineEvent {
        EngineEvent::BookTop {
            seq,
            symbol,
            bid: self.best_bid(),
            ask: self.best_ask(),
        }
    }

    fn get_level_mut(&mut self, side: Side, idx: usize) -> Option<&mut crate::level::PriceLevel> {
        match side {
            Side::Buy  => self.bids[idx].as_mut(),
            Side::Sell => self.asks[idx].as_mut(),
        }
    }

    /// Fill the head of `side`'s level at `idx` by `fill_qty`.
    /// Returns `true` if the head order was fully filled.
    fn fill_level_head(&mut self, side: Side, idx: usize, fill_qty: Qty) -> bool {
        let level = match side {
            Side::Buy  => self.bids[idx].as_mut(),
            Side::Sell => self.asks[idx].as_mut(),
        };
        match level {
            Some(l) => l.fill_head(fill_qty, &mut self.arena),
            None => false,
        }
    }

    fn is_level_empty(&self, side: Side, idx: usize) -> bool {
        let level = match side {
            Side::Buy  => &self.bids[idx],
            Side::Sell => &self.asks[idx],
        };
        level.as_ref().map(|l| l.is_empty()).unwrap_or(true)
    }

    /// Mark a best-pointer as needing a rescan after its level became empty.
    fn clear_empty_best(&mut self, side: Side, idx: usize) {
        match side {
            Side::Buy => {
                if self.best_bid_idx == Some(idx) {
                    self.scan_best_bid_down();
                }
            }
            Side::Sell => {
                if self.best_ask_idx == Some(idx) {
                    self.scan_best_ask_up();
                }
            }
        }
    }
}