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
    commands::{InboundCommand, OrderType, SequencedCommand, TimeInForce},
    events::{EngineEvent, RejectReason},
    AccountId, ClientOrderId, OrderId, Price, Qty, Side, Symbol,
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
    WrongAccount {
        order_id: OrderId,
        expected: AccountId,
    },
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
                client_order_id,
                symbol,
                side,
                price,
                qty,
                order_type,
                time_in_force,
            } => self.apply_new_order(
                cmd.seq,
                cmd.ts_ns,
                account,
                client_order_id,
                symbol,
                side,
                price,
                qty,
                order_type,
                time_in_force,
            ),

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
        client_order_id: ClientOrderId,
        symbol: Symbol,
        side: Side,
        price: Price,
        qty: Qty,
        order_type: OrderType,
        time_in_force: TimeInForce,
    ) -> EventVec {
        let mut events: EventVec = SmallVec::new();

        // ── Validate ──────────────────────────────────────────────────────
        if qty.0 <= 0 {
            let order_id = OrderId(seq); // seq is used as a provisional id
            events.push(EngineEvent::Rejected {
                seq,
                order_id,
                account_id: account,
                client_order_id,
                reason: RejectReason::InvalidQty,
            });
            return events;
        }

        // Market orders are not implemented. Reject explicitly rather than
        // silently matching them as a Limit order at whatever price
        // happened to be on the wire — that would be the outcome if this
        // check weren't here, since `order_type` used to be ignored.
        if order_type == OrderType::Market {
            let order_id = OrderId(seq);
            events.push(EngineEvent::Rejected {
                seq,
                order_id,
                account_id: account,
                client_order_id,
                reason: RejectReason::UnsupportedOrderType,
            });
            return events;
        }

        let Some(price_idx) = self.price_to_idx(price) else {
            let order_id = OrderId(seq);
            events.push(EngineEvent::Rejected {
                seq,
                order_id,
                account_id: account,
                client_order_id,
                reason: RejectReason::PriceOutOfRange,
            });
            return events;
        };

        // Assign the order its identity.  The Sequencer guarantees `seq` is
        // globally unique and monotonically increasing.
        let order_id = OrderId(seq);

        // ── FOK: verify full fillability before touching the book ─────────
        // A FOK order must fill completely or not at all — no partial fill,
        // no resting remainder. Unlike IOC (which matches whatever it can
        // and cancels the rest), FOK must not mutate the book at all if it
        // can't be fully satisfied right now.
        if time_in_force == TimeInForce::Fok && !self.can_fully_fill(side, price, qty, account) {
            events.push(EngineEvent::Rejected {
                seq,
                order_id,
                account_id: account,
                client_order_id,
                reason: RejectReason::FokNotFullyFillable,
            });
            return events;
        }

        // ── Match against the opposite side ───────────────────────────────
        let mut qty_remaining = qty;

        qty_remaining = self.match_against_book(
            seq,
            ts_ns,
            symbol,
            side,
            price,
            qty_remaining,
            order_id,
            account,
            &mut events,
        );

        if qty_remaining.0 == 0 {
            // Fully filled — no resting order to add.
            // Emit a BookTop update for the side we just consumed into.
            events.push(self.book_top_event(seq, symbol));
            return events;
        }

        // ── IOC / FOK: cancel the unfilled remainder immediately ───────────
        // FOK should never actually reach this branch with qty_remaining > 0
        // — `can_fully_fill` already guaranteed a complete fill above, and
        // nothing else can mutate the book between that check and this call
        // (single-threaded, no concurrent access). It's handled here anyway
        // as a defensive fallback so a FOK order can never rest partially
        // filled if that invariant were ever violated by a future change.
        if time_in_force == TimeInForce::Ioc || time_in_force == TimeInForce::Fok {
            if qty_remaining < qty {
                // Partially filled — already emitted Trade events above.
            } else {
                // Zero fills — emit a Rejected (unfilled IOC/FOK).
                events.push(EngineEvent::Rejected {
                    seq,
                    order_id,
                    account_id: account,
                    client_order_id,
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
                account_id: account,
                client_order_id,
                reason: RejectReason::ArenaFull,
            });
            return events;
        }

        let resting = RestingOrder::new(order_id, account, qty_remaining, side, price, seq);
        let key: OrderKey = self.arena.insert(resting);
        self.id_to_key.insert(order_id, key);

        // Link into the price level FIFO.
        let level_slot = match side {
            Side::Buy => &mut self.bids[price_idx],
            Side::Sell => &mut self.asks[price_idx],
        };
        let pl = level_slot.get_or_insert_with(|| crate::level::PriceLevel::new(price));
        pl.push_back(key, &mut self.arena);

        // Update best-bid / best-ask.
        match side {
            Side::Buy => self.refresh_best_bid_up(price_idx),
            Side::Sell => self.refresh_best_ask_down(price_idx),
        }

        events.push(EngineEvent::Accepted {
            seq,
            order_id,
            ts_ns,
            symbol,
            account_id: account,
            client_order_id,
            side,
            price,
            qty: qty_remaining,
        });
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

            // ── Self-trade prevention ──────────────────────────────────
            // Same account on both sides: cancel the resting (maker) order
            // instead of matching against it, then keep trying to fill the
            // incoming order against whatever's next in the book. This is the
            // only STP policy implemented — "cancel incoming" / "reject both"
            // are not, and are not silently approximated by this branch.
            if maker_account == aggressor_account {
                if let Some(cancelled_id) = self.cancel_resting_head(maker_side, best_idx) {
                    self.id_to_key.remove(&cancelled_id);
                    events.push(EngineEvent::Cancelled {
                        seq,
                        order_id: cancelled_id,
                    });
                }
                if self.is_level_empty(maker_side, best_idx) {
                    self.clear_empty_best(maker_side, best_idx);
                }
                continue;
            }

            let maker_qty = self.arena[maker_key].qty_remaining;
            let fill_qty = if qty_remaining.0 < maker_qty.0 {
                qty_remaining
            } else {
                maker_qty
            };

            // Apply the fill to the resting (maker) side.
            let maker_fully_filled = self.fill_level_head(maker_side, best_idx, fill_qty);

            qty_remaining = Qty(qty_remaining.0 - fill_qty.0);

            let maker_remaining = Qty(maker_qty.0 - fill_qty.0);
            let taker_remaining = qty_remaining;
            events.push(EngineEvent::Trade {
                seq,
                ts_ns,
                symbol,
                price: maker_price,
                qty: fill_qty,
                maker_order: maker_order_id,
                taker_order: aggressor_order_id,
                maker_order_id: maker_order_id,
                taker_order_id: aggressor_order_id,
                maker_acct: maker_account,
                taker_acct: aggressor_account,
                maker_side,
                maker_remaining_qty: maker_remaining,
                taker_remaining_qty: taker_remaining,
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

    /// Read-only check: could an order for `side`/`price`/`qty` from
    /// `account` be fully filled immediately? Used to implement FOK,
    /// which must reject outright rather than partially fill or rest.
    ///
    /// This must stay in lockstep with `match_against_book`'s crossing
    /// and self-trade-prevention logic: same-account resting orders are
    /// skipped without counting toward fillable quantity, exactly as
    /// they would be cancelled-and-skipped by STP during real matching.
    /// A mismatch here would let a FOK order either fill partially (if
    /// this over-reports fillable quantity) or reject an order that
    /// could actually have filled (if it under-reports).
    fn can_fully_fill(&self, side: Side, price: Price, qty: Qty, account: AccountId) -> bool {
        let mut remaining: u64 = qty.0;

        // A buy crosses asks, scanning from the lowest price upward; a
        // sell crosses bids, scanning from the highest price downward.
        let ladder = match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };

        let n = ladder.len();
        for step in 0..n {
            if remaining == 0 {
                break;
            }

            let idx = match side {
                Side::Buy => step,
                Side::Sell => n - 1 - step,
            };

            let level_price = self.idx_to_price(idx);
            let crosses = match side {
                Side::Buy => price.0 >= level_price.0,
                Side::Sell => price.0 <= level_price.0,
            };
            if !crosses {
                // Indices are scanned monotonically away from the touch,
                // so once a level doesn't cross, none further out will.
                break;
            }

            let Some(level) = &ladder[idx] else { continue };

            let mut cursor = level.peek_head();
            while let Some(key) = cursor {
                let order = &self.arena[key];
                if order.account != account {
                    remaining = remaining.saturating_sub(order.qty_remaining.0);
                    if remaining == 0 {
                        break;
                    }
                }
                cursor = order.next;
            }
        }
        remaining == 0
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
                    account_id: account,
                    client_order_id: ClientOrderId::new(0),
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
                    account_id: account,
                    client_order_id: ClientOrderId::new(0),
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
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        if let Some(level) = &mut ladder[price_idx] {
            level.remove(key, &mut self.arena);
            if level.is_empty() {
                // Don't reallocate — just leave the slot; the best-pointer
                // scan will skip it.
                match side {
                    Side::Buy => {
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
            Side::Buy => self.bids[idx].as_mut(),
            Side::Sell => self.asks[idx].as_mut(),
        }
    }

    /// Fill the head of `side`'s level at `idx` by `fill_qty`.
    /// Returns `true` if the head order was fully filled.
    fn fill_level_head(&mut self, side: Side, idx: usize, fill_qty: Qty) -> bool {
        let level = match side {
            Side::Buy => self.bids[idx].as_mut(),
            Side::Sell => self.asks[idx].as_mut(),
        };
        match level {
            Some(l) => l.fill_head(fill_qty, &mut self.arena),
            None => false,
        }
    }

    /// Remove the resting head order at `side`/`idx` without filling it.
    /// Used by self-trade prevention to cancel a resting order that would
    /// otherwise cross with the same account's incoming order. Returns
    /// the cancelled order's id, or `None` if the level was empty.
    fn cancel_resting_head(&mut self, side: Side, idx: usize) -> Option<OrderId> {
        let level = match side {
            Side::Buy => self.bids[idx].as_mut(),
            Side::Sell => self.asks[idx].as_mut(),
        }?;
        let head_key = level.peek_head()?;
        let removed = level.remove(head_key, &mut self.arena)?;
        Some(removed.id)
    }

    fn is_level_empty(&self, side: Side, idx: usize) -> bool {
        let level = match side {
            Side::Buy => &self.bids[idx],
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
