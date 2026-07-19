// Unit tests for the matching logic
//! Unit tests for the order book: price-time priority, partial fills,
//! full fills, cancels, IOC, and best-bid/ask tracking.

use core_types::{
    commands::{InboundCommand, OrderType, SequencedCommand, TimeInForce},
    events::{EngineEvent, RejectReason},
    AccountId, OrderId, Price, Qty, Side, Symbol,
};
use order_book::{book::BookConfig, OrderBook};

// ── Helpers ──────────────────────────────────────────────────────────────────

const SYM: Symbol = Symbol(0);
const ACCT_A: AccountId = AccountId(1);
const ACCT_B: AccountId = AccountId(2);

/// Create a book with 1 000 ticks starting at price 100_00 (scaled by 1e2 for
/// test readability), and room for 1 024 open orders.
fn make_book() -> OrderBook {
    OrderBook::new(BookConfig {
        symbol: SYM,
        tick_floor: Price(10000),
        num_ticks: 1000,
        arena_capacity: 1024,
    })
}

fn seq_cmd(seq: u64, cmd: InboundCommand) -> SequencedCommand {
    SequencedCommand {
        seq,
        ts_ns: seq * 1_000,
        cmd,
    }
}

fn new_limit(seq: u64, account: AccountId, side: Side, price: i64, qty: u64) -> SequencedCommand {
    seq_cmd(
        seq,
        InboundCommand::NewOrder {
            account,
            client_order_id: core_types::ClientOrderId::new(0),
            symbol: SYM,
            side,
            price: Price(price),
            qty: Qty(qty),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        },
    )
}

fn new_ioc(seq: u64, account: AccountId, side: Side, price: i64, qty: u64) -> SequencedCommand {
    seq_cmd(
        seq,
        InboundCommand::NewOrder {
            account,
            client_order_id: core_types::ClientOrderId::new(0),
            symbol: SYM,
            side,
            price: Price(price),
            qty: Qty(qty),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Ioc,
        },
    )
}

fn cancel(seq: u64, account: AccountId, order_id: u64) -> SequencedCommand {
    seq_cmd(
        seq,
        InboundCommand::Cancel {
            account,
            order_id: OrderId(order_id),
        },
    )
}

fn trades(events: &[EngineEvent]) -> Vec<&EngineEvent> {
    events
        .iter()
        .filter(|e| matches!(e, EngineEvent::Trade { .. }))
        .collect()
}

fn accepted(events: &[EngineEvent]) -> Vec<&EngineEvent> {
    events
        .iter()
        .filter(|e| matches!(e, EngineEvent::Accepted { .. }))
        .collect()
}

fn rejected(events: &[EngineEvent]) -> Vec<&EngineEvent> {
    events
        .iter()
        .filter(|e| matches!(e, EngineEvent::Rejected { .. }))
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn no_cross_rests_on_book() {
    let mut book = make_book();

    // Buy at 100 — no asks, should rest.
    let evs = book
        .apply(new_limit(1, ACCT_A, Side::Buy, 10000, 10))
        .into_vec();
    assert_eq!(accepted(&evs).len(), 1, "should accept");
    assert_eq!(trades(&evs).len(), 0, "no fills");
    assert_eq!(book.best_bid(), Some(Price(10000)));
    assert_eq!(book.best_ask(), None);
    assert_eq!(book.open_order_count(), 1);
}

#[test]
fn full_fill_crossing_order() {
    let mut book = make_book();

    // Resting sell at 10010.
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 5));

    // Buy at 10010 crosses fully.
    let evs = book
        .apply(new_limit(2, ACCT_B, Side::Buy, 10010, 5))
        .into_vec();

    let t = trades(&evs);
    assert_eq!(t.len(), 1, "exactly one trade");
    if let EngineEvent::Trade {
        qty,
        price,
        maker_acct,
        taker_acct,
        ..
    } = t[0]
    {
        assert_eq!(qty.0, 5);
        assert_eq!(price.0, 10010); // fill at maker price
        assert_eq!(*maker_acct, ACCT_A);
        assert_eq!(*taker_acct, ACCT_B);
    }

    assert_eq!(book.open_order_count(), 0, "both sides consumed");
    assert_eq!(book.best_ask(), None);
    assert_eq!(book.best_bid(), None);
}

#[test]
fn partial_fill_aggressor_rests_remainder() {
    let mut book = make_book();

    // Resting sell 3 @ 10010.
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 3));

    // Buy 10 @ 10010 — 3 fill, 7 rest.
    let evs = book
        .apply(new_limit(2, ACCT_B, Side::Buy, 10010, 10))
        .into_vec();

    let t = trades(&evs);
    assert_eq!(t.len(), 1);
    if let EngineEvent::Trade { qty, .. } = t[0] {
        assert_eq!(qty.0, 3);
    }

    // Maker fully consumed; taker rested with 7 remaining.
    assert_eq!(book.open_order_count(), 1);
    assert_eq!(book.best_bid(), Some(Price(10010)));
    assert_eq!(book.best_ask(), None);
    assert_eq!(book.bid_qty_at(Price(10010)).0, 7);
}

#[test]
fn partial_fill_maker_leaves_remainder() {
    let mut book = make_book();

    // Resting sell 10 @ 10010.
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 10));

    // Buy 3 @ 10010 — fully fills the taker, maker has 7 left.
    let evs = book
        .apply(new_limit(2, ACCT_B, Side::Buy, 10010, 3))
        .into_vec();

    let t = trades(&evs);
    assert_eq!(t.len(), 1);
    if let EngineEvent::Trade { qty, .. } = t[0] {
        assert_eq!(qty.0, 3);
    }

    assert_eq!(book.open_order_count(), 1);
    assert_eq!(book.best_ask(), Some(Price(10010)));
    assert_eq!(book.ask_qty_at(Price(10010)).0, 7);
}

#[test]
fn price_time_priority_fifo_within_level() {
    let mut book = make_book();

    // Three resting sells at the same price, placed in order A, B, C.
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 1));
    book.apply(new_limit(2, ACCT_B, Side::Sell, 10010, 1));
    book.apply(new_limit(3, AccountId(3), Side::Sell, 10010, 1));

    // Buy 2 — should hit orders seq=1 (A) then seq=2 (B) in FIFO order.
    let evs = book
        .apply(new_limit(4, AccountId(4), Side::Buy, 10010, 2))
        .into_vec();
    let t = trades(&evs);
    assert_eq!(t.len(), 2);

    if let EngineEvent::Trade {
        maker_order,
        maker_acct,
        ..
    } = t[0]
    {
        assert_eq!(maker_order.0, 1, "first fill should be seq=1 (ACCT_A)");
        assert_eq!(*maker_acct, ACCT_A);
    }
    if let EngineEvent::Trade {
        maker_order,
        maker_acct,
        ..
    } = t[1]
    {
        assert_eq!(maker_order.0, 2, "second fill should be seq=2 (ACCT_B)");
        assert_eq!(*maker_acct, ACCT_B);
    }

    // seq=3 (AccountId(3)) still resting.
    assert_eq!(book.open_order_count(), 1);
}

#[test]
fn best_price_priority_across_levels() {
    let mut book = make_book();

    // Asks at three prices; buyer should fill the best (lowest) ask first.
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10020, 5));
    book.apply(new_limit(2, ACCT_A, Side::Sell, 10010, 5)); // best ask
    book.apply(new_limit(3, ACCT_A, Side::Sell, 10030, 5));

    assert_eq!(book.best_ask(), Some(Price(10010)));

    // Buy at 10050 — crosses all three, but we only buy 5 so only hits 10010.
    let evs = book
        .apply(new_limit(4, ACCT_B, Side::Buy, 10050, 5))
        .into_vec();
    let t = trades(&evs);
    assert_eq!(t.len(), 1);
    if let EngineEvent::Trade { price, qty, .. } = t[0] {
        assert_eq!(price.0, 10010);
        assert_eq!(qty.0, 5);
    }

    assert_eq!(book.best_ask(), Some(Price(10020)));
}

#[test]
fn self_trade_cancels_resting_order_instead_of_matching() {
    let mut book = make_book();
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 5));
    let evs = book
        .apply(new_limit(2, ACCT_A, Side::Buy, 10010, 5))
        .into_vec();

    assert!(
        trades(&evs).is_empty(),
        "self-trade must not produce a Trade event"
    );
    assert!(
        evs.iter()
            .any(|e| matches!(e, EngineEvent::Cancelled { order_id, .. } if order_id.0 == 1)),
        "resting order from the same account must be cancelled"
    );
    assert_eq!(book.best_ask(), None);
    assert_eq!(book.best_bid(), Some(Price(10010)));
    assert_eq!(book.open_order_count(), 1);
}

#[test]
fn self_trade_prevention_skips_only_same_account_then_matches_next() {
    let mut book = make_book();
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 5));
    book.apply(new_limit(2, ACCT_B, Side::Sell, 10010, 5));

    let evs = book
        .apply(new_limit(3, ACCT_A, Side::Buy, 10010, 5))
        .into_vec();

    let t = trades(&evs);
    assert_eq!(
        t.len(),
        1,
        "should match against ACCT_B after skipping ACCT_A's own order"
    );
    if let EngineEvent::Trade {
        maker_acct,
        taker_acct,
        qty,
        ..
    } = t[0]
    {
        assert_eq!(*maker_acct, ACCT_B);
        assert_eq!(*taker_acct, ACCT_A);
        assert_eq!(qty.0, 5);
    }
    assert!(
        evs.iter()
            .any(|e| matches!(e, EngineEvent::Cancelled { order_id, .. } if order_id.0 == 1)),
        "ACCT_A's own resting order must still be cancelled along the way"
    );
    assert_eq!(
        book.open_order_count(),
        0,
        "both real orders consumed, self-trade order cancelled"
    );
}

#[test]
fn cancel_resting_order() {
    let mut book = make_book();

    book.apply(new_limit(1, ACCT_A, Side::Buy, 10000, 10));
    assert_eq!(book.open_order_count(), 1);
    assert_eq!(book.best_bid(), Some(Price(10000)));

    let evs = book
        .apply(cancel(2, ACCT_A, 1 /*order_id == seq*/))
        .into_vec();
    assert!(evs
        .iter()
        .any(|e| matches!(e, EngineEvent::Cancelled { order_id, .. } if order_id.0 == 1)));

    assert_eq!(book.open_order_count(), 0);
    assert_eq!(book.best_bid(), None);
}

#[test]
fn cancel_wrong_account_rejected() {
    let mut book = make_book();

    book.apply(new_limit(1, ACCT_A, Side::Buy, 10000, 10));

    // ACCT_B tries to cancel ACCT_A's order.
    let evs = book.apply(cancel(2, ACCT_B, 1)).into_vec();
    let r = rejected(&evs);
    assert_eq!(r.len(), 1);
    if let EngineEvent::Rejected { reason, .. } = r[0] {
        assert_eq!(*reason, RejectReason::WrongAccount);
    }

    // Order should still be on the book.
    assert_eq!(book.open_order_count(), 1);
}

#[test]
fn cancel_unknown_order_rejected() {
    let mut book = make_book();

    let evs = book.apply(cancel(1, ACCT_A, 9999)).into_vec();
    let r = rejected(&evs);
    assert_eq!(r.len(), 1);
    if let EngineEvent::Rejected { reason, .. } = r[0] {
        assert_eq!(*reason, RejectReason::OrderNotFound);
    }
}

#[test]
fn cancel_updates_best_bid_to_next_level() {
    let mut book = make_book();

    book.apply(new_limit(1, ACCT_A, Side::Buy, 10010, 5)); // best
    book.apply(new_limit(2, ACCT_A, Side::Buy, 10000, 5)); // second best

    assert_eq!(book.best_bid(), Some(Price(10010)));

    book.apply(cancel(3, ACCT_A, 1));
    assert_eq!(
        book.best_bid(),
        Some(Price(10000)),
        "should fall back to 10000"
    );
}

#[test]
fn ioc_full_fill() {
    let mut book = make_book();

    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 5));
    let evs = book
        .apply(new_ioc(2, ACCT_B, Side::Buy, 10010, 5))
        .into_vec();

    assert_eq!(trades(&evs).len(), 1);
    assert_eq!(book.open_order_count(), 0);
}

#[test]
fn ioc_partial_fill_remainder_cancelled() {
    let mut book = make_book();

    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 3));

    // IOC buy 10 — only 3 match, remaining 7 must be cancelled (not rested).
    let evs = book
        .apply(new_ioc(2, ACCT_B, Side::Buy, 10010, 10))
        .into_vec();
    assert_eq!(trades(&evs).len(), 1);
    assert!(evs
        .iter()
        .any(|e| matches!(e, EngineEvent::Cancelled { .. })));

    // Taker should NOT be on the book.
    assert_eq!(book.open_order_count(), 0);
}

#[test]
fn ioc_no_match_rejected() {
    let mut book = make_book();

    // No resting asks.
    let evs = book
        .apply(new_ioc(1, ACCT_A, Side::Buy, 10010, 5))
        .into_vec();

    assert_eq!(trades(&evs).len(), 0);
    let r = rejected(&evs);
    assert_eq!(r.len(), 1);
    if let EngineEvent::Rejected { reason, .. } = r[0] {
        assert_eq!(*reason, RejectReason::IocNoMatch);
    }
    assert_eq!(book.open_order_count(), 0);
}

#[test]
fn invalid_qty_rejected() {
    let mut book = make_book();

    let evs = book
        .apply(new_limit(1, ACCT_A, Side::Buy, 10000, 0))
        .into_vec();
    assert_eq!(rejected(&evs).len(), 1);
    if let EngineEvent::Rejected { reason, .. } = &evs[0] {
        assert_eq!(*reason, RejectReason::InvalidQty);
    }
}

#[test]
fn price_out_of_range_rejected() {
    let mut book = make_book();

    // Below tick_floor.
    let evs = book
        .apply(new_limit(1, ACCT_A, Side::Buy, 5000, 10))
        .into_vec();
    assert_eq!(rejected(&evs).len(), 1);
}

#[test]
fn sweep_multiple_levels() {
    let mut book = make_book();

    // Three ask levels: 5 @ 10010, 5 @ 10020, 5 @ 10030.
    book.apply(new_limit(1, ACCT_A, Side::Sell, 10010, 5));
    book.apply(new_limit(2, ACCT_A, Side::Sell, 10020, 5));
    book.apply(new_limit(3, ACCT_A, Side::Sell, 10030, 5));

    // Aggressive buy for 12 units at 10050 — sweeps 10010 (5) + 10020 (5)
    // and partially fills 10030 (2 of 5).
    let evs = book
        .apply(new_limit(4, ACCT_B, Side::Buy, 10050, 12))
        .into_vec();

    let t = trades(&evs);
    assert_eq!(t.len(), 3, "three fill events");

    let total_filled: u64 = t
        .iter()
        .map(|e| {
            if let EngineEvent::Trade { qty, .. } = e {
                qty.0
            } else {
                0
            }
        })
        .sum();
    assert_eq!(total_filled, 12);

    // Remaining ask at 10030 with 3 qty.
    assert_eq!(book.best_ask(), Some(Price(10030)));
    assert_eq!(book.ask_qty_at(Price(10030)).0, 3);
    // Buyer fully consumed — not on the book.
    assert_eq!(book.best_bid(), None);
}

#[test]
fn book_top_event_emitted_after_resting() {
    let mut book = make_book();

    let evs = book
        .apply(new_limit(1, ACCT_A, Side::Buy, 10000, 5))
        .into_vec();
    assert!(
        evs.iter().any(|e| matches!(
            e,
            EngineEvent::BookTop {
                bid: Some(Price(10000)),
                ask: None,
                ..
            }
        )),
        "BookTop should reflect new best bid"
    );
}
