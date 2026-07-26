// Criterion benchmarks for the OrderBook hot path.
//
// Run with: cargo bench -p order-book
// HTML report lands in target/criterion/report/index.html

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use core_types::{
    commands::{InboundCommand, OrderType, SequencedCommand, TimeInForce},
    AccountId, ClientOrderId, Price, Qty, Side, Symbol,
};
use order_book::{book::BookConfig, OrderBook};

const SYM: Symbol = Symbol(0);

fn make_book(capacity: usize) -> OrderBook {
    OrderBook::new(BookConfig {
        symbol: SYM,
        tick_floor: Price(1),
        num_ticks: 200_000,
        arena_capacity: capacity,
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
            client_order_id: ClientOrderId::new(0),
            symbol: SYM,
            side,
            price: Price(price),
            qty: Qty(qty),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        },
    )
}

/// A resting order that never crosses — pure "add to book" cost with
/// no matching work, so this isolates arena-insert + FIFO-link overhead.
fn bench_add_resting_order(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_resting_order");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_insert", |b| {
        b.iter_batched(
            || make_book(100_000),
            |mut book| {
                let cmd = new_limit(1, AccountId(1), Side::Buy, 100_00, 10);
                black_box(book.apply(cmd));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Pure insert cost with construction hoisted out of the measurement loop.
/// Reuses one pre-warmed book and inserts at a fresh, never-touched price
/// tick each iteration (so pages are already resident — no allocation or
/// page-fault noise — while still exercising a real empty->occupied
/// PriceLevel transition, not a warm no-op).
fn bench_add_resting_order_warm(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_resting_order_warm");
    group.throughput(Throughput::Elements(1));

    let mut book = make_book(100_000);
    let mut next_tick = 0i64;

    group.bench_function("single_insert_preallocated_book", |b| {
        b.iter(|| {
            let cmd = new_limit(next_tick as u64 + 1, AccountId(1), Side::Buy, next_tick, 10);
            black_box(book.apply(cmd));
            next_tick += 1; // next iteration hits a fresh, still-empty tick
        });
    });
    group.finish();
}

/// Aggressor order that fully crosses a single resting maker at the
/// touch — the minimal matching-loop iteration (one fill, no self-trade
/// branch, no partial-fill bookkeeping beyond one level).
fn bench_single_fill_cross(c: &mut Criterion) {
    let mut group = c.benchmark_group("match_single_fill");
    group.throughput(Throughput::Elements(1));

    group.bench_function("cross_one_resting_order", |b| {
        b.iter_batched(
            || {
                let mut book = make_book(100_000);
                // Resting sell at 100_00 from a different account.
                book.apply(new_limit(1, AccountId(1), Side::Sell, 100_00, 10));
                book
            },
            |mut book| {
                // Buy crosses it fully.
                let cmd = new_limit(2, AccountId(2), Side::Buy, 100_00, 10);
                black_box(book.apply(cmd));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Aggressor that walks through N resting price levels before it's
/// filled — models a large order sweeping the book, which is the
/// worst case for match_against_book's loop.
fn bench_sweep_multiple_levels(c: &mut Criterion) {
    let mut group = c.benchmark_group("match_sweep_levels");
    for depth in [1usize, 10, 50, 200] {
        group.throughput(Throughput::Elements(depth as u64));
        group.bench_with_input(format!("levels_{depth}"), &depth, |b, &depth| {
            b.iter_batched(
                || {
                    let mut book = make_book(100_000);
                    // One resting sell per tick, ascending price, qty 1 each,
                    // all from account 1 so nothing self-trades.
                    for i in 0..depth {
                        book.apply(new_limit(
                            i as u64 + 1,
                            AccountId(1),
                            Side::Sell,
                            100_00 + i as i64,
                            1,
                        ));
                    }
                    book
                },
                |mut book| {
                    // Aggressive buy at a price that crosses all `depth` levels.
                    let cmd = new_limit(
                        1_000_000,
                        AccountId(2),
                        Side::Buy,
                        100_00 + depth as i64,
                        depth as u64,
                    );
                    black_box(book.apply(cmd));
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Same-account crossing order — measures the self-trade-prevention
/// branch (cancel-resting-then-continue) instead of a normal fill.
fn bench_self_trade_prevention(c: &mut Criterion) {
    let mut group = c.benchmark_group("self_trade_prevention");
    group.throughput(Throughput::Elements(1));

    group.bench_function("cancel_resting_then_match_next", |b| {
        b.iter_batched(
            || {
                let mut book = make_book(100_000);
                // Own resting order at the touch (will be cancelled by STP)...
                book.apply(new_limit(1, AccountId(1), Side::Sell, 100_00, 10));
                // ...then a real counterparty order just behind it.
                book.apply(new_limit(2, AccountId(3), Side::Sell, 100_00, 10));
                book
            },
            |mut book| {
                let cmd = new_limit(3, AccountId(1), Side::Buy, 100_00, 10);
                black_box(book.apply(cmd));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Sustained throughput: alternating buy/sell limit orders around a
/// stable mid, roughly modelling steady two-sided flow. Reports both
/// mean latency/op (via throughput) and ops/sec.
fn bench_sustained_flow(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_flow");
    let n = 10_000u64;
    group.throughput(Throughput::Elements(n));

    group.bench_function("alternating_10k_orders", |b| {
        b.iter_batched(
            || make_book(100_000),
            |mut book| {
                for i in 0..n {
                    let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                    let price = 100_00 + (i as i64 % 5) - 2; // small spread churn
                    let cmd = new_limit(i + 1, AccountId((i % 50) + 1), side, price, 10);
                    black_box(book.apply(cmd));
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_add_resting_order,
    bench_add_resting_order_warm,
    bench_single_fill_cross,
    bench_sweep_multiple_levels,
    bench_self_trade_prevention,
    bench_sustained_flow,
);
criterion_main!(benches);
