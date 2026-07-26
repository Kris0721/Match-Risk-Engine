// End-to-end pipeline benchmark: sequencer -> matching engine -> risk
// shard, via SimHarness. This is the deterministic single-threaded
// harness (VecDeque-based, no real SPSC/thread crossing) — it measures
// the per-tick logical cost of the pipeline, NOT wall-clock cross-core
// latency. Pair with ring-buffer's spsc_bench for the transport cost
// this doesn't include.
//
// Run with: cargo bench -p sim

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use core_types::{
    AccountId, ClientOrderId, InboundCommand, OrderType, Price, Qty, Side, Symbol, TimeInForce,
};
use sim::{SimConfig, SimHarness};

fn make_harness(n_accounts: usize) -> SimHarness {
    SimHarness::new(SimConfig {
        n_symbols: 1,
        n_accounts,
        n_risk_shards: 2,
        initial_balance: 100_000_000_000_00,
        snapshot_interval: 1_000_000, // effectively disabled for this bench
        book_tick_floor: Price::ZERO,
        book_num_ticks: 4096,
    })
}

fn new_order_cmd(i: u64, account: AccountId) -> InboundCommand {
    let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
    let price = Price(2000 + (i as i64 % 5) - 2);
    InboundCommand::NewOrder {
        account,
        client_order_id: ClientOrderId::new(i),
        symbol: Symbol(0),
        side,
        price,
        qty: Qty(10),
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
    }
}

/// One full tick: sequence a single order, route it to the matching
/// engine, apply it, and drain resulting events through the risk shards.
fn bench_single_tick(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_single_tick");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sequence_match_risk", |b| {
        b.iter_batched(
            || {
                let mut h = make_harness(4);
                h.push_command(new_order_cmd(1, AccountId(1)));
                h
            },
            |mut h| {
                h.run(1);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Sustained flow: n orders across a handful of accounts on one symbol,
/// run through the full pipeline back-to-back.
fn bench_sustained_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_sustained");
    let n = 10_000u64;
    group.throughput(Throughput::Elements(n));

    group.bench_function("10k_orders_4_accounts", |b| {
        b.iter_batched(
            || {
                let mut h = make_harness(4);
                for i in 0..n {
                    let account = AccountId((i % 4) + 1);
                    h.push_command(new_order_cmd(i + 1, account));
                }
                h
            },
            |mut h| {
                h.run(n);
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

/// Same as above but with more accounts sharded across more risk shards —
/// checks whether shard count changes per-order cost.
fn bench_sustained_pipeline_many_accounts(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_sustained_wide");
    let n = 10_000u64;
    group.throughput(Throughput::Elements(n));

    group.bench_function("10k_orders_64_accounts_8_shards", |b| {
        b.iter_batched(
            || {
                let mut h = SimHarness::new(SimConfig {
                    n_symbols: 1,
                    n_accounts: 64,
                    n_risk_shards: 8,
                    initial_balance: 100_000_000_000_00,
                    snapshot_interval: 1_000_000,
                    book_tick_floor: Price::ZERO,
                    book_num_ticks: 4096,
                });
                for i in 0..n {
                    let account = AccountId((i % 64) + 1);
                    h.push_command(new_order_cmd(i + 1, account));
                }
                h
            },
            |mut h| {
                h.run(n);
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_single_tick,
    bench_sustained_pipeline,
    bench_sustained_pipeline_many_accounts,
);
criterion_main!(benches);
