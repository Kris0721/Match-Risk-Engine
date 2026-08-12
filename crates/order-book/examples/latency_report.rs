//! Standalone latency-percentile harness for the order book hot path.
//!
//! Unlike the criterion suite (`benches/matching_bench.rs`), which reports
//! mean/median/bootstrap-CI statistics tuned for regression detection,
//! this harness records the wall-clock latency of *every individual*
//! operation into an HDR histogram and reports the exact percentiles that
//! matter for a latency-sensitive system: p50, p90, p99, p99.9, p99.99,
//! and max.
//!
//! Run with:
//!   cargo run --release --example latency_report -p order-book
//!
//! Always use `--release`. A debug build's per-op latency is dominated by
//! missing inlining/bounds-check elision, not the thing being measured.
//!
//! See `LATENCY_METHODOLOGY.md` alongside this file for what each number
//! does and does not claim, and the caveats around measurement overhead.

use std::time::Instant;

use hdrhistogram::Histogram;

use core_types::{
    commands::{InboundCommand, OrderType, SequencedCommand, TimeInForce},
    AccountId, ClientOrderId, Price, Qty, Side, Symbol,
};
use order_book::{book::BookConfig, OrderBook};

const SYM: Symbol = Symbol(0);

/// Number of measured operations per phase. Chosen so p99.9 has ~100
/// samples backing it (100_000 * 0.001 = 100) — below that the tail
/// percentile is mostly noise, not signal.
const N_OPS: u64 = 200_000;

/// Iterations discarded before measurement starts, to let CPU caches,
/// branch predictor state, and the allocator settle into steady state.
const WARMUP_OPS: u64 = 20_000;

fn make_book(capacity: usize) -> OrderBook {
    OrderBook::new(BookConfig {
        symbol: SYM,
        tick_floor: Price(1),
        num_ticks: 400_000,
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

/// Pin the current thread to `core_id` if possible. Best-effort: on a
/// machine without enough cores, or if the OS call fails, this silently
/// falls back to unpinned. Cross-platform (`core_affinity`), unlike
/// production's Linux-only `matching-engine::affinity` — see methodology
/// doc for why the benchmark and production pinning paths differ.
fn try_pin_to_core(core_id: usize) -> bool {
    let cores = match core_affinity::get_core_ids() {
        Some(c) => c,
        None => return false,
    };
    match cores.get(core_id) {
        Some(core) => core_affinity::set_for_current(*core),
        None => false,
    }
}

/// Record `n` insertions of never-before-touched resting limit orders
/// (fresh price tick each time — isolates arena-insert + FIFO-link cost,
/// no matching work) into `hist`, starting from sequence `seq_start`.
fn record_inserts(book: &mut OrderBook, hist: &mut Histogram<u64>, seq_start: u64, n: u64) {
    for i in 0..n {
        let seq = seq_start + i;
        let cmd = new_limit(seq, AccountId(1), Side::Buy, seq as i64, 10);
        let t0 = Instant::now();
        let events = book.apply(cmd);
        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        std::hint::black_box(&events);
        hist.record(elapsed_ns)
            .expect("latency exceeded histogram range");
    }
}

/// Record `n` single-fill crossing matches into `hist`. Each iteration
/// places one resting sell, then crosses it fully with a buy — so what's
/// measured is `apply()` for the *aggressor* order only (the resting
/// insert immediately before it is not timed).
fn record_matches(book: &mut OrderBook, hist: &mut Histogram<u64>, seq_start: u64, n: u64) {
    for i in 0..n {
        let seq = seq_start + i * 2;
        let price = 500_000_000 + (i as i64 % 300_000); // spread inserts across ticks
        book.apply(new_limit(seq, AccountId(1), Side::Sell, price, 10));

        let cmd = new_limit(seq + 1, AccountId(2), Side::Buy, price, 10);
        let t0 = Instant::now();
        let events = book.apply(cmd);
        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        std::hint::black_box(&events);
        hist.record(elapsed_ns)
            .expect("latency exceeded histogram range");
    }
}

fn print_report(label: &str, hist: &Histogram<u64>) {
    println!("--- {label} ---");
    println!("  count:    {}", hist.len());
    println!("  min:      {:>8} ns", hist.min());
    println!("  p50:      {:>8} ns", hist.value_at_quantile(0.50));
    println!("  p90:      {:>8} ns", hist.value_at_quantile(0.90));
    println!("  p99:      {:>8} ns", hist.value_at_quantile(0.99));
    println!("  p99.9:    {:>8} ns", hist.value_at_quantile(0.999));
    println!("  p99.99:   {:>8} ns", hist.value_at_quantile(0.9999));
    println!("  max:      {:>8} ns", hist.max());
    println!();
}

/// New histogram sized for 1ns .. 10ms with 3 significant figures of
/// precision at every magnitude — enough resolution to distinguish
/// individual nanoseconds at the low end while still bucketing a page
/// fault or scheduler preemption in the tail without erroring out.
fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 10_000_000, 3).expect("valid histogram bounds")
}

fn main() {
    let pinned = try_pin_to_core(0);
    println!(
        "core pinning: {}\n",
        if pinned {
            "pinned to core 0"
        } else {
            "NOT pinned (unavailable on this machine/OS)"
        }
    );

    // ---- Cold-start phase: measure from the very first operation on a
    // freshly constructed book, no warm-up loop beforehand. This is the
    // "worst case" number: empty caches, cold branch predictor, first
    // touch of every page the arena allocated.
    {
        let mut book = make_book(2 * (N_OPS as usize));
        let mut hist = new_histogram();
        record_inserts(&mut book, &mut hist, 1, N_OPS);
        print_report("insert: COLD (no warmup)", &hist);
    }

    // ---- Warm phase: run WARMUP_OPS first and discard them, then
    // measure the next N_OPS. This is the "steady state" number — what
    // the engine looks like after it's been running for a while.
    {
        let mut book = make_book(2 * ((N_OPS + WARMUP_OPS) as usize));
        let mut warmup_hist = new_histogram(); // discarded, but apply() still run for real
        record_inserts(&mut book, &mut warmup_hist, 1, WARMUP_OPS);

        let mut hist = new_histogram();
        record_inserts(&mut book, &mut hist, WARMUP_OPS + 1, N_OPS);
        print_report("insert: WARM (after 20k discarded ops)", &hist);
    }

    // ---- Matching: cold and warm, same structure as above.
    {
        let mut book = make_book(4 * (N_OPS as usize));
        let mut hist = new_histogram();
        record_matches(&mut book, &mut hist, 1, N_OPS);
        print_report("match (single-fill cross): COLD (no warmup)", &hist);
    }
    {
        let mut book = make_book(4 * ((N_OPS + WARMUP_OPS) as usize));
        let mut warmup_hist = new_histogram();
        record_matches(&mut book, &mut warmup_hist, 1, WARMUP_OPS);

        let mut hist = new_histogram();
        record_matches(&mut book, &mut hist, WARMUP_OPS * 2 + 1, N_OPS);
        print_report(
            "match (single-fill cross): WARM (after 20k discarded ops)",
            &hist,
        );
    }

    println!(
        "NOTE: p50/mean here include ~{}ns of Instant::now() call overhead",
        instant_overhead_ns()
    );
}

/// Measure `Instant::now()`'s own call overhead by timing back-to-back
/// calls with nothing between them. Every latency number above includes
/// two of these calls' worth of overhead — reported here so the reader
/// can see how much of a sub-100ns p50 is measurement artifact rather
/// than the operation itself.
fn instant_overhead_ns() -> u64 {
    let iters = 100_000u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        std::hint::black_box(Instant::now());
    }
    t0.elapsed().as_nanos() as u64 / iters
}
