// Criterion benchmarks for the SPSC ring buffer.
// Run with: cargo bench -p ring-buffer

use std::thread;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use ring_buffer::spsc_queue;

const CAP: usize = 1024;

/// Same-thread push-then-pop: isolates per-op cost (atomic load/store +
/// slot write/read) with no cross-core cache-line bouncing. This is the
/// floor — real cross-thread latency will always be higher.
fn bench_same_thread_push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_same_thread");
    group.throughput(Throughput::Elements(1));

    group.bench_function("push_pop_pair", |b| {
        let (mut p, mut c) = spsc_queue::<u64, CAP>();
        let mut i: u64 = 0;
        b.iter(|| {
            p.try_push(black_box(i)).unwrap();
            let v = c.try_pop().unwrap();
            black_box(v);
            i = i.wrapping_add(1);
        });
    });
    group.finish();
}

/// True cross-thread roundtrip: main thread pushes a sequence number on
/// queue A, a pinned-less worker thread pops it and immediately pushes it
/// back on queue B, main thread spins until it comes back. This measures
/// real producer→consumer→producer latency including cache-coherency
/// traffic between cores — the number that actually matters for
/// gateway → sequencer → matching-engine handoffs in production.
fn bench_cross_thread_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_cross_thread");
    group.throughput(Throughput::Elements(1));

    group.bench_function("ping_pong_roundtrip", |b| {
        b.iter_batched(
            || {
                let (p_out, mut c_out) = spsc_queue::<u64, CAP>();
                let (p_back, c_back) = spsc_queue::<u64, CAP>();
                let handle = thread::spawn(move || {
                    let mut p_back = p_back;
                    // Worker echoes everything it receives until told to stop
                    // via the sentinel u64::MAX.
                    loop {
                        if let Some(v) = c_out.try_pop() {
                            if v == u64::MAX {
                                break;
                            }
                            loop {
                                if p_back.try_push(v).is_ok() {
                                    break;
                                }
                            }
                        }
                    }
                });
                (p_out, c_back, handle)
            },
            |(mut p_out, mut c_back, handle)| {
                for i in 0..1000u64 {
                    loop {
                        if p_out.try_push(i).is_ok() {
                            break;
                        }
                    }
                    loop {
                        if let Some(v) = c_back.try_pop() {
                            black_box(v);
                            break;
                        }
                    }
                }
                p_out.try_push(u64::MAX).ok();
                handle.join().ok();
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Sustained one-way throughput with the queue kept nearly full — the
/// realistic steady-state for a busy gateway feeding the sequencer.
fn bench_sustained_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_sustained");
    let n = 100_000u64;
    group.throughput(Throughput::Elements(n));

    group.bench_function("producer_consumer_threads", |b| {
        b.iter_batched(
            || spsc_queue::<u64, CAP>(),
            |(mut p, mut c)| {
                let consumer = thread::spawn(move || {
                    let mut received = 0u64;
                    while received < n {
                        if c.try_pop().is_some() {
                            received += 1;
                        }
                    }
                });
                for i in 0..n {
                    loop {
                        if p.try_push(i).is_ok() {
                            break;
                        }
                    }
                }
                consumer.join().unwrap();
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_same_thread_push_pop,
    bench_cross_thread_roundtrip,
    bench_sustained_throughput,
);
criterion_main!(benches);
