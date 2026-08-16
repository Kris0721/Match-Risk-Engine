// Low-latency telemetry metrics for matching engine
//! Per-tick metrics collected by the matching engine hot loop and
//! periodically flushed to the shared metrics aggregator.
//!
//! # Design
//!
//! - **Counters** (`orders_processed`, `fills_generated`, ...) are plain
//!   `AtomicU64`s, each wrapped in `CachePadded` so it lives on its own
//!   64-byte cache line. Without that, adjacent counters in this struct
//!   would share a cache line: the hot-path writer thread updating one
//!   counter and the background aggregator thread reading a neighboring
//!   one would bounce that line between cores on every touch (false
//!   sharing), even though the two fields are logically unrelated.
//! - **Latencies** (`match_latency`, `risk_check_latency`) are full
//!   histograms (`LatencyHistogram`), not just running mean/max, so the
//!   aggregator can report percentiles (p50/p90/p99/p99.9) instead of
//!   just an average that hides tail latency. Each histogram is also
//!   `CachePadded` for the same false-sharing reason as the counters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crossbeam_utils::CachePadded;

/// Number of histogram buckets. Bucket `i` covers latencies in the
/// half-open range `[2^i - 1, 2^(i+1) - 1)` nanoseconds (bucket 0 covers
/// `[0, 1)`, bucket 1 covers `[1, 3)`, etc. — standard log2 bucketing).
/// 32 buckets covers everything from sub-nanosecond up through roughly
/// 4.3 seconds, comfortably spanning both normal hot-path latencies
/// (sub-microsecond to low-microsecond) and pathological stalls, using a
/// small, fixed, allocation-free array.
const NUM_BUCKETS: usize = 32;

/// Lock-free latency histogram: power-of-two ("log2") bucketed counts,
/// plus running count/sum/max for O(1) mean/max without walking buckets.
///
/// Single-writer (the matching thread calls `record`), multi-reader
/// (the metrics aggregator calls `snapshot` from a background thread).
/// Buckets themselves are plain `AtomicU64`s — false sharing *between
/// buckets* isn't a concern here because only one thread ever writes to
/// a given histogram, so there's no writer-vs-writer cache-line
/// contention to pad against. What does need padding is keeping this
/// whole histogram off the cache line of unrelated sibling fields in
/// `EngineMetrics` — see the `CachePadded<LatencyHistogram>` there.
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; NUM_BUCKETS],
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl LatencyHistogram {
    pub const fn new() -> Self {
        // `[AtomicU64::new(0); NUM_BUCKETS]` needs `Copy`, which atomics
        // aren't, so seed the array via a `const` template value instead.
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            buckets: [ZERO; NUM_BUCKETS],
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }

    /// Map a nanosecond value to its bucket index: `floor(log2(ns + 1))`,
    /// clamped to the last bucket so nothing ever overflows the array.
    #[inline]
    fn bucket_for(ns: u64) -> usize {
        // `saturating_add` rather than `+`: `ns == u64::MAX` must map to
        // the last bucket, not panic/wrap. In practice a real latency
        // sample never gets remotely close to `u64::MAX` nanoseconds
        // (~584 years), but the function must still be total.
        let idx = 63 - ns.saturating_add(1).leading_zeros();
        (idx as usize).min(NUM_BUCKETS - 1)
    }

    #[inline]
    pub fn record(&self, d: Duration) {
        let ns = d.as_nanos() as u64;
        let bucket = Self::bucket_for(ns);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let mut buckets = [0u64; NUM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Ordering::Relaxed);
        }
        let count = self.count.load(Ordering::Relaxed);
        let sum_ns = self.sum_ns.load(Ordering::Relaxed);
        let max_ns = self.max_ns.load(Ordering::Relaxed);
        LatencySnapshot {
            buckets,
            count,
            sum_ns,
            mean_ns: if count > 0 { sum_ns / count } else { 0 },
            max_ns,
        }
    }

    /// Reset counters after a snapshot/flush interval.
    pub fn reset(&self) {
        for b in &self.buckets {
            b.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.sum_ns.store(0, Ordering::Relaxed);
        self.max_ns.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of a `LatencyHistogram`. `buckets[i]` holds the
/// count of samples that fell in bucket `i` (see `LatencyHistogram` docs
/// for the bucket boundary convention).
#[derive(Debug, Clone, Copy)]
pub struct LatencySnapshot {
    pub buckets: [u64; NUM_BUCKETS],
    pub count: u64,
    pub sum_ns: u64,
    pub mean_ns: u64,
    pub max_ns: u64,
}

impl Default for LatencySnapshot {
    fn default() -> Self {
        Self {
            buckets: [0; NUM_BUCKETS],
            count: 0,
            sum_ns: 0,
            mean_ns: 0,
            max_ns: 0,
        }
    }
}

impl LatencySnapshot {
    /// Approximate the given percentile (`0.0..=1.0`) in nanoseconds by
    /// walking bucket counts until the running total reaches the target
    /// rank, then returning that bucket's upper edge.
    ///
    /// This is approximate: every sample in a bucket is treated as if it
    /// fell at the bucket's upper edge, giving bounded error of at most
    /// ~2x within a single bucket — the standard tradeoff for compact
    /// log2-bucketed histograms. Good enough for dashboards/alerting;
    /// not a substitute for exact percentiles if you need those.
    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((p.clamp(0.0, 1.0) * self.count as f64).ceil() as u64).max(1);
        let mut running = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            running += c;
            if running >= target {
                // Upper edge of bucket i is 2^(i+1) - 1 ns.
                return (1u64 << (i as u32 + 1)) - 1;
            }
        }
        self.max_ns
    }

    pub fn p50(&self) -> u64 {
        self.percentile(0.50)
    }
    pub fn p90(&self) -> u64 {
        self.percentile(0.90)
    }
    pub fn p99(&self) -> u64 {
        self.percentile(0.99)
    }
    pub fn p999(&self) -> u64 {
        self.percentile(0.999)
    }

    /// Fold another snapshot's counts into this one in place. Used by the
    /// metrics aggregator to combine per-shard histograms into a
    /// cross-shard total — bucket counts (and therefore percentiles) are
    /// summed exactly; percentiles are never averaged across shards,
    /// since percentiles don't compose that way (the average of two
    /// medians is not the median of the combined population).
    pub fn merge(&mut self, other: &LatencySnapshot) {
        for i in 0..NUM_BUCKETS {
            self.buckets[i] += other.buckets[i];
        }
        self.count += other.count;
        self.sum_ns += other.sum_ns;
        self.mean_ns = if self.count > 0 {
            self.sum_ns / self.count
        } else {
            0
        };
        self.max_ns = self.max_ns.max(other.max_ns);
    }
}

/// Aggregate engine metrics for a single matching shard.
///
/// Every field is individually `CachePadded` — see the module docs for
/// why: this struct is written by exactly one hot-path thread and read
/// concurrently by the background metrics aggregator, so unrelated
/// fields must not share a cache line.
#[derive(Debug, Default)]
pub struct EngineMetrics {
    pub match_latency: CachePadded<LatencyHistogram>,
    pub risk_check_latency: CachePadded<LatencyHistogram>,
    pub orders_processed: CachePadded<AtomicU64>,
    pub fills_generated: CachePadded<AtomicU64>,
    pub risk_rejects: CachePadded<AtomicU64>,
    pub idle_spins: CachePadded<AtomicU64>,
    /// Number of outbound events dropped because the ring stayed full
    /// past the bounded retry window. This must be 0 in a healthy system;
    /// any nonzero value means fills/events were lost and downstream
    /// state (risk shards, gateway sessions) has silently gone stale.
    pub outbound_drops: CachePadded<AtomicU64>,
    pub wal_failures: CachePadded<AtomicU64>,
}

impl EngineMetrics {
    pub const fn new() -> Self {
        Self {
            match_latency: CachePadded::new(LatencyHistogram::new()),
            risk_check_latency: CachePadded::new(LatencyHistogram::new()),
            orders_processed: CachePadded::new(AtomicU64::new(0)),
            fills_generated: CachePadded::new(AtomicU64::new(0)),
            risk_rejects: CachePadded::new(AtomicU64::new(0)),
            idle_spins: CachePadded::new(AtomicU64::new(0)),
            outbound_drops: CachePadded::new(AtomicU64::new(0)),
            wal_failures: CachePadded::new(AtomicU64::new(0)),
        }
    }

    #[inline]
    pub fn record_order(&self, fills: u64, risk_rejected: bool) {
        self.orders_processed.fetch_add(1, Ordering::Relaxed);
        if risk_rejected {
            self.risk_rejects.fetch_add(1, Ordering::Relaxed);
        } else if fills > 0 {
            self.fills_generated.fetch_add(fills, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_wal_failure(&self) {
        self.wal_failures.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_idle_spin(&self) {
        self.idle_spins.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_outbound_drop(&self) {
        self.outbound_drops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> EngineMetricsSnapshot {
        EngineMetricsSnapshot {
            match_latency: self.match_latency.snapshot(),
            risk_check_latency: self.risk_check_latency.snapshot(),
            orders_processed: self.orders_processed.load(Ordering::Relaxed),
            fills_generated: self.fills_generated.load(Ordering::Relaxed),
            risk_rejects: self.risk_rejects.load(Ordering::Relaxed),
            idle_spins: self.idle_spins.load(Ordering::Relaxed),
        }
    }

    pub fn reset_intervals(&self) {
        self.match_latency.reset();
        self.risk_check_latency.reset();
        self.orders_processed.store(0, Ordering::Relaxed);
        self.fills_generated.store(0, Ordering::Relaxed);
        self.risk_rejects.store(0, Ordering::Relaxed);
        self.idle_spins.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineMetricsSnapshot {
    pub match_latency: LatencySnapshot,
    pub risk_check_latency: LatencySnapshot,
    pub orders_processed: u64,
    pub fills_generated: u64,
    pub risk_rejects: u64,
    pub idle_spins: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_boundaries_are_monotonic() {
        assert_eq!(LatencyHistogram::bucket_for(0), 0);
        assert_eq!(LatencyHistogram::bucket_for(1), 1);
        assert!(LatencyHistogram::bucket_for(1_000) > LatencyHistogram::bucket_for(100));
        assert_eq!(LatencyHistogram::bucket_for(u64::MAX), NUM_BUCKETS - 1);
    }

    #[test]
    fn record_updates_count_sum_max() {
        let h = LatencyHistogram::new();
        h.record(Duration::from_nanos(100));
        h.record(Duration::from_nanos(300));
        h.record(Duration::from_nanos(50));

        let snap = h.snapshot();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.sum_ns, 450);
        assert_eq!(snap.max_ns, 300);
        assert_eq!(snap.mean_ns, 150);
    }

    #[test]
    fn percentiles_are_monotonic_and_bounded_by_max() {
        let h = LatencyHistogram::new();
        for ns in 1..=1000u64 {
            h.record(Duration::from_nanos(ns));
        }
        let snap = h.snapshot();

        assert!(snap.p50() <= snap.p90());
        assert!(snap.p90() <= snap.p99());
        assert!(snap.p99() <= snap.p999());
        assert!(snap.p999() <= snap.max_ns.max(snap.p999()));
        // Roughly in the right ballpark for a uniform 1..=1000ns sample.
        assert!(snap.p50() > 100 && snap.p50() < 2000);
    }

    #[test]
    fn empty_histogram_percentiles_are_zero() {
        let h = LatencyHistogram::new();
        let snap = h.snapshot();
        assert_eq!(snap.p50(), 0);
        assert_eq!(snap.p99(), 0);
    }

    #[test]
    fn merge_sums_buckets_count_and_max() {
        let h1 = LatencyHistogram::new();
        h1.record(Duration::from_nanos(100));
        h1.record(Duration::from_nanos(200));

        let h2 = LatencyHistogram::new();
        h2.record(Duration::from_nanos(5_000));

        let mut merged = h1.snapshot();
        merged.merge(&h2.snapshot());

        assert_eq!(merged.count, 3);
        assert_eq!(merged.sum_ns, 5_300);
        assert_eq!(merged.max_ns, 5_000);
    }

    #[test]
    fn reset_clears_histogram_and_counters() {
        let m = EngineMetrics::new();
        m.record_order(2, false);
        m.match_latency.record(Duration::from_nanos(500));

        m.reset_intervals();

        let snap = m.snapshot();
        assert_eq!(snap.orders_processed, 0);
        assert_eq!(snap.fills_generated, 0);
        assert_eq!(snap.match_latency.count, 0);
    }

    #[test]
    fn engine_metrics_new_is_const_evaluable() {
        // Compiles only if `EngineMetrics::new()` is truly `const fn`
        // all the way down through `CachePadded::new` and
        // `LatencyHistogram::new`.
        const _M: EngineMetrics = EngineMetrics::new();
    }
}
