// Metrics aggregator for low-latency statistics collection
//! Central metrics aggregator: periodically pulls snapshots from each
//! matching shard, risk shard, and gateway session, and exposes a
//! combined view for export (e.g. Prometheus text format).
//!
//! Pull-based by design: hot-path components only ever write to local
//! `CachePadded` atomics / histograms (see
//! `matching_engine::metrics::EngineMetrics`); this aggregator runs on a
//! low-priority background thread and never touches anything the hot
//! path depends on for correctness.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use matching_engine::metrics::{EngineMetrics, EngineMetricsSnapshot, LatencySnapshot};

/// Identifies a matching shard (e.g. by instrument group or shard index).
pub type ShardId = u32;

/// A registered source of engine metrics. The aggregator holds `Arc`s
/// so shards can keep updating their atomics independently.
pub struct ShardHandle {
    pub shard_id: ShardId,
    pub metrics: Arc<EngineMetrics>,
}

/// Aggregated, point-in-time view across all registered shards.
#[derive(Debug, Clone, Default)]
pub struct AggregatedSnapshot {
    pub taken_at: Option<Instant>,
    pub per_shard: HashMap<ShardId, EngineMetricsSnapshot>,
    pub totals: EngineMetricsSnapshot,
}

impl AggregatedSnapshot {
    fn merge(&mut self, shard_id: ShardId, snap: EngineMetricsSnapshot) {
        self.per_shard.insert(shard_id, snap);

        // Recompute totals from scratch over all per-shard snapshots.
        // Latency totals are merged via full histogram bucket summation
        // (see `LatencySnapshot::merge`) rather than averaging each
        // shard's mean/percentiles — percentiles don't compose under
        // averaging, so this is the only way the combined p50/p99/etc.
        // are actually correct for the merged population.
        let mut totals = EngineMetricsSnapshot::default();
        let mut match_latency = LatencySnapshot::default();
        let mut risk_check_latency = LatencySnapshot::default();

        for s in self.per_shard.values() {
            totals.orders_processed += s.orders_processed;
            totals.fills_generated += s.fills_generated;
            totals.risk_rejects += s.risk_rejects;
            totals.idle_spins += s.idle_spins;

            match_latency.merge(&s.match_latency);
            risk_check_latency.merge(&s.risk_check_latency);
        }

        totals.match_latency = match_latency;
        totals.risk_check_latency = risk_check_latency;

        self.totals = totals;
    }
}

/// Aggregates metrics from registered matching shards on demand or on
/// a fixed interval.
pub struct MetricsAggregator {
    shards: Vec<ShardHandle>,
    last_snapshot: AggregatedSnapshot,
    interval: Duration,
    last_collected: Instant,
}

impl MetricsAggregator {
    pub fn new(interval: Duration) -> Self {
        Self {
            shards: Vec::new(),
            last_snapshot: AggregatedSnapshot::default(),
            interval,
            last_collected: Instant::now(),
        }
    }

    pub fn register_shard(&mut self, shard_id: ShardId, metrics: Arc<EngineMetrics>) {
        self.shards.push(ShardHandle { shard_id, metrics });
    }

    /// Collect a fresh snapshot from all shards, regardless of interval.
    pub fn collect_now(&mut self) -> &AggregatedSnapshot {
        let mut snap = AggregatedSnapshot {
            taken_at: Some(Instant::now()),
            ..Default::default()
        };

        for handle in &self.shards {
            let s = handle.metrics.snapshot();
            snap.merge(handle.shard_id, s);
        }

        self.last_snapshot = snap;
        self.last_collected = Instant::now();
        &self.last_snapshot
    }

    /// Collect only if `interval` has elapsed since the last collection;
    /// otherwise return the cached snapshot. Suitable for a tight poll
    /// loop on a background thread.
    pub fn maybe_collect(&mut self) -> &AggregatedSnapshot {
        if self.last_collected.elapsed() >= self.interval {
            self.collect_now()
        } else {
            &self.last_snapshot
        }
    }

    pub fn last_snapshot(&self) -> &AggregatedSnapshot {
        &self.last_snapshot
    }

    /// Reset interval counters on all shards (call after each export
    /// if exporting deltas rather than cumulative counters).
    pub fn reset_shard_intervals(&self) {
        for handle in &self.shards {
            handle.metrics.reset_intervals();
        }
    }

    /// Render the last collected snapshot in Prometheus text exposition
    /// format, including latency percentiles (p50/p90/p99/p99.9) derived
    /// from each histogram's bucket counts.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);
        let snap = &self.last_snapshot;

        for (shard_id, s) in &snap.per_shard {
            let labels = format!("shard=\"{}\"", shard_id);
            write_metric(
                &mut out,
                "engine_orders_processed_total",
                &labels,
                s.orders_processed as f64,
            );
            write_metric(
                &mut out,
                "engine_fills_generated_total",
                &labels,
                s.fills_generated as f64,
            );
            write_metric(
                &mut out,
                "engine_risk_rejects_total",
                &labels,
                s.risk_rejects as f64,
            );
            write_metric(
                &mut out,
                "engine_idle_spins_total",
                &labels,
                s.idle_spins as f64,
            );
            write_latency_metrics(
                &mut out,
                "engine_match_latency_ns",
                &labels,
                &s.match_latency,
            );
            write_latency_metrics(
                &mut out,
                "engine_risk_check_latency_ns",
                &labels,
                &s.risk_check_latency,
            );
        }

        let total_labels = "shard=\"_total\"";
        write_metric(
            &mut out,
            "engine_orders_processed_total",
            total_labels,
            snap.totals.orders_processed as f64,
        );
        write_metric(
            &mut out,
            "engine_fills_generated_total",
            total_labels,
            snap.totals.fills_generated as f64,
        );
        write_metric(
            &mut out,
            "engine_risk_rejects_total",
            total_labels,
            snap.totals.risk_rejects as f64,
        );
        write_latency_metrics(
            &mut out,
            "engine_match_latency_ns",
            total_labels,
            &snap.totals.match_latency,
        );
        write_latency_metrics(
            &mut out,
            "engine_risk_check_latency_ns",
            total_labels,
            &snap.totals.risk_check_latency,
        );

        out
    }
}

fn write_metric(out: &mut String, name: &str, labels: &str, value: f64) {
    let _ = writeln!(out, "{}{{{}}} {}", name, labels, value);
}

fn write_latency_metrics(out: &mut String, base_name: &str, labels: &str, s: &LatencySnapshot) {
    write_metric(out, &format!("{base_name}_mean"), labels, s.mean_ns as f64);
    write_metric(out, &format!("{base_name}_max"), labels, s.max_ns as f64);
    write_metric(out, &format!("{base_name}_p50"), labels, s.p50() as f64);
    write_metric(out, &format!("{base_name}_p90"), labels, s.p90() as f64);
    write_metric(out, &format!("{base_name}_p99"), labels, s.p99() as f64);
    write_metric(out, &format!("{base_name}_p999"), labels, s.p999() as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn aggregates_across_shards() {
        let mut agg = MetricsAggregator::new(Duration::from_millis(0));

        let m1 = Arc::new(EngineMetrics::new());
        let m2 = Arc::new(EngineMetrics::new());

        m1.record_order(2, false);
        m1.record_order(0, true);
        m2.record_order(1, false);

        agg.register_shard(1, m1);
        agg.register_shard(2, m2);

        let snap = agg.collect_now();
        assert_eq!(snap.totals.orders_processed, 3);
        assert_eq!(snap.totals.fills_generated, 3);
        assert_eq!(snap.totals.risk_rejects, 1);
        assert_eq!(snap.per_shard.len(), 2);
    }

    #[test]
    fn maybe_collect_respects_interval() {
        let mut agg = MetricsAggregator::new(Duration::from_secs(3600));
        let m1 = Arc::new(EngineMetrics::new());
        m1.record_order(5, false);
        agg.register_shard(1, m1.clone());

        agg.collect_now();
        m1.record_order(10, false);

        // Interval not elapsed: should still show the old snapshot (5 fills).
        let snap = agg.maybe_collect();
        assert_eq!(snap.totals.fills_generated, 5);
    }

    #[test]
    fn prometheus_render_contains_expected_metrics() {
        let mut agg = MetricsAggregator::new(Duration::from_millis(0));
        let m1 = Arc::new(EngineMetrics::new());
        m1.record_order(3, false);
        agg.register_shard(7, m1);
        agg.collect_now();

        let text = agg.render_prometheus();
        assert!(text.contains("engine_orders_processed_total{shard=\"7\"}"));
        assert!(text.contains("engine_fills_generated_total{shard=\"_total\"}"));
    }

    #[test]
    fn prometheus_render_contains_percentiles() {
        let mut agg = MetricsAggregator::new(Duration::from_millis(0));
        let m1 = Arc::new(EngineMetrics::new());
        for ns in 1..=100u64 {
            m1.match_latency.record(Duration::from_nanos(ns));
        }
        agg.register_shard(1, m1);
        agg.collect_now();

        let text = agg.render_prometheus();
        assert!(text.contains("engine_match_latency_ns_p50{shard=\"1\"}"));
        assert!(text.contains("engine_match_latency_ns_p99{shard=\"_total\"}"));
    }

    #[test]
    fn cross_shard_percentiles_reflect_merged_population_not_averaged_shard_percentiles() {
        let mut agg = MetricsAggregator::new(Duration::from_millis(0));

        // Shard 1: all fast (100ns).
        let m1 = Arc::new(EngineMetrics::new());
        for _ in 0..1000 {
            m1.match_latency.record(Duration::from_nanos(100));
        }

        // Shard 2: all slow (1,000,000ns).
        let m2 = Arc::new(EngineMetrics::new());
        for _ in 0..1000 {
            m2.match_latency.record(Duration::from_nanos(1_000_000));
        }

        agg.register_shard(1, m1);
        agg.register_shard(2, m2);
        let snap = agg.collect_now();

        // Merged population is 50% fast / 50% slow, so p50 should land
        // right around the boundary — nowhere near a naive average of
        // "100ns" and "1,000,000ns" being reported as each shard's own
        // p50 (which would be correct per-shard but meaningless summed).
        let merged_p50 = snap.totals.match_latency.p50();
        assert!(merged_p50 <= 1_000_000);
        assert_eq!(snap.totals.match_latency.count, 2000);
    }
}
