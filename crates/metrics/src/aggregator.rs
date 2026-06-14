// Metrics aggregator for low-latency statistics collection
//! Central metrics aggregator: periodically pulls snapshots from each
//! matching shard, risk shard, and gateway session, and exposes a
//! combined view for export (e.g. Prometheus text format).
//!
//! Pull-based by design: hot-path components only ever write to local
//! atomics (see `matching_engine::metrics::EngineMetrics`); this
//! aggregator runs on a low-priority background thread and never
//! touches anything the hot path depends on for correctness.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use matching_engine::metrics::{EngineMetrics, EngineMetricsSnapshot};

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
        let mut totals = EngineMetricsSnapshot::default();
        let mut weighted_match_sum: u128 = 0;
        let mut weighted_risk_sum: u128 = 0;

        for s in self.per_shard.values() {
            totals.orders_processed += s.orders_processed;
            totals.fills_generated += s.fills_generated;
            totals.risk_rejects += s.risk_rejects;
            totals.idle_spins += s.idle_spins;

            totals.match_latency.count += s.match_latency.count;
            totals.match_latency.max_ns = totals.match_latency.max_ns.max(s.match_latency.max_ns);
            weighted_match_sum += s.match_latency.mean_ns as u128 * s.match_latency.count as u128;

            totals.risk_check_latency.count += s.risk_check_latency.count;
            totals.risk_check_latency.max_ns =
                totals.risk_check_latency.max_ns.max(s.risk_check_latency.max_ns);
            weighted_risk_sum +=
                s.risk_check_latency.mean_ns as u128 * s.risk_check_latency.count as u128;
        }

        totals.match_latency.mean_ns = if totals.match_latency.count > 0 {
            (weighted_match_sum / totals.match_latency.count as u128) as u64
        } else {
            0
        };
        totals.risk_check_latency.mean_ns = if totals.risk_check_latency.count > 0 {
            (weighted_risk_sum / totals.risk_check_latency.count as u128) as u64
        } else {
            0
        };

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
    /// format.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(1024);
        let snap = &self.last_snapshot;

        for (shard_id, s) in &snap.per_shard {
            let labels = format!("shard=\"{}\"", shard_id);
            write_metric(&mut out, "engine_orders_processed_total", &labels, s.orders_processed as f64);
            write_metric(&mut out, "engine_fills_generated_total", &labels, s.fills_generated as f64);
            write_metric(&mut out, "engine_risk_rejects_total", &labels, s.risk_rejects as f64);
            write_metric(&mut out, "engine_idle_spins_total", &labels, s.idle_spins as f64);
            write_metric(&mut out, "engine_match_latency_ns_mean", &labels, s.match_latency.mean_ns as f64);
            write_metric(&mut out, "engine_match_latency_ns_max", &labels, s.match_latency.max_ns as f64);
            write_metric(&mut out, "engine_risk_check_latency_ns_mean", &labels, s.risk_check_latency.mean_ns as f64);
            write_metric(&mut out, "engine_risk_check_latency_ns_max", &labels, s.risk_check_latency.max_ns as f64);
        }

        write_metric(&mut out, "engine_orders_processed_total", "shard=\"_total\"", snap.totals.orders_processed as f64);
        write_metric(&mut out, "engine_fills_generated_total", "shard=\"_total\"", snap.totals.fills_generated as f64);
        write_metric(&mut out, "engine_risk_rejects_total", "shard=\"_total\"", snap.totals.risk_rejects as f64);
        write_metric(&mut out, "engine_match_latency_ns_mean", "shard=\"_total\"", snap.totals.match_latency.mean_ns as f64);
        write_metric(&mut out, "engine_match_latency_ns_max", "shard=\"_total\"", snap.totals.match_latency.max_ns as f64);

        out
    }
}

fn write_metric(out: &mut String, name: &str, labels: &str, value: f64) {
    let _ = writeln!(out, "{}{{{}}} {}", name, labels, value);
}

#[cfg(test)]
mod tests {
    use super::*;

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
}