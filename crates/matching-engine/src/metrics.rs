// Low-latency telemetry metrics for matching engine
//! Per-tick metrics collected by the matching engine hot loop and
//! periodically flushed to the shared metrics aggregator.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Histogram-free latency tracker: keeps running count/sum/max in
/// nanoseconds. Designed for single-writer (the matching thread),
/// multi-reader (metrics aggregator) access.
#[derive(Debug, Default)]
pub struct LatencyStats {
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl LatencyStats {
    pub const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record(&self, d: Duration) {
        let ns = d.as_nanos() as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let sum_ns = self.sum_ns.load(Ordering::Relaxed);
        let max_ns = self.max_ns.load(Ordering::Relaxed);
        LatencySnapshot {
            count,
            mean_ns: if count > 0 { sum_ns / count } else { 0 },
            max_ns,
        }
    }

    /// Reset counters after a snapshot/flush interval.
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.sum_ns.store(0, Ordering::Relaxed);
        self.max_ns.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LatencySnapshot {
    pub count: u64,
    pub mean_ns: u64,
    pub max_ns: u64,
}

/// Aggregate engine metrics for a single matching shard.
#[derive(Debug, Default)]
pub struct EngineMetrics {
    pub match_latency: LatencyStats,
    pub risk_check_latency: LatencyStats,
    pub orders_processed: AtomicU64,
    pub fills_generated: AtomicU64,
    pub risk_rejects: AtomicU64,
    pub idle_spins: AtomicU64,
    /// Number of outbound events dropped because the ring stayed full
    /// past the bounded retry window. This must be 0 in a healthy system;
    /// any nonzero value means fills/events were lost and downstream
    /// state (risk shards, gateway sessions) has silently gone stale.
    pub outbound_drops: AtomicU64,
}

impl EngineMetrics {
    pub const fn new() -> Self {
        Self {
            match_latency: LatencyStats::new(),
            risk_check_latency: LatencyStats::new(),
            orders_processed: AtomicU64::new(0),
            fills_generated: AtomicU64::new(0),
            risk_rejects: AtomicU64::new(0),
            idle_spins: AtomicU64::new(0),
            outbound_drops: AtomicU64::new(0),
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