//! Shared monotonic clock — every pipeline stage's now_ns() must read off
//! the same origin, or timestamp_out - timestamp_in is meaningless.

use std::time::Instant;

pub trait Clock: Send + Sync + 'static {
    fn now_ns(&self) -> u64;
}

pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Clock for MonotonicClock {
    #[inline]
    fn now_ns(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}
