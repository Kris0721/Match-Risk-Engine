// Single-Producer Multi-Consumer ring buffer
//! Single-Producer Multi-Consumer ring buffer (broadcast / fan-out).
//!
//! # Design
//! This is a **broadcast** ring buffer: every consumer independently tracks its
//! own read cursor and receives every message the producer publishes. This is the
//! primitive used for the matching-engine → (risk shards, WAL, metrics, market-data)
//! fan-out.
//!
//! The producer writes each slot once and publishes it by advancing a single
//! `head` atomic. Each consumer has its own `tail` cursor stored locally (no
//! shared atomic for consumer tails). The producer must be informed of the
//! slowest consumer so it knows when slots can be overwritten; this is done
//! by giving the producer access to each consumer's `tail` through a
//! `Arc<AtomicUsize>` per consumer — but these are only read by the producer
//! during the "is queue full?" check, which is the slow path.
//!
//! # Lagging consumers
//! If a slow consumer falls more than `CAP` slots behind, the producer will
//! stall (spin) or, optionally, drop the lagging consumer. For the matching
//! engine the producer **must never stall**, so ensure consumers are fast enough
//! or `CAP` is large enough to absorb bursts.

#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(feature = "loom"))]
use std::sync::Arc;
#[cfg(feature = "loom")]
use loom::sync::Arc;

use std::cell::{Cell, UnsafeCell};
use std::mem::MaybeUninit;

use crate::cache_pad::CachePadded;

struct Shared<T, const CAP: usize> {
    /// Number of items published so far. Written only by the producer.
    head: CachePadded<AtomicUsize>,
    /// One tail cursor per consumer. Each consumer owns exactly one of these.
    /// The producer reads all of them during the full-check.
    consumer_tails: Vec<Arc<CachePadded<AtomicUsize>>>,
    slots: [UnsafeCell<MaybeUninit<T>>; CAP],
}

// SAFETY: same reasoning as SPSC — single writer, cursor-guarded reads.
unsafe impl<T: Send, const CAP: usize> Send for Shared<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for Shared<T, CAP> {}

impl<T, const CAP: usize> Shared<T, CAP> {
    fn new(n_consumers: usize) -> Self {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        let slots = unsafe {
            let mut arr: [UnsafeCell<MaybeUninit<T>>; CAP] = MaybeUninit::uninit().assume_init();
            for slot in arr.iter_mut() {
                *slot = UnsafeCell::new(MaybeUninit::uninit());
            }
            arr
        };
        let consumer_tails = (0..n_consumers)
            .map(|_| Arc::new(CachePadded::new(AtomicUsize::new(0))))
            .collect();
        Self {
            head: CachePadded::new(AtomicUsize::new(0)),
            consumer_tails,
            slots,
        }
    }

    #[inline(always)]
    fn mask(&self, idx: usize) -> usize {
        idx & (CAP - 1)
    }

    /// Returns the minimum tail across all consumers (i.e. the furthest behind).
    fn min_tail(&self) -> usize {
        self.consumer_tails
            .iter()
            .map(|t| t.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }
}

/// The single producing end of an SPMC queue.
pub struct SpmcProducer<T, const CAP: usize> {
    shared: Arc<Shared<T, CAP>>,
    cached_head: usize,
    // !Sync by construction: this type must not be shared across threads.
    // PhantomData<*const ()> makes the compiler infer !Sync + !Send without
    // requiring the unstable negative_impls feature.
    _not_sync: std::marker::PhantomData<Cell<()>>,
}

/// One consuming end of an SPMC queue. Each `SpmcConsumer` has an independent cursor.
pub struct SpmcConsumer<T, const CAP: usize> {
    shared: Arc<Shared<T, CAP>>,
    /// This consumer's tail cursor, shared (by `Arc`) so the producer can check it.
    tail: Arc<CachePadded<AtomicUsize>>,
    cached_tail: usize,
}


/// Create an SPMC queue with `n_consumers` independent consumer cursors.
///
/// Returns the producer and a `Vec` of consumers (one per cursor).
pub fn spmc_queue<T, const CAP: usize>(
    n_consumers: usize,
) -> (SpmcProducer<T, CAP>, Vec<SpmcConsumer<T, CAP>>) {
    let shared = Arc::new(Shared::new(n_consumers));

    let consumers = shared
        .consumer_tails
        .iter()
        .map(|tail_arc| SpmcConsumer {
            shared: Arc::clone(&shared),
            tail: Arc::clone(tail_arc),
            cached_tail: 0,
        })
        .collect();

    let producer = SpmcProducer {
        shared,
        cached_head: 0,
        _not_sync: std::marker::PhantomData,
    };

    (producer, consumers)
}

impl<T: Clone, const CAP: usize> SpmcProducer<T, CAP> {
    /// Attempt to publish `item` to all consumers.
    ///
    /// Returns `Err(item)` if the slowest consumer is still `CAP` slots behind
    /// (i.e. would be overwritten). The caller should spin or apply backpressure.
    #[inline]
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        let head = self.cached_head;
        let min_tail = self.shared.min_tail();

        if head.wrapping_sub(min_tail) == CAP {
            return Err(item);
        }

        let slot = self.shared.mask(head);
        // SAFETY: We are the sole producer. All consumer tails are at least
        // `head - CAP + 1`, so slot `head % CAP` is no longer in use by anyone.
        unsafe {
            (*self.shared.slots[slot].get()).write(item);
        }

        self.shared.head.store(head.wrapping_add(1), Ordering::Release);
        self.cached_head = head.wrapping_add(1);
        Ok(())
    }
}

impl<T: Clone, const CAP: usize> SpmcConsumer<T, CAP> {
    /// Attempt to read the next item. Returns `None` if there is nothing new.
    #[inline]
    pub fn try_pop(&mut self) -> Option<T> {
        let tail = self.cached_tail;
        let head = self.shared.head.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        let slot = self.shared.mask(tail);
        // SAFETY: The producer has published this slot (head > tail), so the
        // write is complete and visible after the Acquire load of `head`.
        // Multiple consumers may read the same slot concurrently — that is safe
        // because they only read and `T: Clone`; no consumer writes to slots.
        let item = unsafe { (*self.shared.slots[slot].get()).assume_init_ref().clone() };

        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        self.cached_tail = tail.wrapping_add(1);
        Some(item)
    }
}