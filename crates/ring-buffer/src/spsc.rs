// Single-Producer Single-Consumer ring buffer
//! Single-Producer Single-Consumer ring buffer.
//!
//! # Design
//! - Power-of-two capacity so index masking is a single `& (CAP - 1)` with no division.
//! - Producer owns the `head` cursor; consumer owns the `tail` cursor.
//! - Each cursor lives on its own cache line (`CachePadded`) to prevent false sharing.
//! - Communication uses only `Acquire`/`Release` pairs — no CAS, no contention.
//!
//! # Safety contract
//! There must be **at most one producer and one consumer** at any point in time.
//! The type system enforces this: `SpscProducer` and `SpscConsumer` are `!Sync`.

#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(feature = "loom"))]
use std::sync::Arc;
#[cfg(feature = "loom")]
use loom::sync::Arc;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

use crate::cache_pad::CachePadded;

/// The shared backing store. `CAP` must be a power of two.
struct Shared<T, const CAP: usize> {
    /// Written by producer, read by consumer to know how many slots are filled.
    head: CachePadded<AtomicUsize>,
    /// Written by consumer, read by producer to know how many slots are free.
    tail: CachePadded<AtomicUsize>,
    /// The ring buffer storage; access is guarded by head/tail cursors.
    slots: [UnsafeCell<MaybeUninit<T>>; CAP],
}

// SAFETY: We manually enforce single-producer / single-consumer access via the
// cursor protocol. `T` must be `Send` because items cross thread boundaries.
unsafe impl<T: Send, const CAP: usize> Send for Shared<T, CAP> {}
unsafe impl<T: Send, const CAP: usize> Sync for Shared<T, CAP> {}

impl<T, const CAP: usize> Shared<T, CAP> {
    fn new() -> Self {
        assert!(CAP.is_power_of_two(), "CAP must be a power of two");
        // SAFETY: MaybeUninit arrays can be zero-initialized this way.
        let slots = unsafe {
            let mut arr: [UnsafeCell<MaybeUninit<T>>; CAP] = MaybeUninit::uninit().assume_init();
            for slot in arr.iter_mut() {
                *slot = UnsafeCell::new(MaybeUninit::uninit());
            }
            arr
        };
        Self {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            slots,
        }
    }

    #[inline(always)]
    fn mask(&self, idx: usize) -> usize {
        idx & (CAP - 1)
    }
}

/// The producing end of an SPSC queue. `!Sync` — must not be shared across threads.
pub struct SpscProducer<T, const CAP: usize> {
    shared: Arc<Shared<T, CAP>>,
    /// Cached local copy of head to avoid redundant atomic loads.
    cached_head: usize,
   // !Sync by construction: this type must not be shared across threads.
   // PhantomData<*const ()> makes the compiler infer !Sync + !Send without
   // requiring the unstable negative_impls feature.
   _not_sync: std::marker::PhantomData<*const ()>,
}
 
/// The consuming end of an SPSC queue. `!Sync` — must not be shared across threads.
pub struct SpscConsumer<T, const CAP: usize> {
    shared: Arc<Shared<T, CAP>>,
    /// Cached local copy of tail to avoid redundant atomic loads.
    cached_tail: usize,
    // !Sync by construction: this type must not be shared across threads.
    // PhantomData<*const ()> makes the compiler infer !Sync + !Send without
    // requiring the unstable negative_impls feature.
    _not_sync: std::marker::PhantomData<*const ()>,
}
 
// Explicitly not Sync.


/// Construct a new SPSC queue of capacity `CAP` (must be a power of two).
pub fn spsc_queue<T, const CAP: usize>() -> (SpscProducer<T, CAP>, SpscConsumer<T, CAP>) {
    let shared = Arc::new(Shared::new());
    (
        SpscProducer { shared: Arc::clone(&shared), cached_head: 0, _not_sync: std::marker::PhantomData },
        SpscConsumer { shared, cached_tail: 0, _not_sync: std::marker::PhantomData },
    )
}

impl<T, const CAP: usize> SpscProducer<T, CAP> {
    /// Attempt to enqueue `item`. Returns `Err(item)` if the queue is full.
    ///
    /// Never blocks, never parks.
    #[inline]
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        let head = self.cached_head;
        // Load tail with Acquire so that writes to slots by the consumer
        // (freeing them) are visible before we overwrite them.
        let tail = self.shared.tail.load(Ordering::Acquire);

        if head.wrapping_sub(tail) == CAP {
            // Queue is full.
            return Err(item);
        }

        let slot = self.shared.mask(head);
        // SAFETY: We are the sole producer. The slot at `head` is not
        // currently owned by the consumer (we checked via the tail cursor).
        unsafe {
            (*self.shared.slots[slot].get()).write(item);
        }

        // Release: makes the slot write visible to the consumer before
        // the head counter is incremented.
        self.shared.head.store(head.wrapping_add(1), Ordering::Release);
        self.cached_head = head.wrapping_add(1);
        Ok(())
    }
}

impl<T, const CAP: usize> SpscConsumer<T, CAP> {
    /// Attempt to dequeue an item. Returns `None` if the queue is empty.
    ///
    /// Never blocks, never parks.
    #[inline]
    pub fn try_pop(&mut self) -> Option<T> {
        let tail = self.cached_tail;
        // Acquire: ensures the slot data written by the producer is visible
        // after we observe the incremented head.
        let head = self.shared.head.load(Ordering::Acquire);

        if head == tail {
            // Queue is empty.
            return None;
        }

        let slot = self.shared.mask(tail);
        // SAFETY: We are the sole consumer. The slot at `tail` is fully
        // written by the producer (we verified head > tail).
        let item = unsafe { (*self.shared.slots[slot].get()).assume_init_read() };

        // Release: makes the slot read (freeing the slot) visible to the
        // producer before the tail counter is incremented.
        self.shared.tail.store(tail.wrapping_add(1), Ordering::Release);
        self.cached_tail = tail.wrapping_add(1);
        Some(item)
    }
}