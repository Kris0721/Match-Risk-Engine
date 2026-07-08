//! Single-threaded sanity tests for the SPSC ring buffer. These run under
//! plain `cargo test` (no `loom` feature needed) and catch basic
//! off-by-one / wraparound bugs quickly, before reaching for the slower
//! exhaustive loom model-checking in spsc_loom.rs.

use crate::spsc::spsc_queue;

#[test]
fn push_then_pop_returns_same_value() {
    let (mut p, mut c) = spsc_queue::<u64, 4>();
    assert!(p.try_push(42).is_ok());
    assert_eq!(c.try_pop(), Some(42));
}

#[test]
fn pop_on_empty_queue_returns_none() {
    let (_p, mut c) = spsc_queue::<u64, 4>();
    assert_eq!(c.try_pop(), None);
}

#[test]
fn push_until_full_then_rejects() {
    let (mut p, _c) = spsc_queue::<u64, 4>();
    for i in 0..4 {
        assert!(p.try_push(i).is_ok(), "slot {i} should not be full yet");
    }
    // Queue is now full (CAP = 4); the 5th push must be rejected and
    // must hand the item back rather than dropping it silently.
    match p.try_push(999) {
        Err(returned) => assert_eq!(returned, 999),
        Ok(()) => panic!("push succeeded on a full queue"),
    }
}

#[test]
fn fifo_order_is_preserved() {
    let (mut p, mut c) = spsc_queue::<u64, 8>();
    for i in 0..5 {
        p.try_push(i).unwrap();
    }
    for i in 0..5 {
        assert_eq!(c.try_pop(), Some(i), "items must come out in push order");
    }
    assert_eq!(c.try_pop(), None);
}

#[test]
fn wraparound_across_capacity_boundary() {
    // CAP = 4. Push/pop past the point where the ring index wraps around
    // zero, to catch off-by-one bugs in the mask/modulo arithmetic that a
    // test never filling the buffer more than once would miss.
    let (mut p, mut c) = spsc_queue::<u64, 4>();
    for round in 0..10u64 {
        p.try_push(round).unwrap();
        assert_eq!(c.try_pop(), Some(round));
    }
}

#[test]
fn interleaved_push_pop_never_loses_or_duplicates_items() {
    let (mut p, mut c) = spsc_queue::<u64, 4>();
    let mut next_push = 0u64;
    let mut next_expected_pop = 0u64;
    let mut in_flight = 0usize;

    for step in 0..100 {
        if in_flight < 4 && step % 3 != 0 {
            p.try_push(next_push).unwrap();
            next_push += 1;
            in_flight += 1;
        } else if let Some(v) = c.try_pop() {
            assert_eq!(v, next_expected_pop, "FIFO order violated");
            next_expected_pop += 1;
            in_flight -= 1;
        }
    }
    // Drain whatever's left.
    while let Some(v) = c.try_pop() {
        assert_eq!(v, next_expected_pop);
        next_expected_pop += 1;
    }
    assert_eq!(next_expected_pop, next_push, "some items were lost");
}