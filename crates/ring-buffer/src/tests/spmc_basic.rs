//! Single-threaded sanity tests for the SPMC ring buffer.

use crate::spmc::spmc_queue;

#[test]
fn single_consumer_sees_pushed_item() {
    let (mut p, mut consumers) = spmc_queue::<u64, 4>(1);
    let mut c0 = consumers.remove(0);
    p.try_push(7).unwrap();
    assert_eq!(c0.try_pop(), Some(7));
}

#[test]
fn every_consumer_independently_sees_every_item() {
    let (mut p, mut consumers) = spmc_queue::<u64, 4>(3);
    p.try_push(11).unwrap();
    p.try_push(22).unwrap();

    for c in consumers.iter_mut() {
        assert_eq!(c.try_pop(), Some(11));
        assert_eq!(c.try_pop(), Some(22));
        assert_eq!(c.try_pop(), None);
    }
}

#[test]
fn push_blocked_by_slowest_consumer() {
    // CAP = 2. With 2 consumers, one of which never reads, the producer
    // must be backpressured once the un-read consumer would have its
    // oldest un-consumed slot overwritten.
    let (mut p, mut consumers) = spmc_queue::<u64, 2>(2);
    let mut fast = consumers.remove(0);
    let _slow = consumers.remove(0); // never popped from

    assert!(p.try_push(1).is_ok());
    assert!(p.try_push(2).is_ok());
    // Buffer is full relative to the slow consumer's cursor (still at 0).
    match p.try_push(3) {
        Err(returned) => assert_eq!(returned, 3),
        Ok(()) => panic!("push should have been blocked by the slow consumer"),
    }

    // Once the slow consumer catches up, the fast one is unaffected and
    // the producer can push again.
    fast.try_pop().unwrap();
}

#[test]
fn fifo_order_per_consumer() {
    let (mut p, mut consumers) = spmc_queue::<u64, 8>(2);
    let mut c0 = consumers.remove(0);
    let mut c1 = consumers.remove(0);

    for i in 0..5 {
        p.try_push(i).unwrap();
    }
    for i in 0..5 {
        assert_eq!(c0.try_pop(), Some(i));
    }
    // c1 hasn't read anything yet — must still see all 5 in order,
    // independently of c0's cursor.
    for i in 0..5 {
        assert_eq!(c1.try_pop(), Some(i));
    }
}