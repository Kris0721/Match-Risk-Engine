// Loom-based tests for spsc ring buffer
#![cfg(feature = "loom")]

use loom::thread;
use crate::spsc::spsc_queue;

#[test]
fn spsc_send_recv_loom() {
    loom::model(|| {
        let (mut producer, mut consumer) = spsc_queue::<u64, 4>();

        let producer_thread = thread::spawn(move || {
            // Push two items; may need to retry if full (model explores both paths).
            loop {
                if producer.try_push(1u64).is_ok() { break; }
                loom::hint::spin_loop();
            }
            loop {
                if producer.try_push(2u64).is_ok() { break; }
                loom::hint::spin_loop();
            }
        });

        let consumer_thread = thread::spawn(move || {
            let mut sum = 0u64;
            let mut received = 0usize;
            while received < 2 {
                if let Some(v) = consumer.try_pop() {
                    sum += v;
                    received += 1;
                } else {
                    loom::hint::spin_loop();
                }
            }
            assert_eq!(sum, 3, "expected 1+2=3");
        });

        producer_thread.join().unwrap();
        consumer_thread.join().unwrap();
    });
}

#[test]
fn spsc_no_item_lost_loom() {
    loom::model(|| {
        let (mut producer, mut consumer) = spsc_queue::<u32, 2>();

        let t = thread::spawn(move || {
            loop { if producer.try_push(42u32).is_ok() { break; } loom::hint::spin_loop(); }
        });

        let mut got = false;
        loop {
            if consumer.try_pop().is_some() { got = true; break; }
            loom::hint::spin_loop();
        }
        t.join().unwrap();
        assert!(got);
    });
}