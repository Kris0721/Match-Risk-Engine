// Loom-based tests for spmc ring buffer
#![cfg(feature = "loom")]

use loom::thread;
use crate::spmc::spmc_queue;

/// Two consumers each independently see every published item.
#[test]
fn spmc_broadcast_loom() {
    loom::model(|| {
        let (mut producer, mut consumers) = spmc_queue::<u64, 4>(2);
        let mut c0 = consumers.remove(0);
        let mut c1 = consumers.remove(0);

        let prod_thread = thread::spawn(move || {
            loop { if producer.try_push(99u64).is_ok() { break; } loom::hint::spin_loop(); }
        });

        let t0 = thread::spawn(move || {
            loop {
                if let Some(v) = c0.try_pop() {
                    assert_eq!(v, 99);
                    break;
                }
                loom::hint::spin_loop();
            }
        });

        let t1 = thread::spawn(move || {
            loop {
                if let Some(v) = c1.try_pop() {
                    assert_eq!(v, 99);
                    break;
                }
                loom::hint::spin_loop();
            }
        });

        prod_thread.join().unwrap();
        t0.join().unwrap();
        t1.join().unwrap();
    });
}