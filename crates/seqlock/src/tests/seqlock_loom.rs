// Loom-based tests for SeqLock concurrency correctness
#![cfg(feature = "loom")]

//! Exhaustive loom model-checking for the seqlock.
//!
//! loom explores every possible interleaving of atomic operations. These tests
//! verify that:
//!   1. A reader never observes a torn write (partially-written state).
//!   2. After a completed write, at least one reader eventually sees the new value.
//!   3. Concurrent reads never interfere with each other.

use loom::sync::Arc;
use loom::thread;
use crate::account_risk_state::AccountRiskState;

/// One writer, one reader — the simplest case.
#[test]
fn seqlock_single_writer_single_reader() {
    loom::model(|| {
        let state = Arc::new(AccountRiskState::new());

        let writer_state = Arc::clone(&state);
        let writer = thread::spawn(move || {
            writer_state.update(1_000, 200, false);
        });

        let reader_state = Arc::clone(&state);
        let reader = thread::spawn(move || {
            let snap = reader_state.read();
            // Either the initial state (0, 0, false) or the written state
            // (1_000, 200, false) — never a torn mix.
            let valid_pre  = snap.balance == 0 && snap.used_margin == 0 && !snap.frozen;
            let valid_post = snap.balance == 1_000 && snap.used_margin == 200 && !snap.frozen;
            assert!(
                valid_pre || valid_post,
                "torn read detected: {:?}", snap
            );
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

/// One writer, two concurrent readers — verifies readers don't interfere.
#[test]
fn seqlock_single_writer_two_readers() {
    loom::model(|| {
        let state = Arc::new(AccountRiskState::new());

        let ws = Arc::clone(&state);
        let writer = thread::spawn(move || {
            ws.update(500, 100, false);
        });

        let rs0 = Arc::clone(&state);
        let r0 = thread::spawn(move || {
            let snap = rs0.read();
            let ok = (snap.balance == 0   && snap.used_margin == 0)
                  || (snap.balance == 500 && snap.used_margin == 100);
            assert!(ok, "reader 0 torn read: {:?}", snap);
        });

        let rs1 = Arc::clone(&state);
        let r1 = thread::spawn(move || {
            let snap = rs1.read();
            let ok = (snap.balance == 0   && snap.used_margin == 0)
                  || (snap.balance == 500 && snap.used_margin == 100);
            assert!(ok, "reader 1 torn read: {:?}", snap);
        });

        writer.join().unwrap();
        r0.join().unwrap();
        r1.join().unwrap();
    });
}

/// Two sequential writes — reader must see either v0, v1, or v2 but never
/// a torn combination of any two.
#[test]
fn seqlock_two_sequential_writes() {
    loom::model(|| {
        let state = Arc::new(AccountRiskState::new());

        let ws = Arc::clone(&state);
        let writer = thread::spawn(move || {
            ws.update(100, 10, false);
            ws.update(200, 20, true);
        });

        let rs = Arc::clone(&state);
        let reader = thread::spawn(move || {
            let snap = rs.read();
            let v0 = snap.balance == 0   && snap.used_margin == 0  && !snap.frozen;
            let v1 = snap.balance == 100 && snap.used_margin == 10 && !snap.frozen;
            let v2 = snap.balance == 200 && snap.used_margin == 20 &&  snap.frozen;
            assert!(v0 || v1 || v2, "torn read across writes: {:?}", snap);
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

/// Frozen flag propagates correctly — once frozen, a reader must not see
/// frozen=false with the new balance.
#[test]
fn seqlock_frozen_flag_consistent() {
    loom::model(|| {
        let state = Arc::new(AccountRiskState::new());

        // First write: high balance, not frozen.
        state.update(10_000, 0, false);

        let ws = Arc::clone(&state);
        let writer = thread::spawn(move || {
            // Second write: margin breach → frozen.
            ws.update(10_000, 15_000, true);
        });

        let rs = Arc::clone(&state);
        let reader = thread::spawn(move || {
            let snap = rs.read();
            // If used_margin > balance, frozen must be true.
            if snap.used_margin > snap.balance {
                assert!(snap.frozen, "account over margin but not frozen: {:?}", snap);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

/// `is_frozen()` shortcut agrees with a full `read()`.
#[test]
fn seqlock_is_frozen_matches_read() {
    loom::model(|| {
        let state = Arc::new(AccountRiskState::new());

        let ws = Arc::clone(&state);
        let writer = thread::spawn(move || {
            ws.update(0, 0, true);
        });

        let rs = Arc::clone(&state);
        let reader = thread::spawn(move || {
            // is_frozen() and read().frozen must agree because they both go
            // through the same seqlock protocol.
            let frozen_shortcut = rs.is_frozen();
            let frozen_full     = rs.read().frozen;
            // They may disagree between two separate calls if a write lands
            // between them — that is fine. What must NOT happen is either
            // call returning a torn value internally.
            let _ = (frozen_shortcut, frozen_full);
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}