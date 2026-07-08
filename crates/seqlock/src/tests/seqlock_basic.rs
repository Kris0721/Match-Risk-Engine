//! Single-threaded sanity tests for `AccountRiskState`. No concurrency
//! here — these just confirm the seqlock's read/write plumbing and field
//! ordering are correct before the loom tests explore interleavings.

use crate::account_risk_state::AccountRiskState;

#[test]
fn new_state_defaults_to_zero() {
    let s = AccountRiskState::new();
    let snap = s.read();
    assert_eq!(snap.balance, 0);
    assert_eq!(snap.used_margin, 0);
    assert!(!snap.frozen);
    assert!(!snap.halted);
    assert_eq!(snap.position, 0);
    assert_eq!(snap.open_order_count, 0);
}

#[test]
fn update_then_read_roundtrips_all_fields() {
    let s = AccountRiskState::new();
    s.update(10_000_00000000, 2_500_00000000, true, false, 5_00000000, 3);
    let snap = s.read();
    assert_eq!(snap.balance, 10_000_00000000);
    assert_eq!(snap.used_margin, 2_500_00000000);
    assert!(snap.frozen);
    assert!(!snap.halted);
    assert_eq!(snap.position, 5_00000000);
    assert_eq!(snap.open_order_count, 3);
}

#[test]
fn repeated_updates_each_fully_overwrite_previous_state() {
    let s = AccountRiskState::new();
    s.update(100, 10, false, false, 0, 0);
    s.update(200, 20, true, true, 1, 1);
    let snap = s.read();
    // Must reflect only the *latest* update, no stale fields left over
    // from the first one.
    assert_eq!(snap.balance, 200);
    assert_eq!(snap.used_margin, 20);
    assert!(snap.frozen);
    assert!(snap.halted);
    assert_eq!(snap.position, 1);
    assert_eq!(snap.open_order_count, 1);
}

#[test]
fn convenience_accessors_agree_with_full_read() {
    let s = AccountRiskState::new();
    s.update(0, 0, false, true, 42, 7);
    assert_eq!(s.is_halted(), true);
    assert_eq!(s.is_frozen(), false);
    assert_eq!(s.position(), 42);
    assert_eq!(s.open_order_count(), 7);
}

#[test]
fn set_halted_preserves_other_fields() {
    let s = AccountRiskState::new();
    s.update(500, 50, true, false, 3, 2);
    s.set_halted(true);
    let snap = s.read();
    assert!(snap.halted);
    // Everything else must be untouched.
    assert_eq!(snap.balance, 500);
    assert_eq!(snap.used_margin, 50);
    assert!(snap.frozen);
    assert_eq!(snap.position, 3);
    assert_eq!(snap.open_order_count, 2);
}

#[test]
fn set_position_preserves_other_fields() {
    let s = AccountRiskState::new();
    s.update(500, 50, false, true, 3, 2);
    s.set_position(99);
    let snap = s.read();
    assert_eq!(snap.position, 99);
    assert_eq!(snap.balance, 500);
    assert_eq!(snap.used_margin, 50);
    assert!(!snap.frozen);
    assert!(snap.halted);
    assert_eq!(snap.open_order_count, 2);
}