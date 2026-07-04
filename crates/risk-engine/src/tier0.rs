// Ultra-fast Tier 0 risk check logic path
//! Tier-0 risk check — runs on the **gateway thread** before an order enters
//! the sequencer. Must be allocation-free and branch-predictable.
//!
//! Tier-0 only has access to:
//!   - The static `RiskLimits` (cheap `Arc` read or inline copy).
//!   - The `AccountRiskState` seqlock (a single SWMR read — no mutex).
//!
//! It does **not** have access to the full position map (that lives on the risk
//! shard thread). Full margin recomputation (Tier-1) happens post-match on the
//! risk shard.

use core_types::{Price, Qty};
use seqlock::AccountRiskState;
use crate::config::{RejectionReason, RiskLimits, tier0_order_check};

/// All the state Tier-0 needs per account. Obtained via seqlock read.
#[derive(Clone, Copy, Debug)]
pub struct AccountSnapshot {
    pub balance: i64,
    pub used_margin: i64,
    pub frozen: bool,
}

/// Perform a Tier-0 static risk check.
///
/// Returns `Ok(())` if the order is safe to forward to the sequencer.
/// Returns `Err(RejectionReason)` if the order must be rejected at the gateway.
///
/// # Arguments
/// * `account_state` — seqlock snapshot for the account.
/// * `limits`        — current `RiskLimits` for this account's tier.
/// * `price`         — order price.
/// * `qty`           — order quantity.
#[inline]
pub fn check(
    account_state: &AccountRiskState,
    limits: &RiskLimits,
    price: Price,
    qty: Qty,
) -> Result<(), RejectionReason> {
    // 1. Read the seqlock — this is a single SWMR read, no mutex.
    let snapshot = account_state.read();

    // 2. Account frozen?
    if snapshot.frozen {
        return Err(RejectionReason::AccountFrozen);
    }

    // 3. Static order-level checks (notional, qty > 0, price > 0).
    tier0_order_check(price, qty, limits)?;

    // 4. Rough margin check: will this order's initial margin fit within
    //    available balance?  (Full mark-to-market check happens on the shard.)
    let qty_i: i64 = qty.0 as i64;
    let order_notional = price.0.saturating_mul(qty_i) / 100_000_000;
    let required_margin = order_notional
        .saturating_mul(limits.initial_margin_fraction)
        / 1_000_000; // scale back from 1e6 fraction representation

    let available_margin = snapshot.balance.saturating_sub(snapshot.used_margin);
    if required_margin > available_margin {
        return Err(RejectionReason::InsufficientMargin);
    }

    Ok(())
}