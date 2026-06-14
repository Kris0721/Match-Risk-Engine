// Risk parameters and tier rules config
//! Per-account-tier risk limits, hot-reloadable at runtime.
//!
//! `RiskConfig` is intentionally a plain `Copy` struct so it can be read from
//! an `arc_swap::ArcSwap` on the cold path (Tier-0 gateway check) without any
//! allocation on the hot path (the shard loop reads its own cached copy).

use core_types::{Price, Qty};

/// Risk limits that apply to every account on this shard.
/// All fields use the same fixed-point scale as `Price` / `Qty`.
#[derive(Clone, Copy, Debug)]
pub struct RiskLimits {
    /// Maximum notional value (price * qty) of a single order.
    pub max_order_notional: i64,
    /// Maximum absolute net position (in base-asset Qty ticks) per symbol.
    pub max_position_qty: i64,
    /// Maximum total unrealised loss before liquidation is triggered (in quote ticks).
    pub max_unrealised_loss: i64,
    /// Initial margin fraction (fixed-point, scaled by 1e6).
    /// e.g. 0.10 → 100_000
    pub initial_margin_fraction: i64,
    /// Maintenance margin fraction (fixed-point, scaled by 1e6).
    /// When margin falls below this, the account is liquidated.
    pub maintenance_margin_fraction: i64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_notional:          100_000_000_00, // 100,000 USD (1e8 scale)
            max_position_qty:            10_000_000_000, // 100 BTC (1e8 scale)
            max_unrealised_loss:         10_000_000_00,  // 10,000 USD
            initial_margin_fraction:     100_000,        // 10%
            maintenance_margin_fraction: 50_000,         // 5%
        }
    }
}

/// Shard-level config: limits shared by all accounts on this shard.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub limits: RiskLimits,
    /// Number of accounts owned by this shard.
    pub account_count: usize,
}

impl ShardConfig {
    pub fn new(account_count: usize) -> Self {
        Self {
            limits: RiskLimits::default(),
            account_count,
        }
    }
}

/// Tier-0 static check: can this order even enter the system?
/// Called on the gateway thread — must be cheap (no allocation, no I/O).
#[inline]
pub fn tier0_order_check(
    price: Price,
    qty: Qty,
    limits: &RiskLimits,
) -> Result<(), RejectionReason> {
    let notional = price.0.saturating_mul(qty.0) / 100_000_000; // normalise scale
    if notional > limits.max_order_notional {
        return Err(RejectionReason::NotionalTooLarge);
    }
    if qty.0 <= 0 {
        return Err(RejectionReason::InvalidQty);
    }
    if price.0 <= 0 {
        return Err(RejectionReason::InvalidPrice);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    NotionalTooLarge,
    InvalidQty,
    InvalidPrice,
    PositionLimitExceeded,
    InsufficientMargin,
    AccountFrozen,
}