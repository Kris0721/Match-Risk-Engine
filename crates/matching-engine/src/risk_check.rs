// Inline risk check logic before order matching
//! Tier-0 pre-trade risk checks performed inline on the matching hot path.
//!
//! These are intentionally minimal — fixed-cost, branch-predictable,
//! no allocation — because they run for every inbound order before it
//! touches the book. Deeper / cross-account risk runs asynchronously
//! in the `risk-engine` crate via the seqlock-published account state.

use core_types::commands::NewOrder;
use core_types::ids::AccountId;
use core_types::price::Price;
use core_types::qty::Qty;
use core_types::side::Side;
use seqlock::account_risk_state::AccountRiskState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskRejectReason {
    MaxOrderQtyExceeded,
    MaxOrderNotionalExceeded,
    AccountHalted,
    PositionLimitExceeded,
    OpenOrderLimitExceeded,
    PriceOutOfBand,
}

/// Static, per-instrument limits configured at startup (cheap to copy).
#[derive(Debug, Clone, Copy)]
pub struct Tier0Limits {
    pub max_order_qty: Qty,
    pub max_order_notional: u64,
    pub max_open_orders: u32,
    pub max_position_abs: i64,
    /// Reject if order price is more than this many bps away from
    /// the reference price (0 disables the check).
    pub price_band_bps: u32,
}

impl Default for Tier0Limits {
    fn default() -> Self {
        Self {
            max_order_qty: Qty::from_raw(u64::MAX),
            max_order_notional: u64::MAX,
            max_open_orders: u32::MAX,
            max_position_abs: i64::MAX,
            price_band_bps: 0,
        }
    }
}

/// Result of a tier-0 check: either accept, or reject with a reason
/// that the gateway / sequencer can turn into a rejection event.
#[inline]
pub fn check_new_order(
    order: &NewOrder,
    account: AccountId,
    limits: &Tier0Limits,
    risk_state: &AccountRiskState,
    reference_price: Option<Price>,
) -> Result<(), RiskRejectReason> {
    let _ = account;

    if risk_state.is_halted() {
        return Err(RiskRejectReason::AccountHalted);
    }

    if order.qty > limits.max_order_qty {
        return Err(RiskRejectReason::MaxOrderQtyExceeded);
    }

    let px = order.price;
    let notional = notional_of(px, order.qty);
    if notional > limits.max_order_notional {
        return Err(RiskRejectReason::MaxOrderNotionalExceeded);
    }

    if limits.price_band_bps > 0 {
        if let Some(reference) = reference_price {
            if price_out_of_band(px, reference, limits.price_band_bps) {
                return Err(RiskRejectReason::PriceOutOfBand);
            }
        }
    }

    if risk_state.open_order_count() >= limits.max_open_orders {
        return Err(RiskRejectReason::OpenOrderLimitExceeded);
    }

    let prospective_position = projected_position(risk_state.position(), order.side, order.qty);
    if prospective_position.unsigned_abs() as i64 > limits.max_position_abs {
        return Err(RiskRejectReason::PositionLimitExceeded);
    }

    Ok(())
}

#[inline]
fn notional_of(price: Price, qty: Qty) -> u64 {
    // Compute notional as price * qty with safe widening to avoid
    // sign/size issues between i64 and u64.
    let p = price.raw() as i128;
    let q = qty.raw() as i128;
    if p <= 0 {
        return 0;
    }
    let prod = p.saturating_mul(q);
    if prod <= 0 {
        return 0;
    }
    if prod > i128::from(u64::MAX) {
        u64::MAX
    } else {
        prod as u64
    }
}

#[inline]
fn price_out_of_band(price: Price, reference: Price, band_bps: u32) -> bool {
    let p = price.raw() as i128;
    let r = reference.raw() as i128;
    if r == 0 {
        return false;
    }
    let diff = (p - r).abs();
    let bps = (diff * 10_000) / r;
    bps > band_bps as i128
}

#[inline]
fn projected_position(current: i64, side: Side, qty: Qty) -> i64 {
    let delta = qty.raw() as i64;
    match side {
        Side::Buy => current.saturating_add(delta),
        Side::Sell => current.saturating_sub(delta),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::ids::{InstrumentId, OrderId};
    use core_types::commands::OrderType;

    fn mk_order(side: Side, qty: u64, price: u64) -> NewOrder {
        NewOrder {
             account_id: AccountId(1),
             instrument_id: InstrumentId(1),
             client_order_id: core_types::ClientOrderId::new(0),
             side,
             order_type: OrderType::Limit,
             price: Price  ::from_raw(price as i64),
             qty: Qty::from_raw(qty),
             time_in_force: core_types::TimeInForce::Gtc,
        }
    }

    #[test]
    fn rejects_when_halted() {
        let mut state = AccountRiskState::default();
        state.set_halted(true);
        let limits = Tier0Limits::default();
        let order = mk_order(Side::Buy, 10,100);

        let res = check_new_order(&order, AccountId(1), &limits, &state, None);
        assert_eq!(res, Err(RiskRejectReason::AccountHalted));
    }

    #[test]
    fn rejects_qty_over_limit() {
        let state = AccountRiskState::default();
        let limits = Tier0Limits { max_order_qty: Qty::from_raw(5), ..Default::default() };
        let order = mk_order(Side::Buy, 10,100);

        let res = check_new_order(&order, AccountId(1), &limits, &state, None);
        assert_eq!(res, Err(RiskRejectReason::MaxOrderQtyExceeded));
    }

    #[test]
    fn rejects_notional_over_limit() {
        let state = AccountRiskState::default();
        let limits = Tier0Limits { max_order_notional: 500, ..Default::default() };
        let order = mk_order(Side::Buy, 10, 100); // notional = 1000

        let res = check_new_order(&order, AccountId(1), &limits, &state, None);
        assert_eq!(res, Err(RiskRejectReason::MaxOrderNotionalExceeded));
    }

    #[test]
    fn rejects_price_out_of_band() {
        let state = AccountRiskState::default();
        let limits = Tier0Limits { price_band_bps: 100, ..Default::default() }; // 1%
        let order = mk_order(Side::Buy, 1, 110); // 10% away from 100

        let res = check_new_order(&order, AccountId(1), &limits, &state, Some(Price::from_raw(100)));
        assert_eq!(res, Err(RiskRejectReason::PriceOutOfBand));
    }

    #[test]
    fn rejects_position_limit() {
        let mut state = AccountRiskState::default();
        state.set_position(95);
        let limits = Tier0Limits { max_position_abs: 100, ..Default::default() };
        let order = mk_order(Side::Buy, 10,100); // would go to 105

        let res = check_new_order(&order, AccountId(1), &limits, &state, None);
        assert_eq!(res, Err(RiskRejectReason::PositionLimitExceeded));
    }

    #[test]
    fn accepts_within_limits() {
        let state = AccountRiskState::default();
        let limits = Tier0Limits::default();
        let order = mk_order(Side::Buy, 10, 100);

        let res = check_new_order(&order, AccountId(1), &limits, &state, None);
        assert!(res.is_ok());
    }
}