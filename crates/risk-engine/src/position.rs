// Account positions and margin balance tracking
//! Per-account, per-symbol position tracking.
//!
//! All arithmetic is integer fixed-point (same scale as `Price` / `Qty`).
//! No `f64` anywhere — non-deterministic rounding breaks WAL replay.

use core_types::{Price, Qty, Side};

/// Running position for one (account, symbol) pair.
#[derive(Default, Debug, Clone)]
pub struct Position {
    /// Net quantity: positive = long, negative = short (Qty ticks, 1e8 scale).
    pub net_qty: i64,
    /// Volume-weighted average entry price (Price ticks, 1e8 scale).
    /// Stored as a scaled integer: avg_entry_price_scaled = sum(price * qty) / total_qty.
    pub avg_entry_price: i64,
    /// Cumulative realised PnL in quote ticks (1e8 scale).
    pub realised_pnl: i64,
}

impl Position {
    /// Apply a fill to this position.
    ///
    /// `side`  — the side **of the fill** (Buy increases net_qty, Sell decreases it).
    /// `price` — fill price in Price ticks.
    /// `qty`   — fill quantity in Qty ticks (always positive).
    pub fn apply_fill(&mut self, side: Side, price: Price, qty: Qty) {
        debug_assert!(qty.0 > 0, "fill qty must be positive");

        let signed_qty: i64 = match side {
            Side::Buy  =>  qty.0 as i64,
            Side::Sell => -(qty.0 as i64),
        };

        let prev_qty = self.net_qty;
        let new_qty  = prev_qty + signed_qty;

        if prev_qty == 0 {
            // Opening a fresh position.
            self.avg_entry_price = price.0;
            self.net_qty = new_qty;
            return;
        }

        // Same direction: increase position, recalculate VWAP.
        if (prev_qty > 0) == (signed_qty > 0) {
            // avg = (prev_avg * |prev_qty| + price * qty) / |new_qty|
            let numerator = self.avg_entry_price
                .saturating_mul(prev_qty.abs())
                .saturating_add(price.0.saturating_mul(qty.0 as i64));
            self.avg_entry_price = numerator / new_qty.abs();
            self.net_qty = new_qty;
            return;
        }

        // Opposite direction: partial or full close.
        let close_qty = signed_qty.abs().min(prev_qty.abs());
        // Realised PnL per unit: (fill_price - avg_entry) * direction_sign
        let direction: i64 = if prev_qty > 0 { 1 } else { -1 };
        let pnl_per_unit = (price.0 - self.avg_entry_price) * direction;
        self.realised_pnl = self
            .realised_pnl
            .saturating_add(pnl_per_unit.saturating_mul(close_qty));

        self.net_qty = new_qty;

        if new_qty == 0 {
            self.avg_entry_price = 0;
        } else if (new_qty > 0) != (prev_qty > 0) {
            // Position flipped direction: the remaining qty is a new entry.
            self.avg_entry_price = price.0;
        }
        // If net_qty still same direction (partial close), avg_entry unchanged.
    }

    /// Unrealised PnL given the current mark price (in quote ticks, 1e8 scale).
    #[inline]
    pub fn unrealised_pnl(&self, mark_price: Price) -> i64 {
        if self.net_qty == 0 {
            return 0;
        }
        let direction: i64 = if self.net_qty > 0 { 1 } else { -1 };
        (mark_price.0 - self.avg_entry_price)
            .saturating_mul(self.net_qty.abs())
            .saturating_mul(direction)
            // Divide out double-scale: price(1e8) * qty(1e8) / 1e8 = quote(1e8)
            / 100_000_000
    }

    /// Notional value of the current position at mark price (always positive).
    #[inline]
    pub fn notional(&self, mark_price: Price) -> i64 {
        mark_price.0.saturating_mul(self.net_qty.abs()) / 100_000_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{Price, Qty, Side};

    #[test]
    fn open_long_then_close() {
        let mut pos = Position::default();
        pos.apply_fill(Side::Buy, Price(50_000_00000000), Qty(1_00000000)); // buy 1 BTC @ 50000
        assert_eq!(pos.net_qty, 1_00000000);
        assert_eq!(pos.avg_entry_price, 50_000_00000000);

        pos.apply_fill(Side::Sell, Price(51_000_00000000), Qty(1_00000000)); // sell 1 BTC @ 51000
        assert_eq!(pos.net_qty, 0);
        // realised pnl = (51000 - 50000) * 1e8 = 1000_00000000
        assert_eq!(pos.realised_pnl, 1_000_00000000);
    }

    #[test]
    fn partial_close_then_flip() {
        let mut pos = Position::default();
        pos.apply_fill(Side::Buy, Price(100_00000000), Qty(3_00000000));
        pos.apply_fill(Side::Sell, Price(110_00000000), Qty(5_00000000));
        // net_qty should be -2 (short 2)
        assert_eq!(pos.net_qty, -2_00000000);
        // avg entry for the new short leg = 110
        assert_eq!(pos.avg_entry_price, 110_00000000);
    }

    #[test]
    fn unrealised_pnl_long() {
        let mut pos = Position::default();
        pos.apply_fill(Side::Buy, Price(100_00000000), Qty(2_00000000)); // buy 2 @ 100
        // mark @ 120 → pnl = (120-100)*2 = 40
        let upnl = pos.unrealised_pnl(Price(120_00000000));
        assert_eq!(upnl, 40_00000000);
    }
}