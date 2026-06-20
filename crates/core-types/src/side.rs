use std::fmt;

/// Order side: `Buy` (bid) or `Sell` (ask).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(u8)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    /// Returns the opposite side â€” used heavily during matching to
    /// find the resting book to cross against.
    #[inline(always)]
    pub const fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    /// Sign convention used for position/PnL math: `+1` for Buy,
    /// `-1` for Sell.
    #[inline(always)]
    pub const fn sign(self) -> i64 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }

    #[inline(always)]
    pub const fn is_buy(self) -> bool {
        matches!(self, Side::Buy)
    }

    #[inline(always)]
    pub const fn is_sell(self) -> bool {
        matches!(self, Side::Sell)
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opposite_is_involution() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
        assert_eq!(Side::Buy.opposite().opposite(), Side::Buy);
    }

    #[test]
    fn sign_convention() {
        assert_eq!(Side::Buy.sign(), 1);
        assert_eq!(Side::Sell.sign(), -1);
    }
}

