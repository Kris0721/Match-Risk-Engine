// Price definitions and operations
use std::fmt;
use std::ops::{Add, Sub, Neg};

/// Fixed-point price represented as an integer number of "ticks".
///
/// The actual decimal scale (tick size) is defined per-instrument and
/// is NOT encoded here — this keeps `Price` a plain `i64` for max
/// performance. Conversion to/from decimal happens at the edges
/// (gateway / market data), never on the matching hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(transparent)]
pub struct Price(pub i64);

impl Price {
    pub const ZERO: Price = Price(0);
    pub const MIN: Price = Price(i64::MIN);
    pub const MAX: Price = Price(i64::MAX);

    #[inline(always)]
    pub const fn new(ticks: i64) -> Self {
        Price(ticks)
    }

    #[inline(always)]
    pub const fn ticks(self) -> i64 {
        self.0
    }

    #[inline(always)]
    pub fn from_raw(v: i64) -> Self { Self::new(v) }

    #[inline(always)]
    pub fn raw(self) -> i64 { self.0 }

    #[inline(always)]
    pub fn checked_add(self, rhs: Price) -> Option<Price> {
        self.0.checked_add(rhs.0).map(Price)
    }

    #[inline(always)]
    pub fn checked_sub(self, rhs: Price) -> Option<Price> {
        self.0.checked_sub(rhs.0).map(Price)
    }

    #[inline(always)]
    pub fn is_positive(self) -> bool {
        self.0 > 0
    }
}

impl Add for Price {
    type Output = Price;
    #[inline(always)]
    fn add(self, rhs: Price) -> Price {
        Price(self.0 + rhs.0)
    }
}

impl Sub for Price {
    type Output = Price;
    #[inline(always)]
    fn sub(self, rhs: Price) -> Price {
        Price(self.0 - rhs.0)
    }
}

impl Neg for Price {
    type Output = Price;
    #[inline(always)]
    fn neg(self) -> Price {
        Price(-self.0)
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for Price {
    #[inline(always)]
    fn from(v: i64) -> Self {
        Price(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic() {
        let a = Price::new(100);
        let b = Price::new(50);
        assert_eq!(a + b, Price::new(150));
        assert_eq!(a - b, Price::new(50));
        assert_eq!(-a, Price::new(-100));
    }

    #[test]
    fn checked_ops_overflow() {
        assert_eq!(Price::MAX.checked_add(Price::new(1)), None);
        assert_eq!(Price::MIN.checked_sub(Price::new(1)), None);
    }

    #[test]
    fn ordering() {
        assert!(Price::new(10) < Price::new(20));
        assert!(Price::ZERO < Price::new(1));
    }
}