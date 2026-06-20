// Quantity definitions and operations
use std::fmt;
use std::ops::{Add, Sub, AddAssign, SubAssign};

/// Fixed-point quantity represented as an integer number of "lots"
/// (smallest tradable unit for the instrument).
///
/// Always non-negative by convention; direction is conveyed by `Side`,
/// not by sign. Use `i64` internally to allow safe subtraction checks
/// without wrapping, but enforce non-negativity via constructors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(transparent)]
pub struct Qty(pub u64);

impl Qty {
    pub const ZERO: Qty = Qty(0);
    pub const MAX: Qty = Qty(u64::MAX);

    #[inline(always)]
    pub const fn new(lots: u64) -> Self {
        Qty(lots)
    }

    #[inline(always)]
    pub const fn lots(self) -> u64 {
        self.0
    }

    #[inline(always)]
    pub fn from_raw(v: u64) -> Self { Self::new(v) }

    #[inline(always)]
    pub fn raw(self) -> u64 { self.0 }

    #[inline(always)]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Saturating subtraction â€” clamps to zero instead of underflowing.
    #[inline(always)]
    pub fn saturating_sub(self, rhs: Qty) -> Qty {
        Qty(self.0.saturating_sub(rhs.0))
    }

    /// Checked subtraction â€” returns `None` if it would underflow.
    #[inline(always)]
    pub fn checked_sub(self, rhs: Qty) -> Option<Qty> {
        self.0.checked_sub(rhs.0).map(Qty)
    }

    #[inline(always)]
    pub fn checked_add(self, rhs: Qty) -> Option<Qty> {
        self.0.checked_add(rhs.0).map(Qty)
    }

    /// Returns the smaller of `self` and `rhs` â€” used heavily when
    /// computing fill quantities during matching.
    #[inline(always)]
    pub fn min(self, rhs: Qty) -> Qty {
        if self.0 < rhs.0 { self } else { rhs }
    }
}

impl Add for Qty {
    type Output = Qty;
    #[inline(always)]
    fn add(self, rhs: Qty) -> Qty {
        Qty(self.0 + rhs.0)
    }
}

impl Sub for Qty {
    type Output = Qty;
    #[inline(always)]
    fn sub(self, rhs: Qty) -> Qty {
        // Debug builds: panic on underflow (catches bugs).
        // Release builds: wraps â€” callers on the hot path should
        // prefer `checked_sub`/`saturating_sub` for safety.
        Qty(self.0 - rhs.0)
    }
}

impl AddAssign for Qty {
    #[inline(always)]
    fn add_assign(&mut self, rhs: Qty) {
        self.0 += rhs.0;
    }
}

impl SubAssign for Qty {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: Qty) {
        self.0 -= rhs.0;
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Qty {
    #[inline(always)]
    fn from(v: u64) -> Self {
        Qty(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_and_saturating() {
        let a = Qty::new(10);
        let b = Qty::new(7);
        assert_eq!(a.min(b), b);
        assert_eq!(b.saturating_sub(a), Qty::ZERO);
        assert_eq!(a.saturating_sub(b), Qty::new(3));
    }

    #[test]
    fn checked_ops() {
        assert_eq!(Qty::new(5).checked_sub(Qty::new(10)), None);
        assert_eq!(Qty::MAX.checked_add(Qty::new(1)), None);
    }

    #[test]
    fn assign_ops() {
        let mut q = Qty::new(10);
        q += Qty::new(5);
        assert_eq!(q, Qty::new(15));
        q -= Qty::new(3);
        assert_eq!(q, Qty::new(12));
    }
}

