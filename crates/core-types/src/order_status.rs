//! Atomic order-status state machine for dual-engine synchronization.
//!
//! ```text
//! Pending ──CAS──► Addressed        (Primary Engine claimed it)
//! Pending ──CAS──► Unaddressed      (Sorter timed-out and escalated)
//! Unaddressed ──CAS──► FinallyHandled (Second Engine processed it)
//! ```
//!
//! Each transition is performed via `compare_exchange` (CAS) on an `AtomicU8`.
//! Only **one** actor can ever win a CAS — this is the mathematical guarantee
//! that prevents double-fills.

use std::fmt;

/// The status of an order in the dual-engine pipeline.
///
/// Stored as a `u8` so it can be loaded/stored via `AtomicU8` without locking.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    /// Just arrived in the log, not yet processed by either engine.
    Pending       = 0,
    /// Primary Engine successfully claimed and processed it.
    Addressed     = 1,
    /// Sorter determined Primary missed it; escalated to Second Engine.
    Unaddressed   = 2,
    /// Second Engine successfully claimed and processed it.
    FinallyHandled = 3,
}

impl OrderStatus {
    /// Convert from raw `u8`. Returns `None` for values outside the enum range.
    #[inline]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(OrderStatus::Pending),
            1 => Some(OrderStatus::Addressed),
            2 => Some(OrderStatus::Unaddressed),
            3 => Some(OrderStatus::FinallyHandled),
            _ => None,
        }
    }

    /// Returns `true` if this order has been fully handled (by either engine).
    #[inline]
    pub const fn is_terminal(self) -> bool {
        matches!(self, OrderStatus::Addressed | OrderStatus::FinallyHandled)
    }

    /// Returns `true` if this order is still awaiting processing.
    #[inline]
    pub const fn is_pending(self) -> bool {
        matches!(self, OrderStatus::Pending)
    }
}

impl From<OrderStatus> for u8 {
    #[inline]
    fn from(s: OrderStatus) -> u8 {
        s as u8
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderStatus::Pending        => write!(f, "Pending"),
            OrderStatus::Addressed      => write!(f, "Addressed"),
            OrderStatus::Unaddressed    => write!(f, "Unaddressed"),
            OrderStatus::FinallyHandled => write!(f, "FinallyHandled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_u8() {
        for status in [
            OrderStatus::Pending,
            OrderStatus::Addressed,
            OrderStatus::Unaddressed,
            OrderStatus::FinallyHandled,
        ] {
            let v: u8 = status.into();
            assert_eq!(OrderStatus::from_u8(v), Some(status));
        }
    }

    #[test]
    fn invalid_u8_returns_none() {
        assert_eq!(OrderStatus::from_u8(4), None);
        assert_eq!(OrderStatus::from_u8(255), None);
    }

    #[test]
    fn terminal_states() {
        assert!(!OrderStatus::Pending.is_terminal());
        assert!(OrderStatus::Addressed.is_terminal());
        assert!(!OrderStatus::Unaddressed.is_terminal());
        assert!(OrderStatus::FinallyHandled.is_terminal());
    }
}
