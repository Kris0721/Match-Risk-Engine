// Identifier types (e.g., Order ID, User ID, Asset ID)
use std::fmt;
use std::num::NonZeroU64;

/// Macro to generate a newtype wrapper around `u64` with the common
/// trait impls every ID type in this engine needs.
macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
                #[repr(transparent)]
        pub struct $name(pub u64);

        impl $name {
            #[inline(always)]
            pub const fn new(v: u64) -> Self {
                $name(v)
            }

            #[inline(always)]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<u64> for $name {
            #[inline(always)]
            fn from(v: u64) -> Self {
                $name(v)
            }
        }
    };
}

id_type!(
    /// Globally unique order identifier, assigned by the sequencer
    /// at the moment an order is admitted into the system.
    OrderId
);

id_type!(
    /// Identifier for a trading account. Used as the shard key for
    /// the risk engine (`risk-engine/shard.rs` hashes on this).
    AccountId
);

id_type!(
    /// Identifier for a tradable instrument (one order book per
    /// `InstrumentId`).
    InstrumentId
);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Symbol(pub u16);

id_type!(
    /// Client-supplied order identifier, echoed back in execution
    /// reports for client-side correlation. Distinct from `OrderId`,
    /// which is the engine's internal identifier.
    ClientOrderId
);

/// Monotonically increasing sequence number assigned by the
/// sequencer to every inbound command and outbound event.
///
/// Backed by `NonZeroU64` so that `0` can be reserved as a sentinel
/// for "no sequence number yet" without needing `Option`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(transparent)]
pub struct SequenceNo(NonZeroU64);

impl SequenceNo {
    pub const FIRST: SequenceNo = SequenceNo(match NonZeroU64::new(1) {
        Some(v) => v,
        None => unreachable!(),
    });

    /// Creates a `SequenceNo` from a raw `u64`. Returns `None` if `v == 0`.
    #[inline(always)]
    pub const fn new(v: u64) -> Option<Self> {
        match NonZeroU64::new(v) {
            Some(nz) => Some(SequenceNo(nz)),
            None => None,
        }
    }

    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the next sequence number. Panics on overflow (would
    /// require ~584 years at 1 billion seq/sec â€” treated as
    /// unreachable in practice).
    #[inline(always)]
    pub fn next(self) -> SequenceNo {
        SequenceNo(NonZeroU64::new(self.0.get() + 1).expect("sequence number overflow"))
    }
}

impl fmt::Display for SequenceNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_roundtrip() {
        let id = OrderId::new(42);
        assert_eq!(id.get(), 42);
        assert_eq!(OrderId::from(42u64), id);
    }

    #[test]
    fn sequence_no_basic() {
        assert_eq!(SequenceNo::new(0), None);
        let first = SequenceNo::FIRST;
        assert_eq!(first.get(), 1);
        assert_eq!(first.next().get(), 2);
    }
}


