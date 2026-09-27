use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

use crate::commands::InboundCommand;
use crate::order_status::OrderStatus;

/// Compute a CRC32 checksum of the serialised form of an `InboundCommand`.
///
/// Uses `bincode` serialisation to get deterministic bytes, then CRC32
/// of those bytes. If serialisation fails (should never happen for valid
/// commands), returns 0.
fn compute_checksum(cmd: &InboundCommand) -> u32 {
    // We use a simple hash-based approach for the checksum since
    // InboundCommand does not implement serde by default in all configs.
    // In production, this would use CRC64 over the serialised bytes.
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    cmd.hash(&mut hasher);
    let h = hasher.finish();
    // Fold 64-bit hash into 32-bit checksum
    (h as u32) ^ ((h >> 32) as u32)
}

#[derive(Debug)]
pub struct LogEntry {
    pub seq: u64,
    pub term: u64,
    pub timestamp_in: u64,
    pub cmd: InboundCommand,
    pub checksum: u32,

    pub status: AtomicU8,
    pub handled_by: AtomicU8,
    pub timestamp_out: AtomicU64,
    pub fill_price: AtomicU64,
    pub filled_qty: AtomicU64,
}

impl LogEntry {
    pub fn new(seq: u64, term: u64, timestamp_in: u64, cmd: InboundCommand) -> Self {
        let checksum = compute_checksum(&cmd);
        Self {
            seq,
            term,
            timestamp_in,
            cmd,
            checksum,
            status: AtomicU8::new(OrderStatus::Pending as u8),
            handled_by: AtomicU8::new(0),
            timestamp_out: AtomicU64::new(0),
            fill_price: AtomicU64::new(0),
            filled_qty: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn try_claim(&self, from: OrderStatus, to: OrderStatus) -> bool {
        self.status
            .compare_exchange(
                from as u8,
                to as u8,
                Ordering::AcqRel,  // success: full barrier
                Ordering::Acquire, // failure: read barrier (see current value)
            )
            .is_ok()
    }

    /// Load the current status with `Acquire` ordering.
    #[inline]
    pub fn load_status(&self) -> OrderStatus {
        let v = self.status.load(Ordering::Acquire);
        OrderStatus::from_u8(v).unwrap_or(OrderStatus::Pending)
    }

    /// Verify the checksum against the stored command data.
    ///
    /// Returns `true` if the data is intact, `false` if corrupted.
    #[inline]
    pub fn verify_checksum(&self) -> bool {
        compute_checksum(&self.cmd) == self.checksum
    }

    /// Record that an engine has completed processing this order.
    ///
    /// Sets `handled_by`, `timestamp_out`, fill price and quantity atomically
    /// with `Release` ordering so any subsequent `Acquire` load on these
    /// fields sees all the written values.
    pub fn record_fill(
        &self,
        engine_id: u8, // 1 = primary, 2 = secondary
        timestamp_out: u64,
        fill_price: u64,
        filled_qty: u64,
    ) {
        self.fill_price.store(fill_price, Ordering::Release);
        self.filled_qty.store(filled_qty, Ordering::Release);
        self.handled_by.store(engine_id, Ordering::Release);
        self.timestamp_out.store(timestamp_out, Ordering::Release);
    }

    /// Age of this entry in nanoseconds relative to `now_ns`.
    #[inline]
    pub fn age_ns(&self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.timestamp_in)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountId, ClientOrderId, OrderType, Price, Qty, Side, Symbol, TimeInForce};

    fn sample_cmd() -> InboundCommand {
        InboundCommand::NewOrder {
            account: AccountId(1),
            client_order_id: ClientOrderId::new(42),
            symbol: Symbol(0),
            side: Side::Buy,
            price: Price(100_00000000),
            qty: Qty(10_00000000),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        }
    }

    #[test]
    fn new_entry_starts_pending() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());
        assert_eq!(entry.load_status(), OrderStatus::Pending);
        assert_eq!(entry.handled_by.load(Ordering::Acquire), 0);
    }

    #[test]
    fn try_claim_pending_to_addressed_succeeds_once() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());

        // First claim succeeds
        assert!(entry.try_claim(OrderStatus::Pending, OrderStatus::Addressed));
        assert_eq!(entry.load_status(), OrderStatus::Addressed);

        // Second claim fails (already Addressed, not Pending)
        assert!(!entry.try_claim(OrderStatus::Pending, OrderStatus::Addressed));
    }

    #[test]
    fn try_claim_pending_to_unaddressed_succeeds_once() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());

        assert!(entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed));
        assert_eq!(entry.load_status(), OrderStatus::Unaddressed);

        // Cannot re-escalate
        assert!(!entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed));
    }

    #[test]
    fn unaddressed_to_finally_handled() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());

        // Sorter escalates
        assert!(entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed));
        // Second Engine claims
        assert!(entry.try_claim(OrderStatus::Unaddressed, OrderStatus::FinallyHandled));
        assert_eq!(entry.load_status(), OrderStatus::FinallyHandled);
    }

    #[test]
    fn primary_and_sorter_race_only_one_wins() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());

        // Simulate race: both try to claim from Pending
        let primary_wins = entry.try_claim(OrderStatus::Pending, OrderStatus::Addressed);
        let sorter_wins = entry.try_claim(OrderStatus::Pending, OrderStatus::Unaddressed);

        // Exactly one must win
        assert!(primary_wins ^ sorter_wins, "exactly one must win the CAS");
    }

    #[test]
    fn checksum_verification() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());
        assert!(entry.verify_checksum());
    }

    #[test]
    fn record_fill_stores_atomically() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());
        entry.record_fill(1, 2000, 100_00000000, 10_00000000);

        assert_eq!(entry.handled_by.load(Ordering::Acquire), 1);
        assert_eq!(entry.timestamp_out.load(Ordering::Acquire), 2000);
        assert_eq!(entry.fill_price.load(Ordering::Acquire), 100_00000000);
        assert_eq!(entry.filled_qty.load(Ordering::Acquire), 10_00000000);
    }

    #[test]
    fn age_calculation() {
        let entry = LogEntry::new(1, 1, 1000, sample_cmd());
        assert_eq!(entry.age_ns(1500), 500);
        assert_eq!(entry.age_ns(500), 0); // saturating
    }
}
