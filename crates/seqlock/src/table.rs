//! Shared per-account risk-state table.
//!
//! Before this module existed, each `MatchingEngine` shard owned its own
//! standalone `AccountRiskState`, and each `RiskShard` owned a different
//! `Vec<AccountRiskState>`. Nothing wired the two together: tier-0 checks
//! were reading a cell no risk shard ever wrote to.
//!
//! `AccountRiskTable` is the single allocation both sides share via `Arc`:
//! risk shards get write access to accounts they own, matching engines get
//! read access to the whole table (any account can trade any symbol).
//! Indexed directly by account id — no hashing, no resizing.

use crate::account_risk_state::AccountRiskState;

pub struct AccountRiskTable {
    states: Box<[AccountRiskState]>,
}

impl AccountRiskTable {
    /// `capacity` must exceed the highest account id ever used.
    pub fn new(capacity: usize) -> Self {
        Self {
            states: (0..capacity).map(|_| AccountRiskState::default()).collect(),
        }
    }

    /// Panics on out-of-range — an undersized account universe is a
    /// startup config error, not something to silently wrap around.
    #[inline]
    pub fn get(&self, account_id: u64) -> &AccountRiskState {
        &self.states[account_id as usize]
    }

    pub fn capacity(&self) -> usize {
        self.states.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_are_isolated() {
        let table = AccountRiskTable::new(4);
        table.get(0).update(100, 0, false, false, 5, 1);
        table.get(1).update(200, 0, false, false, -5, 2);
        assert_eq!(table.get(0).read().balance, 100);
        assert_eq!(table.get(1).read().balance, 200);
        assert_eq!(table.get(2).read().balance, 0); // untouched account
    }

    #[test]
    #[should_panic]
    fn out_of_range_panics_instead_of_wrapping() {
        AccountRiskTable::new(2).get(2);
    }
}
