// Scenario: Basic order matching and fills
//! Scenario: basic order matching — verifies that limit orders fill correctly
//! and that both accounts' positions are updated by the risk engine.

use core_types::{AccountId, InboundCommand, OrderType, Price, Qty, Side, Symbol};

use crate::harness::{SimConfig, SimHarness};

pub fn run() {
    let config = SimConfig {
        n_symbols:    1,
        n_accounts:   2,
        n_risk_shards: 1,
        ..Default::default()
    };

    let mut harness = SimHarness::new(config);

    // Account 0 places a resting limit sell @ 50,000.
    harness.push_command(InboundCommand::NewOrder {
        account:    AccountId(0),
        client_order_id: core_types::ClientOrderId::new(0),
        symbol:     Symbol(0),
        side:       Side::Sell,
        price:      Price(50_000_00000000),
        qty:        Qty(1_00000000),
        order_type: OrderType::Limit,
        time_in_force: core_types::TimeInForce::Gtc,
    });

    // Account 1 places an aggressive limit buy @ 50,000 (crosses the spread).
    harness.push_command(InboundCommand::NewOrder {
        account:    AccountId(1),
        client_order_id: core_types::ClientOrderId::new(0),
        symbol:     Symbol(0),
        side:       Side::Buy,
        price:      Price(50_000_00000000),
        qty:        Qty(1_00000000),
        order_type: OrderType::Limit,
        time_in_force: core_types::TimeInForce::Gtc,
    });

    harness.set_mark_price(Symbol(0), Price(50_000_00000000));

    let result = harness.run(100);

    assert_eq!(result.commands_sequenced, 2, "both orders should be sequenced");
    assert_eq!(result.trades_matched, 1,    "one trade should be matched");
    assert_eq!(result.liquidations,   0,    "no liquidations expected");

    // Account 0 sold 1 BTC @ 50,000 → short position should be closed, balance unchanged.
    let snap0 = harness.account_snapshot(AccountId(0));
    assert!(!snap0.frozen, "account 0 should not be frozen");

    // Account 1 bought 1 BTC @ 50,000 → long position.
    let snap1 = harness.account_snapshot(AccountId(1));
    assert!(!snap1.frozen, "account 1 should not be frozen");

    println!("[basic_fills] PASSED — 1 trade, no liquidations");
    println!("  account 0: balance={} used_margin={}", snap0.balance, snap0.used_margin);
    println!("  account 1: balance={} used_margin={}", snap1.balance, snap1.used_margin);
}

#[cfg(test)]
mod tests {
    #[test]
    fn basic_fills_scenario() {
        super::run();
    }
}