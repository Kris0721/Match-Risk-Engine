// Scenario: Account margin liquidation
//! Scenario: liquidation — an account takes on a position so large relative
//! to its balance that maintenance margin is breached immediately, triggering
//! an automatic liquidation command from the risk shard.

use core_types::{AccountId, InboundCommand, OrderType, Price, Qty, Side, Symbol};
use crate::harness::{SimConfig, SimHarness};

pub fn run() {
    let config = SimConfig {
        n_symbols:     1,
        n_accounts:    2,
        n_risk_shards: 1,
        // Tiny initial balance so margin is breached easily.
        initial_balance: 100_00000000, // 100 USD
        ..Default::default()
    };

    let mut harness = SimHarness::new(config);

    // Account 1 rests a sell to provide liquidity.
    harness.push_command(InboundCommand::NewOrder {
        account:    AccountId(1),
        client_order_id: core_types::ClientOrderId::new(0),
        symbol:     Symbol(0),
        side:       Side::Sell,
        price:      Price(50_000_00000000),
        qty:        Qty(10_00000000), // 10 BTC
        order_type: OrderType::Limit { price: Price(50_000_00000000) },
        time_in_force: core_types::TimeInForce::Gtc,
    });

    // Account 0 buys 10 BTC @ 50,000 → notional = 500,000 USD,
    // maintenance margin (5%) = 25,000 USD >> balance of 100 USD → liquidation.
    harness.push_command(InboundCommand::NewOrder {
        account:    AccountId(0),
        client_order_id: core_types::ClientOrderId::new(0),
        symbol:     Symbol(0),
        side:       Side::Buy,
        price:      Price(50_000_00000000),
        qty:        Qty(10_00000000),
        order_type: OrderType::Limit { price: Price(50_000_00000000) },
        time_in_force: core_types::TimeInForce::Gtc,
    });

    // Set mark price to trigger realistic margin calc.
    harness.set_mark_price(Symbol(0), Price(50_000_00000000));

    // Run enough ticks for the fill + risk shard processing + liquidation
    // re-injection to all complete.
    let result = harness.run(500);

    assert_eq!(result.trades_matched, 1, "one trade should match");
    assert!(result.liquidations >= 1,    "at least one liquidation should fire");

    let snap0 = harness.account_snapshot(AccountId(0));
    assert!(snap0.frozen, "account 0 should be frozen after margin breach");

    println!("[liquidation] PASSED — liquidation triggered and account frozen");
    println!("  account 0: balance={} used_margin={} frozen={}",
        snap0.balance, snap0.used_margin, snap0.frozen);
}

#[cfg(test)]
mod tests {
    #[test]
    fn liquidation_scenario() {
        super::run();
    }
}