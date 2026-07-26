use criterion::{black_box, criterion_group, criterion_main, Criterion};

use core_types::commands::{NewOrder, OrderType, TimeInForce};
use core_types::{AccountId, ClientOrderId, InstrumentId, Price, Qty, Side};
use seqlock::account_risk_state::AccountRiskState;

use matching_engine::risk_check::{check_new_order, Tier0Limits};

fn sample_order() -> NewOrder {
    NewOrder {
        account_id: AccountId::new(1),
        instrument_id: InstrumentId::new(1),
        client_order_id: ClientOrderId::new(1),
        side: Side::Buy,
        price: Price::new(100_00),
        order_type: OrderType::Limit,
        qty: Qty::new(10),
        time_in_force: TimeInForce::Gtc,
    }
}

fn bench_check_new_order(c: &mut Criterion) {
    let order = sample_order();
    let limits = Tier0Limits {
        max_order_qty: Qty::new(1_000),
        max_order_notional: 10_000_000,
        max_open_orders: 500,
        max_position_abs: 100_000,
        price_band_bps: 500,
    };
    let risk_state = AccountRiskState::default();

    c.bench_function("check_new_order_pass", |b| {
        b.iter(|| {
            black_box(check_new_order(
                black_box(&order),
                black_box(order.account_id),
                black_box(&limits),
                black_box(&risk_state),
                black_box(Some(Price::new(100_00))),
            ))
        });
    });
}

criterion_group!(benches, bench_check_new_order);
criterion_main!(benches);
