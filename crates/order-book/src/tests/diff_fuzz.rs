// Differential fuzzing for the matching engine
//! Differential fuzzing: compare `OrderBook` against a naive reference matcher.
//!
//! Both receive the same randomly-generated command sequence.  After each
//! command the total filled quantities must agree and the reference must not
//! produce a fill where the book doesn't (or vice-versa).
//!
//! Run with `cargo test --test diff_fuzz -- --nocapture` for verbose output,
//! or integrate with `cargo fuzz` / `libfuzzer` for coverage-guided fuzzing.

use core_types::{
    commands::{InboundCommand, OrderType, SequencedCommand, TimeInForce},
    events::EngineEvent,
    AccountId, OrderId, Price, Qty, Side, Symbol,
};
use order_book::{book::BookConfig, OrderBook};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};

// ── Naive reference ───────────────────────────────────────────────────────────

/// Naive order book: `BTreeMap` keyed by (price, seq) for correct
/// price-time priority.  Correct but allocation-heavy — used only as a
/// reference oracle.
struct RefBook {
    /// (price, seq) → (order_id, account, qty_remaining)
    bids: BTreeMap<(i64, u64), (OrderId, AccountId, Qty)>,
    asks: BTreeMap<(i64, u64), (OrderId, AccountId, Qty)>,
    /// order_id → side + key
    index: HashMap<OrderId, (Side, (i64, u64))>,
}

impl RefBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: HashMap::new(),
        }
    }

    fn apply(&mut self, cmd: &SequencedCommand) -> u64 {
        match &cmd.cmd {
            InboundCommand::NewOrder { side, price, qty, account, order_type, time_in_force, .. } => {
                self.ref_new_order(cmd.seq, *side, *price, *qty, *account, *order_type, *time_in_force )
            }
            InboundCommand::Cancel { account, order_id } => {
                self.ref_cancel(*order_id, *account);
                0
            }
            _ => 0,
        }
    }

    fn ref_new_order(
        &mut self,
        seq: u64,
        side: Side,
        price: Price,
        qty: Qty,
        account: AccountId,
        order_type: OrderType,
        time_in_force: TimeInForce,
    ) -> u64 {
        let order_id = OrderId(seq);
        let mut qty_rem = qty.0;
        let mut total_filled = 0u64;

        loop {
            if qty_rem == 0 {
                break;
            }
            // Find best opposing price.
            let (crosses, maker_key) = match side {
                Side::Buy => {
                    // Best ask = smallest (price, seq).
                    match self.asks.iter().next() {
                        Some((&k, _)) if price.0 >= k.0 => (true, Some(k)),
                        _ => (false, None),
                    }
                }
                Side::Sell => {
                    // Best bid = largest price. Among orders tied at that price, we
                    // need the *earliest* (smallest seq) for correct time priority —
                    // `next_back()` alone would give the largest seq at that price,
                    // which is backwards. So find the best price first, then scan
                    // for the minimum-seq entry within that price band.
                    match self.bids.iter().next_back() {
                        Some((&(best_price, _), _)) if price.0 <= best_price => {
                            let key = self
                                .bids
                                .range((best_price, 0)..=(best_price, u64::MAX))
                                .next()
                                .map(|(&k, _)| k);
                            (true, key)
                        }
                        _ => (false, None),
                    }
                }
            };

            if !crosses {
                break;
            }

            let key = maker_key.unwrap();
            let (maker_opposite, maker_map) = match side {
                Side::Buy  => (Side::Sell, &mut self.asks),
                Side::Sell => (Side::Buy, &mut self.bids),
            };
            let entry = maker_map.get_mut(&key).unwrap();
            let fill = qty_rem.min(entry.2 .0);
            entry.2 = Qty(entry.2 .0 - fill);
            qty_rem -= fill;
            total_filled += fill;

            if entry.2 .0 == 0 {
                let maker_oid = entry.0;
                let _ = maker_map.remove(&key);
                self.index.remove(&maker_oid);
            }
        }

        // Rest remainder for Limit orders.
        if qty_rem > 0 && order_type == OrderType::Limit && time_in_force == TimeInForce::Gtc  {
            let k = (price.0, seq);
            match side {
                Side::Buy  => { self.bids.insert(k, (order_id, account, Qty(qty_rem))); }
                Side::Sell => { self.asks.insert(k, (order_id, account, Qty(qty_rem))); }
            }
            self.index.insert(order_id, (side, k));
        }

        total_filled
    }

    fn ref_cancel(&mut self, order_id: OrderId, _account: AccountId) {
        if let Some((side, key)) = self.index.remove(&order_id) {
            match side {
                Side::Buy  => { self.bids.remove(&key); }
                Side::Sell => { self.asks.remove(&key); }
            }
        }
    }
}

// ── Fuzz harness ─────────────────────────────────────────────────────────────

const SYM: Symbol = Symbol(0);
const TICK_FLOOR: i64 = 10_000;
const NUM_TICKS: usize = 200;
const ARENA_CAP: usize = 512;

fn make_book() -> OrderBook {
    OrderBook::new(BookConfig {
        symbol: SYM,
        tick_floor: Price(TICK_FLOOR),
        num_ticks: NUM_TICKS,
        arena_capacity: ARENA_CAP,
    })
}

fn gen_commands(rng: &mut StdRng, n: usize) -> Vec<SequencedCommand> {
    let mut cmds = Vec::with_capacity(n);
    let mut live_orders: Vec<(u64, AccountId)> = Vec::new(); // (seq/order_id, account)
    let accounts = [AccountId(1), AccountId(2), AccountId(3)];

    for seq in 1..=(n as u64) {
        let action = rng.gen_range(0u8..4);
        let account = accounts[rng.gen_range(0..accounts.len())];

        let cmd = if action < 2 || live_orders.is_empty() {
            // New order (weight 2/4 or when no live orders to cancel)
            let side = if rng.gen_bool(0.5) { Side::Buy } else { Side::Sell };
            let price = Price(TICK_FLOOR + rng.gen_range(0..(NUM_TICKS as i64)));
            let qty = Qty(rng.gen_range(1..=20));
            let (order_type, tif) = if rng.gen_bool(0.2) {
                (OrderType::Limit, TimeInForce::Ioc)
            } else {
                (OrderType::Limit, TimeInForce::Gtc)
            };
            live_orders.push((seq, account));
            InboundCommand::NewOrder {
                account,
                client_order_id: core_types::ClientOrderId::new(0),
                symbol: SYM,
                side,
                price,
                qty,
                order_type,
                time_in_force: tif,
            }
        } else {
            // Cancel a random live order
            let idx = rng.gen_range(0..live_orders.len());
            let (oid, acct) = live_orders.swap_remove(idx);
            InboundCommand::Cancel {
                account: acct,
                order_id: OrderId(oid),
            }
        };

        cmds.push(SequencedCommand { seq, ts_ns: seq * 1000, cmd });
    }
    cmds
}

/// Sum the filled quantity from a slice of events.
fn total_filled(events: &[EngineEvent]) -> u64 {
    events
        .iter()
        .map(|e| {
            if let EngineEvent::Trade { qty, .. } = e {
                qty.0
            } else {
                0
            }
        })
        .sum()
}

fn run_one_trial(seed: u64, n_cmds: usize) {
    let mut rng = StdRng::seed_from_u64(seed);
    let cmds = gen_commands(&mut rng, n_cmds);

    let mut book = make_book();
    let mut reference = RefBook::new();

    for cmd in &cmds {
        let events = book.apply(cmd.clone()).into_vec();
        let book_filled = total_filled(&events);
        let ref_filled = reference.apply(cmd);

        assert_eq!(
            book_filled,
            ref_filled,
            "seed={seed} seq={} book_filled={book_filled} ref_filled={ref_filled}\ncmd={:?}",
            cmd.seq,
            cmd.cmd,
        );
    }
}

// ── Test entry points ──────────────────────────────────────────────────────────

#[test]
fn differential_fuzz_fixed_seeds() {
    // A deterministic set of seeds for CI — fast and reproducible.
    let seeds: &[u64] = &[0, 1, 42, 1337, 0xDEAD_BEEF, 999_999, u64::MAX / 3];
    for &seed in seeds {
        run_one_trial(seed, 500);
    }
}

#[test]
#[ignore] // run with `cargo test -- --ignored` for a longer soak
fn differential_fuzz_soak() {
    let mut rng = StdRng::seed_from_u64(0xCAFE_F00D);
    for _ in 0..200 {
        let seed: u64 = rng.gen();
        run_one_trial(seed, 2_000);
    }
}

