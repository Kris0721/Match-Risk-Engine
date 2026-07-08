// Simulation test harness
//! Deterministic simulation harness.
//!
//! # Design
//! The harness runs the entire engine — sequencer, matching engines, risk
//! shards — **single-threaded** with a **simulated clock**. There are no real
//! OS threads, no real ring buffers crossing thread boundaries, and no real
//! time. This gives us:
//!
//! - **Full determinism**: given the same `SimConfig` and input sequence, the
//!   harness always produces byte-identical output.
//! - **Reproducibility**: any bug found during a fuzz run can be replayed
//!   exactly from the recorded event log.
//! - **Speed**: no scheduler jitter, no core pinning, no busy-spin overhead.
//!
//! # How it works
//! Instead of real SPSC/SPMC ring buffers the harness uses plain `VecDeque`s
//! and calls each component's `step()` function in a fixed round-robin order
//! once per tick. The logical tick counter is the simulated clock.
//!
//! The harness intentionally mirrors the production topology so that bugs
//! found here are structurally identical to bugs that would appear in the
//! real system.

use std::collections::{HashMap, VecDeque};

use core_types::{
    AccountId, EngineEvent, InboundCommand,
    Price, SequencedCommand, Symbol,
};
use order_book::{OrderBook, book::BookConfig};
use risk_engine::{RiskShard, ShardConfig, MarkPrices};
use seqlock::AccountRiskState;

/// A deterministic clock: just a counter incremented once per harness tick.
#[derive(Default, Clone, Copy, Debug)]
pub struct SimClock(pub u64);

impl SimClock {
    pub fn now_ns(&self) -> u64 { self.0 * 1_000 } // 1 µs per tick
    pub fn tick(&mut self) { self.0 += 1; }
}

/// Configuration for a single simulation run.
#[derive(Clone, Debug)]
pub struct SimConfig {
    /// Number of symbols to simulate.
    pub n_symbols: usize,
    /// Number of accounts to simulate.
    pub n_accounts: usize,
    /// Number of risk shards (must divide n_accounts evenly).
    pub n_risk_shards: usize,
    /// Initial balance credited to every account (quote ticks, 1e8 scale).
    pub initial_balance: i64,
    /// Snapshot interval in sequenced commands.
    pub snapshot_interval: u64,
    /// Lowest price the per-symbol order-book ladder covers.
    pub book_tick_floor: Price,
    /// Number of price ticks the ladder covers, starting at `book_tick_floor`.
    pub book_num_ticks: usize,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            n_symbols:         2,
            n_accounts:        4,
            n_risk_shards:     2,
            initial_balance:   100_000_00000000, // 100,000 USD
            snapshot_interval: 1_000,
            book_tick_floor:   Price::ZERO,
            book_num_ticks:    1024,
        }
    }
}

/// Recorded output of one simulation run.
#[derive(Default, Debug)]
pub struct SimResult {
    /// All engine events emitted in order.
    pub events:            Vec<EngineEvent>,
    /// Total commands sequenced.
    pub commands_sequenced: u64,
    /// Total trades matched.
    pub trades_matched:     u64,
    /// Total liquidations triggered.
    pub liquidations:       u64,
    /// Snapshots taken.
    pub snapshots:          u64,
}

/// Simulated per-symbol matching engine state.
struct SimEngine {
    book:   OrderBook,
    symbol: Symbol,
}
 
impl SimEngine {
    fn new(symbol: Symbol, tick_floor: Price, num_ticks: usize) -> Self {
        let cfg = BookConfig {
            symbol,
            tick_floor,
            num_ticks,
            arena_capacity: 4096,
        };
        Self { book: OrderBook::new(cfg), symbol }
    }

    /// Process one `SequencedCommand` and return any emitted events.
    fn step(&mut self, cmd: SequencedCommand) -> Vec<EngineEvent> {
        // Routing to this engine is done by the harness purely via vector
        // index (`me_queues[symbol.0 as usize]`), so nothing enforces that
        // the command's own `symbol` actually matches this engine. Assert
        // it here to catch a misrouted command instead of silently
        // matching it against the wrong instrument's book.
        debug_assert!(
            match &cmd.cmd {
                InboundCommand::NewOrder { symbol, .. }
                | InboundCommand::Liquidate { symbol, .. } => *symbol == self.symbol,
                InboundCommand::Cancel { .. } | InboundCommand::FreezeAccount { .. } => true,
            },
            "command routed to wrong per-symbol engine: engine_symbol={:?} cmd={:?}",
            self.symbol,
            cmd.cmd
        );
        self.book.apply(cmd).into_iter().collect()
    }
}

/// The top-level simulation harness.
pub struct SimHarness {
    pub config:  SimConfig,
    pub clock:   SimClock,

    // --- Sequencer state ---
    next_seq:    u64,
    /// Commands waiting to be sequenced (arrive from the test / scenario).
    inbound:     VecDeque<InboundCommand>,
    /// Sequenced commands routed to each matching engine.
    me_queues:   Vec<VecDeque<SequencedCommand>>,
    /// WAL log of every sequenced command (in-memory for the sim).
    pub wal:     Vec<SequencedCommand>,

    // --- Matching engines (one per symbol) ---
    engines:     Vec<SimEngine>,
    /// Events emitted by matching engines, waiting to be consumed by
    /// risk shards and other subscribers.
    event_queue: VecDeque<EngineEvent>,

    // --- Risk shards ---
    shards:      Vec<RiskShard>,
    mark_prices: MarkPrices,

    // --- Account risk states (shared via seqlock in production) ---
    /// In the sim these live here for easy inspection.
    pub account_states: Vec<AccountRiskState>,

    /// Snapshot interval (in sequenced commands).
    snapshot_interval: u64,
}

impl SimHarness {
    /// Construct a new harness from `SimConfig`.
    pub fn new(config: SimConfig) -> Self {
        assert!(
            config.n_accounts % config.n_risk_shards == 0,
            "n_accounts must be divisible by n_risk_shards"
        );

        let engines: Vec<SimEngine> = (0..config.n_symbols)
            .map(|i| SimEngine::new(Symbol(i as u16), config.book_tick_floor, config.book_num_ticks))
            .collect();

        let me_queues = (0..config.n_symbols)
            .map(|_| VecDeque::new())
            .collect();

        let accounts_per_shard = config.n_accounts / config.n_risk_shards;
        let shards: Vec<RiskShard> = (0..config.n_risk_shards)
            .map(|i| {
                let start = (i * accounts_per_shard) as u64;
                let end   = start + accounts_per_shard as u64;
                let shard_config = ShardConfig::new(accounts_per_shard);
                let mut shard = RiskShard::new(start..end, shard_config);
                // Seed every account with the initial balance.
                for account_id in start..end {
                    shard.seed_deposit(AccountId(account_id), config.initial_balance);
                }
                shard
            })
            .collect();

        // Flat account state array for inspection (mirrors each shard's states).
        let account_states: Vec<AccountRiskState> = (0..config.n_accounts)
            .map(|_| {
                let s = AccountRiskState::new();
                s.update(config.initial_balance, 0, false, false, 0, 0);
                s
            })
            .collect();

        Self {
            clock:             SimClock::default(),
            next_seq:          0,
            inbound:           VecDeque::new(),
            me_queues,
            wal:               Vec::new(),
            engines,
            event_queue:       VecDeque::new(),
            shards,
            mark_prices:       HashMap::new(),
            account_states,
            snapshot_interval: config.snapshot_interval,
            config,
        }
    }

    /// Inject a command into the inbound queue (called by scenarios / fuzzers).
    pub fn push_command(&mut self, cmd: InboundCommand) {
        self.inbound.push_back(cmd);
    }

    /// Set a mark price for a symbol (used by risk shards for margin calculation).
    pub fn set_mark_price(&mut self, symbol: Symbol, price: Price) {
        self.mark_prices.insert(symbol, price);
    }

    /// Run the harness for `n_ticks` simulation ticks.
    /// Returns a `SimResult` describing what happened.
    pub fn run(&mut self, n_ticks: u64) -> SimResult {
        let mut result = SimResult::default();

        for _ in 0..n_ticks {
            self.clock.tick();

            // --- Step 1: Sequencer — assign seq, route to ME queues ---
            if let Some(cmd) = self.inbound.pop_front() {
                self.next_seq += 1;
                let seq = self.next_seq;
                let ts_ns = self.clock.now_ns();

                let sequenced = SequencedCommand { seq, ts_ns, cmd: cmd.clone() };
                self.wal.push(sequenced.clone());
                result.commands_sequenced += 1;

                // Route to per-symbol ME queue.
                match &cmd {
                    InboundCommand::NewOrder { symbol, .. }
                    | InboundCommand::Liquidate { symbol, .. } => {
                        self.me_queues[symbol.0 as usize].push_back(sequenced);
                    }
                    InboundCommand::Cancel { .. }
                    | InboundCommand::FreezeAccount { .. } => {
                        // Broadcast to all ME queues.
                        for q in self.me_queues.iter_mut() {
                            q.push_back(sequenced.clone());
                        }
                    }
                }

                // Inject snapshot marker if scheduled.
                if seq % self.snapshot_interval == 0 {
                    let marker = EngineEvent::SnapshotMarker { seq };
                    self.event_queue.push_back(marker);
                    result.snapshots += 1;
                }
            }

            // --- Step 2: Matching engines — process one command each ---
            for (i, engine) in self.engines.iter_mut().enumerate() {
                if let Some(cmd) = self.me_queues[i].pop_front() {
                    let events = engine.step(cmd);
                    for ev in events {
                        if matches!(ev, EngineEvent::Trade { .. }) {
                            result.trades_matched += 1;
                        }
                        self.event_queue.push_back(ev);
                    }
                }
            }

            // --- Step 3: Risk shards — process events ---
            while let Some(ev) = self.event_queue.pop_front() {
                result.events.push(ev.clone());

                for shard in self.shards.iter_mut() {
                    for liquidate_cmd in shard.process_event(ev.clone(), &self.mark_prices) {
                        result.liquidations += 1;
                        // Re-inject as a privileged command.
                        self.inbound.push_front(liquidate_cmd);
                    }
                }

                // Sync the flat account_states for test inspection.
                self.sync_account_states();
            }
        }

        result
    }

    /// Copy seqlock state from each shard into the flat `account_states` vec
    /// so tests can inspect any account without knowing which shard owns it.
    fn sync_account_states(&mut self) {
        for shard in &self.shards {
            for (local_idx, state) in shard.states.iter().enumerate() {
                let global_idx = shard.owned.start as usize + local_idx;
                let snap = state.read();
                self.account_states[global_idx]
                    .update(snap.balance, snap.used_margin, snap.frozen, snap.halted, snap.position, snap.open_order_count);
            }
        }
    }

    /// Convenience: read the risk snapshot for one account.
    pub fn account_snapshot(
        &self,
        account: AccountId,
    ) -> seqlock::AccountRiskSnapshot {
        self.account_states[account.0 as usize].read()
    }

    /// Drain and return all events emitted so far.
    pub fn drain_events(&mut self) -> Vec<EngineEvent> {
        std::mem::take(&mut self.event_queue).into_iter().collect()
    }

    /// Return the WAL as a slice (for snapshot/recovery tests).
    pub fn wal(&self) -> &[SequencedCommand] {
        &self.wal
    }
}