// Order book implementation containing bids and asks
use core_types::{OrderId, AccountId, Price, Qty, Side, Symbol};
use slotmap::SlotMap;

use crate::level::PriceLevel;
use crate::order::{OrderKey, RestingOrder};

/// Configuration supplied at startup for a single symbol.
#[derive(Debug, Clone)]
pub struct BookConfig {
    pub symbol: Symbol,
    /// Lowest price representable in the ladder (index 0).
    pub tick_floor: Price,
    /// Number of price ticks the ladder covers.
    pub num_ticks: usize,
    /// Pre-allocated capacity for the resting-order arena.
    pub arena_capacity: usize,
}

/// An order book for a single symbol.
///
/// # Ownership / thread model
///
/// `OrderBook` is `!Sync` by default (it contains `SlotMap` which is not
/// `Sync`).  It is owned exclusively by one matching-engine thread and is
/// *never* shared across threads — Rust's type system is the architectural
/// enforcement, not runtime locking.
///
/// # Memory layout
///
/// ```text
/// bids:  [ None, None, Some(PriceLevel), Some(PriceLevel), ... ]
///                         ^                    ^
///                       index 0           index N-1
///                      (tick_floor)       (tick_floor + N-1)
/// ```
///
/// `best_bid_idx` tracks the highest index with a non-empty `PriceLevel` on
/// the bid side; `best_ask_idx` tracks the lowest index on the ask side.
/// Scans move from `best_*` inward, so they visit very few empty slots in
/// liquid conditions.
pub struct OrderBook {
    pub(crate) symbol: Symbol,
    /// Generational arena — `OrderKey` handles make double-free impossible.
    pub(crate) arena: SlotMap<OrderKey, RestingOrder>,
    /// Bid side of the ladder (buy orders), indexed `[0..num_ticks]`.
    pub(crate) bids: Vec<Option<PriceLevel>>,
    /// Ask side of the ladder (sell orders), indexed `[0..num_ticks]`.
    pub(crate) asks: Vec<Option<PriceLevel>>,
    /// Index into `bids` of the current best bid (highest price with resting qty).
    pub(crate) best_bid_idx: Option<usize>,
    /// Index into `asks` of the current best ask (lowest price with resting qty).
    pub(crate) best_ask_idx: Option<usize>,
    /// Price at ladder index 0.
    pub(crate) tick_floor: Price,
    /// Reverse lookup: `OrderId` → `OrderKey` for O(1) cancel.
    pub(crate) id_to_key: std::collections::HashMap<OrderId, OrderKey>,
}

impl OrderBook {
    /// Create a new, empty order book according to `cfg`.
    ///
    /// All allocations happen here — the matching loop never allocates after
    /// `new()` as long as open-order count stays below `arena_capacity`.
    pub fn new(cfg: BookConfig) -> Self {
        let mut arena = SlotMap::with_capacity_and_key(cfg.arena_capacity);
        // Touch the arena so SlotMap initialises its internal storage now.
        let dummy_ids: Vec<OrderKey> = (0..1)
            .map(|_| {
                arena.insert(RestingOrder::new(
                    OrderId(0),
                    AccountId(0),
                    Qty(0),
                    Side::Buy,
                    cfg.tick_floor,
                    0,
                ))
            })
            .collect();
        for k in dummy_ids {
            arena.remove(k);
        }

        let bids: Vec<Option<PriceLevel>> = (0..cfg.num_ticks).map(|_| None).collect();
        let asks: Vec<Option<PriceLevel>> = (0..cfg.num_ticks).map(|_| None).collect();

        Self {
            symbol: cfg.symbol,
            arena,
            bids,
            asks,
            best_bid_idx: None,
            best_ask_idx: None,
            tick_floor: cfg.tick_floor,
            id_to_key: std::collections::HashMap::with_capacity(cfg.arena_capacity),
        }
    }

    // ── Price ↔ ladder-index conversions ─────────────────────────────────

    /// Convert a `Price` to a ladder index.
    ///
    /// Returns `None` if the price is outside the ladder's range.
    #[inline]
    pub fn price_to_idx(&self, price: Price) -> Option<usize> {
        let offset = price.0 - self.tick_floor.0;
        if offset < 0 {
            return None;
        }
        let idx = offset as usize;
        if idx >= self.bids.len() {
            return None;
        }
        Some(idx)
    }

    #[inline]
    pub fn idx_to_price(&self, idx: usize) -> Price {
        Price(self.tick_floor.0 + idx as i64)
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Ensure a `PriceLevel` exists at `idx` on `side`; return a mutable ref.
    #[allow(dead_code)]
    pub(crate) fn level_mut(&mut self, side: Side, idx: usize) -> &mut PriceLevel {
        let price = self.idx_to_price(idx);
        let ladder = match side {
            Side::Buy  => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        ladder[idx].get_or_insert_with(|| PriceLevel::new(price))
    }

    /// Update `best_bid_idx` upward from `hint` (used after an insert).
    pub(crate) fn refresh_best_bid_up(&mut self, hint: usize) {
        match self.best_bid_idx {
            None => self.best_bid_idx = Some(hint),
            Some(cur) if hint > cur => self.best_bid_idx = Some(hint),
            _ => {}
        }
    }

    /// Update `best_ask_idx` downward from `hint` (used after an insert).
    pub(crate) fn refresh_best_ask_down(&mut self, hint: usize) {
        match self.best_ask_idx {
            None => self.best_ask_idx = Some(hint),
            Some(cur) if hint < cur => self.best_ask_idx = Some(hint),
            _ => {}
        }
    }

    /// Walk `best_bid_idx` downward to find the next occupied bid level.
    /// Called after the best bid level becomes empty.
    pub(crate) fn scan_best_bid_down(&mut self) {
        let start = match self.best_bid_idx {
            Some(i) => i,
            None => return,
        };
        for idx in (0..=start).rev() {
            if let Some(lvl) = &self.bids[idx] {
                if !lvl.is_empty() {
                    self.best_bid_idx = Some(idx);
                    return;
                }
            }
        }
        self.best_bid_idx = None;
    }

    /// Walk `best_ask_idx` upward to find the next occupied ask level.
    /// Called after the best ask level becomes empty.
    pub(crate) fn scan_best_ask_up(&mut self) {
        let start = match self.best_ask_idx {
            Some(i) => i,
            None => return,
        };
        for idx in start..self.asks.len() {
            if let Some(lvl) = &self.asks[idx] {
                if !lvl.is_empty() {
                    self.best_ask_idx = Some(idx);
                    return;
                }
            }
        }
        self.best_ask_idx = None;
    }

    // ── Public read accessors ──────────────────────────────────────────────

    pub fn best_bid(&self) -> Option<Price> {
        self.best_bid_idx
            .and_then(|i| self.bids[i].as_ref())
            .filter(|l| !l.is_empty())
            .map(|l| l.price)
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.best_ask_idx
            .and_then(|i| self.asks[i].as_ref())
            .filter(|l| !l.is_empty())
            .map(|l| l.price)
    }

    pub fn symbol(&self) -> Symbol {
        self.symbol
    }

    /// Total resting quantity on the bid side at `price`.
    pub fn bid_qty_at(&self, price: Price) -> Qty {
        self.price_to_idx(price)
            .and_then(|i| self.bids[i].as_ref())
            .map(|l| l.total_qty)
            .unwrap_or(Qty(0))
    }

    /// Total resting quantity on the ask side at `price`.
    pub fn ask_qty_at(&self, price: Price) -> Qty {
        self.price_to_idx(price)
            .and_then(|i| self.asks[i].as_ref())
            .map(|l| l.total_qty)
            .unwrap_or(Qty(0))
    }

    /// Number of resting orders currently in the arena.
    pub fn open_order_count(&self) -> usize {
        self.arena.len()
    }

    /// Look up an open order by its `OrderId`.
    pub fn get_order(&self, id: OrderId) -> Option<&RestingOrder> {
        self.id_to_key.get(&id).and_then(|&k| self.arena.get(k))
    }
}