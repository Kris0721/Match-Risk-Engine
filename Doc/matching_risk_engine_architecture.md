# Contention-Free Matching & Risk Engine — Architecture Design (Rust)

A production-grade design for a sharded, single-writer matching engine and risk engine, built around the principle that **the fastest synchronization is the synchronization that never has to happen.**

---

## 1. Design Goals & Non-Goals

| Goal | Target |
|---|---|
| Per-symbol matching throughput | 1–5M orders/sec/core (price-time priority, FIFO) |
| p99.9 matching latency | < 1µs from ring-buffer dequeue to event publish |
| Hot-path heap allocations | Zero |
| Hot-path syscalls | Zero (no logging, no I/O, no locks that can park) |
| Determinism | Full system state is a pure function of `(snapshot, ordered_input_log)` |
| Recovery | Replay from WAL + periodic per-shard snapshots |

**Non-goals:** this document does not cover wire protocols, auth, or matching algorithm variants (pro-rata, etc.) — it assumes standard price-time priority and focuses on the *concurrency and ownership architecture*.

---

## 2. Core Thesis: Contention-Free, Not Just Lock-Free

"Lock-free" in the formal CS sense (CAS loops, hazard pointers, etc.) is the wrong target for the core engine. Generic lock-free data structures still suffer **retry storms** under contention — the CAS loop degrades exactly when load is highest, which is the worst possible failure mode for an exchange.

The actual goal is **contention-free by construction**: every piece of mutable state has **exactly one thread that can write it**, enforced by Rust's ownership model, not by runtime checks. Communication between owners happens via **single-producer/single-consumer (SPSC) channels**, which are the only place a CAS-equivalent (atomic load/store with `Acquire`/`Release`) is needed — and SPSC channels under normal operation have *zero* contention because there's only ever one producer and one consumer touching each cursor.

> **Golden rule:** the matching engine thread must never stall — not for the allocator, not for a slow consumer, not for I/O, not for a lock. If something *can* block, it does not live on this thread.

This gives us three categories of synchronization, in order of preference:

1. **None** — single-threaded ownership (order books, account state).
2. **SPSC/SPMC ring buffers** — atomics used only for cursor publication, never CAS, never contended in steady state.
3. **Single-Writer-Multi-Reader (SWMR) atomics / seqlocks** — for state that many threads need to *read* but only one thread *writes* (e.g., account balances).

CAS-based structures (lock-free queues, `DashMap`, etc.) are deliberately **excluded from the hot path** and reserved for cold/admin paths only.

---

## 3. System Topology

```
                 ┌────────────────────────────────────────────┐
                 │           Ingress Gateways (N, tokio)        │
                 │   parse → authz → Tier-0 static risk check   │
                 └───────────────────┬──────────────────────────┘
                                      │ SPSC → Sequencer
                                      ▼
                 ┌────────────────────────────────────────────┐
                 │      Sequencer (1 thread, pinned core)        │
                 │  assigns global monotonic seq, routes by      │
                 │  symbol to per-symbol SPSC inbound queues      │
                 └──┬───────────────┬───────────────┬────────────┘
                     │ SPSC          │ SPSC          │ SPSC
                     ▼               ▼               ▼
              ┌───────────┐   ┌───────────┐   ┌───────────┐
              │ ME[BTCUSD]│   │ ME[ETHUSD]│   │ ME[...]   │  ← single-writer
              │ pinned    │   │ pinned    │   │ pinned    │     order books,
              │ core      │   │ core      │   │ core      │     no allocation
              └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
                     │  EngineEvent fan-out (SPMC, independent consumer cursors)
        ┌────────────┼────────────┬───────────────┬───────────────┐
        ▼            ▼             ▼               ▼               ▼
   ┌─────────┐ ┌─────────┐  ┌──────────────┐ ┌────────────┐ ┌────────────┐
   │RiskShard│ │RiskShard│  │ Market Data   │ │  WAL Writer │ │  Metrics    │
   │ [accts  │ │ [accts  │  │ Publisher     │ │  (mmap log) │ │ Aggregator  │
   │ 0..N/2) │ │ N/2..N) │  │ (best-effort) │ │             │ │             │
   └────┬────┘ └────┬────┘  └──────────────┘ └────────────┘ └────────────┘
        │ Liquidate/Freeze commands (re-enter via Sequencer, treated as
        └──────────────────────────────────────────────────────► privileged input)
```

Key properties:
- Every box is **a single OS thread, pinned to a dedicated core** (no preemption from other engine threads; isolate via `isolcpus`/`taskset`).
- Arrows are **SPSC or SPMC ring buffers** — never shared mutexes.
- Risk shards subscribe to the fan-out from **every** matching engine (an account can hold positions across symbols), but each shard owns a disjoint, contiguous range of `AccountId`s — no two threads ever write the same account.

---

## 4. Core Types & Numeric Representation

All prices/quantities are fixed-point integers. Floats are banned from the hot path — non-deterministic rounding across platforms is unacceptable for a system whose state must be exactly replayable.

```rust
/// Fixed-point price, scaled by 1e8.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
pub struct Price(pub i64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
pub struct Qty(pub i64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct OrderId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct AccountId(pub u32);

/// Interned symbol index (not a string) — used for array indexing into
/// per-symbol engine/risk tables.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Symbol(pub u16);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side { Buy, Sell }
```

The **Sequencer** is the only component allowed to mint identity:

```rust
pub enum InboundCommand {
    NewOrder { account: AccountId, symbol: Symbol, side: Side,
               price: Price, qty: Qty, order_type: OrderType },
    Cancel   { account: AccountId, order_id: OrderId },
    // Privileged: emitted by risk shards, re-enters via Sequencer for
    // total ordering and audit-log inclusion.
    Liquidate { account: AccountId, symbol: Symbol },
    FreezeAccount { account: AccountId },
}

pub struct SequencedCommand {
    pub seq: u64,        // global monotonic, assigned by Sequencer
    pub ts_ns: u64,       // TSC or CLOCK_MONOTONIC at sequencing time
    pub cmd: InboundCommand,
}
```

---

## 5. Matching Engine

### 5.1 Sharding: one symbol, one thread, forever

Each symbol is permanently bound to exactly one matching-engine thread for its lifetime. The order book is a plain `struct` with **no internal synchronization at all** — Rust's `!Sync` is the architectural enforcement: the type simply never appears behind a `&` that crosses a thread boundary.

### 5.2 Order Book: arena-backed price ladder

Two design choices matter most for cache behavior:

1. **Price levels are array-indexed**, not tree-based. For liquid markets the active price range is bounded, so `Vec<Option<PriceLevel>>` indexed by `(price - tick_floor) / tick_size` gives O(1) access and excellent locality for best-bid/ask scans.
2. **Orders live in a generational arena (`slotmap`)**, referenced by `OrderKey`, not by pointer. This avoids pointer-chasing through heap allocations and makes use-after-free / double-cancel structurally impossible without `unsafe`.

```rust
use slotmap::{SlotMap, new_key_type};

new_key_type! { pub struct OrderKey; }

pub struct RestingOrder {
    pub id: OrderId,
    pub account: AccountId,
    pub qty_remaining: Qty,
    pub side: Side,
    next: Option<OrderKey>,   // intrusive doubly-linked FIFO queue
    prev: Option<OrderKey>,
}

pub struct PriceLevel {
    pub price: Price,
    head: Option<OrderKey>,
    tail: Option<OrderKey>,
    pub total_qty: Qty,
}

pub struct OrderBook {
    arena: SlotMap<OrderKey, RestingOrder>,
    bids: Vec<Option<PriceLevel>>,   // index 0 = lowest tick in active range
    asks: Vec<Option<PriceLevel>>,
    best_bid_idx: Option<usize>,
    best_ask_idx: Option<usize>,
    tick_floor: Price,                // price at index 0
}
```

The arena is **pre-sized at startup** to the expected max open-order count for that symbol. `slotmap` reuses freed slots, so steady-state operation performs zero allocations after warm-up.

### 5.3 Main loop

```rust
pub fn run_matching_engine(
    symbol: Symbol,
    inbound: SpscConsumer<SequencedCommand, INBOUND_CAP>,
    outbound: SpmcProducer<EngineEvent, OUTBOUND_CAP>,
    mut book: OrderBook,
) -> ! {
    core_affinity::set_for_current(pinned_core_for(symbol));
    loop {
        match inbound.try_pop() {
            Some(cmd) => {
                let events = book.apply(cmd);   // pure fn, no I/O, no alloc
                for ev in events {
                    outbound.publish(ev);       // never blocks (see §6.2)
                }
            }
            None => std::hint::spin_loop(),
        }
    }
}
```

`book.apply()` is a pure state transition: `(OrderBook, SequencedCommand) -> (OrderBook, Vec<EngineEvent>)` — conceptually, in practice the event list is a small fixed-size `SmallVec` on the stack to avoid even that allocation.

```rust
pub enum EngineEvent {
    Accepted { seq: u64, order_id: OrderId, ts_ns: u64 },
    Rejected { seq: u64, order_id: OrderId, reason: RejectReason },
    Trade {
        seq: u64, symbol: Symbol, price: Price, qty: Qty, ts_ns: u64,
        maker_order: OrderId, taker_order: OrderId,
        maker_acct: AccountId, taker_acct: AccountId,
    },
    Cancelled { seq: u64, order_id: OrderId },
    BookTop { seq: u64, symbol: Symbol, bid: Option<Price>, ask: Option<Price> },
}
```

---

## 6. The Lock-Free Fabric: Ring Buffers

### 6.1 SPSC — cursor-based, cache-line padded

```rust
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(align(64))]
struct CachePadded<T>(T);

pub struct Spsc<T, const N: usize> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    head: CachePadded<AtomicUsize>, // written only by consumer
    tail: CachePadded<AtomicUsize>, // written only by producer
}

// SAFETY: head/tail live in separate cache lines (CachePadded) so producer
// and consumer never invalidate each other's cache line on cursor updates.
// Slot N is only touched by the producer until `tail` publishes it with
// Release, and only by the consumer after it observes that publish with
// Acquire — this establishes the happens-before edge for the slot's data,
// satisfying the aliasing requirements for the UnsafeCell access below.
unsafe impl<T: Send, const N: usize> Send for Spsc<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for Spsc<T, N> {}

impl<T, const N: usize> Spsc<T, N> {
    const MASK: usize = N - 1; // N must be a power of two

    pub fn try_push(&self, val: T) -> Result<(), T> {
        let tail = self.tail.0.load(Ordering::Relaxed);   // only producer writes this
        let head = self.head.0.load(Ordering::Acquire);   // observe consumer progress
        if tail.wrapping_sub(head) == N { return Err(val); } // full
        unsafe { (*self.buf[tail & Self::MASK].get()).write(val); }
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release); // publish slot
        Ok(())
    }

    pub fn try_pop(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);   // observe producer's data
        if head == tail { return None; }
        let val = unsafe { (*self.buf[head & Self::MASK].get()).assume_init_read() };
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(val)
    }
}
```

Production version adds **batch claim/publish** (Disruptor-style): claim a contiguous range of slots with one cursor update, write N items, publish once. This amortizes the cache-coherency cost of cursor updates across many messages — critical when the Sequencer is fanning out to several matching engines per microsecond.

### 6.2 SPMC fan-out — independent consumer cursors, producer never blocks

The matching engine's `EngineEvent` stream is consumed by multiple independent parties (risk shards, market data, WAL, metrics). Each consumer owns its **own** read cursor — a plain local `usize`, not shared — so consumers never contend with each other; the only shared cursor is the single producer's `write_cursor`.

```rust
pub struct Spmc<T, const N: usize> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    write_cursor: CachePadded<AtomicUsize>,
    consumer_min_read: [CachePadded<AtomicUsize>; MAX_CONSUMERS], // for backpressure detection
}

pub struct SpmcConsumer<'a, T, const N: usize> {
    ring: &'a Spmc<T, N>,
    slot: usize,        // which entry in consumer_min_read this consumer updates
    read_cursor: usize, // PRIVATE — no atomics needed for self-tracking
}
```

**Backpressure policy:** the producer (matching engine) **never blocks**. `consumer_min_read` is scanned only by a low-priority background watchdog, not by the producer's publish path. If a consumer falls more than `N` slots behind, it is **dropped from the live fan-out** and must resynchronize from the WAL — a slow consumer degrades to "eventually consistent via replay," never to "the exchange stops trading."

### 6.3 Spin vs. park

| Thread | Strategy | Rationale |
|---|---|---|
| Matching engine | Busy-spin (`spin_loop()`), `SCHED_FIFO`, isolated core | Cannot tolerate scheduler latency |
| Risk shards | Busy-spin or short exponential backoff | Near-hot-path, but tolerates µs jitter |
| WAL writer, market data, metrics | Park/wake (`eventfd`/condvar) | Not latency-critical; free up the core |

---

## 7. Risk Engine

### 7.1 Account sharding

`AccountId` space is partitioned into **contiguous, disjoint ranges**, one per risk-shard thread. Each shard owns a plain `Vec<AccountRiskState>` (or array) for its range — `std::collections::HashMap`, not `DashMap`, since there is exactly one writer. Each shard subscribes to the `EngineEvent` fan-out from **every** matching engine (cross-symbol exposure).

### 7.2 SWMR account state via seqlock

Gateways and matching engines need to **read** account balance/margin (Tier-1 check, §7.3) without ever blocking the risk shard's writes, and without the risk shard ever blocking on readers. A **seqlock** gives wait-free reads with single-writer updates:

```rust
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

#[repr(align(64))]
pub struct AccountRiskState {
    seq: AtomicU64,              // even = stable, odd = writer in flight
    balance: UnsafeCell<i64>,     // written only by the owning risk shard
    used_margin: UnsafeCell<i64>, // written only by the owning risk shard
    pub frozen: AtomicBool,        // independent flag, set by risk shard / kill switch
}

// SAFETY: `balance`/`used_margin` are written by exactly one thread (the
// owning risk shard). The seq-number Release/Acquire pair establishes
// happens-before for readers: a reader that observes an even `seq` before
// AND after reading the fields is guaranteed to see a value that was fully
// written by the single writer (no torn read across the two i64 fields).
unsafe impl Sync for AccountRiskState {}

impl AccountRiskState {
    /// Writer path — risk shard only, called after processing a Trade event.
    pub fn update(&self, new_balance: i64, new_margin: i64) {
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Release); // mark "in flight"
        unsafe {
            *self.balance.get() = new_balance;
            *self.used_margin.get() = new_margin;
        }
        self.seq.store(s.wrapping_add(2), Ordering::Release); // mark "stable"
    }

    /// Reader path — gateways and matching engines. Wait-free except under
    /// the vanishingly rare race with an in-flight write (retries, never blocks).
    pub fn snapshot(&self) -> (i64, i64) {
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 == 1 { continue; } // writer mid-update
            let vals = unsafe { (*self.balance.get(), *self.used_margin.get()) };
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 { return vals; }
        }
    }

    pub fn available_balance(&self) -> i64 {
        let (bal, margin) = self.snapshot();
        bal - margin
    }
}
```

For pure top-of-book broadcast (price + qty, where joint consistency is "nice to have" but not safety-critical), a cheaper alternative is **packing both fields into a single `AtomicU64`** and doing one atomic store/load — no seqlock needed when the data fits.

### 7.3 Three-tier risk model

| Tier | Where | Latency budget | Mechanism | Can it stall matching? |
|---|---|---|---|---|
| 0 — Static limits (fat-finger, price collars, max notional) | Gateway, pre-sequencing | tens of ns | Pure function over `ArcSwap<RiskConfig>` | No — rejected before entering the system |
| 1 — Pre-acceptance balance/margin | Inside matching engine, before book mutation | a few ns (2 atomic loads via seqlock) | `AccountRiskState::available_balance()` | No — read-only, wait-free |
| 2 — Post-trade reconciliation, margin calls, liquidation | Risk shard, fully async | µs–ms | Full position/PnL/margin recompute on `Trade` events | No — fully decoupled |

Tier 1 is the only place the matching engine touches "external" state, and it is a **read-only seqlock snapshot** — it cannot block, deadlock, or allocate.

### 7.4 Risk shard loop

```rust
pub fn run_risk_shard(
    owned_accounts: Range<AccountId>,
    inbound: SpmcConsumer<EngineEvent, FANOUT_CAP>,
    states: &mut [AccountRiskState],            // owned exclusively by this shard
    positions: &mut HashMap<(AccountId, Symbol), Position>, // plain HashMap — single writer
    commands_out: SpscProducer<InboundCommand, CMD_CAP>,    // -> Sequencer
) -> ! {
    loop {
        let Some(ev) = inbound.try_pop() else { std::hint::spin_loop(); continue };
        if let EngineEvent::Trade { maker_acct, taker_acct, symbol, price, qty, .. } = ev {
            for acct in [maker_acct, taker_acct] {
                if !owned_accounts.contains(&acct) { continue; }
                let pos = positions.entry((acct, symbol)).or_default();
                apply_fill(pos, price, qty);
                let (balance, margin) = recompute_margin(pos, &positions, acct);
                states[idx_of(acct)].update(balance, margin);

                if margin > balance {
                    // re-enter via Sequencer for total ordering + audit trail
                    commands_out.try_push(InboundCommand::Liquidate { account: acct, symbol }).ok();
                }
            }
        }
    }
}
```

### 7.5 Kill switches

- **Global halt**: a single `static GLOBAL_HALT: AtomicBool`, checked by the Sequencer before fan-out. One store, instant effect across all symbols — no per-shard coordination needed.
- **Per-account freeze**: the `frozen: AtomicBool` field in `AccountRiskState`, set by its owning risk shard (or an ops tool routed through the Sequencer as a privileged command). Checked at Tier 0/1.

---

## 8. Memory Management

- **Arenas at startup**: `SlotMap` for resting orders, sized to expected peak open-order count per symbol. No `Vec::push`-driven reallocation in steady state.
- **Generational keys, not pointers**: `OrderKey` from `slotmap` makes double-free/use-after-free a type-level impossibility without `unsafe`, at zero runtime cost vs. raw indices.
- **No `Arc`/`Rc` in the hot path**: atomic refcounting is a hidden contention point. Ownership is static (one engine owns its book; one risk shard owns its accounts).
- **Global allocator**: even though the hot path never calls it, set `mimalloc` or `jemalloc` as the global allocator — cold-path allocations (config reload, connection setup) shouldn't cause glibc malloc lock contention that bleeds into scheduling jitter for pinned cores.

---

## 9. Persistence & Recovery

- **WAL**: the Sequencer writes `(seq, ts_ns, cmd)` to an mmap'd append-only log *before or concurrently with* fan-out. Recovery detects gaps by sequence number, not by trusting any single component's state.
- **Snapshotting via sequence barrier**: the Sequencer periodically injects a `SnapshotMarker(seq)` system command into every per-symbol and per-shard queue. When a thread processes that marker, it serializes its **own** state (order book / account states) to disk via `rkyv` (zero-copy deserialization on restart) — no cross-thread coordination, because every thread snapshots at the *same logical sequence point* independently.
- **Recovery**: load each shard's latest snapshot, then replay the WAL from `snapshot.seq + 1`. Because the whole system is `(snapshot, ordered_log) -> state`, recovery is deterministic and trivially parallel across shards.

---

## 10. Observability

- **Per-thread `CachePadded<AtomicU64>` counters**, written with `Ordering::Relaxed` (single-writer per counter — no contention, store is essentially free).
- A dedicated, parked aggregator thread periodically reads all counters and exports them — never on the hot path.
- **`hdrhistogram`** per thread for latency percentiles (e.g., "Sequencer dequeue → `Trade` publish"); merged by the aggregator.
- **No `println!`/`log!` macros on hot threads** — `std::io::Stdout` is internally `Mutex`-guarded. Route log messages through an SPSC to a dedicated logging thread.

---

## 11. Testing Strategy

| Technique | What it catches |
|---|---|
| `loom` (exhaustive concurrency model checking) | Correctness of the SPSC/SPMC/seqlock primitives under all interleavings |
| Deterministic simulation testing (single-threaded, simulated clock, recorded event sequences) | Whole-system behavior — since the design is `(snapshot, ordered_log) -> state`, any bug is reproducible from a recorded log |
| Differential fuzzing vs. a naive reference matcher | Matching-logic correctness independent of the performance architecture |
| Chaos testing (kill/restart shards mid-replay) | WAL/snapshot recovery reaches byte-identical state |

---

## 12. On `unsafe`

This design is **not** "100% safe Rust" — and pretending otherwise would be dishonest. The SPSC/SPMC ring buffers and the seqlock use `UnsafeCell` and raw reads/writes guarded by manually-reasoned `Acquire`/`Release` pairs. The discipline that makes this acceptable:

- All `unsafe` is confined to a handful of small, `loom`-tested primitives in `ring-buffer` and a single `seqlock` module.
- Every `unsafe` block carries a `// SAFETY:` comment stating the invariant and which atomic operation establishes the happens-before edge.
- Everything built *on top of* these primitives (order books, risk shards, business logic) is ordinary safe Rust — the unsafety doesn't leak.

---

## 13. Anti-Patterns to Avoid

| Anti-pattern | Why it hurts | Fix |
|---|---|---|
| `Arc<Mutex<OrderBook>>` | One lock serializes all activity for a symbol | Per-symbol single-owner thread |
| `tokio::sync::Mutex` in the matching path | Async runtime overhead, cross-core wakeups | Plain SPSC message passing |
| `DashMap` for hot account lookups | Internally sharded `RwLock` — tail-latency spikes on hot keys | Per-account-range ownership + seqlock |
| Heap allocation per order | Allocator contention, page faults, latency spikes | Pre-sized `SlotMap` arena |
| `f64` for price/qty | Non-deterministic rounding, breaks replay | `i64` fixed-point ticks |
| `println!`/`log!` on hot threads | `Stdout` is `Mutex`-guarded in `std` | SPSC to dedicated logger thread |
| Two hot counters sharing a cache line | False sharing → invalidation ping-pong | `#[repr(align(64))]` / `CachePadded` |
| Cross-NUMA-node ring buffers | Remote memory access latency | Pin producer + consumer to same NUMA node |

---

## 14. Crate Bill of Materials

| Need | Crate | Notes |
|---|---|---|
| Cache-line padding | `crossbeam-utils::CachePadded` | or hand-roll `#[repr(align(64))]` |
| CPU pinning | `core_affinity` | pair with `isolcpus` kernel boot param |
| Generational arenas | `slotmap` | O(1) insert/remove, no pointer chasing |
| Concurrency model checking | `loom` (dev-dep) | exhaustively tests SPSC/SPMC/seqlock |
| Latency histograms | `hdrhistogram` | per-thread, merged periodically |
| Zero-copy snapshots | `rkyv` | WAL snapshots, fast cold-start recovery |
| Hot-reloadable config | `arc-swap` | Tier-0 limits, fee schedules |
| Optional global allocator | `mimalloc` / `jemalloc` | guards against cold-path malloc jitter |

---

## 15. Suggested Workspace Layout

```
matching-risk-engine/
├── crates/
│   ├── core-types/      # Price, Qty, OrderId, Symbol — fixed-point, no_std-friendly
│   ├── ring-buffer/      # hand-rolled SPSC + SPMC, loom-tested
│   ├── seqlock/          # AccountRiskState primitive, loom-tested
│   ├── order-book/        # price ladder + slotmap-based order arena
│   ├── matching-engine/  # per-symbol engine loop
│   ├── risk-engine/       # per-account-shard risk loop
│   ├── sequencer/          # global ordering + fan-out routing
│   ├── wal/                # mmap append log + rkyv snapshots
│   ├── gateway/            # tokio, async I/O — the only async code in the system
│   └── sim/                # deterministic simulation harness for replay testing
```

---

## 16. Build Order (what to prototype first)

1. `ring-buffer` (SPSC) + `seqlock`, with full `loom` coverage — everything else depends on these being *provably* correct.
2. `order-book` as a pure data structure with a unit-test suite (no concurrency yet) — validate matching logic against a naive reference.
3. Single-threaded `matching-engine` wired to the SPSC primitives — measure baseline latency.
4. Add `sequencer` + multi-symbol fan-out (`SPMC`).
5. Add `risk-engine` shards consuming the fan-out — validate Tier-1 checks add negligible latency.
6. `wal` + snapshot/recovery, validated via the `sim` harness (deterministic replay).
7. Wrap with `gateway` (the only place `tokio` appears).
