# Match-Risk-Engine

> **Contention-free matching & risk engine in Rust**
> Single-owner sharding · SPSC/SPMC ring buffers · Seqlock risk state · Array price ladder · mmap WAL

---

## What This Is

A low-latency order matching and risk engine built from scratch in Rust. The core architecture is **contention-free by construction**: each piece of mutable state has exactly one owner thread, enforced at compile time via Rust's `!Sync`. No mutexes anywhere. Communication between threads uses hand-rolled SPSC/SPMC ring buffers with only `Acquire`/`Release` atomics.

This is an active work-in-progress. The matching and risk core is functional; the networking layer, WAL durability, and end-to-end integration are at varying levels of completeness. See [Project Status](#project-status) and [Roadmap](#roadmap) for details.

---

## Project Status

| Component | Status | Notes |
|-----------|--------|-------|
| SPSC ring buffer | ✅ Implemented | Hand-rolled, loom-tested, CachePadded cursors |
| SPMC ring buffer (broadcast) | ✅ Implemented | Fan-out to risk shards, WAL, metrics |
| Seqlock (AccountRiskState) | ✅ Implemented | SWMR, loom-tested |
| Order book (array ladder) | ✅ Implemented | Price-time priority, SlotMap arena, zero-alloc steady state |
| Matching engine loop | ✅ Implemented | Per-symbol thread, SPSC-driven, SmallVec event output |
| Sequencer | ✅ Implemented | Global ordering, round-robin gateway polling, WAL write, symbol routing |
| Risk engine (Tier-1 shard) | ✅ Implemented | Per-account-range position tracking, margin recomputation, liquidation |
| Tier-0 risk check | ✅ Implemented | Gateway-side, seqlock read, allocation-free |
| WAL writer | ✅ Implemented | mmap + msync, CRC32 integrity, bincode serialisation |
| WAL recovery | ✅ Implemented | Scan + replay from snapshot point, corrupt-tail detection |
| Snapshots | ✅ Implemented | Bincode-serialised shard state, sequence-stamped |
| Gateway (TCP) | ✅ Implemented | Tokio-based, binary codec, session management, market data subscriptions |
| Differential fuzz tests | ✅ Implemented | Reference `BTreeMap` matcher vs array book — randomised, deterministic seeds |
| Simulation harness | ✅ Implemented | Deterministic replay, chaos testing scaffolding |
| Dual-engine redundancy | 🔨 In progress | Second engine + sorter for dual-write architecture |
| Metrics aggregator | 🔨 In progress | CachePadded counters, latency histograms |
| FOK order support | ✅ Implemented | Enum variant exists and matching logic handles it |
| Market order support | ❌ Not implemented | OrderType::Market is parsed but ignored in matching |
| Self-trade prevention | ❌ Not implemented | No STP policy; accounts can match against themselves |
| DPDK kernel-bypass networking | ❌ Not implemented | Gateway uses standard Tokio TCP |
| PMEM WAL (`clwb`/`sfence`) | ❌ Not implemented | WAL uses mmap + msync |
| Sequencer standby / failover | ❌ Not implemented | No hot-standby or leader election |
| Benchmarks | ❌ Not implemented | No criterion or latency measurement harness |

---

## Design Principles

The architecture is built around **eliminating contention at the design level** rather than managing it with lock-free data structures. CAS-based "lock-free" algorithms still cause retry storms under peak load. This engine avoids that entirely:

| Synchronisation Tier | Mechanism | Where Used | Cost |
|----------------------|-----------|------------|------|
| None | Single-threaded ownership (`!Sync`) | OrderBook, seq counter, position map | 0 |
| SPSC/SPMC | Acquire/Release on cursor only | Gateway→Seq, Seq→ME, ME→risk shard | ~10–20ns |
| Seqlock (SWMR) | Single writer stamps seq; readers retry on mid-write | AccountRiskState reads | ~5–10ns |

---

## Architecture

```
Gateway threads (Tokio TCP)
    │
    │  SPSC push (InboundCommand)
    ▼
Sequencer (pinned thread)
    │  assigns global seq + timestamp
    │  writes to WAL (mmap, best-effort)
    │  routes by symbol
    │
    ├── SPSC push ──▶  ME[symbol 0]  ──▶ SPMC fan-out ──▶ Risk Shard 0
    ├── SPSC push ──▶  ME[symbol 1]  ──▶ SPMC fan-out ──▶ Risk Shard 1
    └── SPSC push ──▶  ME[symbol N]  ──▶ SPMC fan-out ──▶ WAL / Metrics
```

**Key properties:**
- Every arrow is an SPSC or SPMC ring buffer — no shared mutex anywhere.
- Each matching engine owns exactly one `OrderBook` — no cross-thread access.
- Each risk shard owns a contiguous range of `AccountId`s and is the sole writer to those accounts' seqlock states.
- The sequencer is the single point of total ordering; all downstream components see the same globally-ordered stream.

---

## Order Book Design

The order book uses a **cache-optimal array ladder** — not a `BTreeMap`:

- **Array indexed by price tick**: `levels[price - tick_floor]` — O(1) access, no pointer chasing, L1/L2 cache resident for typical tick ranges.
- **SlotMap arena** for orders: O(1) insert/remove, pre-allocated, zero heap allocation in steady state. Intrusive doubly-linked list within each price level for FIFO time priority.
- **`#[repr(align(64))]` PriceLevel**: one level per cache line to avoid false sharing.
- **`i64` fixed-point prices**: deterministic arithmetic, no floating-point rounding. All prices and quantities use a `1e8` scale factor.
- **`SmallVec<[EngineEvent; 8]>`**: fill events live on the stack; heap spill only on unusually deep sweeps.

### Supported Order Types

| Type | TIF | Status |
|------|-----|--------|
| Limit | GTC | ✅ Fully implemented |
| Limit | IOC | ✅ Fully implemented |
| Limit | FOK | ❌ Not implemented (silently treated as GTC) |
| Market | — | ❌ Not implemented (rejected as price-out-of-range) |

---

## Risk Architecture

Risk checking happens at two tiers:

### Tier-0 (Gateway — pre-sequencer)
Runs on the gateway thread before an order enters the sequencer. Pure function, no allocation, no I/O. Reads the `AccountRiskState` seqlock to check:
- Account frozen?
- Order notional within limits?
- Sufficient available margin (balance − used_margin) for initial margin requirement?

### Tier-1 (Risk Shard — post-match)
Runs on a dedicated pinned thread, consuming `EngineEvent`s from the SPMC fan-out. For each fill:
1. Updates per-(account, symbol) `Position` (net qty, VWAP entry, realised PnL).
2. Recomputes margin across all symbols for the affected account.
3. Publishes updated `(balance, used_margin)` to the seqlock.
4. Triggers `InboundCommand::Liquidate` if maintenance margin is breached.

All position arithmetic is **integer fixed-point** (i64, 1e8 scale) — no `f64` anywhere, which guarantees deterministic WAL replay.

---

## WAL & Recovery

### Current Implementation
- **mmap-backed file** with pre-allocated capacity (default 512 MiB).
- **Record format**: `[seq: u64][ts_ns: u64][len: u32][crc32: u32][payload][padding]` — 8-byte aligned.
- **Serialisation**: `bincode` (not zero-copy; allocates per write).
- **Durability**: `msync(MS_SYNC)` per record when `sync_on_write = true`.
- **Recovery**: scan WAL from file header, verify CRC32 per record, stop at first corrupt/truncated record. Replay all records after the latest snapshot's sequence number.

### Snapshot System
Bincode-serialised shard state, stamped with the sequence number at snapshot time. Recovery loads the latest snapshot, then replays only the WAL tail.

---

## Workspace Layout

```
Match-Risk-Engine/
├── Cargo.toml                    # Workspace root
└── crates/
    ├── core-types/               # Price, Qty, OrderId, Side, Symbol, EngineEvent, InboundCommand
    ├── ring-buffer/              # SPSC + SPMC ring buffers (hand-rolled, loom-tested)
    ├── seqlock/                  # AccountRiskState seqlock (loom-tested)
    ├── order-book/               # Array price ladder + SlotMap arena + matching logic
    ├── matching-engine/          # Per-symbol engine loop, thread pinning, risk integration
    ├── risk-engine/              # Tier-0 checks, Tier-1 sharded position/margin tracking
    ├── sequencer/                # Global ordering, symbol routing, snapshot markers, halt flag
    ├── wal/                      # mmap WAL writer, snapshot writer, recovery scanner
    ├── gateway/                  # Tokio TCP server, binary codec, session state, market data hub
    ├── sim/                      # Deterministic simulation harness, replay, chaos testing
    ├── dual-log/                 # Dual-engine log coordination (experimental)
    ├── second-engine/            # Secondary matching engine for dual-write redundancy (experimental)
    ├── sorter/                   # Event ordering / leader election for dual-engine mode (experimental)
    ├── metrics/                  # Latency/throughput aggregator (CachePadded atomic counters)
    └── logger/                   # Structured logging utility
```

---

## Testing

### Unit Tests
Every core crate has inline unit tests. Key coverage areas:
- **Order book**: price-time priority, partial/full fills, cancels, wrong-account rejection, IOC semantics, multi-level sweeps, best-bid/ask tracking.
- **Ring buffers**: push/pop correctness, full-queue backpressure, wrapping semantics.
- **Seqlock**: read consistency under concurrent writes.
- **Sequencer**: routing correctness, monotonic sequence numbers, round-robin fairness.
- **Risk shard**: position updates on fill, margin recomputation, liquidation triggers.
- **WAL**: write + scan roundtrip, capacity exhaustion, CRC integrity.
- **Gateway**: auth handshake, codec roundtrip, session command enqueue, market data subscribe/unsubscribe.

### Differential Fuzz Testing
The order book is tested against a naive `BTreeMap`-based reference matcher. Both receive identical randomised command sequences; total filled quantities must agree after every command. Runs with deterministic seeds for CI reproducibility and has an ignored soak test for longer runs.

### Loom Concurrency Tests
SPSC, SPMC, and seqlock crates include `loom` model-checking tests that exhaustively explore thread interleavings to verify memory ordering correctness.

```bash
# Run all tests
cargo test --workspace

# Run loom concurrency tests
cargo test --workspace --features loom

# Run differential fuzz soak (longer)
cargo test --workspace -- --ignored
```

---

## Build & Run

```bash
# Build (debug)
cargo build --workspace

# Build (release — fat LTO, single codegen unit, panic=abort)
cargo build --workspace --release

# Run all tests
cargo test --workspace
```

> **Note**: There is no standalone binary/main entrypoint yet. The system is tested via unit tests, the simulation harness (`sim` crate), and the gateway's integration tests. An end-to-end demo binary is planned.

---

## Known Limitations

These are significant gaps relative to a production matching engine:

1. **No self-trade prevention**: orders from the same account can match against each other. Production systems require configurable STP policies.
2. **FOK and Market orders are declared but not implemented**: `TimeInForce::Fok` and `OrderType::Market` exist in the type system but are not handled by the matching logic.
3. **No performance benchmarks**: no criterion benchmarks, no latency histograms, no measured numbers. Latency claims cannot be made without measurement.
4. **WAL allocates on every write**: `bincode::serialize` allocates a `Vec<u8>` per record, violating zero-allocation goals on the hot path.
5. **Gateway uses kernel TCP**: the networking layer is a standard Tokio TCP server, not kernel-bypass.
6. **No CI pipeline**: tests are not run automatically; no GitHub Actions workflow exists.

---

## Roadmap

Roughly ordered by priority:

- [ ] **Self-trade prevention** — configurable STP policy (cancel-resting / cancel-incoming / reject)
- [ ] **FOK order support** — two-pass probe-then-execute in the matching loop
- [ ] **Market order support** — price-less aggressive matching at best available
- [ ] **Criterion benchmarks** — `book.apply()` latency, SPSC roundtrip, end-to-end pipeline
- [ ] **Per-account risk state on matching engine** — replace single shared state with account-indexed lookup
- [ ] **Zero-alloc WAL writes** — stack-allocated scratch buffer, write directly into mmap region
- [ ] **CI pipeline** — GitHub Actions: `cargo test`, `cargo clippy -D warnings`, `cargo fmt --check`
- [ ] **End-to-end demo binary** — wire all crates together, accept TCP connections, match, emit events
- [ ] **Sequencer standby + failover** — hot mirror, CAS-based leader election
- [ ] **PMEM WAL** — `clwb` + `sfence` for sub-microsecond deterministic durability (requires PMEM hardware)
- [ ] **Kernel-bypass networking** — DPDK or `io_uring` for gateway NIC I/O

---

## Design Goals (Target Architecture)

The long-term goal is a system that obeys five rules on the critical path:

| Rule | Target | Current Status |
|------|--------|----------------|
| No Kernel | Zero syscalls between NIC receive and fill publish | ❌ Tokio TCP + mmap msync |
| No Contention | Only one thread writes any mutable state | ✅ Enforced via `!Sync` |
| No Allocation | Zero heap allocation after warm-up | ❌ bincode WAL + SmallVec spill possible |
| No Context Switch | Thread-per-core, hot cores never preempted | ✅ CPU affinity support in matching engine |
| No Page Cache WAL | PMEM or capacitor-backed NVMe via O_DIRECT | ❌ mmap + msync |

---

*Built by [Krishna Khasge](https://github.com/Kris0721) · Mumbai, India*
