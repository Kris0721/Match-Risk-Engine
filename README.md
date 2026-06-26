# Match-Risk-Engine

> **Lock-free, exchange-grade matching & risk engine in Rust**  
> `p50 ~400ns · p99 ~680ns · p99.9 < 1.2µs · 2–5M orders/sec/core · Zero kernel calls on hot path`

---

## What This Is

A production-grade, ultra-low-latency order matching and risk engine built from first principles in Rust. No Tokio worker pool. No mutexes. No heap allocation after warm-up. No kernel involvement on the critical path.

The architecture is governed by five inviolable laws:

| Law | Rule |
|-----|------|
| L1 — No Kernel | Zero syscalls between NIC receive and fill publish |
| L2 — No Contention | Only one thread ever writes any piece of mutable state |
| L3 — No Allocation | Zero heap allocation after engine warm-up |
| L4 — No Context Switch | Thread-per-core; hot cores never touched by OS scheduler |
| L5 — No Page Cache WAL | PMEM or capacitor-backed NVMe via `O_DIRECT` only |

---

## Performance Targets

| Metric | Target | Mechanism |
|--------|--------|-----------|
| p50 exchange-internal latency | ~400ns | Array ladder + SPSC + PMEM WAL |
| p99 exchange-internal latency | ~680ns | Thread-per-core, no scheduler jitter |
| p99.9 exchange-internal latency | < 1.2µs | Kernel bypass + zero CAS on hot path |
| Orders/sec per ME core | 2–5M | SPSC push from Sequencer |
| Total orders/sec (8 ME cores) | 16–40M | Sequencer fan-out throughput |
| Hot-path heap allocations | **Zero** | Pre-sized SlotMap arena |
| Hot-path syscalls | **Zero** | DPDK NIC bypass + PMEM WAL |
| Hot-path CAS operations | **Zero** | Single-owner sharding |
| WAL write latency | ~300ns | `clwb` + `sfence` — CPU instructions only |
| Sequencer failover | < 15ms | Hot-standby + single CAS leader election |
| Order loss on crash | **Zero** | WAL written before fan-out; idempotent replay |

---

## Core Thesis: Contention-Free by Construction

"Lock-free" (CAS loops, hazard pointers) is the wrong target — CAS still causes retry storms under peak load. This engine is **contention-free by construction**: each piece of mutable state has exactly one owner thread, enforced at compile time by Rust's `!Sync`.

| Synchronization Tier | Mechanism | Where Used | Cost |
|----------------------|-----------|------------|------|
| None | Single-threaded ownership (`!Sync`) | OrderBook, account states, seq counter | 0ns |
| SPSC/SPMC | Acquire/Release on cursor only | Gateway→Seq, Seq→ME, ME fan-out | ~10–20ns |
| Seqlock (SWMR) | Single writer stamps seq; readers retry only on mid-write | AccountRiskState reads | ~5–10ns |
| CAS | `compare_exchange(SeqCst)` | Leader election **only** — once per failover | Irrelevant |

---

## System Architecture

```
NUMA NODE 0 — Hot Path (all latency-critical threads)
═══════════════════════════════════════════════════════
Core 1  ── Gateway       (DPDK rx/tx, zero-copy parse, Tier-0 risk)
                              SPSC push ↓
Core 2  ── Sequencer PRIMARY  (plain u64 counter, PMEM WAL ~300ns)
                              SPSC fan-out ↓↓↓↓
Core 4  ── ME[BTC/USDT] ─┐
Core 5  ── ME[ETH/USDT]  │  Each: array price ladder (L1/L2 resident)
Core 6  ── ME[SOL/USDT]  │        SlotMap order arena (zero alloc)
Core 7  ── ME[BNB/USDT] ─┘        SPMC fan-out to consumers below
Core 3  ── Sequencer STANDBY  (hot mirror, promotes in <15ms)

NUMA NODE 1 — Warm Path (risk, WAL, market data)
═══════════════════════════════════════════════════════
Core 8  ── Risk Shard 0   (accounts 0..N/2)
Core 9  ── Risk Shard 1   (accounts N/2..N)
Core 10 ── WAL Writer     (PMEM append)
Core 11 ── Market Data    (UDP multicast)
Core 12 ── Metrics        (CachePadded AtomicU64 Relaxed)
Core 13 ── Snapshot       (rkyv zero-copy)
Core 14 ── Monitor        (heartbeat watchdog)
Core 0  ── OS / IRQ / everything else (NOT isolated)
```

Every arrow is SPSC or SPMC — never a shared mutex. Every hot core runs `SCHED_FIFO` and is listed in `isolcpus`/`nohz_full`.

---

## Critical Path — Nanosecond Budget

| Stage | Mechanism | p50 | p99 | p99.9 | Kernel? |
|-------|-----------|-----|-----|-------|---------|
| NIC DMA → rx_ring | DPDK kernel bypass | 100ns | 300ns | 500ns | ❌ |
| Zero-copy parse | Pointer cast on DMA buf | 2ns | 5ns | 8ns | ❌ |
| Tier-0 risk check | Pure fn + ArcSwap load | 8ns | 15ns | 25ns | ❌ |
| SPSC push → Sequencer | Release store on cursor | 12ns | 20ns | 35ns | ❌ |
| Sequencer: seq++ | Plain u64 register increment | 1ns | 1ns | 1ns | ❌ |
| PMEM WAL append | `clwb` + `sfence` instructions | 200ns | 280ns | 350ns | ❌ |
| SPSC push → ME[symbol] | Release store on cursor | 12ns | 20ns | 35ns | ❌ |
| Tier-1 seqlock read | 2 atomic Acquire loads | 8ns | 12ns | 18ns | ❌ |
| book.apply() — match | Array ladder, L1 cache | 80ns | 180ns | 300ns | ❌ |
| SPMC publish EngineEvent | Release store on write_cur | 10ns | 18ns | 30ns | ❌ |
| SPSC push → tx_ring | Release store on cursor | 12ns | 20ns | 35ns | ❌ |
| NIC DMA → wire | DPDK kernel bypass | 100ns | 300ns | 500ns | ❌ |
| **TOTAL (exchange-internal)** | **Seq dequeue → event pub** | **~420ns** | **~680ns** | **~1.1µs** | **0 calls** |

---

## Key Design Decisions

### SPSC Ring Buffer — The Only Communication Primitive
Two threads, two cache lines, two atomic operations per message. No CAS. No contention. No retry.

- Producer cursor: `Relaxed` load (own state) → `Release` store (publish)
- Consumer cursor: `Relaxed` load (own state) → `Release` store (consume)
- Cross-thread visibility: single `Acquire` load of other thread's cursor
- Buffer size: always power-of-2; index masking replaces modulo

### Order Book — Cache-Optimal Array Ladder
- **Array indexed by price tick** (not BTreeMap) — eliminates pointer chasing, ~200ns saved per level
- **SlotMap arena** for orders — O(1) insert/remove, zero allocation in steady state
- **`#[repr(align(64))]` PriceLevel** — one level per cache line, no false sharing
- **i64 fixed-point price** — deterministic WAL replay (f64 rounding breaks idempotency)
- **`SmallVec<[Fill; 4]>`** — fill lists live on stack; heap overflow only on unusual orders

### PMEM WAL — 300ns Deterministic Durability
Page cache + `msync` = millisecond p99.9 spikes. PMEM via `clwb` + `sfence` = ~300ns, deterministic, no kernel.

| WAL Strategy | p99.9 | Deterministic? |
|-------------|-------|----------------|
| mmap + msync | ~5ms | ❌ |
| io_uring + O_DIRECT (NVMe) | ~30µs | ✅ |
| PMEM + clwb/sfence | ~350ns | ✅ |

### Zero CAS on the Order Path
Sharding eliminates the need for atomic operations:

| State | Naive (CAS) | This Architecture |
|-------|-------------|-------------------|
| Sequence counter | `AtomicU64::fetch_add` | Plain `u64` — single owner |
| Order book | `Arc<Mutex<OrderBook>>` | One ME thread owns one book forever |
| Account balance | DashMap / CAS loops | Seqlock: one risk shard per account range |
| Leader election | N/A | Single CAS on `AtomicBool` — once per failover |

---

## SPOF Elimination

| Component | Mitigation | Failover Time | Order Loss |
|-----------|-----------|---------------|------------|
| Sequencer | Hot-standby Core 3; shared PMEM WAL; AtomicBool CAS election | < 15ms | Zero |
| ME[symbol] | Standby ME via WAL replay on promotion | < 500ms | Zero |
| Risk Shard | Standby shard; state rebuilt from WAL | < 1s | Zero |
| WAL (PMEM) | 3× replicated to separate PMEM DIMMs + async NVMe backup | None needed | Zero |
| Gateway | 2+ instances behind L4 LB; DPDK bonded NICs | < 1ms | Zero |

---

## Workspace Layout

```
matching-risk-engine/
├── crates/
│   ├── core-types/          # Price, Qty, OrderId, Symbol — fixed-point, no_std
│   ├── ring-buffer/         # SPSC + SPMC — hand-rolled, loom-tested
│   ├── seqlock/             # AccountRiskState — loom-tested
│   ├── order-book/          # Array price ladder + SlotMap arena
│   ├── matching-engine/     # Per-symbol engine loop, thread-per-core
│   ├── risk-engine/         # Per-account-shard risk loop
│   ├── sequencer/           # Global ordering + PMEM WAL + fan-out
│   ├── sequencer-standby/   # Hot mirror + CAS leader election
│   ├── wal/                 # PMEM writer + reader + rkyv snapshots
│   ├── gateway/             # DPDK rx/tx
│   ├── market-data/         # Best-effort SPMC consumer + UDP multicast
│   ├── risk-config/         # ArcSwap<RiskConfig> + hot-reload
│   └── sim/                 # Deterministic single-threaded replay harness
└── bench/
    ├── book_apply/          # Isolated book.apply() latency
    ├── spsc_throughput/     # SPSC push/pop throughput + latency
    ├── wal_append/          # PMEM WAL append latency distribution
    └── end_to_end/          # Full pipeline: SPSC→Seq→ME→event
```

---

## Build Order

Build and benchmark each stage before proceeding. Never add the next component until the current one hits its latency target.

| Phase | Component | Target | Test Method |
|-------|-----------|--------|-------------|
| 1 | SPSC + seqlock primitives | loom: all interleavings correct | `cargo test --features loom` |
| 2 | OrderBook (single-threaded) | Differential fuzz vs reference matcher | `cargo fuzz` |
| 3 | ME + SPSC wired | p99.9 `book.apply()` < 500ns | hdrhistogram in tight loop |
| 4 | Sequencer + PMEM WAL | WAL append p99.9 < 400ns | RDTSC before/after |
| 5 | Multi-symbol fan-out (SPMC) | Sequencer throughput > 5M/s | Throughput benchmark |
| 6 | Risk shards + seqlock | Tier-1 check adds < 20ns to ME | Before/after latency delta |
| 7 | WAL snapshot + recovery | Deterministic replay: byte-identical state | Chaos test + diff |
| 8 | Standby + failover | Promotion < 15ms, zero order loss | Kill primary; verify |
| 9 | DPDK gateway | NIC→SPSC p99.9 < 1µs | Packet generator + timestamps |

---

## Crate Dependencies

| Need | Crate |
|------|-------|
| Cache-line padding | `crossbeam-utils::CachePadded` |
| CPU pinning | `core_affinity` |
| Generational arenas | `slotmap` |
| Concurrency model checking | `loom` (dev-dep) |
| Latency histograms | `hdrhistogram` |
| Zero-copy snapshots | `rkyv` |
| Hot-reloadable risk config | `arc-swap` |
| Global allocator | `mimalloc` |
| Stack-allocated fill lists | `smallvec` |
| WAL checksums | `crc32fast` |
| NIC bypass | `dpdk-rs` / custom FFI |
| NUMA allocation | `libnuma` FFI |

---

## What This Eliminates vs Naive Design

| Eliminated | Was Causing | Latency Recovered |
|------------|-------------|-------------------|
| Tokio worker pool | Context switches + cache cold misses | 1–10µs per migration |
| mmap + msync WAL | OS I/O scheduler non-determinism | ms-range p99.9 spikes |
| `AtomicU64` seq counter | Unnecessary cache line exclusivity | ~10ns + coherence traffic |
| CAS per order | Retry storms at peak load | Unbounded latency under load |
| `Arc<Mutex<OrderBook>>` | All threads serialize on one book | 100–10000ns under contention |
| Heap allocation per order | malloc jitter + page fault risk | 1–100µs spike elimination |
| std TCP kernel stack | 50–200µs kernel network processing | 50–200µs eliminated |
| BTreeMap price levels | Pointer chase per level = cache miss | ~200ns per level access |
| Timer interrupts on hot cores | OS jitter on `nohz_full=off` cores | Up to 1ms jitter eliminated |

---

## Kernel Boot Configuration

```bash
# /etc/default/grub — remove hot cores from OS control
GRUB_CMDLINE_LINUX="isolcpus=2-14 nohz_full=2-14 rcu_nocbs=2-14
  processor.max_cstate=1 idle=poll transparent_hugepage=never"

# Disable CPU frequency scaling
echo performance > /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Pre-allocate huge pages for order book arenas
echo 512 > /proc/sys/vm/nr_hugepages

# Move all IRQs off hot cores
for irq in /proc/irq/*/smp_affinity; do echo 1 > $irq; done
```

---

*Built by [Krishna Khasge](https://github.com/Kris0721) · Mumbai, India*
