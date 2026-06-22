# matching-risk-engine — file structure

```
matching-risk-engine/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── .cargo/
│   └── config.toml
└── crates/
    ├── core-types/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── price.rs
    │       ├── qty.rs
    │       ├── ids.rs
    │       ├── side.rs
    │       ├── commands.rs
    │       └── events.rs
    ├── ring-buffer/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── spsc.rs
    │       ├── spmc.rs
    │       ├── cache_pad.rs
    │       └── tests/
    │           ├── spsc_loom.rs
    │           └── spmc_loom.rs
    ├── seqlock/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── account_risk_state.rs
    │       └── tests/
    │           └── seqlock_loom.rs
    ├── order-book/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── book.rs
    │       ├── level.rs
    │       ├── order.rs
    │       ├── apply.rs
    │       └── tests/
    │           ├── matching_unit.rs
    │           └── diff_fuzz.rs
    ├── matching-engine/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── engine.rs
    │       ├── risk_check.rs
    │       ├── metrics.rs
    │       └── affinity.rs
    ├── sequencer/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── sequencer.rs
    │       ├── halt.rs
    │       └── snapshot_marker.rs
    ├── risk-engine/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── shard.rs
    │       ├── position.rs
    │       ├── config.rs
    │       └── tier0.rs
    ├── wal/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── log.rs
    │       ├── snapshot.rs
    │       └── recovery.rs
    ├── gateway/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── server.rs
    │       ├── session.rs
    │       ├── codec.rs
    │       └── market_data.rs
    ├── sim/
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── harness.rs
    │       ├── replay.rs
    │       ├── chaos.rs
    │       └── scenarios/
    │           ├── basic_fills.rs
    │           ├── liquidation.rs
    │           └── snapshot_recovery.rs
    ├── metrics/
    │   ├── Cargo.toml
    │   └── src/
    │       └── aggregator.rs
    └── logger/
        ├── Cargo.toml
        └── src/
            └── logger.rs
```
