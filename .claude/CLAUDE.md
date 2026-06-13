# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Skills

Three project skills are available for domain-specific work. Invoke them with `/skill-name` or by mentioning the topic:

| Skill | When to use |
|-------|-------------|
| **coding** | Default guardrail for all implementation work. Enforces four principles: Think Before Coding, Simplicity First, Surgical Changes, Goal-Driven Execution — each with barter-rs specific applications. |
| **architecture** | Crate boundaries, trait design, dependency direction, concurrency models, refactoring, proc-macro design. Covers the generic composition patterns (`Engine`, `SystemBuilder`, `ExchangeWsStream`) and the indexed data structure approach. |
| **trading-architecture** | Exchange integration, market data pipelines, strategy/risk design, order lifecycle, Engine Command flows, miraelis signal modules, execution client patterns. |

## Build, Test, Lint

```bash
# Check compilation
cargo check

# Build specific crate
cargo build -p barter
cargo build -p barter-data

# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p barter

# Run a single test
cargo test -p barter -- engine::tests::test_name

# Format (edition 2024, crate-level import granularity)
cargo fmt --all

# Lint (warnings are errors)
cargo clippy -- -D warnings

# Benchmarks (barter crate only)
cargo bench -p barter

# Run examples
cargo run --example engine_sync_with_live_market_data_and_mock_execution_and_audit
cargo run --example aggregate_trades_streams --manifest-path barter-data/Cargo.toml
```

## Crate Map

Six published libraries in strict dependency order:

```
barter-macro          (proc-macro: DeExchange, SerExchange, DeSubKind, SerSubKind)
barter-integration    (foundation: Transformer, Validator, RestClient, ExchangeStream, feature-gated)
    ↓
barter-instrument     (ExchangeId, Asset, Instrument, Side, IndexedInstruments, ExchangeIndex/AssetIndex/InstrumentIndex)
    ↓
barter-data           (MarketStream, ExchangeWsStream, exchange WebSocket connectors — 8 exchanges)
barter-execution      (AccountEvent, AccountSnapshot, ExecutionClient, order lifecycle, mock + Binance clients)
    ↓
barter                (Engine<C,S,E,St,R>, SystemBuilder, AlgoStrategy, RiskManager, EngineState, statistics, backtesting)
```

`barter-integration` is the foundation — it must never depend on sibling crates.

## Application Binaries

Three standalone tokio binaries (root `Cargo.toml`, not published):

- **`binance-futures-risk-manager`** (`risk/main.rs`) — monitors Binance Futures positions, enforces risk limits
- **`binance-futures-strategy`** (`strategy/main.rs`) — strategy execution bot with Telegram command interface
- **`miraelis`** (`miraelis/src/main.rs`) — market data ingest + signal generation. Uses `StrategyEngine` with pluggable `StrategyModule` trait. Signal modules: Frame, HugeMomentum, Rocket. Writes sharded JSONL via `AsyncRollbackWriter`, notifies via Telegram

## Tooling & Conventions

- **Toolchain**: `stable` (via `rust-toolchain.toml`)
- **Editions**: `barter-*` crates = 2024; root package and miraelis = 2021
- **Formatting**: `rustfmt.toml` — `edition = "2024"`, `imports_granularity = "crate"`
- **Lints**: `#![forbid(unsafe_code)]` everywhere; `clippy::cognitive_complexity`, `unused_crate_dependencies`, `rust_2024_compatibility` are warnings
- **CI** (`.github/workflows/ci.yml`): `cargo check` → `cargo test` → `cargo fmt --all -- --check` → `cargo clippy -- -D warnings`
- **Release**: `release-plz` (configured in `release-plz.toml`)
- **Key external deps**: tokio (multi-thread), tokio-tungstenite (WebSocket), reqwest (REST), hmac+sha2 (Binance signing), rust_decimal with maths feature, vecmap-rs (indexed lookups)
