# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build, Test, Lint

```bash
# Check compilation (fast, no codegen)
cargo check

# Build everything
cargo build

# Build a specific crate
cargo build -p barter
cargo build -p barter-data

# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p barter

# Run a single test
cargo test -p barter -- engine::tests::test_name

# Format all code (edition 2024 style, crate-level import granularity)
cargo fmt --all

# Lint (strict — warnings are errors)
cargo clippy -- -D warnings

# Run the benchmark (barter crate only)
cargo bench -p barter

# Run examples
cargo run --example engine_sync_with_live_market_data_and_mock_execution_and_audit
cargo run --example aggregate_trades_streams --manifest-path barter-data/Cargo.toml
```

## Architecture Overview

This is a Rust workspace for the **Barter** algorithmic trading ecosystem. There are two distinct layers:

### Core Libraries (published crates with CHANGELOGs)

The six `barter-*` crates form a layered dependency stack used by external consumers:

| Crate | Purpose |
|-------|---------|
| **barter-integration** | Low-level REST/WebSocket framework. Defines `Transformer`, `Validator`, `Terminal`, `Unrecoverable` traits, `RestClient`, `ExchangeStream`. Feature-gated modules (`protocol`, `channel`, `socket`, `stream`, `metric`, etc.) |
| **barter-instrument** | Data types: `ExchangeId`, `Asset`, `Instrument` (Spot/Future/Perpetual/Option), `Side`, `IndexedInstruments` for O(1) lookups |
| **barter-data** | Public market data streaming via WebSocket. `MarketStream` trait, `ExchangeWsStream` generic impl, exchange connectors (Binance, Coinbase, Kraken, OKX, Gate.io, Bybit, Bitfinex, Bitmex). Subscription kinds: PublicTrades, OrderBooksL1/L2/L3, Candles, Ticker, etc. Built on `barter-integration` |
| **barter-execution** | Private account data streaming and order execution. `AccountEvent`, `AccountSnapshot`, mock execution client, Binance execution client. Order lifecycle management |
| **barter** | Core trading engine. `Engine<Clock, State, ExecutionTxs, Strategy, Risk>` with plug-and-play `AlgoStrategy`, `RiskManager`, `ClosePositionsStrategy`, `OnDisconnectStrategy`, `OnTradingDisabled` traits. `SystemBuilder`/`System` for composing a full trading system. Backtesting utilities, statistics (Sharpe, Sortino, Drawdown, PnL) |
| **barter-macro** | Proc-macro crate: `#[derive(DeExchange, SerExchange, DeSubKind, SerSubKind)]` |

### Application Binaries (not published; live in repo root)

Three standalone tokio async binaries defined in the root `Cargo.toml`:

- **`binance-futures-risk-manager`** (`risk/main.rs`) — monitors Binance Futures positions via REST + WebSocket user data streams, enforces risk limits. Modules: `app`, `binance`, `config`, `market_stream`, `state`, `supervisor`, `user_data_stream`
- **`binance-futures-strategy`** (`strategy/main.rs`) — strategy execution bot with Telegram command interface. Modules: `app`, `binance`, `command`, `config`, `telegram`
- **`miraelis`** (`miraelis/src/main.rs`) — market data ingest and signal generation. Uses `StrategyEngine` with pluggable `StrategyModule` trait. Signal modules: `FrameSignalModule`, `HugeMomentumSignalModule`, `RocketSignalModule`. Sends signals via Telegram, can execute trades on Binance. Writes market data to sharded JSONL files via `AsyncRollbackWriter`

### Key Architecture Patterns

**barter Engine event loop**: `Engine<Clock, State, ExecutionTxs, Strategy, Risk>` implements `Processor<Event>`, receiving `EngineEvent` variants (Shutdown, Command, TradingStateUpdate, Account, Market). On each event, if `TradingState::Enabled`, it calls the `AlgoStrategy` to generate algo orders, runs them through `RiskManager::check()`, and sends approved requests via `ExecutionTxMap`.

**barter System composition**: `SystemBuilder` takes `SystemArgs` (instruments, executions, strategy, risk, market_stream) and builds a `System` that spawns the engine on a tokio task, with market-to-engine and account-to-engine forwarding tasks. Commands can be sent to the engine via `System` methods (`close_positions`, `cancel_orders`, `trading_state`, etc.).

**miraelis StrategyEngine**: Independent signal framework in `miraelis/src/strategy/`. `StrategyModule` trait has lifecycle methods (`init`, `start`) and event handlers (`handle_trade`, `handle_candle_1m`, etc.). `StrategyEngine::dispatch()` persists events into per-symbol `UhfTradeWindow`s, then fans out to all registered modules. Signal modules send `TradeSignalBase` variants over an `order_tx` channel and/or notify via Telegram.

**Indexed data structures**: The barter ecosystem uses `ExchangeIndex`, `AssetIndex`, `InstrumentIndex` (from `barter-instrument`) with `vecmap-rs` for O(1) constant-time lookups instead of hash maps. `IndexedInstruments` is the canonical collection builder.

### Rust Edition & Tooling

- All six `barter-*` crates use `edition = "2024"`. Root package and `miraelis` use `edition = "2021"`
- Rustfmt: `edition = "2024"`, `imports_granularity = "crate"`
- Toolchain: `stable` (via `rust-toolchain.toml`)
- Lint level: `#![forbid(unsafe_code)]` across all crates; `clippy::cognitive_complexity`, `unused_crate_dependencies`, `rust_2024_compatibility` warnings
- Release automation: `release-plz` (configured in `release-plz.toml`)
- CI: `.github/workflows/ci.yml` runs `cargo check`, `cargo test`, `cargo fmt --all -- --check`, `cargo clippy -- -D warnings` on pushes/PRs to `develop`

### External Dependencies

- **Async runtime**: tokio (multi-thread), tokio-tungstenite (WebSocket), futures
- **SerDe**: serde/serde_json; `hmac` + `sha2` for Binance API signing
- **Data structures**: `vecmap-rs`, `rust_decimal` (with maths feature), `indexmap`, `smol_str`, `fnv`
- **Protocols**: `reqwest` with rustls, `tokio-tungstenite` with rustls-webpki-roots
- **Binance exchange**: REST API (account, orders, exchange info) + WebSocket (market data, user data streams)
- **Telegram**: Bot API via `reqwest` for signal notifications and command-based control
