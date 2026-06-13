---
name: architecture
description: Analyze, explain, review and evolve the software architecture of barter-rs. Use when discussing crate boundaries, traits, ownership, dependencies, concurrency models, refactoring or system design.
---

# When To Use

Use this skill when:
* explaining or reviewing the crate dependency graph (barter-integration → barter-instrument → barter-data / barter-execution → barter)
* evaluating trait design (Processor, Transformer, MarketStream, RiskManager, AlgoStrategy, StrategyModule)
* assessing generic composition patterns (Engine<Clock, State, ExecutionTxs, Strategy, Risk>, SystemBuilder)
* reviewing module boundaries within a crate
* analyzing ownership, borrowing, and synchronization across async boundaries
* evaluating the indexed data structure pattern (ExchangeIndex, AssetIndex, InstrumentIndex via vecmap-rs)
* designing new crates or moving code between crates
* reviewing proc-macro design (barter-macro: DeExchange, SerExchange, DeSubKind, SerSubKind)
* assessing feature flag boundaries in barter-integration (protocol, channel, socket, stream, etc.)
* evaluating the two distinct architecture styles: barter library Engine pattern vs miraelis StrategyEngine pattern

Do not use this skill for:
* trading strategy logic design — use trading-architecture
* exchange-specific integration details — use trading-architecture
* order lifecycle or risk control design — use trading-architecture
* signal generation or market data pipeline design — use trading-architecture

# Crate Dependency Map

Always start analysis from this verified dependency graph:

```
barter-macro (proc-macro, no deps on sibling crates)
barter-integration (foundational: Validator, Transformer, Terminal, RestClient, ExchangeStream, feature-gated)
    ↓
barter-instrument (ExchangeId, Asset, Instrument, Side, IndexedInstruments, ExchangeIndex/AssetIndex/InstrumentIndex)
    ↓
┌───────────────┬──────────────────────┐
↓               ↓                      ↓
barter-data     barter-execution       (external consumers)
(MarketStream,  (AccountEvent,
ExchangeWsStream, AccountSnapshot,
SnapshotFetcher, ExecutionClient,
exchange        order lifecycle)
connectors)
    ↓               ↓
    └───────┬───────┘
            ↓
          barter
          (Engine, SystemBuilder, System,
          AlgoStrategy, RiskManager,
          EngineState, statistics, backtest)
```

Applications (`risk/main.rs`, `strategy/main.rs`, `miraelis/src/main.rs`) consume the libraries but are not workspace members — they are binaries defined in the root Cargo.toml.

# Key Architectural Patterns

## Pattern 1: Generic Engine Composition (barter)

`Engine<Clock, State, ExecutionTxs, Strategy, Risk>` is the central abstraction. It composes five generic parameters:
- **Clock**: `EngineClock` trait — provides `time()`. `LiveClock` for live, `HistoricalClock` for backtesting.
- **State**: typically `EngineState<GlobalData, InstrumentData>` with trading state, connectivity, assets, instruments
- **ExecutionTxs**: `ExecutionTxMap` trait — routes `ExecutionRequest`s to exchange-specific transmitters
- **Strategy**: implements `AlgoStrategy`, `ClosePositionsStrategy`, `OnDisconnectStrategy`, `OnTradingDisabled`
- **Risk**: `RiskManager` trait — `check()` returns `(approved_cancels, approved_opens, refused_cancels, refused_opens)`

Engine implements `Processor<EngineEvent>`, receiving events and delegating to strategy/risk.

## Pattern 2: SystemBuilder Composition (barter)

`SystemBuilder` takes `SystemArgs` (instruments, executions, clock, strategy, risk, market_stream) and builds a `System`:
- Constructs `EngineState` via `EngineStateBuilder` from indexed instruments
- Builds execution infrastructure (`MultiExchangeTxMap`)
- Spawns market-to-engine and account-to-engine forwarding tasks
- Runs engine in one of 4 modes: `{Iterator, Stream} × {AuditMode::Enabled, Disabled}`
- Returns `System` with `feed_tx` for sending commands, `engine` JoinHandle, and optional audit stream

## Pattern 3: Stream-Based Market Data (barter-data)

`ExchangeWsStream<Parser, Transformer>` implements `MarketStream<Exchange, Instrument, Kind>`. Init flow:
1. `Subscriber::subscribe()` — connect WebSocket, validate subscriptions
2. `SnapshotFetcher::fetch_snapshots()` — optional initial snapshot (e.g., OrderBooksL2)
3. Split WebSocket into WsSink + WsStream, spawn ping/pong tasks
4. `ExchangeTransformer::init()` — initialize transformer with instrument map

## Pattern 4: StrategyModule Plugin System (miraelis)

`StrategyEngine` owns `Vec<Box<dyn StrategyModule>>` and a shared `StrategyContext`:
- `StrategyModule` trait: `id()`, `name()`, `init()`, `start()`, event handlers (`handle_trade`, `handle_candle_1m`, etc.)
- `dispatch()` persists events into per-symbol `UhfTradeWindow`s, then fans out to all modules
- Signal modules send `TradeSignalBase` variants over `order_tx` channel and/or Telegram

## Pattern 5: Indexed Data Structures

`IndexedInstruments` maps each instrument to `InstrumentIndex` (newtype over usize). Used with `vecmap-rs` for O(1) lookups. `ExchangeIndex` and `AssetIndex` follow the same pattern. This is preferred over HashMap throughout the ecosystem.

# Analysis Workflow

Before answering any architecture question:

1. **Identify affected crates** from the dependency graph above
2. **Trace the trait boundaries** — which traits are involved, who implements them
3. **Trace ownership flow** — where is data created, who owns it, how is it shared (channels, Arc, &ref)
4. **Trace concurrency** — sync blocking threads (spawn_blocking) vs async tokio tasks, channel types (mpsc::unbounded)
5. **Verify with source** — always inspect actual impl blocks, never assume

# Design Principles

Prefer patterns already established in the codebase:
* Generic composition over trait objects (see Engine's five type parameters)
* Indexed lookups over HashMap for instrument/asset/exchange access
* `Processor<Event>` trait for event-handling components
* Channel-based communication between async tasks (tokio::mpsc::unbounded_channel)
* `#[async_trait]` only where unavoidable — barter-integration uses sync `Transformer` trait with spawned tasks for async I/O

Avoid:
* Introducing new crates without clear separation from existing ones
* Mixing the barter Engine pattern with the miraelis StrategyEngine pattern — they are distinct
* Adding dependencies that reverse the crate dependency direction
* `Arc<Mutex<T>>` where channel-based ownership transfer would work

# Refactoring Rules

Refactor only if:
* the change reduces generic parameter count without loss of flexibility
* the change removes a crate dependency that shouldn't exist
* the change consolidates duplicated trait impls across exchanges
* the change simplifies the public API (fewer traits, fewer type parameters)

Do not refactor for style alone. The codebase uses explicit type parameters extensively — this is intentional for monomorphization.

# Output Format

## Summary
Architecture overview specific to the crates involved.

## Crate Impact
Which crates are affected and how the dependency graph changes.

## Trait/Interface Analysis
Key traits involved, their impls, and proposed changes.

## Ownership & Concurrency
Data flow, ownership boundaries, synchronization points.

## Risks
Architectural concerns (circular deps, layering violations, generic complexity).

## Recommendations
Evidence-based, referencing specific files and trait impls.
