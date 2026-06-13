---
name: trading-architecture
description: Analyze and design barter-rs trading system architecture. Use when discussing exchanges, market data, order books, strategies, risk management, execution, portfolio management or trading workflows.
---

# When To Use

Use this skill when:
* adding a new exchange connector to barter-data or barter-execution
* designing market data pipelines (WebSocket subscriptions, transformers, snapshot fetchers)
* implementing AlgoStrategy or RiskManager for the barter Engine
* designing miraelis StrategyModule signal generators
* reviewing order lifecycle logic in barter-execution
* designing Engine Command flows (ClosePositions, CancelOrders, etc.)
* evaluating TradingState transitions and their strategy implications
* analyzing EngineState management (connectivity, assets, instruments, positions)
* discussing the audit stream and state replication patterns
* designing execution client integrations (Binance REST, mock execution)

# barter-rs Trading Architecture

The trading system spans four crates in a strict pipeline:

```
barter-integration (protocol layer)
    → barter-data (market data ingestion)
    → barter (engine, strategy, risk, state)
    → barter-execution (order execution, account state)
```

## Layer 1: Protocol & Connectivity (barter-integration)

Owns the low-level networking abstractions used by all other layers:
- **RestClient** — configurable signed/unsigned HTTP communication. Used by Binance execution clients for REST API calls (account info, order placement, exchange info).
- **ExchangeStream** — generic async stream protocol wrapper. Used by barter-data WebSocket market streams.
- **Transformer** trait — converts raw protocol input into iterator of output events. Synchronous trait; async I/O (pings, pongs) handled via spawned tokio tasks fed by mpsc channels.
- **Validator** trait — validates exchange subscription responses.
- Feature flags: `protocol`, `channel`, `socket`, `stream`, `subscription`, `metric`, `serde`, `collection`, `error`. The `channel` feature provides `Tx`, `UnboundedTx`, `UnboundedRx` used across all crates.

## Layer 2: Market Data (barter-data)

Converts exchange-specific WebSocket messages into normalized `MarketEvent<InstrumentKey, DataKind>`:
- **Exchange connectors** (Binance Spot/Futures, Coinbase, Kraken, OKX, Gate.io Spot/Future/Perpetual/Option, Bybit Spot/Futures, Bitfinex, Bitmex)
- **Subscription kinds**: PublicTrades, AggregatePublicTrades, OrderBooksL1/L2/L3, Candles (1m/1h), Ticker, Liquidation, MarkPrice
- **ExchangeWsStream<Parser, Transformer>** — generic implementation of `MarketStream`, parameterized by protocol parser and exchange transformer
- **SnapshotFetcher** — optional initial snapshot on connect (used by OrderBooksL2/L3 to bootstrap the book state)
- **ReconnectingStream** — wraps MarketStream with automatic reconnect logic
- **Streams builder API** — `Streams::<Kind>::builder().subscribe([...]).init().await` for multi-exchange setup

Exchange-specific logic is isolated per exchange module (e.g., `barter-data/src/exchange/binance/futures/`), each providing its own `Connector`, `Subscriber`, `ExchangeTransformer`, and optionally `SnapshotFetcher` implementations.

## Layer 3: Trading Engine (barter)

The core decision-making layer. Key components:

### Engine Event Processing

`Engine` implements `Processor<EngineEvent>`. On each event:
1. **Shutdown** → return immediately with shutdown audit
2. **Command** → action the command (SendCancelRequests, SendOpenRequests, ClosePositions, CancelOrders)
3. **TradingStateUpdate** → update state; if transitioning to Disabled, call `OnTradingDisabled` strategy
4. **Account** → update state from account event; on disconnect, call `OnDisconnectStrategy`
5. **Market** → update state from market event; on disconnect, call `OnDisconnectStrategy`
6. **If TradingState::Enabled** → call `AlgoStrategy::generate_algo_orders()`, run through `RiskManager::check()`, send approved requests via `ExecutionTxMap`

### EngineState

`EngineState<GlobalData, InstrumentData>` maintains:
- `trading: TradingState` — Enabled or Disabled
- `connectivity: ConnectivityStates` — per-exchange connection health
- `assets: AssetStates` — per-asset balances and statistics
- `instruments: InstrumentStates` — per-instrument market data, open orders, positions
- `global: GlobalData` — user-defined global state

`InstrumentDataState` trait allows pluggable per-instrument market data tracking.

### Strategy Interfaces

- **AlgoStrategy** — generates `(cancel_requests, open_requests)` from current EngineState. The primary strategy entry point.
- **ClosePositionsStrategy** — generates requests to close open positions, used by `Command::ClosePositions`
- **OnDisconnectStrategy** — called when an exchange's market or account stream disconnects
- **OnTradingDisabled** — called when TradingState transitions to Disabled

`DefaultStrategy` provides naive implementations (no-op for algo, market-order closes for positions). Real strategies implement these traits with custom logic.

### Risk Manager

`RiskManager::check()` receives algo-generated cancel and open requests, returns `(approved_cancels, approved_opens, refused_cancels, refused_opens)`. Risk decisions are authoritative — refused orders are dropped.

### Command System

External processes send `Command` variants to the Engine via `System` methods:
- `SendCancelRequests` / `SendOpenRequests` — directly submit orders
- `ClosePositions(filter)` — close positions matching InstrumentFilter
- `CancelOrders(filter)` — cancel orders matching InstrumentFilter

### System Composition

`SystemBuilder` → `SystemBuild` → `System`:
- **EngineFeedMode::Iterator** — engine runs in `spawn_blocking` sync thread (default for live)
- **EngineFeedMode::Stream** — engine runs as async task (useful for concurrent backtests)
- **AuditMode::Enabled** — engine sends `AuditTick`s on a channel for state replication/monitoring

## Layer 4: Execution (barter-execution)

Orders and account state:
- **AccountEvent** — variants: Snapshot (full account state), BalanceSnapshot, OrderSnapshot, OrderCancelled, Trade
- **AccountSnapshot** — full exchange account picture: balances + per-instrument orders
- **ExecutionClient** trait — unified interface for live and mock order execution
- **Order lifecycle**: Open → PartiallyFilled → Filled | Cancelled. State tracked via `OrderState` enum.
- **Mock execution** — for paper trading and backtesting. Simulates fills without real exchange interaction.
- **Binance execution** — real Binance Futures REST API integration for order placement, cancellation, and account queries.

## miraelis: Application-Level Signal Architecture

The miraelis binary implements an independent signal generation pattern distinct from the barter Engine:

### StrategyEngine + StrategyModule

```
MarketEvent (Trade, Candle1m, Candle1h, BestBidAsk, Ticker)
    → StrategyEngine::dispatch()
        → persist into per-symbol UhfTradeWindow (in StrategyContext)
        → fan out to all registered StrategyModules
            → each module analyzes the window
            → emits SignalBase variants over order_tx channel
            → notifies via Telegram (TelegramNotifier)
```

### Signal Modules
- **FrameSignalModule** — frame breakout signals with stop/reversal/exhaust logic
- **HugeMomentumSignalModule** — large momentum detection from volume/price patterns
- **RocketSignalModule** — sub-second bid/ask imbalance detection

### Signal Types
Each signal module emits typed signal structs (TradeFrameSignal, TradeRocketSignal, HugeMomentumSignal, MomentumSignal, PullbackSignal, OrderNotifySignal) with `format()` methods for Telegram rendering.

### Execution Integration
When `execution_cfg.enabled`, signals are sent over `order_tx` → Binance execution tasks that manage order lifecycle, tracking open/close/pnl, and reporting back via `order_response_rx`.

# Order Lifecycle (barter-execution)

```
OrderRequestOpen (generated by strategy)
    → ExecutionClient::open_order()
    → OrderState::Open (exchange accepted)
    → AccountEvent::Trade (partial fill)
        → OrderState::PartiallyFilled { filled_qty, avg_price }
    → AccountEvent::Trade (complete fill)
        → OrderState::Filled
    OR
    → OrderRequestCancel
        → AccountEvent::OrderCancelled
        → OrderState::Cancelled
```

Verify every state transition. `OrderState` tracks `time_exchange` for each transition, enabling ordering verification.

# Exchange Integration Checklist

When adding a new exchange or subscription kind:

### barter-data (market data)
1. Implement `Connector` (exchange ID, base URL, ping interval)
2. Implement `Subscriber` (WebSocket connect + subscription request/validation)
3. Implement `ExchangeTransformer` (exchange message → normalized `MarketEvent`)
4. Optionally implement `SnapshotFetcher` (initial state on connect)
5. Add to `barter-data/src/exchange/` following existing module structure

### barter-execution (account/orders)
1. Implement `ExecutionClient` for REST order management
2. Handle authentication (API key signing — typically HMAC-SHA256)
3. Implement account stream (user data WebSocket for order/trade/balance updates)
4. Add to `barter-execution/src/exchange/`

# Failure Analysis

Assume all of these will happen:
* WebSocket disconnects — `ReconnectingStream` handles automatic reconnect; `OnDisconnectStrategy` is called on each disconnect
* Out-of-order/dropped messages — order books use sequence numbers; snapshot reconciliation on sequence gap
* Exchange downtime — REST fallback for account snapshots; strategy continues with stale data or pauses per `OnDisconnectStrategy`
* Rate limiting — handled at the exchange adapter level (barter-integration protocol layer)
* Duplicate events — EngineState update logic is idempotent for most event types

# Latency Principles

The barter ecosystem is designed for low-latency operation:
* Zero-copy where possible — `SmolStr` for exchange identifiers, indexed lookups avoid hashing
* Pre-allocated buffers — `ExchangeStream` reuses parse buffers
* Synchronous `Transformer` trait avoids per-event allocation from `#[async_trait]`
* `spawn_blocking` for CPU-bound engine processing keeps tokio worker threads free for I/O
* Lock-free patterns preferred — channel-based state transfer over Arc<Mutex<T>>

# Output Format

## Trading Flow
End-to-end flow through the four layers for the feature being analyzed.

## Component Analysis
Which traits/structs are involved, what they own, what they produce.

## State Transitions
How EngineState or OrderState changes through the flow.

## Risk & Failure Analysis
What breaks, how recovery works, which strategy callbacks fire.

## Recommendations
Evidence-based, referencing specific trait impls and exchange modules.
