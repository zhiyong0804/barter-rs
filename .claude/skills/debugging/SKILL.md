---
name: debugging
description: Investigate, diagnose and resolve bugs, incidents and issues in barter-rs. Use when analyzing logs, crashes, unexpected behavior, message loss, order issues, exchange integration failures, performance regressions or system instability.
---

# Philosophy

- Symptoms are not root causes. Logs are evidence, not conclusions.
- Generate multiple hypotheses. Rank by probability. Attempt to falsify each one.
- Every conclusion must be supported by evidence from source code, logs, or metrics.
- State confidence explicitly: HIGH / MEDIUM / LOW.

# Investigation Workflow

## Phase 1 — Define & Scope

Produce a clear problem statement: expected behavior vs actual behavior, when it happened, which binary (risk-manager, strategy-bot, miraelis).

Then determine scope. For barter-rs, the key question is **which component in which crate**:

| Symptom area | Likely crate(s) | Key types to trace |
|-------------|----------------|-------------------|
| WebSocket disconnected, missing market data | barter-data, barter-integration | `ExchangeWsStream`, `ReconnectingStream`, `Subscriber`, `WsStream` |
| Engine not generating orders | barter | `Engine::process()`, `AlgoStrategy`, `TradingState`, `EngineState` |
| Orders not reaching exchange | barter, barter-execution | `ExecutionTxMap`, `ExecutionClient`, `RiskManager::check()` |
| Signal not firing | miraelis | `StrategyEngine::dispatch()`, `StrategyModule::handle_*()`, `UhfTradeWindow` |
| Wrong position/balance | barter, barter-execution | `EngineState`, `AccountEvent`, `PositionExited` |
| Performance regression | any | Hot path: market data → engine → strategy → execution |

## Phase 2 — Gather Evidence

barter-rs specific evidence sources, in priority order:

1. **Tracing logs.** JSON format with env-filter, written via `init_logging_with_prefix()`. Check `miraelis.log`, strategy logs, risk-manager logs. Key spans to look for: `EngineEvent` processing, `AlgoStrategy` output, `RiskManager` decisions, WebSocket connect/disconnect.
2. **Engine audit stream.** If `AuditMode::Enabled`, the `AuditTick` stream records every event processed, every algo order generated, every risk decision. Replay it to reconstruct engine state.
3. **Market data files.** `bin/data/market/` contains JSONL shard files written by `AsyncRollbackWriter`. Check for gaps, duplicates, or malformed records.
4. **Exchange info cache.** `bin/data/binance_futures_exchange_info.json` — verify symbol specs match the exchange's current state.
5. **Source code.** Never infer behavior — trace the actual impl. Key trace paths:
   - Market data ingest: `ExchangeWsStream::init()` → `Subscriber::subscribe()` → `ExchangeTransformer::transform()`
   - Engine processing: `Engine::process()` → match `EngineEvent` → `AlgoStrategy::generate_algo_orders()` → `RiskManager::check()` → `ExecutionTxMap::send()`
   - Execution: `ExecutionClient::open_order()` → Binance REST → `AccountEvent`
   - miraelis: `FutureQuotation::run_market_streams()` → `StrategyEngine::dispatch()` → `StrategyModule::handle_trade()` / `handle_candle_1m()`

## Phase 3 — Isolate Root Cause

### For market data issues
- Sequence gaps in the JSONL shard files? → Check `ReconnectingStream` reconnect logic, `Subscriber` subscription validation
- Duplicate or out-of-order events? → Check `ExchangeTransformer` output, sequence number handling in subscription module
- Missing instruments? → Check `IndexedInstruments` construction, subscription filter, exchange info sync
- WebSocket connect fails → Check TLS setup (rustls-webpki-roots), exchange base URL config, API credentials

### For engine / strategy issues
- Algo orders not generated? → Check `TradingState` (is it Disabled?), `AlgoStrategy::generate_algo_orders()` impl, `EngineState` instrument data
- Orders generated but not sent? → Check `RiskManager::check()` — are orders being refused? Check `ExecutionTxMap` — is the transmitter working?
- Wrong position state? → Trace `EngineState::update_from_account()` → `AccountEvent::Trade` → `PositionExited`
- Strategy called on disconnect but shouldn't be? → Check `ConnectivityStates` update logic, `OnDisconnectStrategy` impl

### For execution issues
- Duplicate orders? → Check `ClientOrderId` generation (is it idempotent?), retry logic in `ExecutionClient`
- Order timeout? → Check REST client timeout config, Binance API rate limits, exchange status
- Account snapshot stale? → Check user data stream WebSocket, `AccountEvent` channel delivery

### For miraelis signal issues
- Signal not firing? → Check `StrategyModule::handle_*()` impl, threshold values in config, `UhfTradeWindow` data accumulation
- Wrong signal output? → Check warm-up logic (`warm_up_trade_windows`), kline data in `QuotationKline`, `OrderResponse` handling
- Execution not triggered? → Check `execution_cfg.enabled`, `order_tx` channel, execution task spawn

### For concurrency issues
- Deadlock or hang? → Check `Mutex`/`RwLock` acquisition order, `spawn_blocking` saturation (engine blocks all trading while processing)
- Data race? → `#![forbid(unsafe_code)]` makes this unlikely; if suspected, check any `unsafe` blocks (there shouldn't be any)
- Channel backpressure? → `mpsc::unbounded_channel` is used throughout — no backpressure by design, but check for dropped senders

### For performance issues
- Latency spike in market data path? → Check for `clone()` on large structs, `format!()` in hot path, `Vec` growth without `with_capacity`
- Engine processing slow? → Check `AlgoStrategy` complexity, `RiskManager::check()` iteration, `EngineState` update logic
- Memory growth? → Check `UhfTradeWindow` retention in miraelis, audit stream buffer accumulation, unbounded channel buildup

## Phase 4 — Resolution

1. **Immediate mitigation** — Can trading continue safely? Should `TradingState` be set to Disabled? Should a specific exchange be disconnected?
2. **Minimal fix** — The smallest change that addresses the root cause. No speculative rewrites.
3. **Regression test** — A test that reproduces the bug and verifies the fix. Reference the crate's `test_utils` module for test helpers.
4. **Prevention** — Does this class of bug exist elsewhere? (e.g., if one exchange connector has a sequence gap bug, check all others.)

# Output Format

```
## Problem Statement
Expected vs actual behavior. When it happened. Which binary/crate.

## Evidence
Logs, audit events, market data, source code traces.

## Hypotheses
Ranked by probability with supporting/contradicting evidence. Confidence per hypothesis.

## Root Cause
Most likely root cause with evidence chain.

## Contributing Factors
Secondary causes that enabled or amplified the issue.

## Fix
Minimal code change. Where (file + line range). Why it resolves the root cause.

## Regression Test
What test to add. Which crate. What it verifies.

## Prevention
Other locations that may have the same class of bug.

## Confidence
HIGH / MEDIUM / LOW — overall confidence in root cause determination.
```
