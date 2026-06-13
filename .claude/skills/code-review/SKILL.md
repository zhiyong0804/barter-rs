---
name: code-review
description: Review barter-rs code changes for correctness, trading safety, concurrency, latency, reliability, maintainability and architecture consistency. Use when reviewing pull requests, git diffs, bug fixes, refactors, exchange adapters, strategy implementations, risk engines or performance-sensitive code.
---

# Purpose

Prevent production incidents and trading losses. Preserve architectural consistency. Maintain low-latency characteristics.

Base findings on evidence from the code. Do not review personal style. State uncertainty explicitly when confidence is low.

# Review Phases

## Phase 1 — Context & Architecture

Before looking at code, identify: what crate(s) are affected, what trait(s) are involved, what workflow changes.

Then check:

- **Dependency direction.** Does the change make a lower crate depend on a higher one? `barter-integration` must never import from sibling crates.
- **Generic sprawl.** Did they add a type parameter to `Engine<...>` that should be an associated type on an existing trait? Did they add a type parameter where a value would suffice?
- **Pattern consistency.** Is this following the established pattern for its domain?
  - New exchange → `Connector → Subscriber → ExchangeTransformer → SnapshotFetcher`
  - New strategy → `AlgoStrategy` trait impl, tested with `Engine`
  - New signal module → `StrategyModule` trait impl, registered in `StrategyEngine`
  - New feature in barter-integration → feature-gated behind a new or existing flag
- **Architecture mixing.** Is this change crossing the boundary between the barter generic-Engine pattern and the miraelis trait-object StrategyEngine pattern? If so, is it intentional and documented?
- **Duplication.** Does this reimplement logic that already exists in another crate or module?

## Phase 2 — Correctness & Trading Safety

- **State transitions.** Every `OrderState` transition, every `TradingState` transition — are all paths covered? Does `EngineState` update idempotently on duplicate events?
- **Risk bypass.** Can algo orders reach execution without passing through `RiskManager::check()`? The path is `AlgoStrategy → RiskManager::check() → ExecutionTxMap` — verify no shortcuts.
- **Command handling.** Are new `Command` variants properly handled in `Engine::action()`?
- **Event processing.** Does the `Processor<Event>` impl handle all `EngineEvent` variants? Does `TradingState::Disabled` correctly suppress algo order generation?
- **Error propagation.** Are errors surfaced through the appropriate `thiserror` types? Are `Unrecoverable` errors marked correctly?
- **Edge cases.** Empty subscription lists, zero quantity, max precision, exchange returning unexpected fields.

For **market data code** specifically:
- Sequence continuity and gap handling
- Duplicate/out-of-order message handling
- Reconnect recovery — is `ReconnectingStream` used? Is `OnDisconnectStrategy` called?

For **execution code** specifically:
- Duplicate order prevention (idempotent order IDs)
- Retry safety — can a retry create a duplicate order?
- Exchange failure handling — what happens when Binance returns an error?

For **signal modules** (miraelis) specifically:
- Is signal emission deterministic given the same market data?
- Are signal thresholds/configurations isolated per module?
- Does `dispatch()` correctly persist events before fanning out to modules?

## Phase 3 — Rust, Performance & Concurrency

- **Allocations in hot paths.** Market data ingest, order book updates, strategy execution, order submission — are there `clone()`, `to_string()`, `format!()`, `Vec::new()` (without `with_capacity`), or `Box::new()` calls that could be avoided?
- **Arc usage.** Is `Arc<T>` justified, or would channel-based ownership transfer work? The project prefers channels over shared state.
- **Dynamic dispatch.** Does this introduce `dyn Trait` where the existing pattern uses static dispatch (generics)? miraelis is the exception — it intentionally uses `Box<dyn StrategyModule>`.
- **Synchronization.** Any new `Mutex`, `RwLock`, or `Arc` that could become a contention point? The engine runs in `spawn_blocking` — blocking the engine blocks all trading.
- **Indexed lookups.** Does this use `ExchangeIndex`/`InstrumentIndex`/`AssetIndex` with `vecmap-rs` for O(1) access, or did they fall back to `HashMap`?
- **`unwrap()` / `expect()` / `panic!()`.** Banned unless explicitly justified in a comment. The project lints against these.
- **`unsafe`.** Banned. `#![forbid(unsafe_code)]` is crate-level policy.

## Phase 4 — Reliability & Failure Modes

Assume: network failures, WebSocket disconnects, exchange downtime, duplicate messages, packet loss, process restarts.

- **Reconnect.** Does the change work correctly after `ReconnectingStream` cycles? Is state rebuilt or preserved correctly?
- **Idempotency.** If the same event arrives twice, does `EngineState` update identically?
- **Shutdown.** Does the component respect `Shutdown` signals? Does `SyncShutdown` or `AsyncShutdown` get implemented if needed?
- **Recovery.** After an exchange disconnect, does the system recover without manual intervention? Is `OnDisconnectStrategy` called?
- **Failure isolation.** Does a failure in one exchange's stream affect others? (It shouldn't — each exchange gets its own WebSocket connection and task.)

## Phase 5 — Testing

- Does the change include tests that exercise the new code path?
- If a bug fix: is there a regression test that fails before the fix and passes after?
- Are edge cases covered, or only happy path?
- Do existing tests still pass? (`cargo test`)
- For exchange connectors: is there a test that parses a real WebSocket message sample?
- For strategies: is there a test that runs `Engine::process()` with a constructed `EngineEvent` and verifies the output?

# Severity Levels

**🔴 BLOCKING** — Must fix before merge:
- Incorrect trading behavior, state corruption, order duplication, risk bypass
- Deadlock, data race, panic in hot path
- Dependency direction violation (`barter-integration` depending on sibling crates)
- Architecture pattern violation without documented justification

**🟠 IMPORTANT** — Should fix before merge:
- Missing validation or incomplete error handling
- Significant latency regression (allocation in hot path)
- Missing `OnDisconnectStrategy` call on a new disconnect path
- Test gap for a critical code path

**🟡 SUGGESTION** — Optional improvement:
- Simplification opportunity, readability, small optimization
- Pattern consistency improvement (matching existing style more closely)

**⚪ NIT** — Minor style. Don't over-focus.

**🟢 PRAISE** — Call out well-designed code explicitly.

# Output Format

```
## Summary
[One paragraph on what the change does]

## Findings
### 🔴 Blocking
- **File:line** — Finding. Why it matters. Suggested fix.

### 🟠 Important
- **File:line** — Finding. Why it matters. Suggested fix.

### 🟡 Suggestions
- **File:line** — Finding. Why it matters. Suggested fix.

### 🟢 Praise
- What was done well and why.

## Final Recommendation
APPROVE / APPROVE WITH SUGGESTIONS / REQUEST CHANGES
[One sentence justification]
```
