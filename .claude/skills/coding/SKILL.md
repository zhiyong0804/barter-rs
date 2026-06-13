---
name: coding
description: General coding discipline for barter-rs. Use as the default behavioral guardrail for all implementation work — new features, bug fixes, refactoring, code review. For domain-specific architecture analysis, use architecture or trading-architecture skills.
---

# Four Principles

All coding work in barter-rs follows four principles. When in doubt, bias toward the principle.

## 1. Think Before Coding

Stop and surface assumptions before writing anything.

**In barter-rs, specifically:**

- **Generic boundaries.** `Engine<Clock, State, ExecutionTxs, Strategy, Risk>` has 5 type parameters. If you think you need a 6th, you're probably putting something at the wrong layer. Ask: does this belong in `GlobalData`? In the Strategy? In EngineState?
- **Crate dependency direction.** `barter-integration` is the foundation — it cannot depend on any sibling crate. If your change requires reversing the dependency graph, stop. You're in the wrong crate.
- **Two architecture styles.** barter uses static generic composition (`Engine<...>`, `Processor<Event>`). miraelis uses dynamic trait-object dispatch (`Box<dyn StrategyModule>`). Do not mix them unless you have a documented reason.
- **Exchange patterns.** Every exchange connector follows the same `Connector → Subscriber → ExchangeTransformer → (optional) SnapshotFetcher` pattern. If your approach doesn't fit this shape, ask whether the pattern needs extending or your approach needs adjusting.

**When stuck:** name what's confusing. Ask. Do not code through confusion.

## 2. Simplicity First

Write the minimum code that solves the problem. Nothing speculative.

**In barter-rs, specifically:**

- **Don't create a new trait** when `Processor`, `Transformer`, `Validator`, or `MarketStream` already covers the need. The ecosystem already has enough traits.
- **Don't add a feature flag** to barter-integration unless a real use case demands it *now*. "We might need it later" is not a reason.
- **Don't add configuration options** to a miraelis signal module unless they were requested. A hard-coded threshold is better than an over-engineered config system that nobody asked for.
- **Don't add a new crate** for something that's a single module in an existing crate.
- **Follow the litmus test:** if the implementation is 200 lines and could be 50, rewrite before committing.

**Ask yourself:** would a senior Rust engineer maintaining barter-rs look at this and think it's overcomplicated?

## 3. Surgical Changes

Touch only what's necessary. Match the existing style.

**In barter-rs, specifically:**

- **Exchange modules are self-contained.** Adding a Binance Spot subscription kind only touches files under `barter-data/src/exchange/binance/spot/`. Do not "fix" Binance Futures code in the same PR.
- **Style is dictated by the project, not you.** `rustfmt.toml` sets `edition = "2024"` and `imports_granularity = "crate"`. Lint rules enforce `clippy::cognitive_complexity`, `unused_crate_dependencies`, and forbid `unsafe_code`. If the project style differs from your preference, match the project.
- **Match existing patterns exactly.** If every other exchange's `Subscriber::subscribe()` returns `Subscribed { websocket, map, buffered_websocket_events }`, yours should too.
- **Clean up your own orphans.** Unused imports, variables, or functions you introduced must be removed. clippy catches these, but don't rely on clippy to do your cleanup.
- **Don't delete code that isn't yours or isn't broken.** If you see dead code unrelated to your task, mention it — don't delete it.
- **Test:** every changed line should trace back to the user's request.

## 4. Goal-Driven Execution

Convert every task into a verifiable success criterion before starting.

**In barter-rs, the verification gates are built-in:**

| Task type | Verification |
|-----------|-------------|
| Add exchange connector | `cargo check -p barter-data` + run the corresponding example |
| Add strategy / risk | `cargo test -p barter` — write a test that runs Engine with your component |
| Fix a bug | Write a reproducing test first, verify it fails, then fix, verify it passes |
| Add signal module | `cargo run --bin miraelis -- --config <test_config>` — verify signal output |
| Refactor | `cargo test && cargo clippy -- -D warnings` — all tests pass before AND after |
| Any change | `cargo check && cargo test && cargo fmt --all -- --check && cargo clippy -- -D warnings` |

**CI is the final gate.** The CI workflow runs `check → test → fmt → clippy`. All four must pass.

**For multi-step tasks:** write a 3-5 step plan. Each step must have a verification check. Start coding only after the plan is clear.

---

# Workflow

1. **Understand the request.** Clarify ambiguities before writing code.
2. **Search existing implementation.** Find the closest existing code (same exchange, same pattern, same crate) and read it.
3. **Identify the traits involved.** Trace which traits you'll implement or extend.
4. **Write the verification first.** A test, a compile check, an example run — something that proves the work is done.
5. **Implement.** Follow principles 1-4.
6. **Verify.** Run the check from step 4. Then run the full CI suite.
