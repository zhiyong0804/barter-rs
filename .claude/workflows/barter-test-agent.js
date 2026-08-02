export const meta = {
  name: 'barter-test-agent',
  description: 'Autonomous quantitative agent: discover miraelis strategies, validate market data, run iterative backtests to find optimal hyperparameters.',
  phases: [
    { title: 'Discovery', detail: 'List available strategies and validate market data' },
    { title: 'Tuning', detail: '3-run optimization loop per strategy' },
    { title: 'Synthesis', detail: 'Compile final results matrix sorted by Sharpe ratio' },
  ],
}

// ── Phase 1: Discovery ──────────────────────────────────────────────

phase('Discovery')

const DISCOVERY_SCHEMA = {
  type: 'object',
  properties: {
    strategies: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          name: { type: 'string' },
          id: { type: 'number' },
          source_file: { type: 'string' },
          parameters: { type: 'array' },
        },
      },
    },
  },
  required: ['strategies'],
}

const MARKET_SCHEMA = {
  type: 'object',
  properties: {
    file: { type: 'string' },
    size_bytes: { type: 'number' },
    format: { type: 'string' },
    columns: { type: 'array' },
    total_lines: { type: 'number' },
    snapshot: { type: 'array' },
    error: { type: 'string' },
  },
}

const strategiesResult = await agent(
  `Scan the miraelis source code at miraelis/src/strategy/ to find all StrategyModule implementations.
   For each module, extract:
   - name (e.g., "frame", "huge_momentum", "rocket")
   - strategy id (from module_id constants or fn id())
   - the config struct fields (parameter names and types)
   
   Return a structured list of all discovered strategies with their parameters.`,
  { schema: DISCOVERY_SCHEMA, label: 'discover-strategies' }
)

const file_name = args?.file_name || 'ALTUSDT.candle_1m'
const market_dir = args?.market_dir || 'miraelis/tests/data'

const marketResult = await agent(
  `Read and validate the market data file at "${market_dir}/${file_name}".
   Determine:
   - file size
   - format (JSONL, CSV, etc.)
   - column names
   - total number of records
   - a 3-row data snapshot
   
   Return structured metadata about this file.`,
  { schema: MARKET_SCHEMA, label: 'validate-market-data' }
)

log(`Discovered ${strategiesResult?.strategies?.length || 0} strategies`)
log(`Market data: ${marketResult?.total_lines || '?'} rows, format=${marketResult?.format || '?'}`)

// ── Phase 2: Tuning Loop ───────────────────────────────────────────

phase('Tuning')

const BACKTEST_SCHEMA = {
  type: 'object',
  properties: {
    strategy: { type: 'string' },
    run: { type: 'number' },
    parameters: { type: 'object' },
    success: { type: 'boolean' },
    exit_code: { type: 'number' },
    metrics: {
      type: 'object',
      properties: {
        sharpe_ratio: { type: 'number' },
        max_drawdown: { type: 'number' },
        pnl: { type: 'number' },
        win_rate: { type: 'number' },
      },
    },
    stderr_tail: { type: 'string' },
    error: { type: 'string' },
  },
  required: ['strategy', 'run', 'parameters', 'success'],
}

const strategies = strategiesResult?.strategies || []

async function tuneStrategy(strategy, index) {
  const name = strategy.name
  const configParams = strategy.parameters || []
  
  log(`[${index + 1}/${strategies.length}] Tuning ${name} (id=${strategy.id}, ${configParams.length} params)`)
  
  // Run baseline
  const baselineParams = {}
  for (const p of configParams) {
    // pick default/median values based on type
    if (p.type?.includes('f64') || p.type?.includes('f32')) {
      baselineParams[p.name] = 1.0
    } else if (p.type?.includes('u64') || p.type?.includes('i64') || p.type?.includes('usize')) {
      baselineParams[p.name] = 60
    } else if (p.type?.includes('bool')) {
      baselineParams[p.name] = false
    } else {
      baselineParams[p.name] = 1
    }
  }
  
  const run1 = await agent(
    `Run a backtest for strategy "${name}" with these baseline parameters:
    ${JSON.stringify(baselineParams, null, 2)}
    
    Execute: cargo test -p miraelis-market-ingest -- strategy::${name}::tests --nocapture 2>&1
    
    Parse the output to extract any performance metrics (sharpe, drawdown, pnl, win_rate).
    Even if specific metrics aren't printed, report success/failure and any numeric results found.
    Return the metrics found and the exit code.`,
    { schema: BACKTEST_SCHEMA, label: `${name}:run1-baseline` }
  )
  
  // Run exploration
  const exploreParams = { ...baselineParams }
  // Increase thresholds, tighten windows
  for (const k of Object.keys(exploreParams)) {
    if (typeof exploreParams[k] === 'number' && exploreParams[k] > 1) {
      exploreParams[k] = Math.round(exploreParams[k] * 2)
    }
  }
  
  const run2 = await agent(
    `Run a second backtest for strategy "${name}" with EXPLORATION parameters:
    ${JSON.stringify(exploreParams, null, 2)}
    
    Execute: cargo test -p miraelis-market-ingest -- strategy::${name}::tests --nocapture 2>&1
    
    Parse the output for metrics. Compare with baseline run.
    Return metrics and exit code.`,
    { schema: BACKTEST_SCHEMA, label: `${name}:run2-explore` }
  )
  
  // Run convergence
  const convergeParams = { ...exploreParams }
  // Adjust further based on direction
  for (const k of Object.keys(convergeParams)) {
    if (typeof convergeParams[k] === 'number' && convergeParams[k] > 1) {
      convergeParams[k] = Math.round(convergeParams[k] * 1.5)
    }
  }
  
  const run3 = await agent(
    `Run a third backtest for strategy "${name}" with CONVERGENCE parameters:
    ${JSON.stringify(convergeParams, null, 2)}
    
    Execute: cargo test -p miraelis-market-ingest -- strategy::${name}::tests --nocapture 2>&1
    
    Parse output. Determine which of the 3 runs had the best metrics.
    Return metrics and exit code.`,
    { schema: BACKTEST_SCHEMA, label: `${name}:run3-converge` }
  )
  
  // Determine best run
  const runs = [
    { run: 1, params: baselineParams, result: run1 },
    { run: 2, params: exploreParams, result: run2 },
    { run: 3, params: convergeParams, result: run3 },
  ].filter(r => r.result?.success)
  
  const best = runs.sort((a, b) => {
    const sa = a.result?.metrics?.sharpe_ratio || 0
    const sb = b.result?.metrics?.sharpe_ratio || 0
    return sb - sa
  })[0]
  
  return {
    strategy: name,
    id: strategy.id,
    best_run: best?.run || null,
    best_params: best?.params || {},
    best_metrics: best?.result?.metrics || {},
    runs_completed: runs.length,
  }
}

// Pipeline: each strategy tunes independently
const tuningResults = await pipeline(
  strategies,
  (strategy, _prev, index) => tuneStrategy(strategy, index)
)

// ── Phase 3: Synthesis ──────────────────────────────────────────────

phase('Synthesis')

const validResults = tuningResults.filter(Boolean)
validResults.sort((a, b) => (b.best_metrics?.sharpe_ratio || 0) - (a.best_metrics?.sharpe_ratio || 0))

log(`
========================================
  BARTER TEST AGENT — FINAL REPORT
========================================
`)

log(`| Strategy | Best Run | Sharpe | Max DD | Params |`)
log(`|----------|----------|--------|--------|--------|`)
for (const r of validResults) {
  const sharpe = r.best_metrics?.sharpe_ratio ? r.best_metrics.sharpe_ratio.toFixed(3) : 'N/A'
  const dd = r.best_metrics?.max_drawdown ? (r.best_metrics.max_drawdown * 100).toFixed(1) + '%' : 'N/A'
  const nParams = Object.keys(r.best_params || {}).length
  log(`| ${r.strategy} | ${r.best_run || '-'} | ${sharpe} | ${dd} | ${nParams} params |`)
}

return {
  discovered: strategies.length,
  tuned: validResults.length,
  market_data: {
    file: marketResult?.file,
    rows: marketResult?.total_lines,
  },
  leaderboard: validResults.map(r => ({
    strategy: r.strategy,
    best_run: r.best_run,
    sharpe: r.best_metrics?.sharpe_ratio || null,
    max_drawdown: r.best_metrics?.max_drawdown || null,
    parameters: r.best_params,
  })),
}