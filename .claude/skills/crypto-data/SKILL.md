---
name: crypto-data
description: Comprehensive four-dimension token analysis (fundamentals, tokenomics, technical analysis with real kline data, on-chain data with top-10 address tracking). Fetches OHLCV from Binance/Hyperliquid, computes indicators (EMA/RSI/MACD/Bollinger/ATR), and produces structured analysis reports. Use for any token research or due diligence task.
---

# Crypto Token Analysis — Four Dimension Protocol

When analyzing any token, you MUST systematically work through all four dimensions below. Use the tools provided by this skill for data, and WebSearch/WebFetch for qualitative research.

## Tools in this Skill

| Tool | Location | Purpose | Chains |
|------|----------|---------|--------|
| `fetch_klines.py` | Same directory as this file | Fetch real OHLCV from Binance or Hyperliquid | All (Binance spot + Hyperliquid perps) |
| `compute_indicators.py` | Same directory as this file | Compute 7 indicators from kline CSV | All |
| `scan_holders.py` | Same directory as this file | Fetch top 100 holders from CoinLore (free, no key) | **Ethereum, BSC** (established tokens) |
| `scan_holders_solana.py` | Same directory as this file | Fetch **full Top 100 addresses with balances** from Solana on-chain data (`getProgramAccounts`) + GeckoTerminal safety scores | **Solana** SPL tokens |

Dependencies: `python3` with `requests` (`python3 -m pip install --break-system-packages requests`).

**All tools are FREE and require NO API keys.** Solana public RPC works reliably for `getProgramAccounts`.

---

# Dimension 1: Fundamentals (基本面)

## Objective
Answer: *What is this project? Does it have real substance or is it just a narrative?*

## Data Sources
Use **WebSearch** and **WebFetch** to gather:

### 1.1 Project Identity
- What does the project do? Explain in one paragraph of plain language.
- When was it founded? What problem does it solve?
- Is it a Layer 1, Layer 2, DeFi protocol, infrastructure, meme coin, or something else?

### 1.2 Team
- **Names and roles** of key founders/core contributors.
- **Verifiable background**: LinkedIn, academic publications, previous projects, GitHub activity.
- **Public or anonymous?** Flag anonymous teams as a risk factor.
- Team size and composition (engineering vs marketing ratio).

### 1.3 Investors & Funding
- Who invested? (Binance Labs, a16z, Paradigm, Polychain, etc.)
- Total funding raised and at what valuation.
- Funding rounds and dates (seed, Series A, etc.).
- Grants or ecosystem support (Ethereum Foundation, Solana Foundation, etc.).

### 1.4 Product Maturity
- **Mainnet or testnet?** When did mainnet launch?
- **TVL** (if DeFi). **DAU** or active wallets.
- Real product usage or just whitepaper promises?
- Revenue model: does the protocol generate fees? How much?

### 1.5 Competitive Landscape
- List 2-4 direct competitors.
- What is this project's moat or differentiator?
- Market share within its niche.

## Output Format
```markdown
## 一、项目基本面

### 是什么
[One paragraph plain-language explanation]

### 团队
| 成员 | 角色 | 背景 |
|------|------|------|

### 投资背景
| 机构 | 轮次 | 金额 |
|------|------|------|

### 产品成熟度
| 产品 | 阶段 | 关键指标 |
|------|------|----------|

### 竞争格局
| 项目 | 定位 | 与本项目差异 |
|------|------|-------------|
```

---

# Dimension 2: Tokenomics (代币经济学)

## Objective
Answer: *Is the token designed to accrue value, or is it engineered to extract value from buyers?*

## Data Sources
Use **WebSearch** and **WebFetch** (CoinMarketCap, CoinGecko, project docs, Messari, Token Unlocks).

### 2.1 Supply Metrics
- **Total / max supply**: Is there a cap? Fixed or inflationary?
- **Circulating supply**: What % of max is actually liquid?
- **FDV vs Market Cap**: FDV/MC ratio. >3x = significant dilution risk.

### 2.2 Allocation
Break down the total supply:

| Category | % | Rationale |
|----------|---|-----------|
| Team & Advisors | | |
| Investors | | |
| Ecosystem / Foundation | | |
| Airdrop / Community | | |
| Liquidity / Market Making | | |
| Node / Staking Rewards | | |

### 2.3 Vesting & Unlocks
- **Team vesting**: cliff? linear? duration?
- **Investor vesting**: cliff? linear? duration?
- **Upcoming unlock events**: Dates and amounts. Is there a large cliff unlock coming?
- **Current float vs fully diluted timeline**: When will all tokens be circulating?

### 2.4 Value Accrual
- **What gives the token value?** Fee capture? Buyback & burn? Governance? Gas token? Pure speculation?
- **Fee generation**: Does the protocol generate real revenue in ETH/USDC, or only in its own token?
- **Staking yield**: Real yield (from protocol revenue) or inflationary yield (from token emissions)?

### 2.5 Inflation / Deflation
- Annual inflation rate.
- Any burn mechanism? EIP-1559 style? Buyback-and-burn?
- Net inflation after burns.

## Output Format
```markdown
## 二、代币经济学

### 供应
| 指标 | 数值 |
|------|------|
| 总量 / 流通量 / FDV / MC | |

### 分配
[Table as above]

### 解锁时间表
| 类别 | 占比 | 锁仓 | 解锁方式 |
|------|------|------|----------|

### ⚠️ 核心矛盾
[FDV/MC ratio + unlock timeline → dilution analysis]

### 价值捕获
[How the token accrues — or doesn't accrue — value]
```

---

# Dimension 3: Technical Analysis (盘面/技术分析) — REAL DATA

## Objective
Answer: *What does the price action actually tell us, using independently computed indicators rather than second-hand articles?*

This dimension is special: **do NOT rely on WebSearch for technical readings.** Use the Python scripts in this skill to fetch real data and compute indicators independently. This eliminates selection bias and stale data from analyst articles.

## Step 3.1: Fetch Kline Data

```bash
python3 .claude/skills/crypto-data/fetch_klines.py \
  --symbol {SYMBOL}USDT \
  --interval 1d --days 365 \
  -o /tmp/{token}_1d.csv 2>&1

python3 .claude/skills/crypto-data/fetch_klines.py \
  --symbol {SYMBOL}USDT \
  --interval 4h --days 180 \
  -o /tmp/{token}_4h.csv 2>&1
```

**Interval selection guide:**

| Analysis type | Interval | Range | Reason |
|---------------|----------|-------|--------|
| Macro trend + key S/R | `1d` | 180–365 days | Captures major swing levels, SMA 200 |
| Current trend + momentum | `4h` | 90–180 days | Best for EMA alignment, RSI divergence |
| Short-term entry/exit | `1h` | 30–90 days | Microstructure, near-term levels |

**Exchange selection:**
- **Binance** (`--exchange binance --symbol BTCUSDT`): Spot market. Use for any token listed on Binance.
- **Hyperliquid** (`--exchange hyperliquid --symbol BTC`): Perpetual futures. Use when Binance doesn't list the token, or when you specifically want derivatives data. Note: Hyperliquid uses coin-only symbols (BTC not BTCUSDT).

**If the token is NOT listed on either exchange**, fall back to WebSearch for CoinMarketCap/CoinGecko screenshots or TradingView-embedded charts. Clearly note in the report that indicators are from third-party sources, not independently computed.

## Step 3.2: Compute Indicators

```bash
python3 .claude/skills/crypto-data/compute_indicators.py /tmp/{token}_1d.csv --tail 90 -o /tmp/{token}_1d_ind.csv 2>&1
python3 .claude/skills/crypto-data/compute_indicators.py /tmp/{token}_4h.csv --tail 200 -o /tmp/{token}_4h_ind.csv 2>&1
```

Use `--tail N` to get only the most recent rows for the report.

**Computed indicators (all verified, formula-transparent):**

| Indicator | Column | Parameters | Interpretation |
|-----------|--------|------------|----------------|
| SMA | `sma_20`, `sma_50`, `sma_200` | 20, 50, 200 | Trend direction; dynamic support/resistance |
| EMA | `ema_12`, `ema_26`, `ema_50` | 12, 26, 50 | Faster trend; MACD components |
| RSI | `rsi_14` | 14 (Wilder) | >70 overbought, <30 oversold |
| MACD | `macd`, `macd_signal`, `macd_hist` | 12/26/9 | Momentum, crossovers, divergence |
| Bollinger | `bb_upper`, `bb_middle`, `bb_lower` | 20, 2σ | Volatility regime, mean reversion |
| ATR | `atr_14` | 14 (Wilder) | Volatility magnitude |
| Volume SMA | `volume_sma_20` | 20 | Volume confirmation |

## Step 3.3: Analyze the Indicator CSV

Read the last 5-10 rows of the indicator CSV. Perform structured analysis in this exact order:

### A. Trend Assessment (most important — do this first)

Read the **LAST row** of the daily indicator CSV:

```
1. Price vs SMA_50:   above = bullish trend, below = bearish trend
2. Price vs SMA_200:  above = long-term bullish, below = long-term bearish
3. SMA alignment:     20 > 50 > 200 = aligned bullish
                      20 < 50 < 200 = aligned bearish
                      mixed = consolidation/transition
4. EMA_12 vs EMA_26:  golden cross (12>26) or death cross (12<26)?
```

**Output**: State the overall trend as bullish / bearish / neutral with specific indicator values as evidence. Example: *"Bearish: price ($0.12) is below SMA_50 ($0.15) and SMA_200 ($0.18). SMA_20 ($0.13) < SMA_50 < SMA_200 — fully aligned bearish. EMA_12/26 death cross since March."*

### B. Momentum Check

```
1. RSI_14:  >70 = overbought, <30 = oversold, 30-70 = neutral.
2. MACD histogram direction (last 3-5 bars):
   - Bars growing more positive = bullish momentum accelerating
   - Bars growing more negative = bearish momentum accelerating
   - Bars shrinking toward zero = momentum exhausting (potential reversal)
3. RSI divergence: scan the last 20-30 rows.
   - Price makes lower low + RSI makes higher low = bullish divergence ⚠️
   - Price makes higher high + RSI makes lower high = bearish divergence ⚠️
```

### C. Key Support & Resistance Levels

Scan the indicator CSV for significant price levels:
```
1. Recent swing highs (last 90 periods): horizontal resistance.
2. Recent swing lows (last 90 periods): horizontal support.
3. SMA_200: dynamic support/resistance.
4. Bollinger Band extremes: upper band = potential resistance, lower band = potential support.
5. Volume clusters: sort by volume, identify prices where most volume traded.
```

Output specific price numbers for each level. Example: *"Resistance: $0.139 (June swing high), $0.164 (May high). Support: $0.098 (July low), $0.066 (ATL). SMA_200 at $0.15 acts as overhead resistance."*

### D. Volume Analysis

```
1. Compare last 3 candles' volume to volume_sma_20:
   - Well above SMA = significant interest (confirming move)
   - Well below SMA = low conviction (move may fail)
2. Price-volume relationship:
   - Price up + high volume = accumulation
   - Price down + high volume = distribution
   - Price up + low volume = weak rally, likely to fail
   - Price down + low volume = weak selling, may reverse
```

### E. Multi-Timeframe Check

Compare the daily and 4h indicator readings:
```
- Daily trend = direction. 4h trend = entry timing.
- If daily is bearish and 4h is also bearish → strong trend, don't fight it.
- If daily is bearish but 4h is oversold bullish → potential bounce, but counter-trend.
- If daily SMA alignment and 4h SMA alignment agree → higher conviction.
```

### F. Historical Context

```
1. What is the ATH and when? Current drawdown from ATH.
2. What is the ATL and when? Current rebound from ATL.
3. Monthly returns for the past 6-12 months (build a small table from the daily data).
4. Any notable volume/price anomalies in the data? Sudden spikes without news = potential insider activity.
```

## Step 3.4: Write the TA Section

```markdown
## 三、盘面/技术分析

### 历史走势
| 指标 | 数值 | 日期 |
|------|------|------|
| ATH | $X.XX | YYYY-MM-DD |
| ATL | $X.XX | YYYY-MM-DD |
| 现价 | $X.XX | 当前 |
| ATH 回撤 | -XX% | |

### 月度回顾
[Table: month | open | high | low | close | change%]

### 当前技术形态（日线 + 4H）
| 指标 | 日线 | 4H | 信号 |
|------|------|-----|------|
| SMA 排列 | | | |
| RSI | | | |
| MACD 柱 | | | |
| 成交量 vs SMA | | | |

### 关键价位
```
强阻力: $X.XX (理由)
阻力: $X.XX (理由)
支撑: $X.XX (理由)
强支撑: $X.XX (理由)
多头失效: $X.XX
```
```

---

# Dimension 4: On-chain Data (链上数据)

## Objective
Answer: *Who actually holds this token? Are insiders distributing to retail, or accumulating?*

## Data Sources
Use **WebSearch**, **WebFetch**, and **the `scan_holders.py` script** (in this skill's directory).

### Primary: scan_holders.py (CoinLore Rich List)

**This is the preferred method for established tokens.** CoinLore provides free, structured holder data with addresses, balances, percentages, and aggregate concentration stats. No API key needed. Covers 1000+ tokens across Ethereum, BSC, and other chains.

```bash
# Get top 10 holders as a markdown table
python3 .claude/skills/crypto-data/scan_holders.py --token dodo --format md --top 10

# Get top 10 as JSON (for programmatic use)
python3 .claude/skills/crypto-data/scan_holders.py --token dia --format json --top 10

# Find the correct slug for a token
python3 .claude/skills/crypto-data/scan_holders.py --search "dodo"

# Quick text summary
python3 .claude/skills/crypto-data/scan_holders.py --token dogecoin --top 10
```

**Coverage**: Established tokens on CoinGecko/CMC. For our analyzed tokens: DODO ✅, DIA ✅, DEXE ✅, DOGE ✅, VANRY ✅, but SUI/HYPE/PENGU/ZEROBASE/AKE/IDOL ❌.

### Fallback: WebSearch + WebFetch

When `scan_holders.py` returns 404 (token not on CoinLore), use WebSearch:
- `"{TOKEN} top holders etherscan bscscan distribution 2026"`
- `"{TOKEN} holder concentration whales"`
- WebFetch on Etherscan/BscScan holder pages and analysis articles

## Step 4.1: Basic Metrics

```
1. Total holder count by chain.
2. Contract address on the most active chain.
3. DEX liquidity depth (TVL in main pool, 24h volume).
4. Security audit status. Any red flags (honeypot, 100% tax, unverified contract).
```

## Step 4.2: Top 10 Address Deep Analysis (MANDATORY — most important sub-section)

This is the highest-signal part of on-chain analysis. For EACH of the top 10 addresses, investigate and report:

### Required Table

```
| # | 地址 | 持仓量 | 占比 | 6月前 | 6月变动 | 变动% | 类型 | 风险 |
|---|------|--------|------|-------|----------|-------|------|------|
| 1 | 0x1234..abcd | 1.2亿 | 12% | 1.5亿 | -3000万 | -20% | 交易所 | 🔴减持 |
| 2 | 0x5678..efgh | 0.8亿 | 8%  | 0.5亿 | +3000万 | +60% | 项目方 | ✅增持 |
```

### Field Definitions

1. **地址** — First 6 + last 4 chars (e.g. `0x123456...abcd`). Link to block explorer.
2. **持仓量** — Current balance in token amount + USD value.
3. **占比** — % of circulating supply.
4. **6月前** — Balance ~6 months ago. Search for historical holder snapshots. If exact data unavailable, use closest available date and mark as "约".
5. **6月变动** — Absolute change. `+` = accumulation, `-` = distribution.
6. **变动%** — Percentage change.
7. **类型** — Classify each address:
   - `交易所` — Exchange wallet (labeled on explorer)
   - `项目方/团队` — Team vesting or treasury
   - `做市商` — Market maker (GSR, Wintermute, Jump, etc.)
   - `早期投资者` — Investor vesting
   - `巨鲸/大户` — Unlabeled large holder
   - `流动性池` — LP contract
   - `跨链桥` — Bridge contract
   - `销毁地址` — Burn/null address (0x0000... or 0xdead...)
   - `未知`
8. **风险** — Flag patterns:
   - `🔴 持续减持` — Consecutive monthly outflows
   - `🔴 大额转出` — Recent large transfers to exchange deposit wallets
   - `🟡 即将解锁` — Vesting wallet approaching unlock cliff
   - `🟡 新地址` — Wallet appeared recently with large position
   - `✅ 持续增持` — Consecutive monthly accumulation
   - `✅ 长期锁仓` — No movement in 6+ months

### Aggregated Analysis After the Table

```
1. 集中度评估:
   - Top 5 combined %: ___
   - Top 10 combined %: ___
   - Compare to similar-marketcap tokens. >80% = high risk, <30% = well-distributed.

2. 趋势总结:
   - # accumulating: ___
   - # distributing: ___
   - Net flow direction over 6 months: inflow / outflow / neutral.

3. 扣除已知实体后的真实集中度:
   - Exclude burn, exchange, bridge addresses.
   - What % do team + investors + unknown whales hold?
   - This is the "real" insider concentration.

4. 最大风险地址:
   - Which single address poses the biggest risk and why?
   - (e.g., team wallet with 15% supply nearing unlock;
     unknown whale that's been selling 2% per month)
```

### Finding 6-Month Historical Data

Search strategy (in priority order):
```
1. "{TOKEN} top holders distribution January 2026"
2. "{TOKEN} whale holdings Q1 2026"
3. "{TOKEN} token holder analysis February 2026"
4. "{TOKEN} onchain report March 2026"
5. WebFetch CoinMarketCap holder stats, Etherscan holder historical snapshots via wayback machine if needed
```

**If 6-month data is genuinely unavailable**: mark as "数据不可得" and use the longest available period (e.g., "30日变动"). Note the shorter timeframe clearly.

## Step 4.3: Liquidity Analysis

```
1. Primary DEX pool: TVL and 24h volume.
2. Liquidity-to-market-cap ratio: <1% = extreme slippage risk.
3. CEX volume dominance vs DEX. Is most trading on CEX or DEX?
```

## Output Format

```markdown
## 四、链上数据

### 4.1 基础指标
| 链 | 持币地址 | 合约地址 |
|------|----------|----------|

### 4.2 Top 100 地址分析
#### 集中度总览
| 分组 | 持仓量 | 占总供应% | 风险等级 |
|------|--------|-----------|----------|
| Top 10 | — | — | — |
| Top 50 | — | — | — |
| Top 100 | — | — | — |

#### Top 10 地址详情
| # | 地址 | 持仓量 | 占比 | 类型 | 风险 |
|---|------|--------|------|------|------|

#### Top 11-100 摘要
[Compact summary: exchange wallets, team wallets, notable unknowns]

#### 集中度评估
[Bullet points including real concentration excluding known entities]

### 4.3 流动性
| 池子 | TVL | 24h 量 | TVL/MC 比 |
|------|-----|--------|-----------|
```

---

# Synthesis: Scoring & Report Assembly

After completing all four dimensions, synthesize into a final report.

## Scoring (1-10 per dimension)

| Dimension | Score | Rationale |
|-----------|-------|-----------|
| 基本面 | X/10 | [One sentence why] |
| 代币经济学 | X/10 | [One sentence why] |
| 盘面技术 | X/10 | [One sentence why] |
| 链上数据 | X/10 | [One sentence why] |
| **综合** | **X/10** | Average of above |

Score guidelines:
- **9-10**: Exceptional. Top 1% of tokens.
- **7-8**: Strong. Clear advantages, manageable risks.
- **5-6**: Average. Some strengths, notable weaknesses.
- **3-4**: Weak. Multiple structural problems.
- **1-2**: Severe red flags. Avoid.

## Scenario Analysis

```
📉 空头情景 (概率 X%):
   触发条件: ...
   目标价位: $X → $Y
   逻辑: ...

📊 中性情景 (概率 X%):
   触发条件: ...
   目标价位: $X
   逻辑: ...

📈 多头情景 (概率 X%):
   触发条件: ...
   目标价位: $X → $Y
   逻辑: ...
```

## Report File

Save to `doc/research/{SYMBOL}_分析报告.md` with this structure:

```
# 🔍 {TOKEN_NAME} ({SYMBOL}) 深度分析报告
> 分析日期：YYYY-MM-DD

## 一、项目基本面
## 二、代币经济学
## 三、盘面/技术分析
## 四、链上数据
  ### 4.1 基础指标
  ### 4.2 Top 10 地址深度分析
  ### 4.3 流动性
## 五、综合评估（✅优势 / 🔴风险 / 🎯催化剂）
## 六、多空情景分析
## 七、总结与建议
```

---

# Quick Reference: Commands

```bash
# Fetch
python3 .claude/skills/crypto-data/fetch_klines.py --symbol {S}USDT --interval {I} --days {D} -o /tmp/{name}_{I}.csv

# Compute
python3 .claude/skills/crypto-data/compute_indicators.py /tmp/{name}_{I}.csv --tail {N} -o /tmp/{name}_{I}_ind.csv

# Install deps (once)
python3 -m pip install --break-system-packages requests
```

---

# Guardrails

- **Never skip Dimension 3 (real data TA).** The Python scripts are the core value of this skill — they produce verifiable, independently-computed indicators. WebSearch TA articles are a fallback only when the token is not listed on Binance or Hyperliquid.
- **Never fabricate on-chain data.** If you cannot find Top 10 addresses with 6-month history from web search, mark fields as "数据不可得" and use whatever shorter timeframe data IS available. Do NOT make up wallet addresses or balances.
- **Cross-reference all claims.** If only one search result says something, flag it as unverified.
- **Numbers over adjectives.** Say "price $0.12, RSI 28.5, SMA_50 at $0.15" not "looks oversold on the daily."
- **Distinguish fact from speculation.** Use "分析师推测" / "市场预期" for unconfirmed catalysts.
- **Not investment advice.** Frame as analysis. End reports with disclaimer.
- **Language**: Match the user's language.
- **Date everything.** Crypto data decays fast — always include the analysis date.
