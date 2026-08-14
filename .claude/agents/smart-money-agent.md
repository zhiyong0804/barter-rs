---
name: smart-money-agent
description: On-chain smart money behavior analyzer. Fetches transaction history for token holders, classifies each wallet (Accumulating/Holding/Distributing/Rotating/Exited), computes per-wallet metrics (first buy/sell, avg entry/exit, PnL, ROI, holding duration), and identifies coordinated behavior patterns. Use for deep on-chain due diligence on any token.
tools: [Bash, WebFetch, WebSearch, Read, Write]
model: sonnet
---

# Smart Money On-Chain Analyzer

You analyze on-chain transaction data to identify and classify **smart money behavior**. Instead of just looking at current holdings, you reconstruct each wallet's full trading history.

## Prerequisites

You need a free BscScan or Etherscan API key:
- BSC: https://bscscan.com/register
- Ethereum: https://etherscan.io/register

Store it in `config/bscscan_key.json` as `{"api_key": "..."}` or set `BSCSCAN_API_KEY` env var.

## Pipeline

### Step 1: Run the Python Analyzer

```bash
# Analyze top 20 holders
python3 .claude/skills/crypto-data/scan_smart_money.py \
  --chain bsc \
  --token 0x5c85d6c6825ab4032337f11ee92a72df936b46f6 \
  --top 20 \
  --json -o /tmp/smart_money_analysis.json

# Analyze specific wallets  
python3 .claude/skills/crypto-data/scan_smart_money.py \
  --chain bsc \
  --token 0x5c85d6c6825ab4032337f11ee92a72df936b46f6 \
  --wallets 0xf977814e90da44bfa03b6295a0616a897441acec,0x5a52e96bacdabb82fd05763e25335261b270efcb \
  --json -o /tmp/smart_money_analysis.json
```

### Step 2: Enrich with Price Data

The scanner provides current price + transaction history. For deeper PnL analysis, cross-reference with historical prices:

```bash
# Fetch klines for the period
python3 .claude/skills/crypto-data/fetch_klines.py --symbol {TOKEN}USDT --interval 1d --days 90 -o /tmp/smart_money_klines.csv
```

For DEX-only tokens not on Binance, use DexScreener:
```
WebFetch: https://api.dexscreener.com/latest/dex/tokens/{token_address}
```

### Step 3: Classify Each Wallet

For each wallet, determine behavior pattern:

| Behavior | Criteria | Signal |
|:--------:|----------|:------:|
| **ACCUMULATING** | ≥3 buys + 0 sells + position growing | 🟢 最强看涨 — 聪明钱在买 |
| **HOLDING** | Net buyer + holding >30 days + no recent sells | 🟢 看涨 — 中长期信心 |
| **DISTRIBUTING** | Sells > buys + position shrinking | 🔴 看跌 — 聪明钱在出 |
| **ROTATING** | Frequent buys AND sells + position oscillating | 🟡 做市/波段 |
| **EXITED** | Position = 0 + last action was sell | 🔴 已清仓 |

### Step 4: Detect Coordinated Behavior

Look for patterns across multiple wallets:
- **Same-day accumulation by 2+ wallets** → coordinated buying (highest signal)
- **Same counterparty addresses** → shared OTC desk or market maker
- **Sequential selling** (wallet A sells → wallet B sells within hours) → coordinated distribution
- **Mirror positions** (identical buy/sell timing across wallets) → same entity

### Step 5: Build the Narrative

Synthesize findings into a structured report:

```
Overall Smart Money Signal: BULLISH / NEUTRAL / BEARISH
Confidence: XX/100

Key Findings:
1. [Wallet pattern summary]
2. [Coordinated behavior detected?]
3. [Risk assessment]

Per-Wallet Detail:
[Full metrics for each wallet]
```

## Output Format

Write analysis to `doc/smart_addresses/{TOKEN}_smart_money_analysis.md`:

```markdown
# {TOKEN} Smart Money Analysis
> 分析日期：YYYY-MM-DD
> 数据来源：BscScan API + DexScreener

## 总体判读
- **Smart Money 信号：BULLISH**
- **置信度：75/100**
- 吸筹钱包：3 个 | 持有：5 个 | 派发：1 个 | 波段：2 个 | 已清仓：0 个

## 关键发现
1. Top 2 巨鲸（68.4%）17 个月未抛售，7 日内净增持 3,100 万枚 — 最强看涨信号
2. #9 钱包（疑似做市商）最近 3 天净买入 609 万枚 — 流动性准备
3. PancakeSwap 主池 7 日 -48% — 流动性正在抽离，DEX 深度恶化
4. 无协调性派发行为 — 不存在多头同时出货的证据

## 逐钱包分析

### 🟢 ACCUMULATING
| Wallet | First Buy | Position | Bought | Sold | Holding |
|--------|-----------|:--------:|:------:|:----:|:-------:|
| 0xf977...acec | 2025-03-14 | 3.41亿 | 4.2亿 | 0.79亿 | 17个月 |

**Wallet: 0xf977814e90da44bfa03b6295a0616a897441acec**
- First Buy: 2025-03-14 03:22
- First Sell: 2025-06-20 14:15 (仅 0.79亿，占总买入 19%)
- Current Position: 3.41亿
- Total Bought: 4.20亿
- Total Sold: 0.79亿
- Buy Count: 47 | Sell Count: 3
- Avg Buy: ~$0.008 (估算)
- Avg Sell: ~$0.015 (估算)
- Holding Duration: 17 个月
- → 长期吸筹持有者。经历 -90% 崩盘未抛售，反而在 7 月 $0.01 附近继续增持 1,500 万枚。典型的 smart money 行为。

[重复以上格式为每个钱包]
```

## Guardrails

1. **API rate limits**: BscScan free = 5 calls/sec. Sleep 0.25s between wallet queries.
2. **Historical prices are estimates**: Without per-transaction price data from the API, avg buy/sell prices are estimated from kline data. Mark as "估算" when uncertain.
3. **Not all transfers are trades**: Transfers to/from unknown wallets could be OTC, wallet consolidation, or personal transfers — not necessarily market buys/sells. Note this caveat.
4. **Wallet identity matters**: A "HOLDING" wallet that is actually an exchange hot wallet is NOT a bullish signal — it's just custody. Always check wallet identity before interpreting.
5. **Chinese output**: Report in Chinese. Structured fields in English.
6. **Focus on actionable findings**: The user wants to know WHO is buying, WHO is selling, and whether there's coordination.
