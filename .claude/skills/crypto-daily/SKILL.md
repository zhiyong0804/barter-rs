---
name: crypto-daily
description: Daily crypto market intelligence pipeline. Data collection via Python scripts (Binance/Hyperliquid/CoinGecko/F&G), feature computation (OI delta, Volume Ratio, quadrants, anomalies), and LLM-driven report synthesis. Use for generating daily crypto market reports.
---

# Crypto Daily Market Intelligence

分析框架：**Python 做事实，LLM 做解释**。

数据层用 Python 脚本采集和计算，输出结构化 JSON。Agent 只读取 feature JSON，不做任何数值计算。

## Tools in this Skill

| Tool | Location | Purpose |
|------|----------|---------|
| `daily_market_data.py` | `.claude/skills/crypto-data/` | 采集 Binance/Hyperliquid/CoinGecko/F&G 数据，计算所有特征 |
| `daily_market_score.py` | `.claude/skills/crypto-data/` | 计算加权 Market Score (0-100) |

## Pipeline

### Phase 1: 市场数据采集

```bash
python3 .claude/skills/crypto-data/daily_market_data.py -o /tmp/daily_market_data.json
```

输出 `/tmp/daily_market_data.json`：包含 global 指标、每个币种的 Binance Futures/Spot/Hyperliquid 数据、OI×Price 四象限分类、Funding×OI 状态、异常检测结果。

### Phase 2: 新闻与事件（Agent 层）

并行启动 sub-agent 或使用 WebSearch：
- **Tier 1 官方**: SEC, Fed, ETF flows, protocol upgrades
- **Tier 2 专业媒体**: CoinDesk, Cointelegraph, The Block, Reuters
- **重要人物**: CZ, Vitalik, Saylor, Arthur Hayes, Trump, Powell, Armstrong, Sun, Wood
- **公告**: Binance listing/delisting, Hyperliquid new markets

每条事件结构化：
```json
{
  "event": "事件简述",
  "asset": "BTC/ETH/SOL/...",
  "importance": "HIGH/MEDIUM/LOW",
  "direction": "BULLISH/BEARISH/NEUTRAL",
  "confidence": 85,
  "impact_horizon": "1-24h/1-7d/1-3m"
}
```

### Phase 3: Market Score

```bash
python3 .claude/skills/crypto-data/daily_market_score.py /tmp/daily_market_data.json -o /tmp/daily_market_score.json
```

### Phase 4: 市场 Regime 判定

综合数据判定：
- **Market Regime**: RISK-ON / RISK-OFF / NEUTRAL
- **Trend**: BULLISH / BEARISH / RANGING
- **Leverage**: BUILDING / STABLE / DELEVERAGING
- **Liquidity**: INCREASING / STABLE / DECREASING
- **Smart Money**: ACCUMULATING / NEUTRAL / DISTRIBUTING
- **Narrative**: 当前主导叙事（AI / MEME / DEFI / L1 / L2 / RWA / ...）

### Phase 5: 异常扫描

从 top_movers 中提取：
- Top Gainers (涨幅 > 10%)
- Top Losers (跌幅 > 10%)
- Volume Explosion (Vol Ratio > 5x)
- OI Explosion (OI Δ > ±20%)
- Price/OI Divergence

### Phase 6: 报告生成

保存到 `doc/daily/YYYY-MM-DD_daily_report.md`

---

## Market Score 公式

| 维度 | 权重 | 计算方 | 说明 |
|------|------|--------|------|
| Price Trend | 20% | Python | BTC/ETH/SOL 加权均价变化。涨幅 → 高分 |
| OI | 20% | Python | OI Δ% 均值。增仓 → 看多 |
| Funding | 10% | Python | 平均费率。地板→高分；极端→低分 |
| Volume | 10% | Python | Vol Ratio。正常(0.8-1.5)→健康；极端→异常 |
| Liquidation | 10% | Python | 清算活动。当前为占位 |
| Fear & Greed | 10% | Python | F&G 指数。极端恐惧→反向看多 |
| News Sentiment | 10% | **LLM** | 新闻综合情感 -1 到 +1 |
| Macro | 5% | **LLM** | 宏观背景判断 |
| Exchange Events | 5% | **LLM** | 公告影响评估 |

数据驱动部分 (80%) 由 Python 计算，LLM 部分 (20%) 由 Agent 分析后填入。

---

## OI × Price 四象限 (guide §5)

| Price | OI | 象限 | 含义 | 持续性 |
|-------|-----|------|------|--------|
| ↑ (>+2%) | ↑ (>+5%) | NEW_LONG | 新资金做多 — 趋势启动 | 强 |
| ↑ (>+2%) | ↓ (<-5%) | SHORT_COVER | 空头回补 — 被动上涨 | 弱 |
| ↓ (<-2%) | ↑ (>+5%) | FRESH_SHORT | 新空头进入 — 主动做空 | 强 |
| ↓ (<-2%) | ↓ (<-5%) | LONG_LIQUIDATION | 多头平仓/去杠杆 — 被动下跌 | 中 |
| → (±2%) | ↑ (>+5%) | HIDDEN_ACCUMULATION | 隐藏吸筹 — 最优前置信号 | 极强 |
| → (±2%) | → (±5%) | NEUTRAL | 联动正常 | — |

---

## Funding × OI 组合状态 (guide §6)

| Funding | OI | Price | 状态 | 风险 |
|---------|-----|-------|------|------|
| > +0.05% | ↑ | ↑ | CROWDED_LONG | 高 — 回调风险 |
| < 0 | ↑ | ↓ | CROWDED_SHORT | 高 — 轧空风险 |
| — | ↓ | ↓ | DELEVERAGING | 中 — 下行趋势 |
| < +0.01% | ↑ | → | ACCUMULATION | 低 — 主力建仓 |
| > +0.05% | ↓ | — | LONG_CAPITULATION | 高 — 多头踩踏 |

---

## 异常检测阈值

| 指标 | 阈值 | 含义 |
|------|------|------|
| Price Δ | > ±10% | 异常波动 |
| Volume Ratio | > 5x | 成交量爆炸 |
| OI Δ | > ±20% | OI 异动 |
| Funding | > 0.10% | 极端过热 |
| Price/OI Divergence | Price↑+OI↓ or Price↓+OI↑ (同向>10%) | 背离信号 |

---

## Report Template

日报包含 11 个 Section（模板见下方），必须全部填充。如果某个 Section 没有数据，标注「数据不可得」而不是跳过。

```markdown
# Crypto Daily Intelligence Report
> {YYYY-MM-DD} 00:00 UTC | Market Score: XX/100 | Regime: XXXX

## 一、市场总览
[Market Score, Regime, Confidence]
[BTC/ETH/SOL 价格、涨跌、Volume、OI Δ]

Total Market Cap | BTC Dominance | 24h Volume
Fear & Greed: XX (XXXX) | Δ24h: ±X | Δ7d: ±X

## 二、Binance vs Hyperliquid
[OI, Funding, Volume 对比表]
[差异分析]

## 三、BTC / ETH / SOL 深度
[每个币种：Price, OI Δ(1h/4h/24h), Funding, Vol Ratio, Quadrant, Positioning State, LS Ratio, Basis]
[Hyperliquid 对比]

## 四、过去 24 小时重大事件
[事件列表：importance, direction, confidence, impact_horizon]
[按重要性排序]

## 五、重要人物动态
[CZ, Vitalik, Saylor, Hayes, Trump, Powell, ...]
[判定：Sentiment → Asset → Impact]

## 六、Binance 公告
[Listing / Delisting / Futures / Margin / Parameter Change]

## 七、Hyperliquid
[New Market / Delisted / Parameter Change]

## 八、BTC / ETH / SOL 生态动态
[各生态重大进展]

## 九、异常行情扫描
[Top Gainers, Top Losers, Volume Explosion, OI Explosion, Divergence]
[异常分类和风险评级]

## 十、未来 24 小时风险
[Token Unlock, Listing, Macro Event, Options Expiry, ...]

## 十一、Agent 最终判断
- Market Regime
- Trend
- Leverage
- Liquidity
- Smart Money
- Narrative
- Risk Score
- 一句话总结

> 本报告由 Crypto Daily Intelligence Agent 自动生成。
> 数据来源：Binance, Hyperliquid, CoinGecko, Alternative.me。
> 不构成投资建议。
```

---

## Guardrails

- **绝不编造数据**：如果 feature JSON 没有某个数值，标注「数据不可得」
- **区分事实与推测**：使用「分析师推测」/「市场预期」标注未确认信息
- **不给投资建议**：用「市场状态符合...」代替「BTC会涨」
- **引用数据来源**：每个数字都要有来源标注
- **中文输出**：报告用中文，保持客观、数据驱动
