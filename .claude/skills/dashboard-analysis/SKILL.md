---
name: dashboard-analysis
description: Analytical framework for liquidity, momentum, market structure, narrative and early-momentum signals.
---

# dashboard-analysis

Core metrics:

`volume_ratio = 24h_volume / avg_30d_volume`

`depth_ratio = depth_100 / 24h_volume`

`volume_to_market_cap = avg_30d_volume / circulating_market_cap`

`fdv_premium = fdv / circulating_market_cap`

A signal requires:

1. observed evidence
2. interpretation
3. risk/invalidation

States:

LEADING, BREAKOUT_WATCH, ACCUMULATION_WATCH, HIGH_MOMENTUM,
HIGH_LIQUIDITY, NEUTRAL, WEAKENING, LOW_LIQUIDITY_RISK

Suggested score:

volume acceleration 30%
momentum 20%
liquidity 20%
market-cap efficiency 15%
narrative 15%

Never present an analytical watch state as a guaranteed price prediction.
