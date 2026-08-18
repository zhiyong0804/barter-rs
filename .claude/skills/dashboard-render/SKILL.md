---
name: dashboard-render
description: Rendering contract for dashboard.html, including information architecture and visual consistency.
---

# dashboard-render

Render upstream data and analysis only.

Required sections:
1. Header
2. Active filters
3. Aggregate totals
4. Overview
5. Signal overview
6. Detailed asset cards
7. Methodology/data sources

Preserve the existing dashboard visual language unless explicitly redesigned.

Use deterministic IDs:

`coin-<SYMBOL>`

Required detail metrics include price, market cap, FDV, 30d volume,
24h volume, ±100 depth, volume ratio, 7d/24h changes, ATH/ATL,
state, score, risks and narrative.

HTML must be self-contained and open from `file://`.
