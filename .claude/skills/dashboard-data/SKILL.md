---
name: dashboard-data
description: Market-data collection, normalization, provenance and filtering contract for the dashboard.
---

# dashboard-data

The configured universe is authoritative:

`config/dashboard_universe.json`

Do not silently replace it with a dynamic Binance top-50 list.

Primary market source: Binance USDⓈ-M Futures.

Market metadata: configured provider such as CoinGecko.

30d average:

`sum(30 completed daily quote volumes) / completed days`

±100 depth:

`top100 bid notional + top100 ask notional`

Default filters:

- avg 30d volume >= $5M
- depth >= $1M
- circulating market cap >= $20M

Missing live values remain missing; never fabricate them.

Record source, fetched_at and calculation method for derived values.
