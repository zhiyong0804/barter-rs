#!/usr/bin/env python3
"""
Fetch historical kline (OHLCV) data from Binance or Hyperliquid.

Binance: public REST API, no auth required, spot market klines.
Hyperliquid: public info endpoint, perpetual futures candles.

Usage:
    python fetch_klines.py --symbol BTCUSDT --interval 1h --days 90
    python fetch_klines.py --exchange hyperliquid --symbol BTC --interval 4h --days 30
    python fetch_klines.py --symbol ETHUSDT --interval 1d --start 2026-01-01 --end 2026-07-14 -o eth.csv
    python fetch_klines.py --symbol BTCUSDT --interval 1h --days 90 --format json -o btc.json

Output:
    CSV (default) or JSON to stdout, or to file if --output/-o specified.
    CSV columns: timestamp, datetime, open, high, low, close, volume, quote_volume,
                  trades, taker_buy_volume, taker_buy_quote_volume
    All timestamps are Unix milliseconds. datetime is ISO 8601 UTC.

Dependencies:
    pip install requests
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

# ──────────────────────────────────────────────────────────────────────
# API endpoints (public, no auth)
# ──────────────────────────────────────────────────────────────────────

BINANCE_KLINE_URL = "https://api.binance.com/api/v3/klines"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"

# ──────────────────────────────────────────────────────────────────────
# Kline interval → milliseconds
# ──────────────────────────────────────────────────────────────────────

INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}

# ──────────────────────────────────────────────────────────────────────
# Binance
# ──────────────────────────────────────────────────────────────────────


def fetch_binance_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> list[list]:
    """
    Fetch klines from Binance with automatic pagination.

    Binance returns at most `limit` (default 1000) candles per request.
    This function loops until all candles in [start_ms, end_ms] are fetched.

    Returns raw Binance kline arrays (to be normalized by caller).
    """
    all_klines: list[list] = []
    current_start = start_ms

    while current_start < end_ms:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": limit,
        }

        resp = requests.get(BINANCE_KLINE_URL, params=params, timeout=30)
        resp.raise_for_status()

        batch: list[list] = resp.json()

        if not batch:
            break

        all_klines.extend(batch)

        # Advance past the last candle for the next page
        last_open_time: int = batch[-1][0]
        if last_open_time <= current_start:
            break  # no progress — avoid infinite loop
        current_start = last_open_time + 1

        # Gentle rate-limit courtesy (Binance allows 1200/min, so this is very safe)
        if len(batch) == limit:
            time.sleep(0.1)

    return all_klines


def normalize_binance(raw: list) -> dict:
    """Map a single Binance kline array to a uniform dict."""
    return {
        "open_time": raw[0],
        "open": float(raw[1]),
        "high": float(raw[2]),
        "low": float(raw[3]),
        "close": float(raw[4]),
        "volume": float(raw[5]),
        "close_time": raw[6],
        "quote_volume": float(raw[7]),
        "trades": int(raw[8]),
        "taker_buy_volume": float(raw[9]),
        "taker_buy_quote_volume": float(raw[10]),
    }


# ──────────────────────────────────────────────────────────────────────
# Hyperliquid
# ──────────────────────────────────────────────────────────────────────


def fetch_hyperliquid_klines(
    coin: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """
    Fetch klines from Hyperliquid's info endpoint.

    Chunks requests by 5000-candle windows to handle large ranges.
    Returns list of raw candle dicts (to be normalized by caller).
    """
    all_candles: list[dict] = []
    interval_ms = INTERVAL_MS.get(interval, 3_600_000)
    chunk_candles = 5000
    current_start = start_ms

    while current_start < end_ms:
        chunk_end = min(current_start + chunk_candles * interval_ms, end_ms)

        body = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin.upper(),
                "interval": interval,
                "startTime": current_start,
                "endTime": chunk_end,
            },
        }

        resp = requests.post(HYPERLIQUID_INFO_URL, json=body, timeout=30)
        resp.raise_for_status()

        candles: list[dict] = resp.json()

        if not candles or not isinstance(candles, list):
            break

        all_candles.extend(candles)

        if len(candles) < chunk_candles:
            break  # end of available data

        # Advance past last candle
        current_start = candles[-1]["t"] + interval_ms
        time.sleep(0.1)

    return all_candles


def normalize_hyperliquid(raw: dict) -> dict:
    """Map a single Hyperliquid candle object to a uniform dict.

    Hyperliquid candle fields:
        t: open time (ms)
        T: close time (ms)
        s: coin symbol
        i: interval string (e.g. "1h")
        o, h, l, c: OHLC (string values)
        v: volume (string)
        n: number of trades (int)
    """
    return {
        "open_time": raw["t"],
        "open": float(raw["o"]),
        "high": float(raw["h"]),
        "low": float(raw["l"]),
        "close": float(raw["c"]),
        "volume": float(raw["v"]),
        "close_time": raw.get("T", raw["t"]),
        "quote_volume": 0.0,  # Hyperliquid does not provide quote volume in candles
        "trades": int(raw.get("n", 0)),  # 'n' is number of trades
        "taker_buy_volume": 0.0,  # not available in candle endpoint
        "taker_buy_quote_volume": 0.0,
    }


# ──────────────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────────────

CSV_FIELDS = [
    "timestamp",
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]

JSON_FIELDS = [
    "timestamp",
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades",
]


def to_row(k: dict) -> dict:
    """Convert a normalized kline dict to an output row."""
    ts_ms = k["open_time"]
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    return {
        "timestamp": ts_ms,
        "datetime": dt,
        "open": k["open"],
        "high": k["high"],
        "low": k["low"],
        "close": k["close"],
        "volume": k["volume"],
        "quote_volume": k["quote_volume"],
        "trades": k["trades"],
        "taker_buy_volume": k.get("taker_buy_volume", 0.0),
        "taker_buy_quote_volume": k.get("taker_buy_quote_volume", 0.0),
    }


def write_csv(klines: list[dict], path: Optional[str]) -> None:
    fh = open(path, "w", newline="") if path else sys.stdout
    writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for k in klines:
        writer.writerow(to_row(k))
    if path:
        fh.close()


def write_json(klines: list[dict], path: Optional[str]) -> None:
    output = []
    for k in klines:
        row = to_row(k)
        output.append({f: row[f] for f in JSON_FIELDS})
    if path:
        with open(path, "w") as f:
            json.dump(output, f, indent=2)
    else:
        json.dump(output, sys.stdout, indent=2, default=str)


# ──────────────────────────────────────────────────────────────────────
# Dedup & sort
# ──────────────────────────────────────────────────────────────────────


def dedup_sorted(klines: list[dict]) -> list[dict]:
    klines.sort(key=lambda k: k["open_time"])
    seen: set[int] = set()
    unique: list[dict] = []
    for k in klines:
        ts = k["open_time"]
        if ts not in seen:
            seen.add(ts)
            unique.append(k)
    return unique


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────


def parse_range(args: argparse.Namespace) -> tuple[int, int]:
    """Return (start_ms, end_ms) from CLI args."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if args.start or args.end:
        if not args.start:
            args.start = "2020-01-01"
        if not args.end:
            args.end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        return int(start_dt.timestamp() * 1000), min(int(end_dt.timestamp() * 1000), now_ms)

    days = args.days if args.days else 90
    return now_ms - days * 86_400_000, now_ms


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch historical OHLCV klines from Binance or Hyperliquid"
    )
    parser.add_argument(
        "--exchange", choices=["binance", "hyperliquid"], default="binance",
        help="Exchange: binance (spot) or hyperliquid (perps). Default: binance.",
    )
    parser.add_argument(
        "--symbol", required=True,
        help="Symbol: BTCUSDT (Binance) or BTC (Hyperliquid).",
    )
    parser.add_argument(
        "--interval", default="1h", choices=list(INTERVAL_MS.keys()),
        help="Kline interval. Default: 1h.",
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD (inclusive).")
    parser.add_argument("--end", help="End date YYYY-MM-DD (inclusive).")
    parser.add_argument(
        "--days", type=int,
        help="Days of history from now (ignored if --start/--end given). Default: 90.",
    )
    parser.add_argument(
        "--output", "-o", help="Output file path. Default: stdout.",
    )
    parser.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format. Default: csv.",
    )

    args = parser.parse_args()
    start_ms, end_ms = parse_range(args)

    # ── Fetch ────────────────────────────────────────────────────────

    if args.exchange == "binance":
        raw = fetch_binance_klines(args.symbol, args.interval, start_ms, end_ms)
        klines = [normalize_binance(r) for r in raw]
    else:
        raw = fetch_hyperliquid_klines(args.symbol, args.interval, start_ms, end_ms)
        klines = [normalize_hyperliquid(r) for r in raw]

    klines = dedup_sorted(klines)

    if not klines:
        print(
            f"[crypto-data] No data returned for {args.symbol} @ {args.interval} "
            f"on {args.exchange}. The token may not be listed or the interval "
            f"may not be supported.",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── Output ───────────────────────────────────────────────────────

    if args.format == "json":
        write_json(klines, args.output)
    else:
        write_csv(klines, args.output)

    dt_start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    dt_end = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    actual_start = datetime.fromtimestamp(klines[0]["open_time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    actual_end = datetime.fromtimestamp(klines[-1]["open_time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

    print(
        f"# {len(klines)} candles | {args.exchange} {args.symbol} {args.interval} | "
        f"requested: {dt_start} → {dt_end} | "
        f"actual: {actual_start} → {actual_end}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
