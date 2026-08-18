#!/usr/bin/env python3
"""Aggregate kline CSV(s) from fetch_klines.py into analyst-ready tables.

Produces (stdout, markdown-friendly):
  - Monthly OHLCV table (open/high/low/close, % change, volume)
  - ATH / ATL with dates
  - Top-volume days (anomaly scan)
  - Last N days detail
  - Weekly range from an optional 4h CSV (default: -42 bars = 7 days)

Usage:
  python3 scripts/klines_monthly_agg.py /tmp/zec_1d.csv
  python3 scripts/klines_monthly_agg.py /tmp/zec_1d.csv /tmp/zec_4h.csv
  python3 scripts/klines_monthly_agg.py /tmp/h_1d.csv --top-vol 5 --tail 14
"""

import argparse
import csv
import sys

# Columns written by fetch_klines.py
TS, DT = "timestamp", "datetime"
O, H, L, C = "open", "high", "low", "close"
VOL, QVOL = "volume", "quote_volume"


def load(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def to_f(rows, *keys):
    """Validate numeric access; raises with a clear message if a column is missing."""
    for k in keys:
        if k not in rows[0]:
            raise KeyError(f"column '{k}' not found in CSV (headers: {list(rows[0])})")
    return rows


def fmt_vol(x):
    """Human-readable USD/unit volume: K / M / B."""
    if x >= 1e9:
        return f"${x/1e9:.2f}B"
    if x >= 1e6:
        return f"${x/1e6:.0f}M"
    if x >= 1e3:
        return f"${x/1e3:.0f}K"
    return f"${x:.0f}"


def monthly(rows):
    """Monthly aggregation: open of first bar, close of last, high/low max/min."""
    m = {}
    for r in to_f(rows, DT, O, H, L, C, QVOL):
        key = r[DT][:7]
        if key not in m:
            m[key] = {"open": float(r[O]), "high": float(r[H]),
                      "low": float(r[L]), "close": float(r[C]), "vol": 0.0}
        v = m[key]
        v["high"] = max(v["high"], float(r[H]))
        v["low"] = min(v["low"], float(r[L]))
        v["close"] = float(r[C])
        v["vol"] += float(r[QVOL])
    return m


def _pfmt(rows):
    """Auto decimal precision by price magnitude (sub-dollar tokens keep detail)."""
    closes = [float(r[C]) for r in rows]
    hi = max(closes)
    if hi < 0.1:
        return ".5f"
    if hi < 1:
        return ".4f"
    return ".1f"


def print_monthly(m):
    print("=== monthly ===")
    print(f"{'month':8} {'open':>10} {'high':>10} {'low':>10} {'close':>10} {'chg%':>8} {'vol':>10}")
    prev = None
    p = _pfmt([{"close": v["close"], "open": v["open"], "high": v["high"], "low": v["low"]}
               for v in m.values()])
    for k, v in m.items():
        chg = (v["close"] / prev - 1) * 100 if prev else 0.0
        print(f"{k:8} {v['open']:>10{p}} {v['high']:>10{p}} {v['low']:>10{p}} "
              f"{v['close']:>10{p}} {chg:>+8.1f}% {fmt_vol(v['vol']):>10}")
        prev = v["close"]


def print_ath_atl(rows):
    rows = to_f(rows, DT, H, L)
    ath = max(rows, key=lambda r: float(r[H]))
    atl = min(rows, key=lambda r: float(r[L]))
    p = _pfmt(rows)
    print(f"ATH: {float(ath[H]):{p}} @ {ath[DT]}")
    print(f"ATL: {float(atl[L]):{p}} @ {atl[DT]}")


def print_top_volume(rows, n):
    rows = to_f(rows, DT, H, L, C, QVOL)
    rs = sorted(rows, key=lambda r: float(r[QVOL]), reverse=True)[:n]
    print(f"=== top {n} volume days ===")
    p = _pfmt(rows)
    for r in rs:
        print(f"  {r[DT]}: {fmt_vol(float(r[QVOL]))}  range {float(r[L]):{p}}-{float(r[H]):{p}}  C={float(r[C]):{p}}")


def print_tail(rows, n):
    rows = to_f(rows, DT, O, H, L, C, QVOL)
    print(f"=== last {n} bars ===")
    p = _pfmt(rows)
    for r in rows[-n:]:
        print(f"  {r[DT][:10]}: O={float(r[O]):{p}} H={float(r[H]):{p}} "
              f"L={float(r[L]):{p}} C={float(r[C]):{p}} vol={fmt_vol(float(r[QVOL]))}")


def print_week_range(rows4, bars):
    rows4 = to_f(rows4, H, L)
    w = rows4[-bars:]
    print(f"=== last {bars} bars range ({bars // 6:.0f}d approx for 4h) ===")
    print(f"  low {min(float(r[L]) for r in w):.2f} - high {max(float(r[H]) for r in w):.2f}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("daily_csv", help="daily kline CSV from fetch_klines.py")
    p.add_argument("fourh_csv", nargs="?", default=None,
                   help="optional 4h kline CSV for weekly range")
    p.add_argument("--top-vol", type=int, default=6, help="top volume days to show (default 6)")
    p.add_argument("--tail", type=int, default=14, help="last N bars detail (default 14)")
    p.add_argument("--week-bars", type=int, default=42,
                   help="bars used for weekly range from 4h csv (default 42 = 7d)")
    args = p.parse_args()

    try:
        daily = load(args.daily_csv)
    except FileNotFoundError:
        sys.exit(f"error: no such file: {args.daily_csv}")
    except (KeyError, IndexError) as e:
        sys.exit(f"error: {args.daily_csv}: {e}")

    print_monthly(monthly(daily))
    print()
    print_ath_atl(daily)
    print()
    print_top_volume(daily, args.top_vol)
    print()
    print_tail(daily, args.tail)
    if args.fourh_csv:
        try:
            fourh = load(args.fourh_csv)
        except FileNotFoundError:
            sys.exit(f"error: no such file: {args.fourh_csv}")
        except (KeyError, IndexError) as e:
            sys.exit(f"error: {args.fourh_csv}: {e}")
        print()
        print_week_range(fourh, args.week_bars)


if __name__ == "__main__":
    main()
