#!/usr/bin/env python3
"""
Compute technical indicators from kline CSV data (output of fetch_klines.py).

Reads a CSV with OHLCV columns, appends indicator columns, writes enhanced CSV.

Usage:
    python compute_indicators.py input.csv -o output.csv
    python compute_indicators.py input.csv                  # stdout
    python compute_indicators.py input.csv --tail 60        # last 60 rows only

Indicators computed:
    SMA  (20, 50, 200)    — Simple Moving Average
    EMA  (12, 26, 50)     — Exponential Moving Average
    RSI  (14)             — Relative Strength Index
    MACD (12, 26, 9)      — Moving Average Convergence Divergence
    Bollinger Bands (20, 2)— Upper, middle, lower bands
    ATR  (14)             — Average True Range
    Volume SMA (20)       — Volume moving average

Dependencies:
    None (pure Python standard library).
"""

import argparse
import csv
import sys
from typing import Optional


# ──────────────────────────────────────────────────────────────────────
# Indicator functions
# ──────────────────────────────────────────────────────────────────────


def calc_sma(values: list[float], period: int) -> list[Optional[float]]:
    """Simple Moving Average. First (period-1) entries are None."""
    result: list[Optional[float]] = [None] * len(values)
    if len(values) < period:
        return result
    window_sum = sum(values[:period])
    result[period - 1] = window_sum / period
    for i in range(period, len(values)):
        window_sum += values[i] - values[i - period]
        result[i] = window_sum / period
    return result


def calc_ema(values: list[float], period: int) -> list[Optional[float]]:
    """Exponential Moving Average. Uses SMA for the first EMA seed."""
    result: list[Optional[float]] = [None] * len(values)
    if len(values) < period:
        return result
    multiplier = 2.0 / (period + 1)
    # Seed: SMA of first `period` values
    sma = sum(values[:period]) / period
    result[period - 1] = sma
    for i in range(period, len(values)):
        result[i] = (values[i] - result[i - 1]) * multiplier + result[i - 1]  # type: ignore[operator]
    return result


def calc_rsi(closes: list[float], period: int = 14) -> list[Optional[float]]:
    """Relative Strength Index using Wilder's smoothing."""
    result: list[Optional[float]] = [None] * len(closes)
    if len(closes) < period + 1:
        return result

    gains: list[float] = []
    losses: list[float] = []

    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    # Initial average gain/loss (Simple)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains) + 1):
        if avg_loss == 0:
            result[period + (i - period)] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[period + (i - period)] = 100.0 - (100.0 / (1.0 + rs))

        if i < len(gains):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    return result


def calc_macd(
    closes: list[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[list[Optional[float]], list[Optional[float]], list[Optional[float]]]:
    """MACD line, signal line, histogram."""
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)

    macd_line: list[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd_line[i] = ema_fast[i] - ema_slow[i]  # type: ignore[operator]

    # Calc signal line = EMA of MACD line
    macd_vals = [v if v is not None else 0.0 for v in macd_line]
    signal_raw = calc_ema(macd_vals, signal)
    signal_line: list[Optional[float]] = []
    hist_line: list[Optional[float]] = []

    first_macd = next((i for i, v in enumerate(macd_line) if v is not None), None)
    macd_start = first_macd if first_macd is not None else 0
    signal_start = macd_start + slow - 1 + signal - 1  # approximate

    for i in range(len(closes)):
        s = signal_raw[i] if i >= signal_start and signal_raw[i] is not None else None
        signal_line.append(s)
        if macd_line[i] is not None and s is not None:
            hist_line.append(macd_line[i] - s)  # type: ignore[operator]
        else:
            hist_line.append(None)

    return macd_line, signal_line, hist_line


def calc_bollinger(
    closes: list[float],
    period: int = 20,
    std_mult: float = 2.0,
) -> tuple[list[Optional[float]], list[Optional[float]], list[Optional[float]]]:
    """Bollinger Bands: upper, middle (SMA), lower."""
    middle = calc_sma(closes, period)
    upper: list[Optional[float]] = [None] * len(closes)
    lower: list[Optional[float]] = [None] * len(closes)

    for i in range(period - 1, len(closes)):
        if middle[i] is None:
            continue
        window = closes[i - period + 1 : i + 1]
        mean = middle[i]
        variance = sum((x - mean) ** 2 for x in window) / period  # type: ignore[operator]
        std = variance ** 0.5
        upper[i] = mean + std_mult * std  # type: ignore[operator]
        lower[i] = mean - std_mult * std  # type: ignore[operator]

    return upper, middle, lower


def calc_atr(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int = 14,
) -> list[Optional[float]]:
    """Average True Range (Wilder's smoothing)."""
    result: list[Optional[float]] = [None] * len(closes)
    if len(closes) < period + 1:
        return result

    true_ranges: list[float] = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        true_ranges.append(tr)

    # Initial ATR = SMA of first `period` TRs
    atr_val = sum(true_ranges[:period]) / period
    result[period] = atr_val  # first ATR is at index `period`

    for i in range(period + 1, len(true_ranges)):
        atr_val = (atr_val * (period - 1) + true_ranges[i]) / period
        result[i] = atr_val

    return result


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute technical indicators from OHLCV CSV"
    )
    parser.add_argument("input", help="Input CSV file (from fetch_klines.py)")
    parser.add_argument("--output", "-o", help="Output CSV file. Default: stdout.")
    parser.add_argument(
        "--tail", type=int,
        help="Output only the last N rows (useful for recent snapshot).",
    )
    args = parser.parse_args()

    # ── Read input CSV ───────────────────────────────────────────────

    rows: list[dict] = []
    with open(args.input, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        print("[crypto-data] Input CSV is empty.", file=sys.stderr)
        sys.exit(1)

    # Extract arrays
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]
    volumes = [float(r["volume"]) for r in rows]

    # ── Compute indicators ───────────────────────────────────────────

    sma20 = calc_sma(closes, 20)
    sma50 = calc_sma(closes, 50)
    sma200 = calc_sma(closes, 200)

    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    ema50 = calc_ema(closes, 50)

    rsi14 = calc_rsi(closes, 14)

    macd, macd_signal, macd_hist = calc_macd(closes)

    bb_upper, bb_middle, bb_lower = calc_bollinger(closes)

    atr14 = calc_atr(highs, lows, closes, 14)

    vol_sma20 = calc_sma(volumes, 20)

    # ── Attach to rows ───────────────────────────────────────────────

    for i, row in enumerate(rows):
        row["sma_20"] = _fmt(sma20[i])
        row["sma_50"] = _fmt(sma50[i])
        row["sma_200"] = _fmt(sma200[i])
        row["ema_12"] = _fmt(ema12[i])
        row["ema_26"] = _fmt(ema26[i])
        row["ema_50"] = _fmt(ema50[i])
        row["rsi_14"] = _fmt(rsi14[i])
        row["macd"] = _fmt(macd[i])
        row["macd_signal"] = _fmt(macd_signal[i])
        row["macd_hist"] = _fmt(macd_hist[i])
        row["bb_upper"] = _fmt(bb_upper[i])
        row["bb_middle"] = _fmt(bb_middle[i])
        row["bb_lower"] = _fmt(bb_lower[i])
        row["atr_14"] = _fmt(atr14[i])
        row["volume_sma_20"] = _fmt(vol_sma20[i])

    # ── Tail filtering ───────────────────────────────────────────────

    output_rows = rows[-args.tail :] if args.tail else rows

    # ── Write output ─────────────────────────────────────────────────

    all_fields = list(rows[0].keys())

    if args.output:
        fh = open(args.output, "w", newline="")
    else:
        fh = sys.stdout

    writer = csv.DictWriter(fh, fieldnames=all_fields, extrasaction="ignore")
    writer.writeheader()
    for row in output_rows:
        writer.writerow(row)

    if args.output:
        fh.close()

    # Summary line to stderr
    last = output_rows[-1] if output_rows else rows[-1]
    print(
        f"# {len(rows)} total rows, {len(output_rows)} output | "
        f"last close={last['close']} "
        f"sma_50={last.get('sma_50', 'N/A')} "
        f"rsi_14={last.get('rsi_14', 'N/A')} "
        f"macd_hist={last.get('macd_hist', 'N/A')}",
        file=sys.stderr,
    )


def _fmt(val: Optional[float]) -> str:
    if val is None:
        return ""
    return f"{val:.8f}".rstrip("0").rstrip(".")


if __name__ == "__main__":
    main()
