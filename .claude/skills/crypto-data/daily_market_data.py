#!/usr/bin/env python3
"""
Daily Market Data Collector — 日报市场数据一站式采集器

采集维度：
  A. Binance Futures — BTC/ETH/SOL: ticker, OI, funding, LS ratio, basis, klines
  B. Binance Spot    — BTC/ETH/SOL: ticker
  C. Hyperliquid     — BTC/ETH/SOL/HYPE: OI, funding, mark price, volume
  D. Global          — Fear & Greed (Alternative.me), Market Cap/Dominance (CoinGecko)

计算特征：
  - OI Δ% (1h, 4h, 24h) per asset
  - Volume Ratio (24h vol / 7d avg vol)
  - OI × Price 四象限分类
  - Funding × OI 组合状态
  - Binance vs Hyperliquid OI 差异
  - 异常检测 (Price > ±10%, Vol Ratio > 5x, OI Δ > ±20%)
  - Top movers scanning

用法:
  python3 daily_market_data.py -o /tmp/daily_market_data.json
  python3 daily_market_data.py --json           # stdout
  python3 daily_market_data.py --scan-top 50    # scan top 50 USDT perps
"""

import argparse
import json
import os
import sys
import time
import hmac
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlencode
from typing import Optional

try:
    import requests
except ImportError:
    print("需要 requests 库: python3 -m pip install --break-system-packages requests", file=sys.stderr)
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════

BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com"
HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"
FNG_URL = "https://api.alternative.me/fng/"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

CORE_ASSETS = ["BTC", "ETH", "SOL"]
EXTENDED_ASSETS = ["BNB", "UNI", "LIT", "ASTER", "ADA", "SPX", "HYPE"]
ALL_DEEP_DIVE_ASSETS = CORE_ASSETS + EXTENDED_ASSETS
CORE_SYMBOLS = [f"{a}USDT" for a in ALL_DEEP_DIVE_ASSETS]
HL_COINS = ["BTC", "ETH", "SOL", "HYPE", "BNB", "UNI", "LIT", "ASTER", "ADA", "SPX"]

INTERVAL_MS = {
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}


# ═══════════════════════════════════════════════════════════════
# API Key Loading (same pattern as pump_agent.py)
# ═══════════════════════════════════════════════════════════════

def load_api_keys():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = script_dir
    for _ in range(6):
        if os.path.isdir(os.path.join(project_root, "config")):
            break
        project_root = os.path.dirname(project_root)

    config_path = os.path.join(project_root, "config", "binance_futures_key.json")
    if not os.path.exists(config_path):
        return None, None

    with open(config_path) as f:
        cfg = json.load(f)
    return cfg.get("api_key"), cfg.get("api_secret")


# ═══════════════════════════════════════════════════════════════
# Binance Futures Client
# ═══════════════════════════════════════════════════════════════

class BinanceFutures:
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret

    def _sign(self, params):
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        sig = hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return f"{query}&signature={sig}"

    def _signed_get(self, endpoint, params=None):
        if params is None:
            params = {}
        url = f"{BINANCE_FUTURES_BASE}{endpoint}?{self._sign(params)}"
        r = requests.get(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=15)
        r.raise_for_status()
        return r.json()

    def _public_get(self, endpoint, params=None):
        if params is None:
            params = {}
        r = requests.get(f"{BINANCE_FUTURES_BASE}{endpoint}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    # ── Public endpoints ──

    def current_oi(self, symbol):
        return self._public_get("/fapi/v1/openInterest", {"symbol": symbol})

    def klines(self, symbol, interval, limit=500):
        return self._public_get("/fapi/v1/klines", {
            "symbol": symbol, "interval": interval, "limit": limit
        })

    def funding_rate(self, symbol, limit=300):
        return self._public_get("/fapi/v1/fundingRate", {
            "symbol": symbol, "limit": limit
        })

    def ticker_24hr(self, symbol=None):
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._public_get("/fapi/v1/ticker/24hr", params)

    def ticker_price(self, symbol=None):
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._public_get("/fapi/v1/ticker/price", params)

    def premium_index(self, symbol):
        return self._public_get("/fapi/v1/premiumIndex", {"symbol": symbol})

    def global_ls_ratio(self, symbol, period="1h", limit=24):
        return self._public_get("/futures/data/globalLongShortAccountRatio", {
            "symbol": symbol, "period": period, "limit": limit
        })

    # ── Signed endpoints ──

    def oi_history(self, symbol, period="5m", limit=288):
        """OI history. 5m×288 = 24h, 1h×200 = ~8d."""
        return self._signed_get("/futures/data/openInterestHist", {
            "symbol": symbol, "period": period, "limit": limit
        })

    def taker_buy_sell(self, symbol, period="1h", limit=24):
        try:
            return self._signed_get("/futures/data/takerBuySellVol", {
                "symbol": symbol, "period": period, "limit": limit
            })
        except Exception:
            return None


# ═══════════════════════════════════════════════════════════════
# Hyperliquid Client
# ═══════════════════════════════════════════════════════════════

def hl_post(body, timeout=15):
    r = requests.post(HYPERLIQUID_INFO, json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_hyperliquid_meta_and_ctx():
    """Fetch perp metadata + asset contexts (OI, funding, prices)."""
    return hl_post({"type": "metaAndAssetCtxs"})


def fetch_hyperliquid_candles(coin, interval, start_ms, end_ms):
    return hl_post({
        "type": "candleSnapshot",
        "req": {
            "coin": coin, "interval": interval,
            "startTime": start_ms, "endTime": end_ms,
        }
    })


# ═══════════════════════════════════════════════════════════════
# Global Data
# ═══════════════════════════════════════════════════════════════

def fetch_fear_greed(limit=3):
    """Fetch Fear & Greed from Alternative.me."""
    r = requests.get(FNG_URL, params={"limit": limit}, timeout=15)
    r.raise_for_status()
    data = r.json()
    entries = data.get("data", [])
    result = {"current": None, "yesterday": None, "week_ago": None}
    if len(entries) >= 1:
        result["current"] = {
            "value": int(entries[0]["value"]),
            "classification": entries[0]["value_classification"],
        }
    if len(entries) >= 2:
        result["yesterday"] = {
            "value": int(entries[1]["value"]),
            "classification": entries[1]["value_classification"],
        }
        result["delta_24h"] = result["current"]["value"] - result["yesterday"]["value"]
    if len(entries) >= 3:
        result["week_ago"] = {
            "value": int(entries[2]["value"]),
            "classification": entries[2]["value_classification"],
        }
        if result["current"]:
            result["delta_7d"] = result["current"]["value"] - result["week_ago"]["value"]
    return result


def fetch_coingecko_global():
    """Fetch total market cap, BTC dominance, 24h volume."""
    r = requests.get(f"{COINGECKO_BASE}/global", timeout=15)
    r.raise_for_status()
    data = r.json().get("data", {})
    return {
        "total_market_cap_usd": data.get("total_market_cap", {}).get("usd"),
        "btc_dominance_pct": data.get("market_cap_percentage", {}).get("btc"),
        "eth_dominance_pct": data.get("market_cap_percentage", {}).get("eth"),
        "total_volume_24h_usd": data.get("total_volume", {}).get("usd"),
    }


def fetch_coingecko_prices(symbols):
    """Fetch current prices for cross-validation. symbols: ['bitcoin','ethereum','solana']"""
    ids = ",".join(symbols)
    try:
        r = requests.get(
            f"{COINGECKO_BASE}/simple/price",
            params={"ids": ids, "vs_currencies": "usd",
                    "include_24hr_change": "true", "include_24hr_vol": "true"},
            timeout=15,
        )
        if r.status_code == 429:
            print("[WARN] CoinGecko rate limited, skipping price cross-check", file=sys.stderr)
            return {}
        r.raise_for_status()
        return r.json()
    except requests.exceptions.SSLError:
        print("[WARN] CoinGecko SSL error, skipping price cross-check", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"[WARN] CoinGecko prices: {e}", file=sys.stderr)
        return {}


# ═══════════════════════════════════════════════════════════════
# Feature Computation
# ═══════════════════════════════════════════════════════════════

def classify_oi_price_quadrant(price_chg_pct, oi_chg_pct):
    """OI × Price 四象限分类 (guide §5)."""
    if price_chg_pct > 2 and oi_chg_pct > 5:
        return "NEW_LONG", "新资金做多 — 趋势启动，持续性较强"
    if price_chg_pct > 2 and oi_chg_pct < -5:
        return "SHORT_COVER", "空头回补 — 价格反弹但资金在退出，持续性弱"
    if price_chg_pct < -2 and oi_chg_pct > 5:
        return "FRESH_SHORT", "新空头进入 — 主动性做空，可能继续下跌"
    if price_chg_pct < -2 and oi_chg_pct < -5:
        return "LONG_LIQUIDATION", "多头平仓/去杠杆 — 被动下跌，清算驱动"
    if abs(price_chg_pct) <= 2 and oi_chg_pct > 5:
        return "HIDDEN_ACCUMULATION", "价格横盘 + OI大涨 → 隐藏吸筹，最优前置信号"
    return "NEUTRAL", "价格和OI联动正常，无明显背离"


def classify_funding_oi_state(funding_pct, oi_chg_pct, price_chg_pct):
    """Funding × OI 组合状态 (guide §6)."""
    if funding_pct > 0.05 and oi_chg_pct > 5 and price_chg_pct > 2:
        return "CROWDED_LONG", "多头拥挤 — 费率偏高+OI增长+价格上涨，注意回调风险"
    if funding_pct < 0 and oi_chg_pct > 5 and price_chg_pct < -2:
        return "CROWDED_SHORT", "空头拥挤 — 负费率+OI增长+价格下跌，注意轧空"
    if oi_chg_pct < -5 and (funding_pct < 0.01 or funding_pct < 0):
        return "DELEVERAGING", "去杠杆 — OI下降+费率地板，资金正在退出"
    if funding_pct < 0.01 and oi_chg_pct > 5 and abs(price_chg_pct) <= 2:
        return "ACCUMULATION", "吸筹 — 费率地板+OI增长+价格未动，主力建仓"
    if funding_pct > 0.05 and oi_chg_pct < -5:
        return "LONG_CAPITULATION", "多头投降 — 高费率+OI大降，多头踩踏出局"
    return "NEUTRAL", "费率与OI关系正常"


def compute_vol_ratio(current_vol, historical_klines):
    """Volume Ratio = current 24h vol / average of last N completed daily vols.
    historical_klines: list of kline lists from Binance.
    Kline format: [open_time, open, high, low, close, volume, close_time, quote_volume, ...]
    We use index 7 (quote_volume) since current_vol is quote_volume_24h.
    """
    if not historical_klines or len(historical_klines) < 2:
        return None
    # Exclude the most recent kline (today's partial day)
    # Use quote_volume at index 7 (same unit as 24h ticker's quoteVolume)
    completed_klines = historical_klines[:-1]
    vols = [float(k[7]) for k in completed_klines if len(k) > 7 and float(k[7]) > 0]
    if len(vols) < 1:
        return None
    avg_vol = sum(vols) / len(vols)
    if avg_vol == 0 or current_vol == 0:
        return None
    return round(current_vol / avg_vol, 2)


def compute_oi_delta(current_oi_value, oi_history, window_hours):
    """Compute OI change over a window.
    oi_history is list of dicts: [{symbol, sumOpenInterest, sumOpenInterestValue, timestamp}, ...]
    """
    if not oi_history or not isinstance(oi_history, list):
        return None
    if len(oi_history) == 0:
        return None

    now_ms = int(time.time() * 1000)
    window_ms = window_hours * 3600 * 1000
    target_ms = now_ms - window_ms

    # Current OI value
    try:
        current = float(oi_history[-1].get("sumOpenInterestValue", 0))
    except (IndexError, KeyError, ValueError, TypeError):
        current = current_oi_value

    if current == 0:
        return None

    # Find entry closest to target_ms
    best_entry = None
    for entry in oi_history:
        ts = entry.get("timestamp", 0)
        if ts >= target_ms:
            best_entry = entry
            break

    if best_entry is None and len(oi_history) > 0:
        best_entry = oi_history[0]  # fallback to earliest

    if best_entry is None:
        return None

    try:
        past = float(best_entry.get("sumOpenInterestValue", 0))
    except (ValueError, TypeError):
        return None

    if past == 0:
        return None
    return round((current - past) / past * 100, 2)


def compute_funding_floor_ratio(funding_history):
    """Fraction of funding periods where rate <= 0.01%."""
    if not funding_history:
        return None
    floor_count = sum(
        1 for f in funding_history
        if abs(float(f["fundingRate"]) * 100) <= 0.01
    )
    return round(floor_count / len(funding_history), 2)


def compute_funding_stats(funding_history):
    """Current + avg + trend of funding rates."""
    if not funding_history:
        return None, None, None
    rates = [float(f["fundingRate"]) * 100 for f in funding_history]
    current = rates[-1]
    avg = sum(rates) / len(rates)
    # Trend: compare first half vs second half
    mid = len(rates) // 2
    first_half_avg = sum(rates[:mid]) / mid if mid > 0 else avg
    second_half_avg = sum(rates[mid:]) / (len(rates) - mid) if len(rates) > mid else avg
    if second_half_avg > first_half_avg * 1.5:
        trend = "rising"
    elif second_half_avg < first_half_avg * 0.5:
        trend = "falling"
    else:
        trend = "stable"
    return current, avg, trend


def detenct_anomalies(asset_data):
    """Detect anomalies in price, volume, OI."""
    flags = []
    bf = asset_data.get("binance_futures", {})

    price_chg = bf.get("change_24h_pct", 0) or 0
    vol_ratio = bf.get("vol_ratio_vs_7d") or 0
    oi_delta = bf.get("oi_delta_pct", {}).get("24h") or 0

    if abs(price_chg) > 10:
        flags.append({"type": "PRICE_EXTREME", "value": price_chg,
                       "desc": f"价格异常波动 {price_chg:+.1f}%"})
    if vol_ratio > 5:
        flags.append({"type": "VOLUME_EXPLOSION", "value": vol_ratio,
                       "desc": f"成交量爆炸 Vol Ratio={vol_ratio}x"})
    if abs(oi_delta) > 20:
        flags.append({"type": "OI_EXTREME", "value": oi_delta,
                       "desc": f"OI异常变动 {oi_delta:+.1f}%"})

    # Price/OI divergence
    if price_chg > 5 and oi_delta < -10:
        flags.append({"type": "PRICE_OI_DIVERGENCE",
                       "desc": f"价格涨{price_chg:+.1f}%但OI降{oi_delta:+.1f}% → 空头回补"})
    if price_chg < -5 and oi_delta > 10:
        flags.append({"type": "PRICE_OI_DIVERGENCE",
                       "desc": f"价格跌{price_chg:+.1f}%但OI涨{oi_delta:+.1f}% → 新空头进入"})

    return flags


def compute_ls_ratio(ls_data):
    """Extract latest long/short ratio."""
    if not ls_data:
        return None
    latest = ls_data[-1]
    return {
        "long_ratio": float(latest.get("longAccount", 0)),
        "short_ratio": float(latest.get("shortAccount", 0)),
        "ls_ratio": float(latest.get("longShortRatio", 0)),
    }


def compute_basis(premium_data):
    """Basis = (mark - index) / index * 100."""
    if not premium_data:
        return None
    try:
        mark = float(premium_data.get("markPrice", 0))
        index = float(premium_data.get("indexPrice", 0))
        if index == 0:
            return None
        return round((mark - index) / index * 100, 4)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════
# Main Data Collection
# ═══════════════════════════════════════════════════════════════

def collect_binance_futures(client, symbol):
    """Collect all Binance Futures data for one symbol."""
    asset = symbol.replace("USDT", "")
    now_ms = int(time.time() * 1000)

    result = {}

    # ── 24hr Ticker ──
    try:
        t = client.ticker_24hr(symbol)
        if isinstance(t, list):
            t = t[0] if t else {}
        result["price"] = float(t.get("lastPrice", 0))
        result["change_24h_pct"] = float(t.get("priceChangePercent", 0))
        result["volume_24h"] = float(t.get("volume", 0))
        result["quote_volume_24h"] = float(t.get("quoteVolume", 0))
        result["high_24h"] = float(t.get("highPrice", 0))
        result["low_24h"] = float(t.get("lowPrice", 0))
        result["trade_count"] = int(t.get("count", 0))
    except Exception as e:
        print(f"  [WARN] Binance Futures ticker {symbol}: {e}", file=sys.stderr)
        result["price"] = result["change_24h_pct"] = result["volume_24h"] = 0

    # ── Current OI ──
    oi_raw = None
    try:
        oi_raw = client.current_oi(symbol)
        result["oi_current"] = float(oi_raw.get("openInterest", 0))
    except Exception as e:
        print(f"  [WARN] OI {symbol}: {e}", file=sys.stderr)
        result["oi_current"] = 0

    # ── OI History (signed) ──
    try:
        oi_hist = client.oi_history(symbol, period="5m", limit=288)  # 24h
        if isinstance(oi_hist, dict) and "code" in oi_hist:
            # Binance error response
            print(f"  [WARN] OI history {symbol}: Binance error code={oi_hist.get('code')} msg={oi_hist.get('msg')}", file=sys.stderr)
            result["oi_delta_pct"] = {"1h": None, "4h": None, "24h": None}
        else:
            result["oi_delta_pct"] = {}
            result["oi_delta_pct"]["24h"] = compute_oi_delta(result.get("oi_current", 0), oi_hist, 24)
            result["oi_delta_pct"]["4h"] = compute_oi_delta(result.get("oi_current", 0), oi_hist, 4)
            result["oi_delta_pct"]["1h"] = compute_oi_delta(result.get("oi_current", 0), oi_hist, 1)
    except Exception as e:
        print(f"  [WARN] OI history {symbol}: {e}", file=sys.stderr)
        result["oi_delta_pct"] = {"1h": None, "4h": None, "24h": None}

    # ── Funding Rate ──
    try:
        funding_hist = client.funding_rate(symbol, limit=50)
        current_fr, avg_fr, trend_fr = compute_funding_stats(funding_hist)
        result["funding_current_pct"] = round(current_fr, 4) if current_fr else None
        result["funding_24h_avg_pct"] = round(avg_fr, 4) if avg_fr else None
        result["funding_trend"] = trend_fr
        result["funding_floor_ratio"] = compute_funding_floor_ratio(funding_hist)
    except Exception as e:
        print(f"  [WARN] Funding {symbol}: {e}", file=sys.stderr)
        result["funding_current_pct"] = None
        result["funding_trend"] = None
        result["funding_floor_ratio"] = None

    # ── Long/Short Ratio ──
    try:
        ls_data = client.global_ls_ratio(symbol, period="1h", limit=24)
        result["ls_ratio_data"] = compute_ls_ratio(ls_data)
        result["long_short_ratio"] = result["ls_ratio_data"]["ls_ratio"] if result["ls_ratio_data"] else None
    except Exception as e:
        print(f"  [WARN] LS ratio {symbol}: {e}", file=sys.stderr)
        result["ls_ratio_data"] = None
        result["long_short_ratio"] = None

    # ── Basis (premium index) ──
    try:
        prem = client.premium_index(symbol)
        result["basis_pct"] = compute_basis(prem)
    except Exception as e:
        print(f"  [WARN] Basis {symbol}: {e}", file=sys.stderr)
        result["basis_pct"] = None

    # ── 1d Klines for Volume Ratio ──
    try:
        klines_1d = client.klines(symbol, "1d", limit=10)  # 10 days
        result["vol_ratio_vs_7d"] = compute_vol_ratio(
            result.get("quote_volume_24h", 0), klines_1d
        )
    except Exception as e:
        print(f"  [WARN] Klines {symbol}: {e}", file=sys.stderr)
        result["vol_ratio_vs_7d"] = None

    # ── OI × Price Quadrant ──
    oi_24h = result.get("oi_delta_pct", {}).get("24h") or 0
    price_24h = result.get("change_24h_pct") or 0
    quadrant, quad_desc = classify_oi_price_quadrant(price_24h, oi_24h)
    result["oi_price_quadrant"] = quadrant
    result["oi_price_quadrant_desc"] = quad_desc

    # ── Funding × OI State ──
    funding = result.get("funding_current_pct") or 0
    fstate, fstate_desc = classify_funding_oi_state(funding, oi_24h, price_24h)
    result["funding_oi_state"] = fstate
    result["funding_oi_state_desc"] = fstate_desc

    return result


def collect_binance_spot(symbol):
    """Collect Binance Spot 24hr ticker."""
    try:
        r = requests.get(
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/24hr",
            params={"symbol": symbol}, timeout=15
        )
        r.raise_for_status()
        t = r.json()
        return {
            "price": float(t.get("lastPrice", 0)),
            "change_24h_pct": float(t.get("priceChangePercent", 0)),
            "volume_24h": float(t.get("volume", 0)),
            "quote_volume_24h": float(t.get("quoteVolume", 0)),
            "high_24h": float(t.get("highPrice", 0)),
            "low_24h": float(t.get("lowPrice", 0)),
            "trade_count": int(t.get("count", 0)),
        }
    except Exception as e:
        print(f"  [WARN] Binance Spot {symbol}: {e}", file=sys.stderr)
        return {"price": 0, "change_24h_pct": 0, "volume_24h": 0,
                "quote_volume_24h": 0, "high_24h": 0, "low_24h": 0, "trade_count": 0}


def collect_hyperliquid(coins):
    """Collect Hyperliquid data for specified coins."""
    result = {}
    try:
        meta_ctx = fetch_hyperliquid_meta_and_ctx()
    except Exception as e:
        print(f"[ERROR] Hyperliquid metaAndAssetCtxs: {e}", file=sys.stderr)
        return result

    # meta_ctx[0] = {"universe": [...], "marginTables": [...], "collateralToken": ...}
    # meta_ctx[1] = list of asset contexts (funding, openInterest, markPx, etc.)
    universe = meta_ctx[0].get("universe", []) if len(meta_ctx) > 0 else []
    contexts = meta_ctx[1] if len(meta_ctx) > 1 else []

    # Build symbol → index map
    symbol_to_idx = {}
    for i, item in enumerate(universe):
        name = item.get("name", "")
        symbol_to_idx[name] = i

    now_ms = int(time.time() * 1000)
    day_ms = 24 * 3600 * 1000

    for coin in coins:
        idx = symbol_to_idx.get(coin)
        if idx is None:
            print(f"  [WARN] Hyperliquid {coin} not found in universe", file=sys.stderr)
            continue

        ctx = contexts[idx] if idx < len(contexts) else None
        entry = {}

        if ctx:
            # ctx structure: {dayNtlVlm, funding, impactPxs, markPx, midPx, openInterest, oraclePx, premium, prevDayPx}
            mark_px = float(ctx.get("markPx", 0))
            prev_day_px = float(ctx.get("prevDayPx", 0))
            oracle_px = float(ctx.get("oraclePx", 0))

            entry["mark_price"] = mark_px
            entry["oracle_price"] = oracle_px
            entry["oi"] = float(ctx.get("openInterest", 0))
            entry["funding_pct"] = round(float(ctx.get("funding", 0)) * 100, 4)
            entry["volume_24h"] = float(ctx.get("dayNtlVlm", 0))
            entry["premium_pct"] = round(float(ctx.get("premium", 0)) * 100, 4)

            if prev_day_px > 0:
                entry["change_24h_pct"] = round((mark_px - prev_day_px) / prev_day_px * 100, 2)
            else:
                entry["change_24h_pct"] = 0

            # Try to get candles for volume
            try:
                candles = fetch_hyperliquid_candles(coin, "4h", now_ms - 7 * day_ms, now_ms)
                if candles:
                    daily_vols = []
                    for c in candles:
                        daily_vols.append(float(c.get("v", 0)))
                    if len(daily_vols) > 1:
                        recent_vol = sum(daily_vols[-6:])  # last 24h (6 × 4h)
                        avg_vol = sum(daily_vols) / len(daily_vols)
                        entry["vol_ratio_vs_7d"] = round(recent_vol / avg_vol, 2) if avg_vol > 0 else None
            except Exception:
                entry["vol_ratio_vs_7d"] = None

        result[coin] = entry

    return result


def scan_top_movers(client, top_n=50):
    """Scan top USDT perpetuals for anomalies."""
    try:
        all_tickers = client.ticker_24hr()
    except Exception as e:
        print(f"[WARN] Ticker scan: {e}", file=sys.stderr)
        return {"gainers": [], "losers": [], "volume_explosion": [], "oi_explosion": [], "anomalies": []}

    # Filter USDT perps only
    usdt_tickers = [
        t for t in all_tickers
        if t.get("symbol", "").endswith("USDT")
    ]

    gainers = sorted(usdt_tickers, key=lambda t: float(t.get("priceChangePercent", 0)), reverse=True)[:10]
    losers = sorted(usdt_tickers, key=lambda t: float(t.get("priceChangePercent", 0)))[:10]

    result = {
        "gainers": [
            {"symbol": t["symbol"], "change_pct": float(t["priceChangePercent"]),
             "price": float(t["lastPrice"]), "volume": float(t["quoteVolume"])}
            for t in gainers
        ],
        "losers": [
            {"symbol": t["symbol"], "change_pct": float(t["priceChangePercent"]),
             "price": float(t["lastPrice"]), "volume": float(t["quoteVolume"])}
            for t in losers
        ],
        "volume_explosion": [],
        "oi_explosion": [],
        "anomalies": [],
    }

    # For top N by volume, check OI and volume ratio
    top_by_vol = sorted(usdt_tickers, key=lambda t: float(t.get("quoteVolume", 0)), reverse=True)[:top_n]
    now_ms = int(time.time() * 1000)
    day_ms = 24 * 3600 * 1000

    for t in top_by_vol:
        symbol = t["symbol"]
        try:
            # Skip core assets (already fully analyzed)
            asset = symbol.replace("USDT", "")
            if asset in CORE_ASSETS:
                continue

            vol_24h = float(t.get("quoteVolume", 0))
            price_chg = float(t.get("priceChangePercent", 0))

            # Volume ratio check
            klines = client.klines(symbol, "4h", limit=42)
            vol_ratio = compute_vol_ratio(vol_24h, klines) if klines else None

            # OI check
            oi_hist = client.oi_history(symbol, period="5m", limit=288)
            oi_current = float(client.current_oi(symbol).get("openInterest", 0))
            oi_delta_24h = compute_oi_delta(oi_current, oi_hist, 24) if oi_hist else None

            anomaly_entry = None

            if vol_ratio and vol_ratio > 5:
                result["volume_explosion"].append({
                    "symbol": symbol, "vol_ratio": vol_ratio,
                    "price_chg_pct": price_chg,
                })
                anomaly_entry = {
                    "symbol": symbol, "price_chg_pct": price_chg,
                    "vol_ratio": vol_ratio,
                }

            if oi_delta_24h and abs(oi_delta_24h) > 20:
                result["oi_explosion"].append({
                    "symbol": symbol, "oi_delta_pct": oi_delta_24h,
                    "price_chg_pct": price_chg,
                })
                if anomaly_entry:
                    anomaly_entry["oi_delta_pct"] = oi_delta_24h
                else:
                    anomaly_entry = {
                        "symbol": symbol, "price_chg_pct": price_chg,
                        "oi_delta_pct": oi_delta_24h,
                    }

            if anomaly_entry:
                quadrant, _ = classify_oi_price_quadrant(
                    price_chg, oi_delta_24h or 0
                )
                anomaly_entry["classification"] = quadrant
                result["anomalies"].append(anomaly_entry)

        except Exception:
            continue

    return result


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Daily Market Data Collector — 日报市场数据一站式采集"
    )
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--json", action="store_true", help="Output to stdout")
    parser.add_argument("--scan-top", type=int, default=50,
                        help="Number of top perps to scan for anomalies (default: 50)")
    parser.add_argument("--no-scan", action="store_true",
                        help="Skip anomaly scanning (faster)")
    args = parser.parse_args()

    print("=" * 60, file=sys.stderr)
    print("  Crypto Daily Market Data Collector", file=sys.stderr)
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    # ── Init clients ──
    api_key, api_secret = load_api_keys()
    bf = BinanceFutures(api_key, api_secret)

    if not api_key:
        print("[WARN] No Binance Futures API key found. Signed endpoints will fail.", file=sys.stderr)
        print("       OI history and taker data will be unavailable.", file=sys.stderr)

    # ═══════════════════════════════════════════════════════
    # A & B: Binance Futures + Spot per asset
    # ═══════════════════════════════════════════════════════

    assets_data = {}

    for asset in ALL_DEEP_DIVE_ASSETS:
        symbol = f"{asset}USDT"
        depth_tag = "[CORE]" if asset in CORE_ASSETS else "[EXT]"
        print(f"\n{depth_tag} [{asset}] Collecting...", file=sys.stderr)

        entry = {}

        # Binance Futures
        print(f"  Binance Futures {symbol}...", file=sys.stderr)
        entry["binance_futures"] = collect_binance_futures(bf, symbol)

        # Binance Spot
        print(f"  Binance Spot {symbol}...", file=sys.stderr)
        entry["binance_spot"] = collect_binance_spot(symbol)

        assets_data[asset] = entry

    # ═══════════════════════════════════════════════════════
    # C: Hyperliquid
    # ═══════════════════════════════════════════════════════

    print(f"\n[Hyperliquid] Collecting {HL_COINS}...", file=sys.stderr)
    hl_data = collect_hyperliquid(HL_COINS)

    for coin in HL_COINS:
        if coin in CORE_ASSETS:
            assets_data[coin]["hyperliquid"] = hl_data.get(coin, {})
        else:
            # HYPE only on Hyperliquid
            assets_data[coin] = {"hyperliquid": hl_data.get(coin, {})}

    # ── Binance vs Hyperliquid OI comparison (all assets with both data sources) ──
    for asset in ALL_DEEP_DIVE_ASSETS:
        bf_data = assets_data[asset].get("binance_futures", {})
        hl_entry = assets_data[asset].get("hyperliquid", {})

        binance_oi = bf_data.get("oi_current", 0)
        hl_oi = hl_entry.get("oi", 0)
        binance_oi_delta = bf_data.get("oi_delta_pct", {}).get("24h")
        hl_change = hl_entry.get("change_24h_pct", 0)

        # Compare OI change directions, not absolute OI sizes
        if binance_oi_delta is not None and binance_oi_delta > 5 and hl_change <= 0:
            divergence = "Binance OI growing, HL flat/declining → divergent positioning"
        elif binance_oi_delta is not None and binance_oi_delta < -5 and hl_change > 2:
            divergence = "Binance OI declining, HL stable/growing → divergent positioning"
        elif binance_oi_delta is not None and abs(binance_oi_delta) <= 5:
            divergence = "Both venues showing flat OI → neutral positioning"
        else:
            divergence = "OI direction aligned across venues"

        assets_data[asset]["binance_vs_hyperliquid"] = {
            "binance_oi_usd": binance_oi,
            "hyperliquid_oi": round(hl_oi, 2) if hl_oi else None,
            "binance_oi_delta_24h_pct": binance_oi_delta,
            "hyperliquid_price_chg_24h_pct": hl_change,
            "divergence": divergence,
        }

    # ═══════════════════════════════════════════════════════
    # D: Global
    # ═══════════════════════════════════════════════════════

    print("\n[Global] Fear & Greed...", file=sys.stderr)
    fear_greed = fetch_fear_greed(limit=3)

    print("[Global] CoinGecko market cap...", file=sys.stderr)
    global_data = fetch_coingecko_global()

    print("[Global] CoinGecko prices (cross-check)...", file=sys.stderr)
    cg_prices = fetch_coingecko_prices(["bitcoin", "ethereum", "solana"])

    # ═══════════════════════════════════════════════════════
    # E: Anomaly scan
    # ═══════════════════════════════════════════════════════

    top_movers = {"gainers": [], "losers": [], "volume_explosion": [],
                  "oi_explosion": [], "anomalies": []}

    if not args.no_scan:
        print(f"\n[Scan] Top {args.scan_top} movers...", file=sys.stderr)
        top_movers = scan_top_movers(bf, args.scan_top)
        print(f"  Gainers: {len(top_movers['gainers'])} | "
              f"Losers: {len(top_movers['losers'])} | "
              f"Volume explosions: {len(top_movers['volume_explosion'])} | "
              f"OI explosions: {len(top_movers['oi_explosion'])} | "
              f"Anomalies: {len(top_movers['anomalies'])}", file=sys.stderr)

    # ═══════════════════════════════════════════════════════
    # Build output
    # ═══════════════════════════════════════════════════════

    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generated_at_unix_ms": int(time.time() * 1000),
        "global": {
            **global_data,
            "fear_greed": fear_greed,
            "coingecko_prices": cg_prices,
        },
        "assets": assets_data,
        "top_movers": top_movers,
    }

    # ── Output ──
    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Written to {args.output}", file=sys.stderr)
    else:
        print(json.dumps(output, indent=2, ensure_ascii=False))

    # ── Summary ──
    print("\n── Summary ──", file=sys.stderr)
    for asset in ALL_DEEP_DIVE_ASSETS + ["HYPE"]:
        if asset not in assets_data:
            continue
        data = assets_data[asset]
        bf_d = data.get("binance_futures", {})
        hl_d = data.get("hyperliquid", {})
        price = bf_d.get("price") or hl_d.get("mark_price") or 0
        chg = bf_d.get("change_24h_pct") or hl_d.get("change_24h_pct") or 0
        oi_24h = bf_d.get("oi_delta_pct", {}).get("24h") if bf_d else None
        fund = bf_d.get("funding_current_pct") if bf_d else hl_d.get("funding_pct")
        quad = bf_d.get("oi_price_quadrant", "?") if bf_d else "?"
        vol_r = bf_d.get("vol_ratio_vs_7d") if bf_d else hl_d.get("vol_ratio_vs_7d")
        tag = "[CORE]" if asset in CORE_ASSETS else ("[EXT]" if asset in EXTENDED_ASSETS else "[HL]")

        parts = [f"${price:,.2f}", f"{chg:+.1f}%"]
        if oi_24h is not None:
            parts.append(f"OI {oi_24h:+.1f}%")
        if fund is not None:
            parts.append(f"F {fund:+.4f}%")
        parts.append(quad)
        if vol_r:
            parts.append(f"VR {vol_r}x")
        print(f"  {tag} {asset:>6}: {' | '.join(parts)}", file=sys.stderr)

    print(f"\n✓ Done. {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}", file=sys.stderr)


if __name__ == "__main__":
    main()
