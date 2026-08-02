#!/usr/bin/env python3
"""
Pump Scanner — 基于 OI 增速的暴涨前置扫描器

核心逻辑（用户框架）：
  不是看 OI 绝对值，而是监控 OI 增速（ΔOI）与价格的背离。

  最值得关注的前置信号：
    OI 暴涨 +200%，价格只涨 +3%，Funding 接近 0，成交量开始放大
    → 大量新资金正在进入，但行情还没有充分发酵

OI Ratio 分级：
  OI Ratio = 当前 OI / 24h 前 OI
    < 1.1   → 正常
    1.2~1.5 → 有资金开始流入
    1.5~2.0 → 值得重点关注
    > 2.0   → 极强异动，结合价格判断

Price/Volume/OI 组合矩阵：
  Price ↑  Volume ↑  OI ↑  → 趋势启动（最佳）
  Price ↑  Volume ↑  OI ↓  → 空头回补，持续性较弱
  Price ↓  Volume ↑  OI ↑  → 新增空头，可能继续下跌
  Price ↓  Volume ↓  OI ↓  → 市场降温，关注度下降

用法:
  python3 pump_agent.py --symbol BLESSUSDT
  python3 pump_agent.py --symbol BLESSUSDT --json
  python3 pump_agent.py --symbol BLESSUSDT --oi-only     # 只看 OI 维度
"""

import argparse, json, sys, os, time, hmac, hashlib
from datetime import datetime, timezone
from urllib.parse import urlencode
from typing import Optional

try:
    import requests
except ImportError:
    print("需要 requests 库: python3 -m pip install --break-system-packages requests")
    sys.exit(1)

# ── 配置加载 ──────────────────────────────────────────────

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

# ── Binance Futures API ────────────────────────────────────

BASE = "https://fapi.binance.com"

class BinanceFutures:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def _sign(self, params):
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        sig = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return f"{query}&signature={sig}"

    def _signed_get(self, endpoint, params):
        url = f"{BASE}{endpoint}?{self._sign(params)}"
        r = requests.get(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=15)
        r.raise_for_status()
        return r.json()

    def _public_get(self, endpoint, params):
        r = requests.get(f"{BASE}{endpoint}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    # 公开接口
    def current_oi(self, symbol):
        return self._public_get("/fapi/v1/openInterest", {"symbol": symbol})

    def klines(self, symbol, interval, limit=500):
        return self._public_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def funding_rate(self, symbol, limit=300):
        return self._public_get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})

    def ticker_24hr(self, symbol):
        return self._public_get("/fapi/v1/ticker/24hr", {"symbol": symbol})

    def ticker_price(self, symbol):
        return self._public_get("/fapi/v1/ticker/price", {"symbol": symbol})

    # 签名接口
    def oi_history(self, symbol, period, limit=500):
        return self._signed_get("/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit})

    def taker_buy_sell(self, symbol, period, limit=100):
        """主动买卖成交量 — 用于统计每周期多单/空单"""
        return self._signed_get("/futures/data/takerBuySellVol", {"symbol": symbol, "period": period, "limit": limit})


# ── 数据解析工具 ──────────────────────────────────────────

def parse_oi_series(data):
    """解析 OI 历史数据 → [(ts_ms, qty, value_usd), ...]"""
    return [(d["timestamp"], float(d["sumOpenInterest"]), float(d["sumOpenInterestValue"])) for d in data]

def ts_to_str(ts_ms, fmt="%m-%d %H:%M"):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(fmt)

def parse_klines(klines):
    """解析 K 线 → [{ts, o, h, l, c, vol, quote_vol, taker_buy_vol, taker_sell_vol}, ...]

    Binance kline 格式:
      [0]=open_time, [1]=open, [2]=high, [3]=low, [4]=close,
      [5]=volume, [7]=quote_vol, [9]=taker_buy_base_vol, [10]=taker_buy_quote_vol
    """
    return [{
        "ts": k[0], "date": ts_to_str(k[0], "%Y-%m-%d"),
        "open": float(k[1]), "high": float(k[2]),
        "low": float(k[3]), "close": float(k[4]),
        "volume": float(k[5]), "quote_vol": float(k[7]),
        "taker_buy_quote_vol": float(k[10]),
        "taker_sell_quote_vol": float(k[7]) - float(k[10]),
    } for k in klines]


# ── 核心: Pump Scanner ─────────────────────────────────────

class PumpScanner:
    """
    暴涨扫描器 — 基于 OI 增速 + 价格背离

    核心命题: 最值得关注的是"OI 已经暴涨但价格还没动"的标的
    这意味大量新资金正在建仓，行情还没有被市场定价。
    """

    def __init__(self, client, symbol):
        self.client = client
        self.symbol = symbol

    def fetch(self):
        """拉取所有数据（最小集: OI + Price + Volume + Funding）"""
        print("→ 拉取数据...", end=" ", flush=True)

        self.current_oi = self.client.current_oi(self.symbol)
        self.current_oi_qty = float(self.current_oi["openInterest"])

        # OI 历史: 5m 精细 (近2天) + 1h (近周) + 4h (近月)
        self.oi_5m = parse_oi_series(self.client.oi_history(self.symbol, "5m", limit=500))
        self.oi_1h = parse_oi_series(self.client.oi_history(self.symbol, "1h", limit=200))
        self.oi_4h = parse_oi_series(self.client.oi_history(self.symbol, "4h", limit=200))

        # K 线: 1d (趋势) + 4h (结构) + 1h (微观)
        self.klines_1d = parse_klines(self.client.klines(self.symbol, "1d", limit=30))
        self.klines_4h = parse_klines(self.client.klines(self.symbol, "4h", limit=200))
        self.klines_1h = parse_klines(self.client.klines(self.symbol, "1h", limit=72))

        # 费率 + 行情
        self.funding = self.client.funding_rate(self.symbol, limit=200)
        self.ticker = self.client.ticker_24hr(self.symbol)

        # 主动买卖量 (4h 粒度，用于统计每周期新开多单/空单)
        try:
            self.taker = self.client.taker_buy_sell(self.symbol, "4h", limit=100)
        except Exception:
            self.taker = None

        print("✓")
        return self

    # ── 指标 1: OI Ratio（核心）──

    def oi_ratio_analysis(self):
        """
        OI Ratio = 当前 OI / N 小时前 OI

        分级：
          < 1.1   → 正常
          1.1~1.2 → 轻微流入
          1.2~1.5 → 资金开始流入
          1.5~2.0 → 重点监控
          > 2.0   → 极强异动
        """
        def ratio_at(data, hours_ago):
            """计算当前 OI 相对于 hours_ago 前的比值"""
            target_ts = self.oi_5m[-1][0] - hours_ago * 3600 * 1000
            for i in range(len(data) - 1, -1, -1):
                if data[i][0] <= target_ts:
                    return data[-1][1] / data[i][1], data[i]
            return 1.0, data[0]

        results = {}
        for hours, label in [(1, "1h"), (4, "4h"), (12, "12h"), (24, "24h")]:
            ratio, base = ratio_at(self.oi_5m, hours)
            results[label] = {
                "ratio": ratio,
                "from_time": ts_to_str(base[0]),
                "from_oi": base[1],
                "from_value": base[2],
                "current_oi": self.oi_5m[-1][1],
                "current_value": self.oi_5m[-1][2],
            }

        # OI Ratio 分级 (以 24h 为主要判断)
        ratio_24h = results["24h"]["ratio"]

        if ratio_24h >= 2.0:
            tier = "🔥 Tier 1 — 极强异动"
            tier_score = 10
            tier_desc = "OI 翻倍以上，必须结合价格判断：如果价格涨幅很小，这是最优前置信号"
        elif ratio_24h >= 1.5:
            tier = "🟠 Tier 2 — 重点监控"
            tier_score = 7
            tier_desc = "OI 增长 50%+，资金明显流入，关注价格是否跟上"
        elif ratio_24h >= 1.2:
            tier = "🟡 Tier 3 — 值得注意"
            tier_score = 4
            tier_desc = "OI 温和增长 20%+，可能是早期建仓"
        elif ratio_24h >= 1.1:
            tier = "➖ Tier 4 — 轻微流入"
            tier_score = 2
            tier_desc = "OI 略有增长，不一定有意义"
        else:
            tier = "⚪ Tier 5 — 正常/流出"
            tier_score = 0
            tier_desc = "OI 持平或下降，无异常"

        # OI 加速度: 比较最近 1h 的 OI 增速 vs 24h 平均增速
        ratio_1h = results["1h"]["ratio"]
        hourly_pace = (ratio_1h - 1.0)  # 最近 1 小时的增速
        daily_pace = (ratio_24h - 1.0) / 24  # 24h 平均每小时的增速
        acceleration = hourly_pace / daily_pace if daily_pace > 0.001 else 0

        if acceleration > 3:
            accel_signal = "🔥 OI 增速急剧加速 — 资金正在涌入"
        elif acceleration > 1.5:
            accel_signal = "📈 OI 增速加快"
        elif acceleration > 0.5:
            accel_signal = "➖ OI 增速平稳"
        else:
            accel_signal = "📉 OI 增速放缓"

        return {
            "ratios": results,
            "ratio_24h": ratio_24h,
            "ratio_1h": ratio_1h,
            "acceleration": acceleration,
            "tier": tier,
            "tier_score": tier_score,
            "tier_desc": tier_desc,
            "accel_signal": accel_signal,
            "oi_from_value_usd": self.oi_5m[-1][2],
            "oi_to_value_usd": self.oi_5m[-1][2],
        }

    # ── 指标 2: 价格偏离度 ──

    def price_divergence(self):
        """
        计算价格在 OI 暴涨期间的涨幅。
        核心问题: 资金进来了，价格跟上了吗？

        最优信号: OI ↑↑ + Price ↑ (小) → 资金在默默建仓
        次优信号: OI ↑↑ + Price ↑↑ → 趋势已启动，追入
        风险信号: OI ↑↑ + Price → (平) → 可能是对倒
        """
        latest = self.klines_1d[-1]

        # 24h 价格变化
        price_chg_24h = float(self.ticker.get("priceChangePercent", 0))

        # 最近 7 天价格范围
        recent_closes = [k["close"] for k in self.klines_1d[-7:]]
        price_7d_high = max(k["high"] for k in self.klines_1d[-7:])
        price_7d_low = min(k["low"] for k in self.klines_1d[-7:])
        price_7d_range = (price_7d_high - price_7d_low) / price_7d_low * 100
        current = latest["close"]
        position_in_range = (current - price_7d_low) / (price_7d_high - price_7d_low) * 100 if price_7d_high != price_7d_low else 50

        return {
            "current_price": current,
            "price_chg_24h_pct": price_chg_24h,
            "price_7d_high": price_7d_high,
            "price_7d_low": price_7d_low,
            "price_7d_range_pct": price_7d_range,
            "position_in_range_pct": position_in_range,
            "ath_drawdown_pct": None,  # 需要更长历史
        }

    # ── 指标 3: P/V/O 矩阵 ──

    def pvo_matrix(self):
        """
        Price / Volume / OI 三维组合分析

        P↑ V↑ OI↑ → 🟢 趋势启动（最佳买入信号）
        P↑ V↑ OI↓ → 🟡 空头回补推动（持续性弱）
        P↓ V↑ OI↑ → 🔴 新增空头（可能继续跌）
        P↓ V↓ OI↓ → ⚪ 市场冷却
        P→ V↑ OI↑ → 🔥 隐藏吸筹（价格没动但资金大量涌入 — 最优前置信号！）
        """
        oi_ratio = self.oi_ratio_analysis()
        price = self.price_divergence()

        # 判断方向
        price_up = price["price_chg_24h_pct"] > 2
        price_down = price["price_chg_24h_pct"] < -2
        price_flat = not price_up and not price_down

        # 成交量判断：当前成交量 vs 7日均量
        if len(self.klines_1d) >= 8:
            last_vol = self.klines_1d[-1]["quote_vol"]
            avg_vol_7d = sum(k["quote_vol"] for k in self.klines_1d[-8:-1]) / 7
            vol_up = last_vol > avg_vol_7d * 1.5
            vol_down = last_vol < avg_vol_7d * 0.5
            vol_normal = not vol_up and not vol_down
            vol_ratio = last_vol / avg_vol_7d
        else:
            vol_up = vol_down = vol_normal = False
            vol_ratio = 1.0

        # OI 判断
        oi_up = oi_ratio["ratio_24h"] > 1.15
        oi_down = oi_ratio["ratio_24h"] < 0.90

        # 矩阵判定
        scenarios = []

        if price_flat and oi_up and (vol_up or vol_normal):
            scenarios.append({
                "type": "🔥 隐藏吸筹",
                "desc": "OI 大涨但价格不动 — 大资金在压价建仓。这是最强的暴涨前置信号。",
                "score": 10,
                "priority": 1,
            })

        if price_up and vol_up and oi_up:
            scenarios.append({
                "type": "🟢 趋势启动",
                "desc": "价量齐升 + OI 增长 — 健康的多头趋势，最佳追入窗口",
                "score": 8,
                "priority": 2,
            })

        if price_up and vol_up and oi_down:
            scenarios.append({
                "type": "🟡 空头回补",
                "desc": "价格上涨但 OI 下降 — 空头被迫平仓推动的上涨，不具备持续性",
                "score": 4,
                "priority": 3,
            })

        if price_down and vol_up and oi_up:
            scenarios.append({
                "type": "🔴 新增空头",
                "desc": "价格下跌 + 成交量放大 + OI 上升 — 有空头大规模进场",
                "score": 1,
                "priority": 4,
            })

        if price_down and vol_down:
            scenarios.append({
                "type": "⚪ 市场冷却",
                "desc": "价跌量缩 — 市场关注度下降，短期无交易价值",
                "score": 0,
                "priority": 5,
            })

        # 如果没有精确匹配，给一个综合描述
        if not scenarios:
            scenarios.append({
                "type": "➖ 信号混合",
                "desc": "各维度信号不一致，需进一步观察",
                "score": 3,
                "priority": 6,
            })

        primary = scenarios[0]  # 最重要的场景

        return {
            "primary_signal": primary["type"],
            "primary_desc": primary["desc"],
            "primary_score": primary["score"],
            "all_scenarios": scenarios,
            "price_direction": "↑" if price_up else ("↓" if price_down else "→"),
            "volume_direction": "↑" if vol_up else ("↓" if vol_down else "→"),
            "oi_direction": "↑" if oi_up else ("↓" if oi_down else "→"),
            "vol_ratio_vs_7d": vol_ratio,
            "details": {
                "price_chg_24h": price["price_chg_24h_pct"],
                "vol_vs_7d_avg": vol_ratio,
                "oi_ratio_24h": oi_ratio["ratio_24h"],
            },
        }

    # ── 指标 4: Funding 热度 ──

    def funding_health(self):
        """资金费率健康度 — 必须在拉升前处于地板价"""
        rates = [float(f["fundingRate"]) for f in self.funding]

        if not rates:
            return {"error": "无数据"}

        current = rates[-1] * 100
        avg_50p = sum(rates[-50:]) / min(len(rates), 50) * 100

        # 地板比例: 最近 50 个周期中费率 < 0.01% 的比例
        floor_count = sum(1 for r in rates[-50:] if r * 100 <= 0.01)
        floor_ratio = floor_count / min(len(rates), 50)

        # 拉升前的理想状态: 费率持续在地板
        if floor_ratio > 0.8 and current < 0.02:
            status = "✅ 完美 — 费率长期地板，多头完全未拥挤"
            score = 10
        elif floor_ratio > 0.5:
            status = "🟢 健康 — 大部分时间费率很低"
            score = 7
        elif current < 0.05:
            status = "🟡 温和 — 费率有所抬头但未过热"
            score = 4
        elif current < 0.10:
            status = "🟠 偏高 — 多头开始拥挤"
            score = 2
        else:
            status = "🔴 过热 — 费率极高，回调风险大"
            score = 0

        return {
            "current_pct": current,
            "avg_50p_pct": avg_50p,
            "floor_ratio": floor_ratio,
            "annualized_pct": current * 3 * 365,
            "status": status,
            "score": score,
        }

    # ── 指标 5: 隐藏吸筹检测（综合）──

    def detect_hidden_accumulation(self):
        """
        检测"隐藏吸筹"模式 — 用户框架中最优的前置信号

        条件:
          1. OI Ratio (24h) > 1.5 或更高（资金大规模流入）
          2. 价格涨幅 < 5%（行情未发酵）
          3. Funding 接近 0（无过热）
          4. 成交量开始放大（资金真实进入，不是对倒）

        匹配度越高，越可能是暴涨前夜。
        """
        oi = self.oi_ratio_analysis()
        price = self.price_divergence()
        funding = self.funding_health()
        pvo = self.pvo_matrix()

        score = 0
        checks = []

        # Check 1: OI 大规模增长
        if oi["ratio_24h"] >= 2.0:
            checks.append(("OI 翻倍以上", 3, "🔥"))
            score += 3
        elif oi["ratio_24h"] >= 1.5:
            checks.append(("OI 增长 50%+", 2, "🟠"))
            score += 2
        elif oi["ratio_24h"] >= 1.2:
            checks.append(("OI 增长 20%+", 1, "🟡"))
            score += 1
        else:
            checks.append(("OI 无明显增长", 0, "⚪"))

        # Check 2: 价格未动
        if abs(price["price_chg_24h_pct"]) < 3:
            checks.append(("价格几乎未动 (<3%)", 3, "🔥"))
            score += 3
        elif abs(price["price_chg_24h_pct"]) < 5:
            checks.append(("价格小幅波动 (<5%)", 2, "🟠"))
            score += 2
        elif abs(price["price_chg_24h_pct"]) < 10:
            checks.append(("价格温和波动 (<10%)", 1, "🟡"))
            score += 1
        else:
            checks.append(("价格已明显变动", 0, "⚪"))

        # Check 3: Funding 地板
        if funding.get("current_pct", 99) < 0.01:
            checks.append(("Funding 地板价", 2, "🔥"))
            score += 2
        elif funding.get("current_pct", 99) < 0.03:
            checks.append(("Funding 低水平", 1, "🟢"))
            score += 1
        else:
            checks.append(("Funding 偏高", 0, "🔴"))

        # Check 4: 成交量扩张
        if pvo.get("vol_ratio_vs_7d", 1) > 2.0:
            checks.append(("成交量显著放大 (>2x)", 2, "🔥"))
            score += 2
        elif pvo.get("vol_ratio_vs_7d", 1) > 1.3:
            checks.append(("成交量开始放大 (>1.3x)", 1, "🟢"))
            score += 1
        else:
            checks.append(("成交量未放大", 0, "⚪"))

        # 综合判定
        max_score = 10
        if score >= 8:
            verdict = "🔥 极强隐藏吸筹信号 — 暴涨前夜概率极高"
        elif score >= 5:
            verdict = "🟠 中等吸筹信号 — 值得加入重点监控"
        elif score >= 3:
            verdict = "🟡 轻微吸筹信号 — 持续观察"
        else:
            verdict = "⚪ 无明显吸筹信号"

        return {
            "score": score,
            "max_score": max_score,
            "verdict": verdict,
            "checks": checks,
            "key_metrics": {
                "oi_ratio_24h": oi["ratio_24h"],
                "price_chg_24h": price["price_chg_24h_pct"],
                "funding_pct": funding.get("current_pct", None),
                "vol_vs_7d": pvo.get("vol_ratio_vs_7d", 1),
                "oi_acceleration": oi["acceleration"],
            },
        }

    # ── 指标 6: 4小时 OI 快照 & 7天统计（含多空分解）──

    def oi_4h_snapshot_analysis(self):
        """
        每 4 小时一个快照，统计该周期内：
          - OI qty / OI value / OI Ratio (vs 24h前) / OI Δ%
          - Taker Buy Vol (主动买入量，近似新开多单)
          - Taker Sell Vol (主动卖出量，近似新开空单)
          - 4h 多空比 = Buy Vol / Sell Vol

        7 天汇总：
          - 累计新开多单 (total taker buy)
          - 累计新开空单 (total taker sell)
          - 7 天累计多空比
          - 每日多空比分布
        """
        oi_data = self.oi_4h
        seven_days_ago = oi_data[-1][0] - 7 * 24 * 3600 * 1000
        recent = [(ts, qty, val) for ts, qty, val in oi_data if ts >= seven_days_ago]

        if len(recent) < 12:
            return {"error": f"数据不足，仅有 {len(recent)} 个 4h 快照", "snapshots": []}

        # ── 从 4h K 线取 Taker Buy/Sell ──
        # 4h kline 自带 taker_buy_quote_vol (主动买入) 和 taker_sell_quote_vol (总-买=主动卖出)
        kline_by_ts = {}
        if self.klines_4h:
            for k in self.klines_4h:
                kline_by_ts[k["ts"]] = k

        def find_kline_at(target_ts, tolerance_ms=3*3600*1000):
            best = None
            for ts, k in kline_by_ts.items():
                if abs(ts - target_ts) <= tolerance_ms:
                    if best is None or abs(ts - target_ts) < abs(best[0] - target_ts):
                        best = (ts, k)
            return best[1] if best else None

        # ── 构建快照 ──
        snapshots = []
        oi_ratios = []

        for i, (ts, qty, val) in enumerate(recent):
            # OI Ratio
            target_ts = ts - 24 * 3600 * 1000
            base_qty = None
            for j in range(len(oi_data) - 1, -1, -1):
                if oi_data[j][0] <= target_ts:
                    base_qty = oi_data[j][1]
                    break
            oi_ratio = qty / base_qty if base_qty and base_qty > 0 else None
            if oi_ratio is not None:
                oi_ratios.append(oi_ratio)

            # OI Δ%
            if i > 0:
                prev_qty = recent[i - 1][1]
                delta_pct = (qty - prev_qty) / prev_qty * 100 if prev_qty > 0 else 0
            else:
                delta_pct = 0

            # Taker buy/sell: 从 4h K 线取该窗口的主动买卖量
            kline = find_kline_at(ts)
            buy_vol = kline["taker_buy_quote_vol"] if kline else 0
            sell_vol = kline["taker_sell_quote_vol"] if kline else 0
            ls_ratio_4h = buy_vol / sell_vol if sell_vol > 0 else None

            # 4h 多空比判定
            if ls_ratio_4h is None:
                ls_label = "N/A"
            elif ls_ratio_4h >= 1.5:
                ls_label = "🔥多"
            elif ls_ratio_4h >= 1.2:
                ls_label = "🟢偏多"
            elif ls_ratio_4h >= 0.8:
                ls_label = "➖均衡"
            elif ls_ratio_4h >= 0.5:
                ls_label = "🔴偏空"
            else:
                ls_label = "🧊极空"

            # OI Ratio 分级
            if oi_ratio is None:
                tier = "?"
            elif oi_ratio >= 2.0:
                tier = "T1"
            elif oi_ratio >= 1.5:
                tier = "T2"
            elif oi_ratio >= 1.2:
                tier = "T3"
            elif oi_ratio >= 1.1:
                tier = "T4"
            else:
                tier = "—"

            snapshots.append({
                "time": ts_to_str(ts, "%m-%d %H:%M"),
                "ts": ts,
                "oi_qty": qty,
                "oi_value": val,
                "oi_ratio": oi_ratio,
                "oi_delta_pct": delta_pct,
                "buy_vol": buy_vol,
                "sell_vol": sell_vol,
                "ls_ratio_4h": ls_ratio_4h,
                "ls_label": ls_label,
                "tier": tier,
            })

        # ── 7 天统计 ──
        valid_ratios = [r for r in oi_ratios if r is not None]
        valid_deltas = [s["oi_delta_pct"] for s in snapshots[1:]]

        # OI 趋势
        half = len(snapshots) // 2
        if len(snapshots) >= 12:
            first_half_avg = sum(s["oi_qty"] for s in snapshots[:half]) / half
            second_half_avg = sum(s["oi_qty"] for s in snapshots[half:]) / (len(snapshots) - half)
            if second_half_avg > first_half_avg * 1.1:
                oi_trend = "📈 扩张"
            elif second_half_avg < first_half_avg * 0.9:
                oi_trend = "📉 收缩"
            else:
                oi_trend = "➖ 横盘"
        else:
            oi_trend = "?"

        # OI Ratio 分档
        ratio_bins = {"T1 (>2.0)": 0, "T2 (1.5~2.0)": 0, "T3 (1.2~1.5)": 0, "T4 (1.1~1.2)": 0, "正常 (<1.1)": 0}
        for r in valid_ratios:
            if r >= 2.0:       ratio_bins["T1 (>2.0)"] += 1
            elif r >= 1.5:     ratio_bins["T2 (1.5~2.0)"] += 1
            elif r >= 1.2:     ratio_bins["T3 (1.2~1.5)"] += 1
            elif r >= 1.1:     ratio_bins["T4 (1.1~1.2)"] += 1
            else:              ratio_bins["正常 (<1.1)"] += 1

        # ── 累计多空统计 ──
        total_buy = sum(s["buy_vol"] for s in snapshots)
        total_sell = sum(s["sell_vol"] for s in snapshots)
        cumulative_ls = total_buy / total_sell if total_sell > 0 else None

        # 每日多空汇总
        daily = {}
        for s in snapshots:
            day = s["time"][:5]
            if day not in daily:
                daily[day] = {
                    "oi_start": s["oi_qty"], "oi_end": s["oi_qty"],
                    "ratios": [], "deltas": [],
                    "buy_vol": 0, "sell_vol": 0,
                    "oi_value_end": s["oi_value"],
                }
            daily[day]["oi_end"] = s["oi_qty"]
            daily[day]["oi_value_end"] = s["oi_value"]
            daily[day]["buy_vol"] += s["buy_vol"]
            daily[day]["sell_vol"] += s["sell_vol"]
            if s["oi_ratio"] is not None:
                daily[day]["ratios"].append(s["oi_ratio"])
            daily[day]["deltas"].append(s["oi_delta_pct"])
        for day in daily:
            d = daily[day]
            d["oi_chg_pct"] = (d["oi_end"] - d["oi_start"]) / d["oi_start"] * 100 if d["oi_start"] > 0 else 0
            d["oi_ratio_avg"] = sum(d["ratios"]) / len(d["ratios"]) if d["ratios"] else None
            d["oi_ratio_max"] = max(d["ratios"]) if d["ratios"] else None
            d["daily_ls_ratio"] = d["buy_vol"] / d["sell_vol"] if d["sell_vol"] > 0 else None

        # 价格-OI 相关系数
        price_oi_corr = None
        if self.klines_4h:
            aligned_prices, aligned_ois = [], []
            for s in snapshots:
                for k in self.klines_4h:
                    if abs(k["ts"] - s["ts"]) <= 2 * 3600 * 1000:
                        aligned_prices.append(k["close"])
                        aligned_ois.append(s["oi_qty"])
                        break
            if len(aligned_prices) >= 10:
                n = len(aligned_prices)
                sx, sy = sum(aligned_prices), sum(aligned_ois)
                sxy = sum(x*y for x, y in zip(aligned_prices, aligned_ois))
                sx2, sy2 = sum(x*x for x in aligned_prices), sum(y*y for y in aligned_ois)
                denom = ((n*sx2 - sx**2) * (n*sy2 - sy**2)) ** 0.5
                if denom > 0:
                    price_oi_corr = (n*sxy - sx*sy) / denom

        # 关键发现
        key_findings = []
        if price_oi_corr is not None:
            if price_oi_corr > 0.7:
                key_findings.append("🔗 价格与 OI 高度正相关 — 趋势健康")
            elif price_oi_corr < -0.5:
                key_findings.append("⚠️ 价格与 OI 负相关 — OI 增但价格跌，空头主导")
            elif abs(price_oi_corr) < 0.3:
                key_findings.append("🔍 价格与 OI 相关性弱 — 可能存在背离机会")
        if len(valid_ratios) >= 12:
            recent_6 = sum(valid_ratios[-6:]) / 6
            prior_6 = sum(valid_ratios[-12:-6]) / 6
            if recent_6 > prior_6 * 1.3:
                key_findings.append("🔥 OI Ratio 近 24h 加速攀升 — 资金流入加快")
        if cumulative_ls is not None:
            if cumulative_ls >= 1.3:
                key_findings.append(f"🟢 7天累计多空比 {cumulative_ls:.2f} — 主动买盘显著强于卖盘")
            elif cumulative_ls <= 0.7:
                key_findings.append(f"🔴 7天累计多空比 {cumulative_ls:.2f} — 主动卖盘显著强于买盘")
            else:
                key_findings.append(f"➖ 7天累计多空比 {cumulative_ls:.2f} — 买卖力量均衡")

        return {
            "snapshots": snapshots,
            "total_snapshots": len(snapshots),
            "daily_summary": daily,
            "stats": {
                "oi_qty_min": min(s["oi_qty"] for s in snapshots),
                "oi_qty_max": max(s["oi_qty"] for s in snapshots),
                "oi_qty_avg": sum(s["oi_qty"] for s in snapshots) / len(snapshots),
                "oi_qty_first": snapshots[0]["oi_qty"],
                "oi_qty_last": snapshots[-1]["oi_qty"],
                "oi_qty_change_pct": (snapshots[-1]["oi_qty"] - snapshots[0]["oi_qty"]) / snapshots[0]["oi_qty"] * 100,
                "oi_ratio_min": min(valid_ratios) if valid_ratios else None,
                "oi_ratio_max": max(valid_ratios) if valid_ratios else None,
                "oi_ratio_avg": sum(valid_ratios) / len(valid_ratios) if valid_ratios else None,
                "oi_ratio_latest": valid_ratios[-1] if valid_ratios else None,
                "oi_delta_max_pct": max(valid_deltas) if valid_deltas else None,
                "oi_delta_min_pct": min(valid_deltas) if valid_deltas else None,
                "oi_delta_avg_pct": sum(valid_deltas) / len(valid_deltas) if valid_deltas else None,
                "price_oi_correlation": price_oi_corr,
            },
            "oi_trend": oi_trend,
            "ratio_bins": ratio_bins,
            "trade_summary": {
                "total_buy_vol": total_buy,
                "total_sell_vol": total_sell,
                "cumulative_ls_ratio": cumulative_ls,
            },
            "key_findings": key_findings,
        }

    # ── 综合扫描 ──

    def scan(self):
        """执行完整扫描，给出所有维度的分析"""
        self.fetch()

        oi = self.oi_ratio_analysis()
        price = self.price_divergence()
        pvo = self.pvo_matrix()
        funding = self.funding_health()
        hidden = self.detect_hidden_accumulation()
        snap_4h = self.oi_4h_snapshot_analysis()

        return {
            "symbol": self.symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "current_price": price["current_price"],
            "oi": oi,
            "price": price,
            "pvo": pvo,
            "funding": funding,
            "hidden_accumulation": hidden,
            "oi_4h_snapshots": snap_4h,
        }


# ── 输出格式化 ──────────────────────────────────────────────

def format_report(result):
    """人类可读的报告"""
    oi = result["oi"]
    price = result["price"]
    pvo = result["pvo"]
    funding = result["funding"]
    hidden = result["hidden_accumulation"]
    sym = result["symbol"]

    lines = []
    w = 64

    lines.append("═" * w)
    lines.append(f"  🔭 Pump Scanner — {sym}")
    lines.append(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    lines.append("═" * w)

    # 当前价格
    lines.append(f"\n  💰 当前价格: ${result['current_price']:.6f}")
    lines.append(f"     24h 涨跌: {price['price_chg_24h_pct']:+.2f}%")

    # ── OI Ratio ──
    lines.append(f"\n  {'─' * 56}")
    lines.append(f"  ① OI Ratio（核心指标）")
    lines.append(f"  {'─' * 56}")
    lines.append(f"     分级: {oi['tier']}")
    lines.append(f"     {oi['tier_desc']}")
    lines.append(f"")
    for period in ["1h", "4h", "12h", "24h"]:
        r = oi["ratios"][period]
        bar_len = min(int((r["ratio"] - 1) * 20), 30)
        bar = "█" * max(0, bar_len)
        lines.append(f"     {period:>4}: {r['ratio']:.3f}x  {bar}")
    lines.append(f"")
    lines.append(f"     OI 加速度: {oi['acceleration']:.1f}x (vs 24h 平均)")
    lines.append(f"     {oi['accel_signal']}")

    # ── P/V/O 矩阵 ──
    lines.append(f"\n  {'─' * 56}")
    lines.append(f"  ② Price / Volume / OI 矩阵")
    lines.append(f"  {'─' * 56}")
    lines.append(f"     P:{pvo['price_direction']}  V:{pvo['volume_direction']}  OI:{pvo['oi_direction']}")
    lines.append(f"     → {pvo['primary_signal']}")
    lines.append(f"     {pvo['primary_desc']}")
    lines.append(f"")
    lines.append(f"     细节:")
    lines.append(f"     · 价格 24h:    {pvo['details']['price_chg_24h']:+.2f}%")
    lines.append(f"     · 量 vs 7d 均: {pvo['details']['vol_vs_7d_avg']:.2f}x")
    lines.append(f"     · OI Ratio 24h: {pvo['details']['oi_ratio_24h']:.2f}x")

    # ── Funding ──
    lines.append(f"\n  {'─' * 56}")
    lines.append(f"  ③ Funding Rate 热度")
    lines.append(f"  {'─' * 56}")
    lines.append(f"     当前费率: {funding['current_pct']:+.4f}% (年化 {funding['annualized_pct']:+.1f}%)")
    lines.append(f"     50周期均: {funding['avg_50p_pct']:+.4f}%")
    lines.append(f"     地板占比: {funding['floor_ratio']:.0%}")
    lines.append(f"     → {funding['status']}")

    # ── 隐藏吸筹检测 ──
    lines.append(f"\n  {'─' * 56}")
    lines.append(f"  ④ 隐藏吸筹检测（暴涨前置信号）")
    lines.append(f"  {'─' * 56}")
    for check_name, check_score, emoji in hidden["checks"]:
        indent = "    "
        lines.append(f"     {emoji} {check_name} (+{check_score})")
    lines.append(f"")
    lines.append(f"     吸筹得分: {hidden['score']}/{hidden['max_score']}")
    lines.append(f"     → {hidden['verdict']}")
    lines.append(f"")
    lines.append(f"     关键数据:")
    for k, v in hidden["key_metrics"].items():
        if isinstance(v, float):
            lines.append(f"     · {k}: {v:.4f}")
        else:
            lines.append(f"     · {k}: {v}")

    # ── 4h OI 快照 & 7 天统计 ──
    snap = result.get("oi_4h_snapshots", {})
    if snap and snap.get("snapshots"):
        stats = snap["stats"]
        ts = snap.get("trade_summary", {})
        lines.append(f"\n  {'─' * 56}")
        lines.append(f"  ⑤ 4H OI 快照 & 最近 7 天统计（Taker 买卖量）")
        lines.append(f"  {'─' * 56}")

        # OI 趋势
        lines.append(f"     7天 OI 趋势: {snap['oi_trend']}")
        lines.append(f"     快照数量:    {snap['total_snapshots']} (每4小时)")
        lines.append(f"     OI 变化:     {stats['oi_qty_first']:,.0f} → {stats['oi_qty_last']:,.0f} ({stats['oi_qty_change_pct']:+.1f}%)")
        lines.append(f"")

        # OI Ratio 统计
        lines.append(f"     OI Ratio 7天: 均值 {stats['oi_ratio_avg']:.3f}x  最大 {stats['oi_ratio_max']:.3f}x  最小 {stats['oi_ratio_min']:.3f}x")
        lines.append(f"")

        # OI Ratio 分布
        lines.append(f"     OI Ratio 分布:")
        for tier_name, count in snap["ratio_bins"].items():
            pct = count / snap['total_snapshots'] * 100
            bar_n = min(int(pct / 2), 30)
            bar = "█" * bar_n
            lines.append(f"     {tier_name:>16}: {count:>3}次 ({pct:>5.1f}%) {bar}")
        lines.append(f"")

        # 7 天累计多空
        lines.append(f"     ┌─ 7 天 Taker 买卖量（近似多单/空单）──────────────┐")
        lines.append(f"     │ 累计主动买入 (新开多单): ${ts.get('total_buy_vol', 0):>14,.0f}")
        lines.append(f"     │ 累计主动卖出 (新开空单): ${ts.get('total_sell_vol', 0):>14,.0f}")
        cls = ts.get("cumulative_ls_ratio")
        if cls:
            cls_emoji = "🟢" if cls >= 1.2 else ("🔴" if cls <= 0.8 else "➖")
            lines.append(f"     │ 7天累计多空比:           {cls_emoji} {cls:.2f}")
        lines.append(f"     └{'─'*54}┘")
        lines.append(f"")

        # 价格-OI + 关键发现
        if stats["price_oi_correlation"] is not None:
            corr = stats["price_oi_correlation"]
            corr_emoji = "🟢" if corr > 0.5 else ("🟡" if corr > 0 else "🔴")
            lines.append(f"     Price-OI 相关系数: {corr_emoji} {corr:+.3f}")
        for f in (snap.get("key_findings") or []):
            lines.append(f"     {f}")
        lines.append(f"")

        # 每日汇总
        daily = snap.get("daily_summary", {})
        if daily:
            lines.append(f"     ┌─ 每日汇总 ───────────────────────────────────────────────┐")
            lines.append(f"     │ {'日期':>6}  {'OI Δ%':>9}  {'Ratio均值':>9}  {'Ratio最大':>9}  {'日多空比':>10} │")
            lines.append(f"     ├{'─'*60}┤")
            for day in sorted(daily.keys()):
                d = daily[day]
                ratio_avg = f"{d['oi_ratio_avg']:.3f}x" if d['oi_ratio_avg'] else "N/A"
                ratio_max = f"{d['oi_ratio_max']:.3f}x" if d['oi_ratio_max'] else "N/A"
                daily_ls = f"{d['daily_ls_ratio']:.2f}" if d.get('daily_ls_ratio') else "N/A"
                lines.append(f"     │ {day:>6}  {d['oi_chg_pct']:>+8.2f}%  {ratio_avg:>9}  {ratio_max:>9}  {daily_ls:>10} │")
            lines.append(f"     └{'─'*60}┘")
            lines.append(f"")

        # 完整 4h 快照表
        lines.append(f"     ┌─ 完整 7 天 4H 快照 ──────────────────────────────────────────────────────────────┐")
        lines.append(f"     │ {'时间':>11}  {'OI(M)':>7}  {'OI Ratio':>8}  {'Δ%':>7}  {'档':>4}  {'买入($)':>10}  {'卖出($)':>10}  {'4h多空比':>8} │")
        lines.append(f"     ├{'─'*84}┤")
        for s in snap["snapshots"]:
            oi_m = s["oi_qty"] / 1e6
            ratio_str = f"{s['oi_ratio']:.3f}x" if s['oi_ratio'] else "N/A"
            buy_str = f"${s['buy_vol']:,.0f}" if s['buy_vol'] > 0 else "—"
            sell_str = f"${s['sell_vol']:,.0f}" if s['sell_vol'] > 0 else "—"
            ls_str = f"{s['ls_ratio_4h']:.2f}" if s['ls_ratio_4h'] else "N/A"
            lines.append(
                f"     │ {s['time']:>11}  {oi_m:>6.1f}M  {ratio_str:>8}  "
                f"{s['oi_delta_pct']:>+6.2f}%  {s['tier']:>4}  {buy_str:>10}  {sell_str:>10}  {ls_str:>8} │"
            )
        lines.append(f"     └{'─'*84}┘")

    # ── 综合判定 ──
    lines.append(f"\n  {'═' * 56}")
    # 综合: 隐藏吸筹得分 + P/V/O 得分 + Funding 得分
    pvo_score = pvo["primary_score"]
    funding_score = funding["score"]

    # 归一化各维度到 10 分
    composite = hidden["score"] * 0.5 + pvo_score * 0.3 + funding_score * 0.2

    if composite >= 7:
        verdict = "🟢 强烈关注 — 多维度确认资金涌入，大概率即将变盘"
    elif composite >= 5:
        verdict = "🟡 保持监控 — 有异常但未完全确认"
    elif composite >= 3:
        verdict = "🟠 轻度关注 — 个别指标有信号"
    else:
        verdict = "⚪ 当前无显著异常"

    lines.append(f"  综合评分: {composite:.1f}/10")
    lines.append(f"  {verdict}")
    lines.append(f"  ═{'═' * 54}")

    return "\n".join(lines)


# ── 4h 专用报告 ────────────────────────────────────────────

def _print_4h_report(sym, snap):
    """纯 4h 快照+统计报告，用于定时扫描"""
    stats = snap["stats"]
    ts = snap.get("trade_summary", {})
    daily = snap.get("daily_summary", {})

    lines = []
    lines.append(f"══ 4H OI Snapshot — {sym} ══")
    lines.append(f"  时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    lines.append(f"  7天 OI 趋势: {snap['oi_trend']}")
    lines.append(f"  OI 变化: {stats['oi_qty_first']:,.0f} → {stats['oi_qty_last']:,.0f} ({stats['oi_qty_change_pct']:+.1f}%)")
    lines.append(f"")
    lines.append(f"  OI Ratio 7天: 均值 {stats['oi_ratio_avg']:.3f}x  最大 {stats['oi_ratio_max']:.3f}x  最小 {stats['oi_ratio_min']:.3f}x")
    lines.append(f"")
    lines.append(f"  OI Ratio 分布:")
    for tier_name, count in snap["ratio_bins"].items():
        pct = count / snap["total_snapshots"] * 100
        lines.append(f"    {tier_name:>16}: {count:>3}次 ({pct:>5.1f}%)")
    lines.append(f"")

    # 累计多空
    lines.append(f"  7天 Taker 买卖量 (近似多单/空单):")
    lines.append(f"    累计主动买入: ${ts.get('total_buy_vol', 0):,.0f}")
    lines.append(f"    累计主动卖出: ${ts.get('total_sell_vol', 0):,.0f}")
    cls = ts.get("cumulative_ls_ratio")
    if cls:
        lines.append(f"    累计多空比: {cls:.2f}")
    lines.append(f"")

    if stats["price_oi_correlation"] is not None:
        lines.append(f"  Price-OI 相关系数: {stats['price_oi_correlation']:+.3f}")
    if snap.get("key_findings"):
        for f in snap["key_findings"]:
            lines.append(f"  {f}")
    lines.append(f"")

    # 每日汇总
    if daily:
        lines.append(f"  ┌─ 每日汇总 ──────────────────────────────────────┐")
        lines.append(f"  │ {'日期':>6}  {'OI Δ%':>9}  {'Ratio均值':>9}  {'Ratio最大':>9}  {'日多空比':>10} │")
        lines.append(f"  ├{'─'*54}┤")
        for day in sorted(daily.keys()):
            d = daily[day]
            ratio_avg = f"{d['oi_ratio_avg']:.3f}x" if d['oi_ratio_avg'] else "N/A"
            ratio_max = f"{d['oi_ratio_max']:.3f}x" if d['oi_ratio_max'] else "N/A"
            daily_ls = f"{d['daily_ls_ratio']:.2f}" if d.get('daily_ls_ratio') else "N/A"
            lines.append(f"  │ {day:>6}  {d['oi_chg_pct']:>+8.2f}%  {ratio_avg:>9}  {ratio_max:>9}  {daily_ls:>10} │")
        lines.append(f"  └{'─'*54}┘")
        lines.append(f"")

    # 完整 4h 快照
    lines.append(f"  ┌─ 完整 7 天 4H 快照 ────────────────────────────────────────────────────┐")
    lines.append(f"  │ {'时间':>11}  {'OI(M)':>7}  {'OI Ratio':>8}  {'Δ%':>7}  {'档':>4}  {'买入($)':>10}  {'卖出($)':>10}  {'4h多空比':>8} │")
    lines.append(f"  ├{'─'*84}┤")
    for s in snap["snapshots"]:
        oi_m = s["oi_qty"] / 1e6
        ratio_str = f"{s['oi_ratio']:.3f}x" if s['oi_ratio'] else "N/A"
        buy_str = f"${s['buy_vol']:,.0f}" if s['buy_vol'] > 0 else "—"
        sell_str = f"${s['sell_vol']:,.0f}" if s['sell_vol'] > 0 else "—"
        ls_str = f"{s['ls_ratio_4h']:.2f}" if s['ls_ratio_4h'] else "N/A"
        lines.append(
            f"  │ {s['time']:>11}  {oi_m:>6.1f}M  {ratio_str:>8}  "
            f"{s['oi_delta_pct']:>+6.2f}%  {s['tier']:>4}  {buy_str:>10}  {sell_str:>10}  {ls_str:>8} │"
        )
    lines.append(f"  └{'─'*84}┘")
    lines.append(f"══{'═'*50}")
    print("\n".join(lines))


# ── CLI ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pump Scanner — 基于 OI 增速的暴涨前置扫描")
    parser.add_argument("--symbol", default="BLESSUSDT", help="交易对")
    parser.add_argument("--json", action="store_true", help="JSON 输出")
    parser.add_argument("--oi-only", action="store_true", help="只看 OI 维度")
    parser.add_argument("--4h-report", action="store_true", help="只输出 4h 快照和 7 天统计")
    args = parser.parse_args()

    api_key, api_secret = load_api_keys()
    if not api_key or not api_secret:
        print("[ERROR] 缺少 API 密钥，请在 config/binance_futures_key.json 中配置")
        sys.exit(1)

    client = BinanceFutures(api_key, api_secret)
    scanner = PumpScanner(client, args.symbol)

    try:
        result = scanner.scan()
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    elif args.oi_only:
        oi = result["oi"]
        print(f"OI Ratio 24h: {oi['ratio_24h']:.3f}x | {oi['tier']}")
        print(f"OI 加速度: {oi['acceleration']:.1f}x")
    elif args.__dict__.get("4h_report"):
        # 只输出 4h 快照 + 7 天统计
        snap = result.get("oi_4h_snapshots", {})
        if snap and snap.get("snapshots"):
            _print_4h_report(sym=args.symbol, snap=snap)
        else:
            print("无 4h OI 数据")
    else:
        print(format_report(result))


if __name__ == "__main__":
    main()
