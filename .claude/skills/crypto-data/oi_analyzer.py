#!/usr/bin/env python3
"""
OI Analyzer — Binance Futures Open Interest 多维分析工具

分析维度（用户五指标框架）：
  1. Volume Ratio      — 成交量 / 7日均量（权重 30%）
  2. OI 增长率          — 多周期 OI 变化（权重 25%）
  3. Spot/Futures 同步  — 现货与期货量价对比（权重 20%）
  4. Funding Rate       — 资金费率偏离度（权重 15%）
  5. 链上大额（占位）    — 权重 10%

用法:
  python3 oi_analyzer.py --symbol BLESSUSDT
  python3 oi_analyzer.py --symbol BLESSUSDT --lookback 7
  python3 oi_analyzer.py --symbol BLESSUSDT --json  # JSON 输出
"""

import argparse, json, sys, os, time, hmac, hashlib
from datetime import datetime, timezone
from urllib.parse import urlencode
from collections import deque

try:
    import requests
except ImportError:
    print("需要 requests 库: python3 -m pip install --break-system-packages requests")
    sys.exit(1)

# ── 配置加载 ──────────────────────────────────────────────

def load_api_keys():
    """从项目 config 目录加载 API 密钥"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # 向上找到项目根目录（包含 config/）
    project_root = script_dir
    for _ in range(6):
        if os.path.isdir(os.path.join(project_root, "config")):
            break
        project_root = os.path.dirname(project_root)

    config_path = os.path.join(project_root, "config", "binance_futures_key.json")
    if not os.path.exists(config_path):
        print(f"[WARN] 未找到 API 密钥配置: {config_path}")
        print(f"       请在 config/binance_futures_key.json 中配置")
        return None, None

    with open(config_path) as f:
        cfg = json.load(f)
    return cfg.get("api_key"), cfg.get("api_secret")

# ── Binance Futures API 客户端 ──────────────────────────────

BASE = "https://fapi.binance.com"

class BinanceFuturesClient:
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret

    def _signed_get(self, endpoint, params):
        """带签名的 GET 请求"""
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        url = f"{BASE}{endpoint}?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        return r.json()

    def _public_get(self, endpoint, params):
        """公开 GET 请求"""
        r = requests.get(f"{BASE}{endpoint}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    # ── 公开接口 ──

    def current_oi(self, symbol):
        """当前持仓量 (GET /fapi/v1/openInterest)"""
        return self._public_get("/fapi/v1/openInterest", {"symbol": symbol})

    def klines(self, symbol, interval, limit=500):
        """K 线数据"""
        return self._public_get("/fapi/v1/klines", {
            "symbol": symbol, "interval": interval, "limit": limit
        })

    def funding_rate(self, symbol, limit=200):
        """资金费率历史"""
        return self._public_get("/fapi/v1/fundingRate", {
            "symbol": symbol, "limit": limit
        })

    def ticker_24hr(self, symbol):
        """24 小时行情"""
        return self._public_get("/fapi/v1/ticker/24hr", {"symbol": symbol})

    def premium_index(self, symbol):
        """溢价指数（含最新资金费率）"""
        return self._public_get("/fapi/v1/premiumIndex", {"symbol": symbol})

    # ── 签名接口 ──

    def oi_history(self, symbol, period, limit=500):
        """持仓量历史 (GET /futures/data/openInterestHist)"""
        return self._signed_get("/futures/data/openInterestHist", {
            "symbol": symbol, "period": period, "limit": limit
        })

    def top_ls_ratio(self, symbol, period, limit=50):
        """大户多空比"""
        return self._signed_get("/futures/data/topLongShortAccountRatio", {
            "symbol": symbol, "period": period, "limit": limit
        })

    def global_ls_ratio(self, symbol, period, limit=50):
        """全局多空比"""
        return self._signed_get("/futures/data/globalLongShortAccountRatio", {
            "symbol": symbol, "period": period, "limit": limit
        })

    def taker_buy_sell(self, symbol, period, limit=50):
        """主动买卖成交量"""
        return self._signed_get("/futures/data/takerBuySellVol", {
            "symbol": symbol, "period": period, "limit": limit
        })


# ── 分析引擎 ──────────────────────────────────────────────

class OIAnalyzer:
    def __init__(self, client, symbol="BLESSUSDT"):
        self.client = client
        self.symbol = symbol
        self.data = {}

    def fetch_all(self):
        """拉取所有需要的数据"""
        print(f"[1/7] 当前 OI...")
        self.data["current_oi"] = self.client.current_oi(self.symbol)

        print(f"[2/7] OI 历史 (5m, 15m, 1h, 4h)...")
        self.data["oi_5m"] = self.client.oi_history(self.symbol, "5m", limit=500)
        self.data["oi_15m"] = self.client.oi_history(self.symbol, "15m", limit=500)
        self.data["oi_1h"] = self.client.oi_history(self.symbol, "1h", limit=200)
        self.data["oi_4h"] = self.client.oi_history(self.symbol, "4h", limit=200)

        print(f"[3/7] K 线数据 (1d, 4h, 1h)...")
        self.data["klines_1d"] = self.client.klines(self.symbol, "1d", limit=90)
        self.data["klines_4h"] = self.client.klines(self.symbol, "4h", limit=180)
        self.data["klines_1h"] = self.client.klines(self.symbol, "1h", limit=168)

        print(f"[4/7] 资金费率...")
        self.data["funding"] = self.client.funding_rate(self.symbol, limit=300)
        self.data["premium"] = self.client.premium_index(self.symbol)

        print(f"[5/7] 24h 行情...")
        self.data["ticker"] = self.client.ticker_24hr(self.symbol)

        print(f"[6/7] 多空比 (大户 + 全局)...")
        try:
            self.data["top_ls"] = self.client.top_ls_ratio(self.symbol, "15m", limit=200)
        except Exception:
            self.data["top_ls"] = None
        try:
            self.data["global_ls"] = self.client.global_ls_ratio(self.symbol, "15m", limit=200)
        except Exception:
            self.data["global_ls"] = None

        print(f"[7/7] 主动买卖比...")
        try:
            self.data["taker"] = self.client.taker_buy_sell(self.symbol, "15m", limit=200)
        except Exception:
            self.data["taker"] = None

        print("✓ 数据拉取完成\n")

    # ── 指标 1: Volume Ratio ──

    def analyze_volume_ratio(self):
        """计算每日 Volume Ratio = 当日成交量 / 7日均量"""
        klines = self.data["klines_1d"]
        ratios = []
        quote_vols = []

        for k in klines:
            quote_vols.append(float(k[7]))

        results = []
        for i, qv in enumerate(quote_vols):
            ts = datetime.fromtimestamp(klines[i][0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if i >= 7:
                avg = sum(quote_vols[i - 7 : i]) / 7
                ratio = qv / avg
            else:
                avg = 0
                ratio = 0
            results.append({
                "date": ts,
                "open": float(klines[i][1]),
                "high": float(klines[i][2]),
                "low": float(klines[i][3]),
                "close": float(klines[i][4]),
                "quote_vol": qv,
                "avg_7d": avg,
                "vol_ratio": ratio,
                "change_pct": (float(klines[i][4]) - float(klines[i][1])) / float(klines[i][1]) * 100,
            })

        # 最近几天的总结
        recent = results[-10:]
        min_ratio = min(r["vol_ratio"] for r in recent[-7:])
        latest = recent[-1]

        # 信号判断
        if latest["vol_ratio"] > 10:
            signal = "🔥 极端爆量"
            score = 9
        elif latest["vol_ratio"] > 3:
            signal = "🟢 显著放量"
            score = 7
        elif latest["vol_ratio"] > 1.5:
            signal = "📈 温和放量"
            score = 5
        elif latest["vol_ratio"] < 0.4:
            signal = "❄️ 极致地量 — 弹簧压缩"
            score = 9
        elif latest["vol_ratio"] < 0.7:
            signal = "📉 缩量"
            score = 6
        else:
            signal = "➖ 正常"
            score = 3

        return {
            "recent": recent,
            "latest_ratio": latest["vol_ratio"],
            "latest_change": latest["change_pct"],
            "min_ratio_7d": min_ratio,
            "signal": signal,
            "score": score,
            "weight": 0.30,
        }

    # ── 指标 2: OI 增长率 ──

    def analyze_oi(self):
        """多周期 OI 增长率分析"""
        oi_5m = self.data["oi_5m"]
        oi_1h = self.data["oi_1h"]
        oi_4h = self.data["oi_4h"]

        # 解析 OI 序列
        def parse_oi_series(data):
            return [(d["timestamp"], float(d["sumOpenInterest"]), float(d["sumOpenInterestValue"])) for d in data]

        oi_5m_vals = parse_oi_series(oi_5m)
        oi_1h_vals = parse_oi_series(oi_1h)
        oi_4h_vals = parse_oi_series(oi_4h)

        current_oi = float(self.data["current_oi"]["openInterest"])
        current_time = datetime.fromtimestamp(
            self.data["current_oi"]["time"] / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M")

        # 多周期增长率
        def find_ago(series, hours, idx_from=-1):
            target = series[idx_from][0] - hours * 3600 * 1000
            for i in range(len(series) - 1, -1, -1):
                if series[i][0] <= target:
                    return i
            return 0

        changes = {}
        oi_5m_last = oi_5m_vals[-1]

        for hours, label in [(1, "1h"), (4, "4h"), (12, "12h"), (24, "24h")]:
            idx = find_ago(oi_5m_vals, hours)
            if idx >= 0:
                past = oi_5m_vals[idx]
                qty_chg = (oi_5m_last[1] - past[1]) / past[1] * 100
                val_chg = (oi_5m_last[2] - past[2]) / past[2] * 100
                changes[label] = {
                    "from_time": datetime.fromtimestamp(past[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "from_oi": past[1],
                    "from_value": past[2],
                    "qty_change_pct": qty_chg,
                    "value_change_pct": val_chg,
                }

        # 7 天趋势 (从 1h 数据)
        weekly_trend = []
        for i in range(max(0, len(oi_1h_vals) - 168), len(oi_1h_vals), 24):
            if i < len(oi_1h_vals):
                ts = datetime.fromtimestamp(oi_1h_vals[i][0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                weekly_trend.append({
                    "date": ts,
                    "oi_qty": oi_1h_vals[i][1],
                    "oi_value": oi_1h_vals[i][2],
                })

        # 找到 7 天最低 OI
        oi_qty_week = [d["oi_qty"] for d in weekly_trend]
        min_oi_week = min(oi_qty_week) if oi_qty_week else 0
        oi_from_low = (current_oi - min_oi_week) / min_oi_week * 100 if min_oi_week > 0 else 0

        # 信号判断
        chg_24h = changes.get("24h", {}).get("qty_change_pct", 0)
        if chg_24h > 50:
            signal = "🔥 OI 暴增 — 大量新资金涌入"
            score = 9
        elif chg_24h > 20:
            signal = "🟢 OI 显著增长 — 趋势确认"
            score = 7
        elif chg_24h > 5:
            signal = "📈 OI 温和增长"
            score = 5
        elif chg_24h > -5:
            signal = "➖ OI 平稳"
            score = 4
        elif chg_24h > -15:
            signal = "📉 OI 下降 — 清洗弱手"
            score = 6
        else:
            signal = "❄️ OI 大幅下降"
            score = 3

        return {
            "current_oi": current_oi,
            "current_time": current_time,
            "changes": changes,
            "weekly_trend": weekly_trend,
            "oi_from_7d_low_pct": oi_from_low,
            "signal": signal,
            "score": score,
            "weight": 0.25,
        }

    # ── 指标 3: Spot/Futures 同步 ──

    def analyze_spot_futures_sync(self):
        """分析现货与期货的同步性"""
        # BLESS 无 Binance 现货，此指标不适用
        ticker = self.data["ticker"]
        futs_vol = float(ticker.get("quoteVolume", 0))

        # 尝试检查是否有 Binance 现货
        try:
            r = requests.get(
                "https://api.binance.com/api/v3/ticker/24hr",
                params={"symbol": self.symbol},
                timeout=5,
            )
            has_spot = r.status_code == 200
            if has_spot:
                spot_data = r.json()
                spot_vol = float(spot_data.get("quoteVolume", 0))
            else:
                spot_vol = 0
        except Exception:
            has_spot = False
            spot_vol = 0

        if not has_spot:
            return {
                "applicable": False,
                "reason": f"{self.symbol} 无 Binance 现货市场，纯期货驱动",
                "futures_24h_vol": futs_vol,
                "spot_24h_vol": 0,
                "futures_spot_ratio": None,
                "signal": "⚠️ 不适用 — 纯期货驱动代币",
                "score": 0,
                "weight": 0.20,
                "fallback_recommended": "使用 Taker Buy/Sell Ratio 替代",
            }

        ratio = futs_vol / spot_vol if spot_vol > 0 else float("inf")

        if 0.5 < ratio < 3:
            signal = "✅ 期现同步，量价健康"
            score = 8
        elif ratio < 5:
            signal = "🟡 期货略超前，尚可接受"
            score = 5
        else:
            signal = "🔴 期货过度投机，期现脱节"
            score = 2

        return {
            "applicable": True,
            "futures_24h_vol": futs_vol,
            "spot_24h_vol": spot_vol,
            "futures_spot_ratio": ratio,
            "signal": signal,
            "score": score,
            "weight": 0.20,
        }

    # ── 指标 4: Funding Rate ──

    def analyze_funding(self):
        """分析资金费率的健康度"""
        funding = self.data["funding"]
        rates = [float(f["fundingRate"]) for f in funding]

        if not rates:
            return {"error": "无资金费率数据"}

        current = rates[-1] * 100
        avg = sum(rates[-100:]) / min(len(rates), 100) * 100
        max_rate = max(rates[-100:]) * 100
        min_rate = min(rates[-100:]) * 100

        # 看拉升前费率是否在地板价
        # 找最近 50 个周期中最低的
        recent_rates = rates[-50:]
        floor_count = sum(1 for r in recent_rates if r * 100 < 0.01)
        floor_ratio = floor_count / len(recent_rates)

        # 信号判断
        if abs(current) < 0.01:
            signal = "✅ 费率地板价 — 多头未拥挤，拉升空间大"
            score = 9
        elif abs(current) < 0.05:
            signal = "🟢 费率温和 — 市场情绪健康"
            score = 7
        elif abs(current) < 0.10:
            signal = "🟡 费率偏高 — 注意多头拥挤"
            score = 4
        else:
            signal = "🔴 费率过高 — 多头过热，回调风险大"
            score = 1

        return {
            "current_rate_pct": current,
            "avg_rate_100p_pct": avg,
            "max_rate_100p_pct": max_rate,
            "min_rate_100p_pct": min_rate,
            "floor_ratio": floor_ratio,
            "annualized_pct": current * 3 * 365,
            "signal": signal,
            "score": score,
            "weight": 0.15,
        }

    # ── 指标 5: 链上大额（占位） ──

    def analyze_onchain(self):
        """链上数据 — 当前为占位实现，需接入 Arkham/Nansen/CoinLore"""
        return {
            "available": False,
            "note": "链上大额分析需接入外部数据源，当前为占位",
            "recommendation": "使用 scan_holders.py 或 Arkham API 补充",
            "signal": "⚪ 数据不可得",
            "score": 0,
            "weight": 0.10,
        }

    # ── 综合评分 ──

    def synthesize(self, vol_result, oi_result, spot_result, funding_result, onchain_result):
        """综合五指标打分"""

        dimensions = [
            ("Volume Ratio", vol_result),
            ("OI 增长率", oi_result),
            ("Spot/Futures", spot_result),
            ("Funding Rate", funding_result),
            ("链上大额", onchain_result),
        ]

        total_weight = 0
        weighted_score = 0

        lines = []
        lines.append("=" * 60)
        lines.append(f"  🔬 {self.symbol} 五指标框架分析")
        lines.append(f"  分析时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        lines.append("=" * 60)

        for name, result in dimensions:
            score = result.get("score", 0)
            weight = result.get("weight", 0)
            signal = result.get("signal", "N/A")

            # 对于不适用的指标（如无现货），跳过
            if not result.get("applicable", True) and name == "Spot/Futures":
                lines.append(f"\n  [{name}] → 跳过（不适用）")
                lines.append(f"    原因: {result.get('reason', 'N/A')}")
                continue

            weighted_score += score * weight
            total_weight += weight

            bar = "█" * score + "░" * (10 - score)
            lines.append(f"\n  [{name}] (权重 {weight*100:.0f}%)")
            lines.append(f"    得分: {score}/10 [{bar}]")
            lines.append(f"    信号: {signal}")

        # 归一化
        if total_weight > 0:
            normalized = weighted_score / total_weight
        else:
            normalized = 0

        lines.append(f"\n  {'─' * 50}")
        lines.append(f"  加权总分: {weighted_score:.2f} / {total_weight:.2f}")
        lines.append(f"  归一化:   {normalized:.1f} / 10")

        if normalized >= 7:
            verdict = "🟢 强烈关注 — 多指标共振，高概率交易机会"
        elif normalized >= 5:
            verdict = "🟡 值得监控 — 部分指标积极，等待更多确认"
        elif normalized >= 3:
            verdict = "🟠 谨慎 — 信号混杂，不建议重仓"
        else:
            verdict = "🔴 回避 — 多个指标恶化"

        lines.append(f"  判定:     {verdict}")
        lines.append("=" * 60)

        return "\n".join(lines), weighted_score, total_weight, normalized

    def run(self):
        """执行完整分析"""
        self.fetch_all()

        vol_result = self.analyze_volume_ratio()
        oi_result = self.analyze_oi()
        spot_result = self.analyze_spot_futures_sync()
        funding_result = self.analyze_funding()
        onchain_result = self.analyze_onchain()

        return vol_result, oi_result, spot_result, funding_result, onchain_result


# ── CLI ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Binance Futures OI 五指标分析")
    parser.add_argument("--symbol", default="BLESSUSDT", help="交易对 (默认 BLESSUSDT)")
    parser.add_argument("--json", action="store_true", help="JSON 格式输出")
    args = parser.parse_args()

    api_key, api_secret = load_api_keys()
    if not api_key or not api_secret:
        print("[ERROR] 缺少 API 密钥，请在 config/binance_futures_key.json 中配置")
        sys.exit(1)

    client = BinanceFuturesClient(api_key, api_secret)
    analyzer = OIAnalyzer(client, args.symbol)

    try:
        vol, oi, spot, funding, onchain = analyzer.run()
    except Exception as e:
        print(f"[ERROR] 分析失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    if args.json:
        output = {
            "symbol": args.symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "volume_ratio": vol,
            "oi_growth": oi,
            "spot_futures": spot,
            "funding_rate": funding,
            "onchain": onchain,
        }
        print(json.dumps(output, indent=2, default=str))
    else:
        report, weighted, total, normalized = analyzer.synthesize(
            vol, oi, spot, funding, onchain
        )
        print(report)

        # 额外：打印关键数据细节
        print(f"\n📊 数据快照:")
        print(f"  当前价格: ${analyzer.data['ticker']['lastPrice']}")
        print(f"  24h 涨跌: {float(analyzer.data['ticker']['priceChangePercent']):+.2f}%")
        print(f"  24h 期货量: ${float(analyzer.data['ticker']['quoteVolume']):,.0f}")
        print(f"  当前 OI: {oi['current_oi']:,.0f} 张")
        print(f"  24h OI 变化: {oi['changes'].get('24h', {}).get('qty_change_pct', 0):+.2f}%")
        print(f"  当前费率: {funding['current_rate_pct']:+.4f}% (年化 {funding['annualized_pct']:+.1f}%)")
        print(f"  最新 Volume Ratio: {vol['latest_ratio']:.2f}x")

        # 最近 10 天 Volume Ratio
        print(f"\n📈 近 10 日 Volume Ratio 趋势:")
        for r in vol["recent"]:
            bar = "█" * min(int(r["vol_ratio"] * 2), 40)
            flag = " ← 今日" if r == vol["recent"][-1] else ""
            print(f"  {r['date']}  {r['vol_ratio']:>6.2f}x  {bar}{flag}")


if __name__ == "__main__":
    main()
