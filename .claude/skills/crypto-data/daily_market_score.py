#!/usr/bin/env python3
"""
Daily Market Score Calculator — 市场加权评分计算器

基于 daily_market_data.py 输出的 JSON，计算 6 个数据驱动维度的加权评分，
输出 Market Score (0-100) 和 Regime 判定。

评分维度与权重 (guide §16):
  1. Price Trend    20%  — BTC/ETH/SOL weighted avg change
  2. OI             20%  — 三币种 OI Δ% 均值
  3. Funding        10%  — 资金费率趋势
  4. Volume         10%  — Volume Ratio
  5. Liquidation    10%  — 清算数据（初期占位）
  6. Fear & Greed   10%  — F&G 指数

用法:
  python3 daily_market_score.py /tmp/daily_market_data.json -o /tmp/daily_market_score.json
  python3 daily_market_score.py /tmp/daily_market_data.json --json  # stdout
"""

import argparse
import json
import sys
from datetime import datetime, timezone


# ═══════════════════════════════════════════════════════════════
# Dimension Scoring Functions
# Each returns (score 0-100, label, detail)
# ═══════════════════════════════════════════════════════════════

def score_price_trend(assets_data):
    """Score price trend across BTC/ETH/SOL. Weight 20%.
    Bullish price action → high score; bearish → low score."""
    weights = {"BTC": 0.4, "ETH": 0.25, "SOL": 0.15,
               "BNB": 0.05, "ADA": 0.05, "UNI": 0.03,
               "LIT": 0.02, "ASTER": 0.02, "SPX": 0.02, "HYPE": 0.01}
    total_weight = 0
    weighted_change = 0
    details = {}

    for asset, weight in weights.items():
        if asset not in assets_data:
            continue
        bf = assets_data[asset].get("binance_futures", {})
        chg = bf.get("change_24h_pct") or 0
        weighted_change += chg * weight
        total_weight += weight
        details[asset] = {"change_24h_pct": chg}

    if total_weight == 0:
        return 50, "N/A", details

    avg_change = weighted_change / total_weight

    # Scoring: neutral at 50, each 1% change = ±3 points, capped at 0-100
    score = 50 + avg_change * 3
    score = max(0, min(100, score))

    if avg_change > 3:
        label = "强看涨"
    elif avg_change > 1:
        label = "温和看涨"
    elif avg_change > -1:
        label = "横盘"
    elif avg_change > -3:
        label = "温和看跌"
    else:
        label = "强看跌"

    return round(score), label, {"weighted_avg_change_pct": round(avg_change, 2), "details": details}


def score_oi(assets_data):
    """Score OI trend. Weight 20%.
    Rising OI with rising price = bullish; falling OI = bearish/deleveraging."""
    weights = {"BTC": 0.4, "ETH": 0.25, "SOL": 0.15,
               "BNB": 0.05, "ADA": 0.05, "UNI": 0.03,
               "LIT": 0.02, "ASTER": 0.02, "SPX": 0.02, "HYPE": 0.01}
    total_weight = 0
    weighted_oi_delta = 0
    details = {}

    for asset, weight in weights.items():
        if asset not in assets_data:
            continue
        bf = assets_data[asset].get("binance_futures", {})
        oi_delta = bf.get("oi_delta_pct", {}).get("24h") or 0
        weighted_oi_delta += oi_delta * weight
        total_weight += weight
        details[asset] = {"oi_delta_24h_pct": oi_delta}

    if total_weight == 0:
        return 50, "N/A", details

    avg_oi_delta = weighted_oi_delta / total_weight

    # OI up strongly = bullish (60+); OI down = bearish (<50)
    score = 50 + avg_oi_delta * 1.5
    score = max(0, min(100, score))

    if avg_oi_delta > 10:
        label = "强劲增仓 — 资金大量流入"
    elif avg_oi_delta > 3:
        label = "温和增仓"
    elif avg_oi_delta > -3:
        label = "持仓稳定"
    elif avg_oi_delta > -10:
        label = "温和减仓"
    else:
        label = "大幅减仓 — 去杠杆中"

    return round(score), label, {"weighted_avg_oi_delta_pct": round(avg_oi_delta, 2), "details": details}


def score_funding(assets_data):
    """Score funding rate health. Weight 10%.
    Neutral funding (0-0.01%) = healthy; extreme (>0.05% or <0) = unhealthy."""
    weights = {"BTC": 0.4, "ETH": 0.25, "SOL": 0.15,
               "BNB": 0.05, "ADA": 0.05, "UNI": 0.03,
               "LIT": 0.02, "ASTER": 0.02, "SPX": 0.02, "HYPE": 0.01}
    total_weight = 0
    weighted_funding = 0
    details = {}

    for asset, weight in weights.items():
        if asset not in assets_data:
            continue
        bf = assets_data[asset].get("binance_futures", {})
        fund = abs(bf.get("funding_current_pct") or 0)
        weighted_funding += fund * weight
        total_weight += weight
        details[asset] = {"funding_current_pct": bf.get("funding_current_pct"),
                          "funding_floor_ratio": bf.get("funding_floor_ratio")}

    if total_weight == 0:
        return 50, "N/A", details

    avg_funding = weighted_funding / total_weight

    # Lower (closer to 0) = healthier
    if avg_funding < 0.005:
        score = 80  # very healthy, floor rate
        label = "费率地板 — 多头未拥挤"
    elif avg_funding < 0.01:
        score = 70
        label = "费率健康"
    elif avg_funding < 0.03:
        score = 55
        label = "费率温和"
    elif avg_funding < 0.05:
        score = 40
        label = "费率偏高 — 多头开始拥挤"
    elif avg_funding < 0.10:
        score = 25
        label = "费率过高 — 回调风险"
    else:
        score = 10
        label = "费率极端 — 极可能回调"

    return round(score), label, {"weighted_avg_funding_pct": round(avg_funding, 4), "details": details}


def score_volume(assets_data):
    """Score volume health. Weight 10%.
    Volume near average = normal; explosion = alert; extreme low = low interest."""
    weights = {"BTC": 0.4, "ETH": 0.25, "SOL": 0.15,
               "BNB": 0.05, "ADA": 0.05, "UNI": 0.03,
               "LIT": 0.02, "ASTER": 0.02, "SPX": 0.02, "HYPE": 0.01}
    total_weight = 0
    weighted_vol_ratio = 0
    vol_ratios = []
    details = {}

    for asset, weight in weights.items():
        if asset not in assets_data:
            continue
        bf = assets_data[asset].get("binance_futures", {})
        vr = bf.get("vol_ratio_vs_7d")
        if vr is not None:
            weighted_vol_ratio += vr * weight
            total_weight += weight
            vol_ratios.append(vr)
        details[asset] = {"vol_ratio_vs_7d": vr,
                          "volume_24h": bf.get("quote_volume_24h")}

    if total_weight == 0:
        return 50, "N/A", details

    avg_vr = weighted_vol_ratio / total_weight

    # Near 1.0 = normal (score 60-70)
    if avg_vr > 3:
        score, label = 80, "成交量爆炸 — 市场高度活跃"
    elif avg_vr > 1.5:
        score, label = 75, "显著放量"
    elif avg_vr > 0.8:
        score, label = 65, "成交量正常"
    elif avg_vr > 0.5:
        score, label = 50, "缩量"
    elif avg_vr > 0.3:
        score, label = 40, "明显缩量"
    else:
        score, label = 30, "极致地量 — 关注度极低"

    return round(score), label, {"weighted_avg_vol_ratio": round(avg_vr, 2), "details": details}


def score_liquidation(assets_data):
    """Score based on liquidation data. Weight 10%.
    Currently a placeholder — liquidation data is not directly available."""
    # Placeholder: Binance doesn't easily provide 24h liquidation totals via REST
    # This would need the WebSocket stream (already in barter-data)
    details = {}
    for asset in ["BTC", "ETH", "SOL"]:
        if asset in assets_data:
            details[asset] = {"note": "清算数据需通过 WebSocket 或 data.binance.vision 获取"}
    return 50, "数据不可得（占位）", details


def score_fear_greed(global_data):
    """Score Fear & Greed index. Weight 10%.
    Extreme fear = potential bottom (contrarian); extreme greed = potential top.
    Neutral (40-60) = healthiest."""
    fg = global_data.get("fear_greed", {})
    current = fg.get("current", {})
    value = current.get("value")

    if value is None:
        return 50, "N/A", {}

    # Contrarian scoring: extreme fear → buy opportunity, extreme greed → caution
    if value <= 20:
        score, label = 75, "极度恐惧 — 反向买入信号（历史底部区域）"
    elif value <= 35:
        score, label = 60, "恐惧 — 市场悲观"
    elif value <= 45:
        score, label = 55, "偏恐惧"
    elif value <= 55:
        score, label = 65, "中性 — 最健康区域"
    elif value <= 65:
        score, label = 55, "偏贪婪"
    elif value <= 80:
        score, label = 40, "贪婪 — 注意风险"
    else:
        score, label = 25, "极度贪婪 — 历史顶部区域"

    return round(score), label, {
        "value": value,
        "classification": current.get("classification"),
        "delta_24h": fg.get("delta_24h"),
        "delta_7d": fg.get("delta_7d"),
    }


# ═══════════════════════════════════════════════════════════════
# Main Scoring Logic
# ═══════════════════════════════════════════════════════════════

WEIGHTS = {
    "price_trend": 0.20,
    "oi": 0.20,
    "funding": 0.10,
    "volume": 0.10,
    "liquidation": 0.10,
    "fear_greed": 0.10,
    # Remaining 20% reserved for LLM-judged dimensions:
    # news_sentiment: 0.10, macro: 0.05, exchange_events: 0.05
}


def compute_market_score(data):
    """Compute weighted market score from daily market data JSON."""
    assets_data = data.get("assets", {})
    global_data = data.get("global", {})

    dimensions = {}

    # 1. Price Trend (20%)
    score, label, detail = score_price_trend(assets_data)
    dimensions["price_trend"] = {"score": score, "label": label, "weight": WEIGHTS["price_trend"], "detail": detail}

    # 2. OI (20%)
    score, label, detail = score_oi(assets_data)
    dimensions["oi"] = {"score": score, "label": label, "weight": WEIGHTS["oi"], "detail": detail}

    # 3. Funding (10%)
    score, label, detail = score_funding(assets_data)
    dimensions["funding"] = {"score": score, "label": label, "weight": WEIGHTS["funding"], "detail": detail}

    # 4. Volume (10%)
    score, label, detail = score_volume(assets_data)
    dimensions["volume"] = {"score": score, "label": label, "weight": WEIGHTS["volume"], "detail": detail}

    # 5. Liquidation (10%)
    score, label, detail = score_liquidation(assets_data)
    dimensions["liquidation"] = {"score": score, "label": label, "weight": WEIGHTS["liquidation"], "detail": detail}

    # 6. Fear & Greed (10%)
    score, label, detail = score_fear_greed(global_data)
    dimensions["fear_greed"] = {"score": score, "label": label, "weight": WEIGHTS["fear_greed"], "detail": detail}

    # Compute weighted score (only data-driven dimensions, 80% total)
    data_driven_weight = sum(WEIGHTS.values())  # 0.80
    weighted_score = sum(
        d["score"] * d["weight"] for d in dimensions.values()
    )
    # Normalize to 0-100 for the data-driven portion
    normalized_score = weighted_score / data_driven_weight if data_driven_weight > 0 else 50

    # Determine regime
    if normalized_score >= 70:
        regime = "RISK_ON"
        regime_desc = "市场情绪积极，资金流入，趋势向好"
    elif normalized_score >= 55:
        regime = "NEUTRAL_BULLISH"
        regime_desc = "中性偏多，整体健康但缺乏强驱动"
    elif normalized_score >= 45:
        regime = "NEUTRAL"
        regime_desc = "市场横盘整理，无明显方向"
    elif normalized_score >= 30:
        regime = "NEUTRAL_BEARISH"
        regime_desc = "中性偏空，资金谨慎"
    else:
        regime = "RISK_OFF"
        regime_desc = "风险规避，去杠杆中，市场承压"

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market_score": round(normalized_score),
        "data_driven_weight": data_driven_weight,
        "regime": regime,
        "regime_description": regime_desc,
        "dimensions": dimensions,
        "note": (
            f"Score based on {data_driven_weight*100:.0f}% data-driven weights. "
            "Remaining 20% (news 10%, macro 5%, exchange events 5%) "
            "to be filled by LLM analysis."
        ),
    }


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Daily Market Score Calculator — 市场加权评分计算器"
    )
    parser.add_argument("input", help="Input JSON file from daily_market_data.py")
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--json", action="store_true", help="Output to stdout")
    args = parser.parse_args()

    with open(args.input) as f:
        data = json.load(f)

    print(f"[Score] Computing market score from {args.input}...", file=sys.stderr)

    result = compute_market_score(data)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"✓ Written to {args.output}", file=sys.stderr)
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))

    # Summary
    print(f"\n── Market Score ──", file=sys.stderr)
    print(f"  Score:  {result['market_score']}/100", file=sys.stderr)
    print(f"  Regime: {result['regime']} — {result['regime_description']}", file=sys.stderr)
    print(f"  Data-driven weight: {result['data_driven_weight']*100:.0f}%", file=sys.stderr)
    print(file=sys.stderr)
    for dim_name, dim in result["dimensions"].items():
        bar = "█" * (dim["score"] // 10) + "░" * (10 - dim["score"] // 10)
        print(f"  {dim_name:<16} [{bar}] {dim['score']:3d}  ({dim['label']})", file=sys.stderr)


if __name__ == "__main__":
    main()
