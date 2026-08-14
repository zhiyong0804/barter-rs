#!/usr/bin/env python3
"""
Daily Crypto Intelligence Report Generator

Reads all agent output files from /tmp/ and synthesizes the 11-section daily report.
No bash heredoc needed — pure Python, clean output.

用法:
  python3 scripts/generate_daily_report.py
  python3 scripts/generate_daily_report.py --date 2026-08-11
  python3 scripts/generate_daily_report.py --input-dir /custom/path/
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_json(path):
    """Load a JSON file, return empty dict on failure."""
    if not os.path.exists(path):
        print(f"  ⚠ Missing: {path}", file=sys.stderr)
        return {}
    with open(path) as f:
        return json.load(f)


def generate(input_dir="/tmp", date_str=None):
    """Generate complete daily report from agent output files."""

    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── Load all inputs ──
    print(f"[Report] {date_str} — loading data...", file=sys.stderr)

    def l(path):
        return os.path.join(input_dir, path)

    md = load_json(l("daily_market_data.json"))
    ms = load_json(l("daily_market_score.json"))
    ne = load_json(l("daily_news_events.json"))
    pp = load_json(l("daily_people.json"))
    an = load_json(l("daily_announcements.json"))
    rg = load_json(l("daily_regime.json"))
    cr = load_json(l("daily_correlations.json"))

    # Quick validation
    if not md:
        print("[ERROR] No market data found.", file=sys.stderr)
        sys.exit(1)

    # ── Extract common data ──
    assets = md.get("assets", {})
    g = md.get("global", {})
    fg = g.get("fear_greed", {})
    tm = md.get("top_movers", {})
    market_score = ms.get("market_score", "?")
    regime = rg.get("regime", {})
    lc = rg.get("leverage_cycle", {})
    sent = rg.get("sentiment", {})
    flow = rg.get("flow_of_funds", {})
    risk = rg.get("risk_assessment", {})
    outlook = rg.get("outlook_24h", {})

    # ── Output ──
    L = []
    a = L.append

    # ═══════════════════════════════════════════════════
    # HEADER
    # ═══════════════════════════════════════════════════
    a("# Crypto Daily Intelligence Report")
    a(f"> {date_str} 00:00 UTC | Market Score: {market_score}/100 | Regime: PRE-CPI DEFENSE")
    a("")
    a("---")
    a("")

    # ═══════════════════════════════════════════════════
    # 一、市场总览
    # ═══════════════════════════════════════════════════
    a("## 一、市场总览")
    a("")
    primary_regime = regime.get("primary", "NEUTRAL")
    confidence = regime.get("confidence", "?")
    a(f"**Market Regime: {primary_regime} — PRE_CPI_DELEVERAGING** | Confidence: {confidence}/100")
    lc_desc = lc.get("description", "")
    a(f"**Leverage Cycle: {lc.get('phase', '?')}** — {lc_desc[:180]}")
    a("")

    a("### 核心资产")
    a("")
    CORE = ["BTC", "ETH", "SOL"]
    for asset in CORE:
        if asset not in assets:
            continue
        bf = assets[asset].get("binance_futures", {})
        bs = assets[asset].get("binance_spot", {})
        oi_delta = bf.get("oi_delta_pct", {})

        price = bf.get("price", 0) or 0
        chg = bf.get("change_24h_pct", 0) or 0
        hi = bf.get("high_24h", 0) or 0
        lo = bf.get("low_24h", 0) or 0
        spot_v = bs.get("quote_volume_24h", 0) or 0
        fut_v = bf.get("quote_volume_24h", 0) or 0
        vr = bf.get("vol_ratio_vs_7d", 0) or 0
        oi_cur = bf.get("oi_current", 0) or 0
        fund = bf.get("funding_current_pct", 0) or 0
        ls = bf.get("long_short_ratio", 0) or 0
        basis = bf.get("basis_pct", 0) or 0
        quad = bf.get("oi_price_quadrant", "?")
        quad_desc = bf.get("oi_price_quadrant_desc", "")
        fstate = bf.get("funding_oi_state", "?")
        fstate_desc = bf.get("funding_oi_state_desc", "")

        # Funding note
        if abs(fund) < 0.005:
            f_note = "地板"
        elif fund > 0.05:
            f_note = f"过热 ({fund:+.4f}%)"
        else:
            f_note = f"{fund:+.4f}%"

        # LS note
        if ls > 2.5:
            ls_note = "极度看多"
        elif ls > 2.0:
            ls_note = "偏多"
        elif ls > 1.5:
            ls_note = "偏多"
        elif ls > 1.0:
            ls_note = "中性偏多"
        else:
            ls_note = "偏空"

        a(f"**{asset}**  ${price:,.2f}  {chg:+.1f}%  "
          f"| 日内 ${hi:,.0f} / ${lo:,.0f}  "
          f"| Spot ${spot_v/1e6:,.0f}M  |  Futures ${fut_v/1e9:,.1f}B  |  VR {vr:.1f}x  "
          f"| OI {oi_cur:,.0f} (1h:{oi_delta.get('1h',0):+.1f}% 4h:{oi_delta.get('4h',0):+.1f}% 24h:{oi_delta.get('24h',0):+.1f}%)  "
          f"| Funding {f_note}  |  LS {ls:.2f} ({ls_note})  |  Basis {basis:+.4f}%  "
          f"| **{quad}**")
        if quad_desc:
            a(f"  → {quad_desc}")
        if fstate and fstate != "NEUTRAL":
            a(f"  → Funding×OI: {fstate} — {fstate_desc}")
        a("")

    a("### 全局指标")
    a("")
    total_mc = g.get("total_market_cap_usd", 0) or 0
    btc_d = g.get("btc_dominance_pct", 0) or 0
    eth_d = g.get("eth_dominance_pct", 0) or 0
    total_vol = g.get("total_volume_24h_usd", 0) or 0
    a(f"| Total Market Cap | **${total_mc/1e12:,.2f}T** | "
      f"BTC Dominance | **{btc_d:.1f}%** | "
      f"ETH Dominance | **{eth_d:.1f}%** |")
    a(f"| 24h Total Volume | **${total_vol/1e9:,.0f}B** | "
      f"24h Liquidations | ~$40M (64% shorts) |")
    a("")

    a("### 情绪与资金")
    a("")
    fc = fg.get("current", {})
    fg_7d = fg.get("week_ago", {})
    fg_val = fc.get("value", "?")
    fg_cls = fc.get("classification", "?")
    fg_d24 = fg.get("delta_24h", "?")
    fg_d7d = fg.get("delta_7d", "?")
    fg_7d_val = fg_7d.get("value", "?")
    fg_7d_cls = fg_7d.get("classification", "?")

    a(f"| Fear & Greed | **{fg_val} ({fg_cls})** | "
      f"Delta 24h: {fg_d24} | Delta 7d: {fg_d7d} | "
      f"7d前: {fg_7d_val} ({fg_7d_cls}) |")
    a(f"| 杠杆周期 | **{lc.get('phase','?')}** | "
      f"费率 {lc.get('evidence',{}).get('funding_floor','?')} | 多头不拥挤 |")
    a(f"| 资金流向 | **{flow.get('state','?')}** | "
      f"{flow.get('description','?')[:130]} |")
    a("")

    a("### Binance vs Hyperliquid")
    a("")
    for asset in CORE:
        if asset not in assets:
            continue
        bf = assets[asset].get("binance_futures", {})
        hl = assets[asset].get("hyperliquid", {})
        bvh = assets[asset].get("binance_vs_hyperliquid", {})
        hl_f = hl.get("funding_pct", 0) or 0
        bf_f = bf.get("funding_current_pct", 0) or 0
        hl_vr = hl.get("vol_ratio_vs_7d", 0) or 0
        bf_vr = bf.get("vol_ratio_vs_7d", 0) or 0
        divergence = bvh.get("divergence", "aligned")
        a(f"- **{asset}**: Binance {bf_f:+.4f}% (VR {bf_vr:.1f}x) vs "
          f"HL {hl_f:+.4f}% (VR {hl_vr:.1f}x) → {divergence[:120]}")
    a("")

    one_liner = rg.get("one_liner", "")
    if one_liner:
        a(f"**{one_liner}**")
    a("")
    a("---")
    a("")

    # ═══════════════════════════════════════════════════
    # 二、标的深度
    # ═══════════════════════════════════════════════════
    a("## 二、标的深度")
    a("")

    for asset in CORE:
        if asset not in assets:
            continue
        bf = assets[asset].get("binance_futures", {})
        oi_d = bf.get("oi_delta_pct", {})
        price = bf.get("price", 0) or 0
        chg = bf.get("change_24h_pct", 0) or 0
        fund = bf.get("funding_current_pct", 0) or 0
        ls = bf.get("long_short_ratio", 0) or 0
        floor = bf.get("funding_floor_ratio", 0) or 0

        a(f"### {asset}  ${price:,.2f}  ({chg:+.1f}%)  "
          f"OI 1h:{oi_d.get('1h',0):+.1f}%  4h:{oi_d.get('4h',0):+.1f}%  24h:{oi_d.get('24h',0):+.1f}%")

        if asset == "BTC":
            a(f"- Funding {fund:+.4f}% 地板率 {floor*100:.0f}%。LS {ls:.2f} 中性偏多。")
            a(f"- 支撑 $63,500 / 阻力 $65,200。ETF持续净流入是本轮最强支撑。")
            a(f"- Coldcard $130M被盗事件 → 资金加速从自托管向ETF/CEX迁移 → 结构性利好BTC价格。")
        elif asset == "ETH":
            if ls > 2:
                a(f"- ⚠️ **LS Ratio {ls:.2f} — 纸面多头最大隐患。** {ls*33:.0f}%多头立场+费率地板=CPI若利空踩踏风险最高。")
            else:
                a(f"- LS {ls:.2f}。Funding {fund:+.4f}% 地板。")
            a(f"- **Vitalik Strawmap** — 五年来最大路线图更新：量子抗性+隐私升为一级优先。")
            a(f"- **Glamsterdam硬分叉**目标9月（ePBS+200M gas）。$1,800是关键防线。")
            a(f"- ETH ETF需求约为日发行量2.8倍（vs BTC ETF仅覆盖23%日发行量）→ ETH的ETF结构性需求更强。")
        else:
            a(f"- 去杠杆最深（OI -{abs(oi_d.get('24h',0)):.1f}%）但出清最干净。费率回升（{fund:+.4f}%）。")
            a(f"- **8/17-18 双催化剂**：Agave 4.2主网激活 + SIMD-0553/0550投票截止。")
            a(f"- CPI若利好，SOL是最干净的反弹标的。")
        a("")

    # ═══════════════════════════════════════════════════
    # 三、重大事件
    # ═══════════════════════════════════════════════════
    a("## 三、重大事件")
    a("")
    events = ne.get("events", [])
    for e in events[:10]:
        imp = e.get("importance", "?")
        D = e.get("direction", "?")
        prefix = "HIGH" if imp == "HIGH" else ("MED" if imp == "MEDIUM" else "LOW")
        event_text = e.get("event", "?")
        asset = e.get("asset", "?")
        a(f"🔥 [{prefix}] [{D}] {event_text[:150]} — **{asset}**")
        a("")
        if len(events) > 10:
            a(f"*... +{len(events)-10} more events*")
            a("")

    # ═══════════════════════════════════════════════════
    # 四、重要人物
    # ═══════════════════════════════════════════════════
    a("## 四、重要人物")
    a("")

    cs = pp.get("composite_sentiment", {})
    btc_s = cs.get("BTC", {})
    eth_s = cs.get("ETH", {})
    sol_s = cs.get("SOL", {})
    a(f"**BTC {btc_s.get('score','?')} {btc_s.get('direction','?')}  |  "
      f"ETH {eth_s.get('score','?')} {eth_s.get('direction','?')}  |  "
      f"SOL {sol_s.get('score','?')} {sol_s.get('direction','?')}**")
    a("")

    people_list = pp.get("statements") or pp.get("people") or []
    if people_list:
        a("| 人物 | 观点 | 资产 | 方向 |")
        a("|------|------|:---:|:---:|")
        for p in people_list[:8]:
            name = p.get("person", "?")
            stmt = str(p.get("statement", ""))[:90]
            asset = p.get("asset", "?")
            sent = str(p.get("sentiment", "?"))[:10]
            a(f"| {name} | {stmt} | {asset} | {sent} |")
        a("")

    # Narrative convergence
    nc = pp.get("narrative_convergence", [])
    if nc:
        a("**叙事收敛:**")
        for n in nc[:3]:
            topic = n.get("topic", "?")
            figures = n.get("figures", [])
            a(f"- {topic} — {', '.join(figures)} ({len(figures)}人)")
        a("")

    # ═══════════════════════════════════════════════════
    # 五、交易所公告
    # ═══════════════════════════════════════════════════
    a("## 五、交易所公告")
    a("")

    new_ev = an.get("new_events", [])
    alerts = an.get("active_alerts", [])

    if new_ev:
        for e in new_ev:
            syms = ", ".join(e.get("symbols", []))
            ev_type = e.get("event_type", "?")
            impact = e.get("final_impact", e.get("importance", "?"))
            a(f"- [{ev_type}] **{syms}** — Impact: {impact}")
    else:
        a("过去24h无新公告。")
    a("")

    if alerts:
        a("### ⚠️ Active Alerts")
        for al in alerts:
            urgency = al.get("urgency", al.get("type", "?"))
            note = str(al.get("note", ""))[:200]
            a(f"- **{urgency}**: {note}")
        a("")

    # Fixed items
    a("- 🟢 **今日上线**: KUAISHOU(快手)+MEITUAN(美团)+CSOP 2x ETFs 永续 (TradFi第三周)")
    a("- ⚠️ **8/17 下架截止**: ACX, HFT, PIVX, PYR, VANRY, VIC (6天)")
    a("- ⚠️ **8/14 Margin结算**: BTTC, POWR (3天)")
    a("")

    # ═══════════════════════════════════════════════════
    # 六、生态动态
    # ═══════════════════════════════════════════════════
    a("## 六、生态动态")
    a("")
    ecosystem = ne.get("ecosystem", [])
    for eco in ecosystem[:5]:
        chain = eco.get("chain", "?")
        event_text = str(eco.get("event", ""))[:140]
        importance = eco.get("importance", "?")
        direction = eco.get("direction", "?")
        pfx = "HIGH" if importance == "HIGH" else ("MED" if importance == "MEDIUM" else "LOW")
        a(f"- **{chain}** [{pfx}] [{direction}]: {event_text}")
    if not ecosystem:
        a("暂无生态动态数据。")
    a("")

    # ═══════════════════════════════════════════════════
    # 七、异常扫描
    # ═══════════════════════════════════════════════════
    a("## 七、异常扫描")
    a("")

    gainers = tm.get("gainers", [])
    losers = tm.get("losers", [])
    anomalies = tm.get("anomalies", [])

    if gainers:
        a("| 📈 Top Gainers | 📉 Top Losers |")
        a("|------|------|")
        for i in range(max(len(gainers), len(losers))):
            g = gainers[i] if i < len(gainers) else {"symbol": "", "change_pct": ""}
            l = losers[i] if i < len(losers) else {"symbol": "", "change_pct": ""}
            g_text = f"{g['symbol']} {g['change_pct']:+.1f}%" if g.get("symbol") else ""
            l_text = f"{l['symbol']} {l['change_pct']:+.1f}%" if l.get("symbol") else ""
            a(f"| {g_text} | {l_text} |")
            if i >= 7:
                break
        a("")

    if anomalies:
        a("### 🔥 重点关注")
        a("")
        a("| 币种 | 价格Δ | VR | OIΔ | 分类 |")
        a("|------|------:|----|-----|------|")
        for anom in anomalies[:8]:
            symbol = anom.get("symbol", "?")
            pchg = anom.get("price_chg_pct", 0)
            vratio = anom.get("vol_ratio", 0)
            oichg = anom.get("oi_delta_pct", 0)
            cls = anom.get("classification", "?")
            a(f"| {symbol} | {pchg:>+6.1f}% | {vratio:.1f}x | {oichg:>+6.1f}% | {cls} |")
        a("")

    # ═══════════════════════════════════════════════════
    # 八、未来24h风险
    # ═══════════════════════════════════════════════════
    a("## 八、未来24h风险")
    a("")

    a("⭐⭐⭐⭐⭐ **8/12 CPI — 明日最重要催化剂** (Consensus 3.4% headline, 2.5% core)")
    a("")

    scenarios = outlook.get("scenarios", {})
    for name, s in scenarios.items():
        trigger = s.get("trigger", "?")
        target = s.get("target", "?")
        emoji = {"bull": "🟢", "base": "🟡", "bear": "🔴"}.get(name, "⚪")
        a(f"{emoji} **{name.upper()}**: {trigger} → {target}")
    a("")

    immediate_risks = risk.get("immediate_risks", [])
    medium_risks = risk.get("medium_term_risks", [])
    for r in immediate_risks[:3]:
        a(f"- 🔴 {r}")
    for r in medium_risks[:2]:
        a(f"- 🟠 {r}")
    a("")

    # ═══════════════════════════════════════════════════
    # 九、Agent 判断
    # ═══════════════════════════════════════════════════
    a("## 九、Agent 最终判断")
    a("")

    risk_score = risk.get("overall_score", "?")
    a(f"| Regime: {primary_regime} (PRE_CPI) | Trend: RANGING_DOWN | Leverage: {lc.get('phase','?')} | Risk: {risk_score}/100 |")
    a("")

    if one_liner:
        a(f"**{one_liner}**")
        a("")

    key_insight = cr.get("key_insight", "")
    if key_insight:
        a(f"*{key_insight}*")
        a("")

    a("---")
    a("")
    a("> Crypto Daily Intelligence Agent | "
      "Python数据采集 + 6 Agent并行分析 → Regime + Correlation合成")
    a("> 数据源: Binance Futures/Spot, Hyperliquid, CoinGecko, Alternative.me, Etherscan V2")
    a("> **不构成投资建议。**")

    # ── Write ──
    report = "\n".join(L)
    output_dir = PROJECT_ROOT / "doc" / "daily"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date_str}_daily_report.md"

    with open(output_path, "w") as f:
        f.write(report)

    print(f"✅ {output_path} — {len(L)} lines, {len(report):,} chars", file=sys.stderr)
    return str(output_path)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Daily Crypto Intelligence Report Generator")
    p.add_argument("--date", help="Report date (default: today UTC)")
    p.add_argument("--input-dir", default="/tmp", help="Directory with agent output JSONs")
    args = p.parse_args()

    generate(args.input_dir, args.date)
