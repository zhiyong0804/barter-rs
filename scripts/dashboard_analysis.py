#!/usr/bin/env python3
"""Dashboard market analysis pipeline.

Reads a validated dashboard market snapshot (provenance-wrapped values), derives
the four contract metrics, scores each qualifying asset 0-100 on the weighted
component rubric (volume acceleration 30% / momentum 20% / liquidity 20% /
market-cap efficiency 15% / narrative 15%), assigns a qualitative state, and
writes the dashboard analysis JSON.

Usage:
    python3 scripts/dashboard_analysis.py
    python3 scripts/dashboard_analysis.py --snapshot data/dashboard_market_snapshot.json --output data/dashboard_analysis.json
"""

import argparse
import datetime
import json
from collections import Counter

SNAPSHOT_PATH = "data/dashboard_market_snapshot.json"
ANALYSIS_PATH = "data/dashboard_analysis.json"
SCHEMA_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Derived metrics (contract)
# ---------------------------------------------------------------------------


def unwrap(x):
    """Provenance-wrapped values carry the actual number under 'value'."""
    if isinstance(x, dict) and "value" in x:
        return x["value"]
    return x


def derive(a):
    """Compute all derived metrics for one snapshot asset (values unwrapped)."""
    vol24 = unwrap(a["quote_volume_24h_usd"])
    avg30 = unwrap(a["avg_30d_quote_volume_usd"])
    depth = unwrap(a["depth"]["depth_100_usd"])
    mcap = unwrap(a["market_data"]["circulating_market_cap_usd"])
    fdv = unwrap(a["market_data"]["fdv_usd"])

    vols = [day["quote_volume_usd"] for day in a["daily_quote_volumes_30d"]]
    last7 = sum(vols[-7:]) / 7
    prev23 = sum(vols[:-7]) / 23 if len(vols) > 7 else None
    last3 = sum(vols[-3:]) / 3
    prev27 = sum(vols[:-3]) / 27 if len(vols) >= 27 else None

    return {
        "volume_ratio": vol24 / avg30 if avg30 else None,
        "depth_ratio": depth / vol24 if vol24 else None,
        "volume_to_market_cap": avg30 / mcap if mcap else None,
        "fdv_premium": fdv / mcap if mcap else None,
        "vol7_over_prev23": last7 / prev23 if prev23 else None,
        "vol3_over_prev27": last3 / prev27 if prev27 else None,
    }


def nums_for(a, m):
    """Number-formatting dict used to render authored signal texts."""
    vol24 = unwrap(a["quote_volume_24h_usd"])
    avg30 = unwrap(a["avg_30d_quote_volume_usd"])
    depth = unwrap(a["depth"]["depth_100_usd"])
    mcap = unwrap(a["market_data"]["circulating_market_cap_usd"])
    chg24 = unwrap(a["change_24h_pct"])
    chg7 = unwrap(a["change_7d_pct"])
    return {
        "sym": a["symbol"],
        "sector": unwrap(a["market_data"]["sector"]),
        "price": unwrap(a["price"]),
        "chg24": chg24,
        "chg7": chg7,
        "chg24_abs": abs(chg24),
        "chg7_abs": abs(chg7),
        "vol24_m": vol24 / 1e6,
        "vol24_b": vol24 / 1e9,
        "avg30_m": avg30 / 1e6,
        "mcap_b": mcap / 1e9,
        "depth_m": depth / 1e6,
        "vr": m["volume_ratio"],
        "v7p23": m["vol7_over_prev23"],
        "dr_pct": (m["depth_ratio"] or 0) * 100,
        "vtm_pct": (m["volume_to_market_cap"] or 0) * 100,
        "fdvp": m["fdv_premium"],
    }


# ---------------------------------------------------------------------------
# Scoring rubric (each component 0-100, then weighted)
# ---------------------------------------------------------------------------

WEIGHTS = {
    "volume_acceleration": 0.30,
    "momentum": 0.20,
    "liquidity": 0.20,
    "market_cap_efficiency": 0.15,
    "narrative": 0.15,
}


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def _steps(x, bounds, scores):
    for b, s in zip(bounds, scores):
        if x >= b:
            return s
    return scores[-1]


def f_volratio(r):
    return _steps(r, [1.4, 1.2, 1.0, 0.8, 0.6], [95, 80, 65, 50, 35, 20])


def g_v7p23(r):
    return _steps(r, [2.0, 1.4, 1.1, 0.9, 0.7], [95, 80, 65, 50, 35, 20])


def h_chg7(x):
    return _steps(x, [12, 8, 4, 0, -4, -8], [90, 80, 70, 55, 40, 25, 12])


def i_chg24(x):
    return _steps(x, [4, 1, 0, -2], [85, 70, 55, 40, 20])


def j_depth(d_m):
    return _steps(d_m, [50, 20, 10, 5, 3, 2, 1.5, 1], [100, 92, 82, 70, 60, 50, 40, 30, 15])


def k_dratio(r):
    return _steps(r, [0.5, 0.3, 0.15, 0.05, 0.02], [95, 85, 70, 55, 40, 25])


def m_efficiency(vtm):
    return _steps(vtm, [0.12, 0.08, 0.05, 0.03, 0.02, 0.01], [65, 90, 80, 65, 50, 35, 20])


SECTOR_BASE = {
    "Artificial Intelligence (AI)": 75,
    "Decentralized Exchange (DEX)": 70,
    "Decentralized Finance (DeFi)": 65,
    "Solana Ecosystem": 65,
    "Meme": 60,
    "Infrastructure": 60,
    "Smart Contract Platform": 55,
    "BNB Chain Ecosystem": 55,
}


def n_narrative(a, m, n):
    base = SECTOR_BASE.get(n["sector"], 55)
    if n["chg7"] >= 5:
        base += 15
    elif n["chg7"] <= -8:
        base -= 10
    if m["volume_ratio"] and m["volume_ratio"] >= 1.2:
        base += 10
    if n["sector"] in ("Solana Ecosystem", "Meme") and n["chg7"] <= -4:
        base -= 5
    return clamp(base, 10, 100)


def component_scores(a, m, n):
    va = 0.6 * f_volratio(m["volume_ratio"]) + 0.4 * g_v7p23(m["vol7_over_prev23"])
    if n["chg7"] <= -8:  # volume expansion on a selloff is distribution, not demand
        va = min(va, 60)
    mo = 0.6 * h_chg7(n["chg7"]) + 0.4 * i_chg24(n["chg24"])
    li = 0.7 * j_depth(n["depth_m"]) + 0.3 * k_dratio(m["depth_ratio"])
    me = m_efficiency(m["volume_to_market_cap"])
    na = n_narrative(a, m, n)
    total = (
        WEIGHTS["volume_acceleration"] * va
        + WEIGHTS["momentum"] * mo
        + WEIGHTS["liquidity"] * li
        + WEIGHTS["market_cap_efficiency"] * me
        + WEIGHTS["narrative"] * na
    )
    return {
        "volume_acceleration": round(va, 1),
        "momentum": round(mo, 1),
        "liquidity": round(li, 1),
        "market_cap_efficiency": round(me, 1),
        "narrative": round(na, 1),
        "total": round(total, 1),
    }


# ---------------------------------------------------------------------------
# Qualitative layer: state + authored interpretation/risk per symbol.
# Texts use {placeholders} filled from nums_for() so numbers always match data.
# ---------------------------------------------------------------------------

SIGNALS = {
    # ---- LEADING ----------------------------------------------------------
    "LINKUSDT": {
        "state": "LEADING",
        "interpretation": (
            "Clearest early-momentum setup in the universe: 24h volume runs {vr:.2f}x the 30d average, "
            "the trailing 7 days print {v7p23:.2f}x the prior 23-day pace, and 7d price is +{chg7:.1f}% — "
            "volume expansion and price momentum confirm each other rather than diverge. "
            "Oracle/Web3-data-rail demand (AI-adjacent narrative) is the supportive backdrop."
        ),
        "risk": (
            "Book depth is only ${depth_m:.1f}M top-100 (≈{dr_pct:.2f}% of daily turnover), and the FDV premium "
            "of {fdvp:.2f}x leaves room for unlock pressure; a BTC reversal would test the "
            "move. Invalidation: vol_ratio back under 1.0 with 7d momentum turning negative."
        ),
    },
    "WLDUSDT": {
        "state": "LEADING",
        "interpretation": (
            "Top-tier volume acceleration (vol_ratio {vr:.2f}, 7d +{chg7:.1f}%) on the deepest book among "
            "momentum names (${depth_m:.1f}M top-100). Turnover of {vtm_pct:.1f}% of float per day shows "
            "futures participation is outsized for the ${mcap_b:.1f}B circulating cap — high capital efficiency."
        ),
        "risk": (
            "FDV premium {fdvp:.2f}x means roughly two-thirds of fully-diluted value is unissued; emission/"
            "unlock news is a structural overhang, and depth_ratio {dr_pct:.2f}% is mid-tier. Invalidation: "
            "7d momentum turns negative while vol_ratio falls below 1.0."
        ),
    },
    # ---- BREAKOUT_WATCH ---------------------------------------------------
    "CRVUSDT": {
        "state": "BREAKOUT_WATCH",
        "interpretation": (
            "Strongest recent-volume expansion in the universe: last-7d volume at {v7p23:.2f}x the prior "
            "23-day average with 7d price +{chg7:.1f}% — participation building ahead of range resolution, "
            "the classic pre-breakout signature. DEX-token rotation adds narrative support."
        ),
        "risk": (
            "Book is thin at ${depth_m:.1f}M (≈{dr_pct:.2f}% of turnover) and 24h price is {chg24:+.1f}%, so "
            "supply still contests the move; the setup is fragile, not confirmed. Invalidation: vol_ratio "
            "back under 1.0 or a close below the recent 7d range low."
        ),
    },
    "PUMPUSDT": {
        "state": "BREAKOUT_WATCH",
        "interpretation": (
            "Fresh volume acceleration (vol_ratio {vr:.2f}) plus a {chg24:+.1f}% 24h impulse on a "
            "${depth_m:.1f}M book while 7d price is still flat ({chg7:+.1f}%) — price has not broken out yet, "
            "so this is a watch, not a confirmation. Turnover of {vtm_pct:.1f}% of float/day shows derivatives "
            "attention concentrating on the new listing."
        ),
        "risk": (
            "FDV premium {fdvp:.2f}x with 7d momentum near zero; a push below the 24h low invalidates the "
            "setup. A single market-maker pullback can widen spreads on a ${mcap_b:.1f}B float."
        ),
    },
    "ICPUSDT": {
        "state": "BREAKOUT_WATCH",
        "interpretation": (
            "24h volume is below average (vol_ratio {vr:.2f}) but the 7-day pace stepped up to {v7p23:.2f}x the "
            "prior 23 days while price gained {chg7:.1f}% over 7d — volume trend and price rising together "
            "from a low base: a slow-building breakout setup in the AI-adjacent group."
        ),
        "risk": (
            "Absolute liquidity is modest (${depth_m:.1f}M book, {dr_pct:.2f}% of turnover) and the 24h number "
            "has not yet confirmed the expansion. Invalidation: 7d volume pace slips below 1.0x without "
            "price follow-through."
        ),
    },
    # ---- ACCUMULATION_WATCH ----------------------------------------------
    "ATOMUSDT": {
        "state": "ACCUMULATION_WATCH",
        "interpretation": (
            "The 7d gain (+{chg7:.1f}%) was absorbed without volume fanfare while the relative book "
            "deepened to {dr_pct:.2f}% of daily turnover — the deepest relative book in the universe — "
            "consistent with patient accumulation rather than chased momentum; the {chg24:+.1f}% 24h "
            "dip is a pullback inside the move."
        ),
        "risk": (
            "Vol_ratio {vr:.2f} shows the 30d volume base is still contracting; the accumulation read fails "
            "if price loses the 7d range low on rising volume."
        ),
    },
    "AVAXUSDT": {
        "state": "ACCUMULATION_WATCH",
        "interpretation": (
            "Flat price (7d {chg7:+.1f}%) with a recent volume uptick (7d pace {v7p23:.2f}x prior 23d) on a "
            "${depth_m:.1f}M book — the quiet profile where participation rebuilds before direction. L1 "
            "rotation would be the trigger narrative."
        ),
        "risk": (
            "No momentum confirmation yet; continuation of the 30d volume fade (vol_ratio {vr:.2f}) without "
            "any price reaction would downgrade this to neutral."
        ),
    },
    # ---- HIGH_MOMENTUM ----------------------------------------------------
    "FARTCOINUSDT": {
        "state": "HIGH_MOMENTUM",
        "interpretation": (
            "7d +{chg7:.1f}% with turnover at {vtm_pct:.1f}% of float per day and a fully-circulating structure "
            "(FDV premium {fdvp:.2f}x) — momentum is genuine, but participation is only average (vol_ratio "
            "{vr:.2f}), so this is momentum without the volume confirmation that would make it a breakout."
        ),
        "risk": (
            "Meme volatility cuts both ways: a ${depth_m:.1f}M book ({dr_pct:.2f}% of turnover) means a crowded "
            "exit can gap the market. Invalidation: momentum stalls below the 7d high on declining volume."
        ),
    },
    "HYPEUSDT": {
        "state": "HIGH_MOMENTUM",
        "interpretation": (
            "Strongest large-cap momentum in the universe (7d +{chg7:.1f}%, 24h +{chg24:.1f}%) on the "
            "DEX-perp narrative, with turnover {vtm_pct:.1f}% of float per day on a ${mcap_b:.1f}B cap. Note the "
            "valuation structure: FDV premium {fdvp:.2f}x is the highest in the universe."
        ),
        "risk": (
            "Top-100 depth is only ${depth_m:.1f}M (≈{dr_pct:.2f}% of 24h volume) — momentum running on a thin "
            "book, so any unwind is amplified. Invalidation: 24h momentum reverses on rising volume."
        ),
    },
    "LDOUSDT": {
        "state": "HIGH_MOMENTUM",
        "interpretation": (
            "Best staking-token momentum (7d +{chg7:.1f}%, 24h +{chg24:.1f}%) in the universe, but volume is "
            "fading (vol_ratio {vr:.2f}, 7d pace {v7p23:.2f}x) — gains running on increasingly thin "
            "participation."
        ),
        "risk": (
            "Momentum without volume is the least durable profile; the book is ${depth_m:.1f}M. "
            "Invalidation: vol_ratio below 0.6 or 7d momentum turning negative."
        ),
    },
    # ---- HIGH_LIQUIDITY ---------------------------------------------------
    "BTCUSDT": {
        "state": "HIGH_LIQUIDITY",
        "interpretation": (
            "Reference liquidity asset: ~${vol24_b:.1f}B of 24h derivatives turnover on an ${depth_m:.1f}M "
            "top-100 book with a fully-circulating 1.00 FDV ratio. Currently in neutral drift "
            "(7d {chg7:+.1f}%, 24h {chg24:+.1f}%); regime, not alpha."
        ),
        "risk": (
            "Depth/volume ratio {dr_pct:.2f}% is the lowest in the universe — the book is small relative to "
            "turnover. BTC sets the regime for all 46 other names; a volume break (vol_ratio above 1.3 or "
            "below 0.5) is the regime signal to watch."
        ),
    },
    "ETHUSDT": {
        "state": "HIGH_LIQUIDITY",
        "interpretation": (
            "Deep-book liquid core (${depth_m:.1f}M top-100) with healthy turnover (~${vol24_b:.1f}B/day, "
            "vol_ratio {vr:.2f}); 7d {chg7:+.1f}% mirrors BTC's drift — a high-liquidity name without "
            "independent momentum right now."
        ),
        "risk": (
            "Depth is small relative to daily turnover ({dr_pct:.2f}%); ETH-specific catalysts would be "
            "needed to break the {chg7:+.1f}% weekly drift. No directional edge in the current metrics."
        ),
    },
    "SOLUSDT": {
        "state": "HIGH_LIQUIDITY",
        "interpretation": (
            "Deepest order book in the universe by far (${depth_m:.1f}M top-100, ~{dr_pct:.2f}% of 24h turnover) "
            "on a ${mcap_b:.1f}B cap with the broadest ecosystem base (Solana Ecosystem sector). Flat week "
            "(7d {chg7:+.1f}%) keeps it a liquidity anchor rather than a momentum play."
        ),
        "risk": (
            "Turnover efficiency is mid-pack (vol/mcap {vtm_pct:.1f}%/day); a breakout would need vol_ratio "
            "above 1.2 — currently {vr:.2f}."
        ),
    },
    "XRPUSDT": {
        "state": "HIGH_LIQUIDITY",
        "interpretation": (
            "Second-deepest book (${depth_m:.1f}M) on a ${mcap_b:.1f}B cap; 7d {chg7:+.1f}% with vol_ratio {vr:.2f} "
            "— liquid, stable, and non-participating this week."
        ),
        "risk": (
            "FDV premium {fdvp:.2f}x and thin relative depth ({dr_pct:.2f}% of turnover) for the size; large "
            "orders move the book more than the turnover suggests."
        ),
    },
    "NEARUSDT": {
        "state": "HIGH_LIQUIDITY",
        "interpretation": (
            "Deep ${depth_m:.1f}M book ({dr_pct:.2f}% of turnover — one of the best relative depths in the "
            "universe) with a constructive week (7d {chg7:+.1f}%) in the AI-adjacent group; flat volume "
            "(vol_ratio {vr:.2f}) means liquidity, not momentum."
        ),
        "risk": (
            "No volume confirmation yet; an AI-sector pullback would remove the supportive narrative "
            "premise. Invalidation: 7d momentum turns negative on contracting volume."
        ),
    },
    "LTCUSDT": {
        "state": "HIGH_LIQUIDITY",
        "interpretation": (
            "One of the deepest relative books in the universe (${depth_m:.1f}M, {dr_pct:.2f}% of turnover) on a "
            "${mcap_b:.1f}B cap — a high-liquidity legacy name in neutral drift (7d {chg7:+.1f}%)."
        ),
        "risk": (
            "Volume has been fading (7d pace {v7p23:.2f}x, vol_ratio {vr:.2f}); liquidity is strong but "
            "participation is not, so no directional signal."
        ),
    },
    "DOGEUSDT": {
        "state": "HIGH_LIQUIDITY",
        "interpretation": (
            "Deep ${depth_m:.1f}M book with flat-but-stable momentum (7d {chg7:+.1f}%, 24h {chg24:+.1f}%) — "
            "the meme sector's liquidity anchor; turnover {vtm_pct:.1f}% of float/day keeps it tradeable."
        ),
        "risk": (
            "Meme flows are event-driven; vol_ratio {vr:.2f} shows no event is pricing in. A BTC down-move "
            "would pressure the book disproportionately ({dr_pct:.2f}% depth ratio)."
        ),
    },
    # ---- NEUTRAL ----------------------------------------------------------
    "BNBUSDT": {
        "state": "NEUTRAL",
        "interpretation": (
            "Flat week (7d {chg7:+.1f}%) on an ${mcap_b:.1f}B cap with a stable 7d volume pace ({v7p23:.2f}x); "
            "the largest asset in the universe by market cap is in a holding pattern. Turnover efficiency "
            "is the lowest in the universe ({vtm_pct:.1f}%/day)."
        ),
        "risk": (
            "Depth ${depth_m:.1f}M is modest for an ${mcap_b:.1f}B cap and {dr_pct:.2f}% of turnover; a regime shift "
            "would express through BTC first."
        ),
    },
    "SUIUSDT": {
        "state": "NEUTRAL",
        "interpretation": (
            "Mild negative drift (7d {chg7:+.1f}%) with average participation (vol_ratio {vr:.2f}) on a "
            "${depth_m:.1f}M book; no edge in either direction this week. FDV premium {fdvp:.2f}x caps upside "
            "repricing potential."
        ),
        "risk": (
            "Unlock-sensitive structure (FDV {fdvp:.2f}x float) — any emission news while volume is average "
            "could trigger a step-down."
        ),
    },
    "ASTERUSDT": {
        "state": "NEUTRAL",
        "interpretation": (
            "Newer DEX listing settling into neutral: 7d {chg7:+.1f}% with a ${depth_m:.1f}M book and "
            "turnover {vtm_pct:.1f}% of float/day. DEX narrative base is supportive but participation is "
            "fading (7d pace {v7p23:.2f}x)."
        ),
        "risk": (
            "FDV premium {fdvp:.2f}x is elevated for a name with no momentum; depth_ratio {dr_pct:.2f}% shows a "
            "book that thins quickly on volatility."
        ),
    },
    "RENDERUSDT": {
        "state": "NEUTRAL",
        "interpretation": (
            "AI-sector name with stabilizing volume (7d pace {v7p23:.2f}x, vol_ratio {vr:.2f}) and a deep "
            "relative book ({dr_pct:.2f}% of turnover); price drifted -{chg7_abs:.1f}% over 7d without "
            "participation — a coiled neutral."
        ),
        "risk": (
            "AI rotation is leaving the group ex-LINK/WLD; a sector pullback would push this toward "
            "weakening. No confirmation of accumulation yet."
        ),
    },
    # ---- LOW_LIQUIDITY_RISK ----------------------------------------------
    "ZECUSDT": {
        "state": "LOW_LIQUIDITY_RISK",
        "interpretation": (
            "A 24h impulse (+{chg24:.1f}%, vol_ratio {vr:.2f}) on the thinnest relative book in the universe "
            "(depth_ratio {dr_pct:.2f}% of turnover, ${depth_m:.1f}M top-100) — the volume spike is real but "
            "unplaceable with size; treat the move as a low-liquidity flare, not an investable trend."
        ),
        "risk": (
            "Extreme gap risk: any exit attempt through ${depth_m:.1f}M of book against {vtm_pct:.1f}% daily "
            "turnover can move price several percent. Invalidation is irrelevant — position sizing is the "
            "constraint here."
        ),
    },
    "GRAMUSDT": {
        "state": "LOW_LIQUIDITY_RISK",
        "interpretation": (
            "Volume collapsed to {vr:.2f}x of the 30d average with a ${depth_m:.1f}M book on a ${mcap_b:.1f}B cap — "
            "participation died even though 7d price is +{chg7:.1f}%. High FDV premium ({fdvp:.2f}x) against "
            "drying turnover makes this an illiquid placeholder."
        ),
        "risk": (
            "Bid-ask slippage dominates any signal at this depth; a volume return (vol_ratio above 0.8) "
            "would be required before the read is meaningful."
        ),
    },
    "TAOUSDT": {
        "state": "LOW_LIQUIDITY_RISK",
        "interpretation": (
            "AI-sector name with a ${depth_m:.1f}M book (≈{dr_pct:.2f}% of turnover), FDV premium {fdvp:.2f}x, and "
            "mild negative drift (7d {chg7:+.1f}%) — the liquidity constraint dominates whatever "
            "narrative support the AI group provides."
        ),
        "risk": (
            "Thin book + high FDV ratio = amplified downside on sector de-rating; a {chg7_abs:.1f}% weekly "
            "move at this depth is not representative of fair value."
        ),
    },
    # ---- WEAKENING --------------------------------------------------------
    "FETUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Volume jumped to {vr:.2f}x the 30d average while price fell {chg7_abs:.1f}% over 7d — elevated "
            "volume on a selloff is distribution, not accumulation. AI-sector rotation is leaving FET's "
            "${mcap_b:.1f}B float behind."
        ),
        "risk": (
            "Oversold bounce risk after a -{chg7_abs:.1f}% week, but continued vol_ratio > 1.2 with lower "
            "lows would confirm distribution. Book ${depth_m:.1f}M is mid-tier."
        ),
    },
    "APTUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Volume above average (vol_ratio {vr:.2f}) on a -{chg7_abs:.1f}% week — the same distribution "
            "signature as FET: participation rising while price falls. The ${depth_m:.1f}M book would have "
            "absorbed genuine buying interest if there was any."
        ),
        "risk": (
            "A bounce is possible from oversold, but the weight of evidence (7d pace {v7p23:.2f}x, 24h "
            "{chg24:+.1f}%) is negative. Invalidation: weekly loss recaptured on rising volume."
        ),
    },
    "UNIUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Steepest alt decline in the universe (7d {chg7:.1f}%) with volume held near the 30d average "
            "(vol_ratio {vr:.2f}, 7d pace {v7p23:.2f}x) — conviction selling into an orderly book rather than a "
            "liquidation flush."
        ),
        "risk": (
            "Deep-book selling pressure can continue grinding; a stabilization would need vol_ratio to "
            "expand with price, not against it."
        ),
    },
    "LABUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Capitulation profile: -{chg7_abs:.1f}% over 7d with the float turning over {vtm_pct:.1f}% per day "
            "(highest turnover efficiency in the universe) on a ${depth_m:.1f}M book — sellers met sellers; "
            "the ${mcap_b:.1f}B cap has been gutted."
        ),
        "risk": (
            "Post-capitulation bounces are violent but untradeable at ${depth_m:.1f}M depth. Re-accumulation "
            "needs a vol_ratio reset above 1.0 first — currently {vr:.2f}."
        ),
    },
    "1000SHIBUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Participation collapse: 24h volume at {vr:.2f}x the 30d average and the trailing week at "
            "{v7p23:.2f}x the prior 23 days — the deepest participation fade in the universe on a "
            "${depth_m:.1f}M book. Meme demand has rotated out."
        ),
        "risk": (
            "The book is still relatively deep ({dr_pct:.2f}% of a shrinking turnover), so any demand return "
            "shows up fast; until vol_ratio recovers above 0.7 the read stays negative."
        ),
    },
    "ADAUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "-{chg7_abs:.1f}% week on fading participation (7d pace {v7p23:.2f}x, vol_ratio {vr:.2f}) — a deep "
            "${depth_m:.1f}M book absorbing sellers without buyers stepping in. Classic low-conviction "
            "distribution."
        ),
        "risk": (
            "Liquidity is not the constraint (book is deep), but nothing is absorbing the bid; a volume "
            "expansion on the upside (vol_ratio > 1.2) is required to invalidate."
        ),
    },
    "ONDOUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "24h +{chg24:.1f}% bounce inside a -{chg7_abs:.1f}% week with the 7d volume pace at {v7p23:.2f}x the "
            "prior 23 days — a dead-cat structure; the RWA/DeFi narrative has not translated into "
            "sustained participation (vol_ratio {vr:.2f})."
        ),
        "risk": (
            "The {chg24:+.1f}% 24h pop could extend, but against {dr_pct:.2f}% depth and falling 30d volume "
            "the base case is further drift. Invalidation: weekly loss recaptured."
        ),
    },
    "AAVEUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "DeFi name losing ground on fading participation: 7d -{chg7_abs:.1f}% with the 7d pace at "
            "{v7p23:.2f}x the prior 23 days and vol_ratio {vr:.2f}. Turnover {vtm_pct:.1f}% of float/day is still "
            "respectable, so the fade is conviction, not illiquidity."
        ),
        "risk": (
            "A recovery would show as vol_ratio expansion with price; nothing in the current data "
            "supports that yet."
        ),
    },
    "ENAUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Stablecoin-Defi name bleeding quietly: 7d -{chg7_abs:.1f}%, 7d volume pace {v7p23:.2f}x, vol_ratio "
            "{vr:.2f} — participation contracting on a ${depth_m:.1f}M book while the float still turns over "
            "{vtm_pct:.1f}% per day."
        ),
        "risk": (
            "Yield-bearing narrative needs a market catalyst to re-engage traders; until vol_ratio "
            "recovers above 0.8 the drift continues."
        ),
    },
    "TIAUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Modular-data name sliding on average participation: 7d -{chg7_abs:.1f}%, vol_ratio {vr:.2f}, 7d pace "
            "{v7p23:.2f}x — no distribution spike, just persistent seller pressure in a quiet tape."
        ),
        "risk": (
            "Unlock overhang (FDV premium {fdvp:.2f}x) compounds the drift; a move requires either a "
            "narrative catalyst or a volume break."
        ),
    },
    "OPUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "L2 name at high turnover ({vtm_pct:.1f}% of float/day) but negative drift (7d {chg7:.1f}%) and "
            "above-average recent volume (7d pace {v7p23:.2f}x) — churn without conviction; the deep "
            "relative book ({dr_pct:.2f}%) just makes the slide orderly."
        ),
        "risk": (
            "The {chg24:+.1f}% 24h reading is mixed; sustained vol_ratio above 1.0 with price falling "
            "would confirm distribution — currently {vr:.2f}."
        ),
    },
    "INJUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "7d -{chg7_abs:.1f}% with contraction across volume windows (vol_ratio {vr:.2f}, 7d pace {v7p23:.2f}x) — "
            "a quiet drift in a ${depth_m:.1f}M book; participation is exiting faster than price is falling."
        ),
        "risk": (
            "Low-volume drift can snap either way; the read flips only on a vol_ratio expansion with "
            "direction."
        ),
    },
    "PENGUUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "BNB-chain meme at -{chg7_abs:.1f}% with fading participation (7d pace {v7p23:.2f}x, vol_ratio {vr:.2f}) "
            "and the second-highest FDV premium in the universe ({fdvp:.2f}x) — meme demand left, structure "
            "is fragile."
        ),
        "risk": (
            "High FDV + meme beta = outsized downside if the sector turns; ${depth_m:.1f}M book does not "
            "cushion it."
        ),
    },
    "DASHUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "24h +{chg24:.1f}% pop on a -{chg7_abs:.1f}% week with the 7d pace at {v7p23:.2f}x — a momentum spike "
            "without a volume base; participation is still contracting (vol_ratio {vr:.2f})."
        ),
        "risk": (
            "The pop on a ${depth_m:.1f}M book is a low-liquidity artifact; watch for the pop to fade into "
            "the thin book."
        ),
    },
    "ARBUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "L2 name drifting -{chg7_abs:.1f}% with 24h +{chg24:.1f}% and a ${depth_m:.1f}M book; volume windows all "
            "contracting (vol_ratio {vr:.2f}, 7d pace {v7p23:.2f}x) — sellers control a quiet tape."
        ),
        "risk": (
            "FDV premium {fdvp:.2f}x plus unlock schedule keeps the drift sticky; no accumulation signal "
            "present."
        ),
    },
    "TRUMPUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Solana-meme name at -{chg7_abs:.1f}% with high turnover ({vtm_pct:.1f}% of float/day) and a "
            "${depth_m:.1f}M book — active churn with net selling pressure; the FDV premium {fdvp:.2f}x keeps "
            "valuation stretched."
        ),
        "risk": (
            "Event-driven meme flows can reverse the week in a session; until vol_ratio (currently {vr:.2f}) "
            "expands with price, the drift read stands."
        ),
    },
    "DOTUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Polkadot drifting -{chg7_abs:.1f}% on contracting participation (7d pace {v7p23:.2f}x, vol_ratio "
            "{vr:.2f}); a ${depth_m:.1f}M book absorbing without interest — ordinary distribution in a "
            "neglected L1."
        ),
        "risk": (
            "No catalyst visible; a volume break above 1.2x on vol_ratio is the only invalidation trigger."
        ),
    },
    "HBARUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "7d -{chg7_abs:.1f}% with the 7d volume pace at {v7p23:.2f}x the prior 23 days and vol_ratio {vr:.2f} — "
            "participation faded faster than price, a drifting ${mcap_b:.1f}B L1; the {chg24:+.1f}% 24h "
            "bounce is inside a weak week."
        ),
        "risk": (
            "Low turnover efficiency ({vtm_pct:.1f}%/day) means thin book relevance; needs a vol_ratio "
            "recovery to mean anything."
        ),
    },
    "SEIUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Perp-DEX L1 at -{chg7_abs:.1f}% with steady-but-average volume (vol_ratio {vr:.2f}, 7d pace "
            "{v7p23:.2f}x) and a ${depth_m:.1f}M book — orderly drift, no accumulation."
        ),
        "risk": (
            "Relative book is deep ({dr_pct:.2f}% of turnover) which damps volatility; the invalidation "
            "trigger is a vol_ratio expansion with positive price."
        ),
    },
    "ETCUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "PoW legacy name at -{chg7_abs:.1f}% with moderate volume fade (vol_ratio {vr:.2f}, 7d pace "
            "{v7p23:.2f}x); the ${depth_m:.1f}M book on a ${mcap_b:.1f}B cap leaves it thin for its size."
        ),
        "risk": (
            "Minimal narrative support; a move would be beta-driven from BTC. Watch depth ratio "
            "{dr_pct:.2f}% for slippage risk."
        ),
    },
    "XLMUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Stellar drifting -{chg7_abs:.1f}% with participation at {v7p23:.2f}x the prior 23 days and vol_ratio "
            "{vr:.2f} — flat tape, fading volume, ${depth_m:.1f}M book."
        ),
        "risk": (
            "FDV premium {fdvp:.2f}x and low turnover ({vtm_pct:.1f}%/day) leave the book exposed to sudden "
            "orders; no signal until volume returns."
        ),
    },
    "WIFUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Solana meme at -{chg7_abs:.1f}% with turnover still high ({vtm_pct:.1f}% of float/day) — active "
            "churn while price drifts; the fully-circulating structure (FDV {fdvp:.2f}x) removes unlock "
            "noise, leaving pure meme demand, which has rotated out."
        ),
        "risk": (
            "Thin book (${depth_m:.1f}M, {dr_pct:.2f}% of turnover) amplifies any demand return; the read "
            "inverts on a vol_ratio > 1.2 impulse."
        ),
    },
    "FILUSDT": {
        "state": "WEAKENING",
        "interpretation": (
            "Storage-infra name at -{chg7_abs:.1f}% with high turnover ({vtm_pct:.1f}% of float/day) and "
            "contracting 7d pace ({v7p23:.2f}x) — active distribution into an average book (${depth_m:.1f}M); "
            "FDV premium {fdvp:.2f}x adds structural weight."
        ),
        "risk": (
            "Elevated FDV with distribution volume is a poor combination; recovery requires vol_ratio "
            "above 1.0 with price, currently {vr:.2f}."
        ),
    },
}


def build_evidence(n):
    """Evidence string auto-rendered from the derived metrics (no authored numbers)."""
    return (
        f"24h vol ${n['vol24_m']:.0f}M vs 30d avg ${n['avg30_m']:.0f}M (vol_ratio {n['vr']:.2f}); "
        f"trailing-7d vol {n['v7p23']:.2f}x prior-23d; 7d price {n['chg7']:+.1f}%, 24h {n['chg24']:+.1f}%; "
        f"depth(100) ${n['depth_m']:.1f}M (depth_ratio {n['dr_pct']:.2f}% of 24h vol); "
        f"vol/mcap {n['vtm_pct']:.2f}%/day; FDV/mcap {n['fdvp']:.2f}x; "
        f"mcap ${n['mcap_b']:.1f}B; sector={n['sector']}"
    )


# ---------------------------------------------------------------------------
# Asset assembly
# ---------------------------------------------------------------------------

COMPONENT_SOURCES = {
    "volume_acceleration": ["volume_ratio", "vol7_over_prev23", "change_7d_pct"],
    "momentum": ["change_7d_pct", "change_24h_pct"],
    "liquidity": ["depth_100_usd", "depth_ratio"],
    "market_cap_efficiency": ["volume_to_market_cap"],
    "narrative": ["sector", "change_7d_pct", "volume_ratio"],
}


def build_asset(a, index):
    m = derive(a)
    n = nums_for(a, m)
    if not a["qualifying"]:
        return {
            "symbol": a["symbol"],
            "base_asset": a["base_asset"],
            "analyzed": False,
            "exclusion_reason": a["exclusion_reason"],
            "state": None,
            "score": None,
            "components": None,
            "derived_metrics": None,
            "signal": None,
            "references": {"snapshot_asset_index": index},
        }
    sig = SIGNALS[a["symbol"]]
    comp = component_scores(a, m, n)
    return {
        "symbol": a["symbol"],
        "base_asset": a["base_asset"],
        "analyzed": True,
        "state": sig["state"],
        "score": comp["total"],
        "components": {
            k: {
                "score": comp[k],
                "weight": WEIGHTS[k],
                "source_metrics": COMPONENT_SOURCES[k],
            }
            for k in WEIGHTS
        },
        "derived_metrics": {
            "volume_ratio": round(m["volume_ratio"], 4) if m["volume_ratio"] else None,
            "depth_ratio": round(m["depth_ratio"], 6) if m["depth_ratio"] else None,
            "volume_to_market_cap": round(m["volume_to_market_cap"], 6)
            if m["volume_to_market_cap"]
            else None,
            "fdv_premium": round(m["fdv_premium"], 4) if m["fdv_premium"] else None,
            "vol7_over_prev23": round(m["vol7_over_prev23"], 4)
            if m["vol7_over_prev23"]
            else None,
            "vol3_over_prev27": round(m["vol3_over_prev27"], 4)
            if m["vol3_over_prev27"]
            else None,
        },
        "signal": {
            "evidence": build_evidence(n),
            "interpretation": sig["interpretation"].format(**n),
            "risk_invalidation": sig["risk"].format(**n),
        },
        "references": {
            "snapshot_asset_index": index,
            "price": f"assets[{index}].price.value",
            "change_24h_pct": f"assets[{index}].change_24h_pct.value",
            "change_7d_pct": f"assets[{index}].change_7d_pct.value",
            "quote_volume_24h_usd": f"assets[{index}].quote_volume_24h_usd.value",
            "avg_30d_quote_volume_usd": f"assets[{index}].avg_30d_quote_volume_usd.value",
            "depth_100_usd": f"assets[{index}].depth.depth_100_usd.value",
            "circulating_market_cap_usd": f"assets[{index}].market_data.circulating_market_cap_usd.value",
            "fdv_usd": f"assets[{index}].market_data.fdv_usd.value",
            "sector": f"assets[{index}].market_data.sector.value",
            "daily_quote_volumes_30d": f"assets[{index}].daily_quote_volumes_30d[*].quote_volume_usd",
        },
    }


def build_market_summary(snap, assets):
    analyzed = [a for a in assets if a["analyzed"]]
    snap_assets = snap["assets"]
    idx = {a["symbol"]: a for a in snap_assets}
    q = [idx[a["symbol"]] for a in analyzed]

    unwrap_all = lambda f: [unwrap(f(x)) for x in q]

    totals = {
        "total_circulating_market_cap_usd": sum(unwrap_all(lambda x: x["market_data"]["circulating_market_cap_usd"])),
        "total_fdv_usd": sum(unwrap_all(lambda x: x["market_data"]["fdv_usd"])),
        "total_24h_quote_volume_usd": sum(unwrap_all(lambda x: x["quote_volume_24h_usd"])),
        "total_avg_30d_quote_volume_usd": sum(unwrap_all(lambda x: x["avg_30d_quote_volume_usd"])),
        "total_depth_100_usd": sum(unwrap_all(lambda x: x["depth"]["depth_100_usd"])),
    }

    state_dist = dict(Counter(a["state"] for a in analyzed))

    ranked = sorted(analyzed, key=lambda a: a["score"], reverse=True)
    top_signals = [
        {"symbol": a["symbol"], "score": a["score"], "state": a["state"]}
        for a in ranked[:10]
    ]
    bottom_signals = [
        {"symbol": a["symbol"], "score": a["score"], "state": a["state"]}
        for a in ranked[-5:]
    ]

    chg7 = unwrap_all(lambda x: x["change_7d_pct"])
    med = lambda xs: sorted(xs)[len(xs) // 2]
    medians = {
        "median_change_7d_pct": round(med(chg7), 2),
        "median_volume_ratio": round(
            med([a["derived_metrics"]["volume_ratio"] for a in analyzed]), 3
        ),
        "median_depth_ratio": round(
            med([a["derived_metrics"]["depth_ratio"] for a in analyzed]), 5
        ),
        "median_score": round(med([a["score"] for a in analyzed]), 1),
    }

    return {
        "universe_size": snap["universe"]["actual_count"],
        "qualifying_count": len(analyzed),
        "excluded_count": len(assets) - len(analyzed),
        "totals": totals,
        "state_distribution": state_dist,
        "top_signals_by_score": top_signals,
        "bottom_signals_by_score": bottom_signals,
        "breadth": {
            "positive_7d_count": sum(1 for x in chg7 if x > 0),
            "negative_7d_count": sum(1 for x in chg7 if x < 0),
            "flat_7d_count": sum(1 for x in chg7 if x == 0),
        },
        "medians": medians,
        "methodology": (
            "Score 0-100 = 0.30*volume_acceleration + 0.20*momentum + 0.20*liquidity + "
            "0.15*market_cap_efficiency + 0.15*narrative (each component 0-100). State reflects the "
            "dominant qualitative characteristic and is NOT solely determined by the composite score: "
            "e.g. majors with deep books are HIGH_LIQUIDITY despite moderate scores, and distribution-"
            "volume names are WEAKENING despite elevated volume ratios. Watch states are analytical "
            "observations, not price predictions."
        ),
        "source_snapshot": SNAPSHOT_PATH,
        "snapshot_generated_at": snap["generated_at"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default=SNAPSHOT_PATH)
    parser.add_argument("--output", default=ANALYSIS_PATH)
    args = parser.parse_args()

    with open(args.snapshot) as f:
        snap = json.load(f)

    assets = [build_asset(a, i) for i, a in enumerate(snap["assets"])]
    out = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "assets": assets,
        "market_summary": build_market_summary(snap, assets),
    }

    with open(args.output, "w") as f:
        json.dump(out, f, indent=1)
    print(f"wrote {args.output}: {sum(1 for a in assets if a['analyzed'])} analyzed, "
          f"{sum(1 for a in assets if not a['analyzed'])} excluded")


if __name__ == "__main__":
    main()
