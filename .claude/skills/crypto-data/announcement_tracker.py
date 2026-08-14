#!/usr/bin/env python3
"""
Binance & Hyperliquid Announcement Event Database

Maintains a persistent event database with Listing Impact Scores.
Diffs each run against stored state to identify new events.

用法:
  python3 announcement_tracker.py --fetch          # Fetch latest, diff, update DB
  python3 announcement_tracker.py --list-recent 7   # Show events from last N days
  python3 announcement_tracker.py --json           # Output new events as JSON
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

try:
    import requests
except ImportError:
    print("需要 requests 库", file=sys.stderr)
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════

BINANCE_CMS_API = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
BINANCE_ARTICLE_BASE = "https://www.binance.com/en/support/announcement/detail"
HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "..", "..", "..", "data", "daily", "announcements_db.json")
HL_REF_PATH = os.path.join(SCRIPT_DIR, "..", "..", "..", "data", "daily", "reference", "hl_perp_universe.json")

# ═══════════════════════════════════════════════════════════════
# Event Database
# ═══════════════════════════════════════════════════════════════

def load_db():
    if os.path.exists(DB_PATH):
        with open(DB_PATH) as f:
            return json.load(f)
    return {"events": [], "hl_known_perps": [], "listing_performance": [],
            "last_fetch": None, "version": 1}

def save_db(db):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)

def load_hl_reference():
    """Load cached Hyperliquid perp universe for diffing."""
    if os.path.exists(HL_REF_PATH):
        with open(HL_REF_PATH) as f:
            return json.load(f)
    return []

def save_hl_reference(perps):
    os.makedirs(os.path.dirname(HL_REF_PATH), exist_ok=True)
    with open(HL_REF_PATH, "w") as f:
        json.dump(perps, f, indent=2)

def event_exists(db, event):
    """Check if an event already exists in the database by unique key."""
    key = (event.get("exchange"), event.get("event_type"),
           tuple(sorted(event.get("symbols", []))), event.get("announcement_time"))
    for existing in db["events"]:
        ek = (existing.get("exchange"), existing.get("event_type"),
              tuple(sorted(existing.get("symbols", []))), existing.get("announcement_time"))
        if key == ek:
            return True
    return False

# ═══════════════════════════════════════════════════════════════
# Binance CMS Fetcher
# ═══════════════════════════════════════════════════════════════

CATALOG_MAP = {
    48: "New Listing",
    49: "News",
    161: "Delisting",
    51: "API",
    157: "Maintenance",
}

# Known patterns for extracting symbols from titles
import re

def extract_symbols_from_title(title):
    """Extract token symbols from announcement title.
    Only for listing/delisting type announcements — not general news."""
    # Exclude non-listing titles
    title_lower = title.lower()
    listing_keywords = ["list", "launch", "delist", "remove", "add"]
    if not any(kw in title_lower for kw in listing_keywords):
        return []

    symbols = []
    # Match patterns like: KOUSDT, RDDTUSDT, BTCUSDT
    usdt_pairs = re.findall(r'\b([A-Z0-9]{2,12}USDT)\b', title)
    symbols.extend(usdt_pairs)

    # Match trading pairs like QNT/BTC, RPL/USDC
    pairs = re.findall(r'\b([A-Z0-9]{2,8}/[A-Z0-9]{2,8})\b', title)
    symbols.extend(pairs)

    if symbols:
        return symbols

    # For list-style announcements: "Will Delist ACX, HFT, PIVX, PYR, VANRY, VIC"
    # Extract comma-separated uppercase tokens after keywords
    for kw in ["delist", "list", "launch"]:
        if kw in title_lower:
            # Find the part after the keyword
            idx = title_lower.find(kw)
            rest = title[idx + len(kw):]
            bare = re.findall(r'\b([A-Z]{2,8})\b', rest)
            stopwords = {'USD', 'USDT', 'USDC', 'BNB', 'BTC', 'ETH', 'BUSD',
                         'WILL', 'AND', 'THE', 'FOR', 'WITH', 'SPOT', 'FUTURES',
                         'PERPETUAL', 'MARGIN', 'TRADING', 'PAIR', 'PAIRS',
                         'CONTRACT', 'CONTRACTS', 'MULTIPLE', 'NOTICE',
                         'REMOVAL', 'UPDATE', 'UPDATES', 'FROM', 'THIS',
                         'THAT', 'HAVE', 'HAS', 'BEEN', 'NEW', 'ALL',
                         'COLLATERAL', 'RATIO', 'TIER', 'TIERS', 'UNDER'}
            bare = [s for s in bare if s not in stopwords and len(s) >= 2]
            return bare

    return []


def fetch_binance_announcements(hours=24):
    """Fetch Binance announcements from the CMS API."""
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - hours * 3600 * 1000

    params = {"type": 1, "pageNo": 1, "pageSize": 20}
    try:
        r = requests.get(BINANCE_CMS_API, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[ERROR] Binance CMS API: {e}", file=sys.stderr)
        return []

    catalogs = data.get("data", {}).get("catalogs", [])
    events = []

    for cat in catalogs:
        catalog_id = cat.get("catalogId")
        catalog_name = CATALOG_MAP.get(catalog_id, cat.get("catalogName", f"catalog_{catalog_id}"))

        for article in cat.get("articles", []):
            release_date = article.get("releaseDate", 0)
            if release_date < cutoff_ms:
                continue

            title = article.get("title", "")
            code = article.get("code", "")
            symbols = extract_symbols_from_title(title)

            # Classify event type
            event_type = classify_event(title, catalog_name, catalog_id)

            if event_type == "IGNORE":
                continue

            events.append({
                "exchange": "binance",
                "event_type": event_type,
                "symbols": symbols,
                "market": classify_market(title, event_type),
                "catalog_id": catalog_id,
                "catalog_name": catalog_name,
                "article_code": code,
                "title": title,
                "announcement_time": release_date,
                "announcement_time_utc": datetime.fromtimestamp(
                    release_date / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "url": f"{BINANCE_ARTICLE_BASE}/{code}",
                "_raw_symbols_extracted": symbols,
            })

    return events


def classify_event(title, catalog_name, catalog_id):
    """Classify a Binance announcement into event type."""
    title_lower = title.lower()

    if catalog_id == 161:
        return "DELISTING"

    if catalog_id == 157:
        return "MAINTENANCE"

    if catalog_id == 48:
        if "futures" in title_lower and ("launch" in title_lower or "will launch" in title_lower):
            return "FUTURES_LISTING"
        if "spot" in title_lower or "list" in title_lower:
            return "SPOT_LISTING"
        return "LISTING"

    # catalog 49: Latest News — categorize by title patterns
    if "delist" in title_lower or "remove" in title_lower:
        return "DELISTING"
    if "launch" in title_lower and ("futures" in title_lower or "perpetual" in title_lower):
        return "FUTURES_LISTING"
    if "launch" in title_lower or "list" in title_lower or "will add" in title_lower:
        return "LISTING"
    if "leverage" in title_lower or "margin tier" in title_lower or "collateral ratio" in title_lower:
        return "LEVERAGE_CHANGE"
    if "tick size" in title_lower:
        return "CONTRACT_SPEC_CHANGE"
    if "maintenance" in title_lower or "upgrade" in title_lower or "wallet" in title_lower:
        return "MAINTENANCE"
    if "dividend" in title_lower or "airdrop" in title_lower or "earn" in title_lower:
        return "PROMOTION"
    if "api" in title_lower:
        return "API_UPDATE"

    return "NEWS"


def classify_market(title, event_type):
    """Determine market: SPOT, FUTURES, MARGIN, or ALL."""
    title_lower = title.lower()
    if "futures" in title_lower or "perpetual" in title_lower:
        return "FUTURES"
    if "margin" in title_lower:
        return "MARGIN"
    if "spot" in title_lower:
        return "SPOT"
    if event_type in ("DELISTING",):
        if "futures" in title_lower:
            return "FUTURES"
        return "SPOT"
    return "ALL"


# ═══════════════════════════════════════════════════════════════
# Hyperliquid
# ═══════════════════════════════════════════════════════════════

def fetch_hyperliquid_perps():
    """Fetch current Hyperliquid perp universe and diff against reference."""
    try:
        r = requests.post(HYPERLIQUID_INFO,
                          json={"type": "meta"}, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[ERROR] Hyperliquid meta: {e}", file=sys.stderr)
        return []

    universe = data.get("universe", []) if isinstance(data, dict) else (
        data[0].get("universe", []) if isinstance(data, list) and len(data) > 0 else []
    )

    return [{"name": u.get("name"), "maxLeverage": u.get("maxLeverage"),
             "szDecimals": u.get("szDecimals"), "isDelisted": u.get("isDelisted", False)}
            for u in universe]


def diff_hl_perps(current, reference):
    """Find new, removed, and changed perps."""
    current_map = {p["name"]: p for p in current}
    ref_map = {p["name"]: p for p in reference}

    # Only diff if we have a prior reference (skip first run)
    if not reference:
        return []

    new_perps = [p for name, p in current_map.items()
                 if name not in ref_map and not p.get("isDelisted")]
    delisted_perps = [p for name, p in current_map.items()
                      if p.get("isDelisted") and (name not in ref_map or not ref_map[name].get("isDelisted"))]
    leverage_changed = [
        {"name": name, "old_leverage": ref_map[name].get("maxLeverage"),
         "new_leverage": p.get("maxLeverage")}
        for name, p in current_map.items()
        if name in ref_map and p.get("maxLeverage") != ref_map[name].get("maxLeverage")
    ]

    events = []
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for p in new_perps:
        events.append({
            "exchange": "hyperliquid",
            "event_type": "NEW_PERPETUAL",
            "symbols": [p["name"]],
            "market": "FUTURES",
            "max_leverage": p.get("maxLeverage"),
            "detected_at": now_ts,
            "importance": 7,
        })

    for p in delisted_perps:
        events.append({
            "exchange": "hyperliquid",
            "event_type": "DELISTING",
            "symbols": [p["name"]],
            "market": "FUTURES",
            "detected_at": now_ts,
            "importance": 6,
        })

    for lc in leverage_changed:
        events.append({
            "exchange": "hyperliquid",
            "event_type": "LEVERAGE_CHANGE",
            "symbols": [lc["name"]],
            "market": "FUTURES",
            "old_leverage": lc["old_leverage"],
            "new_leverage": lc["new_leverage"],
            "detected_at": now_ts,
            "importance": 4,
        })

    return events


# ═══════════════════════════════════════════════════════════════
# Listing Impact Score (guide §11)
# ═══════════════════════════════════════════════════════════════

def compute_impact_score(event, db):
    """
    Calculate Listing Impact Score (0-100) for an announcement event.

    Factors:
      1. Base Score (from event type + market)
      2. Novelty Multiplier (first listing on major exchange?)
      3. Historical Similars (how have similar listings performed?)
    """
    base_scores = {
        ("SPOT_LISTING", "SPOT"): 85,
        ("LISTING", "SPOT"): 80,
        ("FUTURES_LISTING", "FUTURES"): 60,
        ("DELISTING", "SPOT"): 75,
        ("DELISTING", "FUTURES"): 55,
        ("LEVERAGE_CHANGE", "FUTURES"): 35,
        ("CONTRACT_SPEC_CHANGE", "FUTURES"): 25,
        ("NEW_PERPETUAL", "FUTURES"): 55,
    }

    base = base_scores.get((event.get("event_type"), event.get("market", "ALL")), 40)

    # Novelty bonus: more symbols = broader impact
    symbols = event.get("symbols", [])
    n_symbols = len(symbols)
    if n_symbols >= 5:
        base += 10
    elif n_symbols >= 2:
        base += 5

    # Market bonus: SPOT listings have higher impact
    if event.get("market") == "SPOT" and event.get("event_type") in ("SPOT_LISTING", "LISTING"):
        base += 5
    elif event.get("market") == "MARGIN":
        base -= 5

    # Historical: check if we have past performance data for similar events
    past_listings = [e for e in db.get("listing_performance", [])
                     if e.get("event_type") == event.get("event_type")]
    if past_listings:
        avg_return = sum(p.get("first_day_return_pct", 0) for p in past_listings) / len(past_listings)
        if avg_return > 100:
            base += 10  # historically very profitable
        elif avg_return > 30:
            base += 5
        elif avg_return < -20:
            base -= 5  # historically poor

    return max(0, min(100, base))


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def cmd_fetch(args):
    """Fetch announcements, diff against DB, save new events."""
    db = load_db()
    print(f"[DB] Loaded {len(db['events'])} historical events", file=sys.stderr)

    # ── Binance ──
    print("[Binance] Fetching announcements...", file=sys.stderr)
    binance_events = fetch_binance_announcements(hours=args.hours)

    # ── Hyperliquid ──
    print("[Hyperliquid] Fetching perp universe...", file=sys.stderr)
    current_perps = fetch_hyperliquid_perps()
    reference_perps = load_hl_reference()
    hl_events = diff_hl_perps(current_perps, reference_perps)

    # Save updated HL reference
    save_hl_reference(current_perps)

    # ── Merge & Dedup ──
    all_events = binance_events + hl_events
    new_events = []
    for event in all_events:
        if not event_exists(db, event):
            event["importance"] = compute_impact_score(event, db)
            event["first_seen"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            db["events"].append(event)
            new_events.append(event)

    db["last_fetch"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Summary ──
    print(f"\n[Binance] {len(binance_events)} fetched, "
          f"{len([e for e in new_events if e['exchange']=='binance'])} new", file=sys.stderr)
    print(f"[Hyperliquid] {len(hl_events)} changes detected, "
          f"{len([e for e in new_events if e['exchange']=='hyperliquid'])} new", file=sys.stderr)
    print(f"[Total] {len(new_events)} new events", file=sys.stderr)

    for e in new_events:
        symbols_str = ", ".join(e.get("symbols", ["?"]))
        print(f"  [{e['exchange']}] {e['event_type']}: {symbols_str} "
              f"(impact={e['importance']})", file=sys.stderr)

    # ── Save ──
    save_db(db)
    print(f"\n✓ DB saved: {len(db['events'])} total events → {DB_PATH}", file=sys.stderr)

    # ── Output new events as JSON ──
    if args.json:
        print(json.dumps({"new_events": new_events, "total_events": len(db["events"]),
                          "last_fetch": db["last_fetch"]}, indent=2, ensure_ascii=False))

    return new_events


def cmd_list(args):
    """List recent events from the database."""
    db = load_db()
    events = db["events"]
    events.sort(key=lambda e: e.get("announcement_time", e.get("detected_at", "")), reverse=True)

    if args.exchange:
        events = [e for e in events if e.get("exchange") == args.exchange]
    if args.type:
        events = [e for e in events if e.get("event_type") == args.type]

    days = args.days
    cutoff = int((time.time() - days * 86400) * 1000)

    print(f"{'Time':<20} {'Exch':<5} {'Type':<22} {'Symbols':<35} {'Imp':>4}")
    print("-" * 95)
    for e in events:
        ts = e.get("announcement_time", 0) or 0
        if isinstance(ts, str):
            ts_str = ts[:19]
        else:
            if ts < cutoff:
                continue
            ts_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        symbols = ", ".join(e.get("symbols", ["?"])[:5])
        if len(e.get("symbols", [])) > 5:
            symbols += f" +{len(e['symbols'])-5} more"
        print(f"{ts_str:<20} {e.get('exchange','?'):<5} {e.get('event_type','?'):<22} "
              f"{symbols:<35} {e.get('importance','?'):>4}")


def main():
    parser = argparse.ArgumentParser(
        description="Binance & Hyperliquid Announcement Event Database"
    )
    sub = parser.add_subparsers(dest="command")

    p_fetch = sub.add_parser("fetch", help="Fetch latest announcements and update DB")
    p_fetch.add_argument("--hours", type=int, default=72,
                         help="Lookback window in hours (default: 72)")
    p_fetch.add_argument("--json", action="store_true", help="Output new events as JSON to stdout")

    p_list = sub.add_parser("list", help="List events from database")
    p_list.add_argument("--days", type=int, default=7, help="Days to look back")
    p_list.add_argument("--exchange", choices=["binance", "hyperliquid"])
    p_list.add_argument("--type", dest="type", help="Event type filter")

    args = parser.parse_args()

    if args.command == "fetch":
        cmd_fetch(args)
    elif args.command == "list":
        cmd_list(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
