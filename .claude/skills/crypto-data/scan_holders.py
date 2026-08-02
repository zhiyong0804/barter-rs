#!/usr/bin/env python3
"""
Fetch top token holder data from CoinLore rich lists (free, no API key).

CoinLore provides rich lists for established tokens with addresses, balances,
percentages, and aggregate concentration stats. Covers 1000+ tokens across
multiple chains (Ethereum, BSC, etc.).

Usage:
    # By token name/symbol (most reliable)
    python3 scan_holders.py --token dodo
    python3 scan_holders.py --token diadata
    python3 scan_holders.py --token pudgy-penguins

    # Output formats
    python3 scan_holders.py --token dodo                   # text summary (default)
    python3 scan_holders.py --token dodo --format json     # JSON for programmatic use
    python3 scan_holders.py --token dodo --format md        # markdown table
    python3 scan_holders.py --token dodo --top 5            # top N only (default 10)

    # Find the right slug
    python3 scan_holders.py --search "dodo"                 # search for matching tokens

Dependencies:
    pip install requests

Limitations:
    - Only covers tokens tracked by CoinLore (established coins on CoinGecko/CMC)
    - BSC/Solana tokens may have spotty coverage
    - No 6-month historical data (snapshot only)
    - Rate limit: ~1 request/second (be polite)
"""

import argparse
import json
import re
import sys
from html import unescape

import requests

COINLORE_BASE = "https://www.coinlore.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TokenAnalyzer/1.0)"}


def fetch_richlist(slug: str) -> str:
    """Fetch the CoinLore rich list page for a token slug."""
    url = f"{COINLORE_BASE}/coin/{slug}/richlist"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_richlist(html: str) -> dict:
    """Parse the CoinLore rich list HTML and return structured data."""
    result = {
        "token_name": "",
        "top_holders": [],
        "aggregates": {},
    }

    # Extract token name from title
    title_match = re.search(r"<title>(.*?) Rich List", html)
    if title_match:
        result["token_name"] = unescape(title_match.group(1).strip())

    # Find all table rows
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)
    holder_section = False

    for row in rows:
        # Extract table cells
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        cleaned = []
        for td in tds:
            text = re.sub(r"<[^>]+>", "", td).strip()
            text = unescape(text)
            cleaned.append(text)

        if not cleaned:
            continue

        # Detect aggregate section
        if len(cleaned) >= 3 and re.match(r"\d+-\d+", cleaned[0]):
            result["aggregates"][cleaned[0]] = {
                "balance": cleaned[1] if len(cleaned) > 1 else "",
                "percentage": cleaned[2] if len(cleaned) > 2 else "",
            }
            continue

        if cleaned[0] == "Else":
            result["aggregates"]["Else"] = {
                "balance": cleaned[1] if len(cleaned) > 1 else "",
                "percentage": cleaned[2] if len(cleaned) > 2 else "",
            }
            continue

        # Detect holder rows: at least 3 columns, first is numeric, second is 0x address
        if len(cleaned) >= 3:
            balance_str = cleaned[0].replace(",", "")
            addr = cleaned[1] if len(cleaned) > 1 else ""
            pct = cleaned[3] if len(cleaned) > 3 else cleaned[2] if "%" in (cleaned[2] if len(cleaned) > 2 else "") else ""

            # Check if this is a holder row (starts with a number, has 0x address)
            if re.match(r"^[\d,.]+$", balance_str) and re.match(r"^0x[a-fA-F0-9]{40}$", addr):
                try:
                    balance = float(balance_str)
                except ValueError:
                    continue

                result["top_holders"].append({
                    "rank": len(result["top_holders"]) + 1,
                    "address": addr,
                    "balance": balance,
                    "percentage": pct,
                })

    # Also extract total supply if available
    supply_match = re.search(r"total supply of.*?(\d[\d,.]*)", html, re.IGNORECASE)
    if supply_match:
        result["total_supply"] = supply_match.group(1).replace(",", "")

    holder_count_match = re.search(r"(\d[\d,]*)\s+holders", html, re.IGNORECASE)
    if holder_count_match:
        result["holder_count"] = holder_count_match.group(1).replace(",", "")

    return result


def format_text(data: dict, top_n: int = 10) -> str:
    """Format results as plain text summary."""
    lines = []
    lines.append(f"=== {data['token_name']} Top {top_n} Holders ===")
    lines.append("")

    # Aggregates
    if data["aggregates"]:
        lines.append("Concentration Overview:")
        for band, info in data["aggregates"].items():
            lines.append(f"  {band}: {info['balance']} ({info['percentage']})")
        lines.append("")

    # Top holders
    holders = data["top_holders"][:top_n]
    if holders:
        lines.append(f"{'Rank':<5} {'Address':<44} {'Balance':>15} {'Share':>8}")
        lines.append("-" * 75)
        for h in holders:
            addr_short = f"{h['address'][:10]}...{h['address'][-6:]}"
            balance_str = f"{h['balance']:,.0f}"
            lines.append(f"{h['rank']:<5} {addr_short:<44} {balance_str:>15} {h['percentage']:>8}")

    # Total supply and holders
    if data.get("total_supply"):
        lines.append(f"\nTotal Supply: {data['total_supply']}")
    if data.get("holder_count"):
        lines.append(f"Total Holders: {data['holder_count']}")

    return "\n".join(lines)


def format_markdown(data: dict, top_n: int = 10) -> str:
    """Format results as a markdown table."""
    lines = []
    lines.append(f"### {data['token_name']} — Top {top_n} 地址")
    lines.append("")

    # Aggregates
    if data["aggregates"]:
        lines.append("| 分组 | 持仓量 | 占比 |")
        lines.append("|------|--------|------|")
        for band, info in data["aggregates"].items():
            lines.append(f"| {band} | {info['balance']} | {info['percentage']} |")
        lines.append("")

    # Top holders table
    holders = data["top_holders"][:top_n]
    if holders:
        lines.append("| # | 地址 | 持仓量 | 占比 |")
        lines.append("|---|------|--------|------|")
        for h in holders:
            addr_short = f"`{h['address'][:10]}...{h['address'][-6:]}`"
            lines.append(f"| {h['rank']} | {addr_short} | {h['balance']:,.0f} | {h['percentage']} |")

    if data.get("holder_count"):
        lines.append(f"\n> 总持币地址: {data['holder_count']}")

    return "\n".join(lines)


def search_token(query: str) -> list[dict]:
    """Search CoinLore for matching tokens."""
    # CoinLore doesn't have a proper search API, but we can try common slug patterns
    candidates = [
        query.lower(),
        query.lower().replace(" ", "-"),
        query.lower().replace(" ", ""),
    ]

    results = []
    for slug in candidates:
        try:
            html = fetch_richlist(slug)
            title_match = re.search(r"<title>(.*?) Rich List", html)
            if title_match:
                results.append({
                    "slug": slug,
                    "name": unescape(title_match.group(1).strip()),
                    "url": f"{COINLORE_BASE}/coin/{slug}/richlist",
                })
        except Exception:
            continue

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Fetch top token holder data from CoinLore rich lists"
    )
    parser.add_argument("--token", help="Token name/slug (e.g., 'dodo', 'diadata')")
    parser.add_argument("--search", help="Search for token slugs matching this query")
    parser.add_argument("--top", type=int, default=10, help="Number of top holders to show (default: 10)")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text",
                        help="Output format (default: text)")
    args = parser.parse_args()

    if args.search:
        results = search_token(args.search)
        if results:
            print(json.dumps(results, indent=2))
        else:
            print(f"No richlist found for '{args.search}'. Try different spelling.", file=sys.stderr)
            print("Common slug patterns: 'dodo', 'diadata', 'pudgy-penguins', 'hyperliquid'", file=sys.stderr)
            sys.exit(1)
        return

    if not args.token:
        parser.error("Either --token or --search is required")

    try:
        html = fetch_richlist(args.token)
    except requests.HTTPError as e:
        print(f"Error fetching richlist for '{args.token}': HTTP {e.response.status_code}", file=sys.stderr)
        print("Try --search to find the correct slug.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    data = parse_richlist(html)

    if not data["top_holders"]:
        print(f"No holder data found for '{args.token}'. The token may not be on CoinLore.", file=sys.stderr)
        sys.exit(1)

    if args.format == "json":
        # Limit to top N in JSON too
        output = {**data, "top_holders": data["top_holders"][:args.top]}
        print(json.dumps(output, indent=2, ensure_ascii=False))
    elif args.format == "md":
        print(format_markdown(data, args.top))
    else:
        print(format_text(data, args.top))


if __name__ == "__main__":
    main()
