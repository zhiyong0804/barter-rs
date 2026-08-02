#!/usr/bin/env python3
"""
Fetch top token holder data for Solana SPL tokens.

Primary: Solana public RPC `getProgramAccounts` — returns ALL token accounts
with individual addresses and balances. Free, no API key needed.
Fallback: GeckoTerminal API for aggregate distribution and safety scores.

Usage:
    python3 scan_holders_solana.py --address Dfh5DzRgSvvCFDoYc2ciTkMrbDfRKybA4SoFbPmApump
    python3 scan_holders_solana.py --address <mint> --top 100 --format json
    python3 scan_holders_solana.py --address <mint> --format md

Dependencies:
    pip install requests
"""

import argparse
import json
import sys
import time
from typing import Optional

import requests

# ──────────────────────────────────────────────────────────────────────
# Solana RPC endpoints (tried in order)
# ──────────────────────────────────────────────────────────────────────

RPC_ENDPOINTS = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
]

TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
GECKO_API = "https://api.geckoterminal.com/api/v2"


def rpc_call(rpc_url: str, method: str, params: list, timeout: int = 45) -> dict:
    """Make a Solana JSON RPC call."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = requests.post(rpc_url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_solana_holders(mint: str, rpc_url: Optional[str] = None) -> list[dict]:
    """
    Fetch ALL token holders via getProgramAccounts.
    Returns list of {address, amount, decimals} sorted by amount desc.
    """
    params = [
        TOKEN_PROGRAM_ID,
        {
            "encoding": "jsonParsed",
            "filters": [
                {"dataSize": 165},
                {"memcmp": {"offset": 0, "bytes": mint}},
            ],
        },
    ]

    rpcs = [rpc_url] if rpc_url else []
    rpcs += [u for u in RPC_ENDPOINTS if u not in rpcs]

    last_error = None
    for i, url in enumerate(rpcs):
        try:
            if i > 0:
                time.sleep(3 * i)
            result = rpc_call(url, "getProgramAccounts", params, timeout=45)
            if "result" in result:
                holders = []
                for a in result["result"]:
                    info = (
                        a.get("account", {})
                        .get("data", {})
                        .get("parsed", {})
                        .get("info", {})
                    )
                    addr = info.get("owner", "")
                    amt = int(info.get("tokenAmount", {}).get("amount", 0))
                    dec = info.get("tokenAmount", {}).get("decimals", 6)
                    if amt > 0:
                        holders.append(
                            {"address": addr, "amount": amt, "decimals": dec}
                        )
                holders.sort(key=lambda h: h["amount"], reverse=True)
                return holders
            elif "error" in result:
                err = result["error"]
                if err.get("code") in (429, -32005):
                    last_error = f"Rate limited: {url[:40]}"
                    continue
                last_error = f"RPC error: {err.get('message', str(err))}"
        except Exception as e:
            last_error = str(e)
            continue

    raise RuntimeError(
        f"All Solana RPCs failed. Last error: {last_error}\n"
        "Try a dedicated RPC: --rpc-url https://mainnet.helius-rpc.com/?api-key=YOUR_KEY\n"
        "Free key at https://dev.helius.xyz"
    )


def fetch_geckoterminal(mint: str) -> dict:
    """Fetch token safety and aggregate distribution from GeckoTerminal."""
    url = f"{GECKO_API}/networks/solana/tokens/{mint}/info"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("attributes", {})


def build_output(holders: list[dict], gt_info: dict, top_n: int) -> dict:
    """Build unified output from Solana RPC + GeckoTerminal data."""
    if not holders:
        return {"error": "No holders found"}

    dec = holders[0]["decimals"]
    div = 10**dec
    # Derive total supply from sum of all holder balances (most accurate for SPL)
    total_raw = sum(h["amount"] for h in holders)
    total_supply = total_raw / div

    # Aggregates
    def pct_of_top(n):
        s = sum(h["amount"] for h in holders[:n])
        return {
            "token_amount": s / div,
            "percentage": round(s / total_raw * 100, 2),
        }

    top_holders = []
    for i, h in enumerate(holders[:top_n]):
        bal = h["amount"] / div
        top_holders.append(
            {
                "rank": i + 1,
                "address": h["address"],
                "balance": h["amount"],
                "ui_balance": round(bal, 2),
                "percentage": round(h["amount"] / total_raw * 100, 4),
            }
        )

    output = {
        "mint": holders[0].get("_mint", ""),
        "total_supply": round(total_supply, 2),
        "total_holders": len(holders),
        "decimals": dec,
        "aggregates": {
            "top_5": pct_of_top(5),
            "top_10": pct_of_top(10),
            "top_20": pct_of_top(20),
            "top_40": pct_of_top(40),
            "top_100": pct_of_top(min(100, len(holders))),
        },
        "top_holders": top_holders,
        "geckoterminal": {
            "name": gt_info.get("name", ""),
            "symbol": gt_info.get("symbol", ""),
            "gt_score": gt_info.get("gt_score"),
            "is_honeypot": gt_info.get("is_honeypot"),
            "holders_count": gt_info.get("holders", {}).get("count"),
        },
    }
    return output


def format_markdown(output: dict) -> str:
    """Format as markdown report section."""
    lines = []
    gt = output.get("geckoterminal", {})
    lines.append(f"### {gt.get('name', '')} ({gt.get('symbol', '')}) — Top 100 持有者")
    lines.append(f"> 数据源：Solana RPC `getProgramAccounts` + GeckoTerminal")
    lines.append(f"> 总持币地址: {output['total_holders']:,} | 总供应: {output['total_supply']:,.0f}")
    lines.append("")

    agg = output["aggregates"]
    lines.append("#### 集中度总览")
    lines.append("| 分组 | 持仓量 | 占比 | 风险 |")
    lines.append("|------|--------|------|------|")
    for band in ["top_5", "top_10", "top_20", "top_40", "top_100"]:
        d = agg[band]
        risk = "🔴" if d["percentage"] > 70 else ("🟡" if d["percentage"] > 40 else "🟢")
        label = band.replace("_", " ").title()
        lines.append(f"| {label} | {d['token_amount']:,.0f} | {d['percentage']:.1f}% | {risk} |")
    lines.append("")

    lines.append("#### Top 10 地址详情")
    lines.append("| # | 地址 | 持仓量 | 占比 |")
    lines.append("|---|------|--------|------|")
    for h in output["top_holders"][:10]:
        addr = h["address"]
        short = f"`{addr[:8]}...{addr[-6:]}`"
        lines.append(f"| {h['rank']} | {short} | {h['ui_balance']:,.0f} | {h['percentage']:.2f}% |")
    lines.append("")

    lines.append("#### Top 11–100 地址")
    lines.append("| # | 地址 | 持仓量 | 占比 |")
    lines.append("|---|------|--------|------|")
    for h in output["top_holders"][10:100]:
        addr = h["address"]
        short = f"`{addr[:8]}...{addr[-6:]}`"
        lines.append(f"| {h['rank']} | {short} | {h['ui_balance']:,.0f} | {h['percentage']:.2f}% |")
    lines.append("")

    safety = []
    if gt.get("gt_score"):
        safety.append(f"安全评分: **{gt['gt_score']:.1f}/100**")
    if gt.get("is_honeypot") is not None:
        safety.append(f"Honeypot: {'🔴 是' if gt['is_honeypot'] == True else '✅ 否'}")
    if safety:
        lines.append(" | ".join(safety))

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Solana SPL token top holders from on-chain data"
    )
    parser.add_argument("--address", "-a", required=True, help="Token mint address")
    parser.add_argument("--top", type=int, default=100, help="Top N (default: 100)")
    parser.add_argument("--rpc-url", help="Custom Solana RPC URL")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    args = parser.parse_args()

    mint = args.address.strip()

    # Fetch on-chain holder data
    try:
        holders = fetch_solana_holders(mint, args.rpc_url)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Fetch GeckoTerminal safety data
    gt_info = {}
    try:
        gt_info = fetch_geckoterminal(mint)
    except Exception:
        pass

    output = build_output(holders, gt_info, args.top)

    if args.format == "json":
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
    elif args.format == "md":
        print(format_markdown(output))
    else:
        # Plain text summary
        print(f"=== {gt_info.get('name', mint)} Top {args.top} ===")
        print(f"Holders: {output['total_holders']:,} | Supply: {output['total_supply']:,.0f}")
        for band, d in output["aggregates"].items():
            print(f"  {band}: {d['token_amount']:,.0f} ({d['percentage']:.1f}%)")
        for h in output["top_holders"][:args.top]:
            print(f"  {h['rank']:3}. {h['address']}  {h['ui_balance']:>14,.0f} ({h['percentage']:.2f}%)")


if __name__ == "__main__":
    main()
