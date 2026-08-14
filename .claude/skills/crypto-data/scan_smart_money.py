#!/usr/bin/env python3
"""
Smart Money On-Chain Analyzer — Multi-chain wallet behavior analysis.

Supported: eth/polygon (Etherscan V2), sol (Alchemy/Helius RPC)
BSC excluded — no free data source available.

用法:
  python3 scan_smart_money.py --chain eth --token 0x... --wallets a,b --days 180
  python3 scan_smart_money.py --supported-chains
"""

import argparse, json, os, sys, time
from datetime import datetime, timezone
try: import requests
except ImportError: print("pip install requests", file=sys.stderr); sys.exit(1)

# Config
ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
CHAIN_IDS = {"eth": 1, "polygon": 137}
KNOWN = {
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance 7 / MUBARAK Whale #1",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "MUBARAK Whale #2",
    "0x000000000000000000000000000000000000dead": "Burn",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = SCRIPT_DIR
for _ in range(6):
    if os.path.isdir(os.path.join(PROJECT_ROOT, "config")): break
    PROJECT_ROOT = os.path.dirname(PROJECT_ROOT)

def load_config(filename):
    p = os.path.join(PROJECT_ROOT, "config", filename)
    return json.load(open(p)) if os.path.exists(p) else {}

# Etherscan V2 (ETH/Polygon)
def fetch_evm(chain, token, wallet, api_key):
    cid = CHAIN_IDS[chain]
    all_tx, decimals = [], 18
    for page in range(1, 100):
        r = requests.get(ETHERSCAN_V2, params={
            "chainid": cid, "module": "account", "action": "tokentx",
            "contractaddress": token, "address": wallet,
            "page": page, "offset": 1000, "sort": "asc", "apikey": api_key,
        }, timeout=20)
        d = r.json()
        if d.get("status") != "1":
            if page == 1: print(f"  API: {d.get('message','?')}", file=sys.stderr)
            break
        txs = d.get("result", [])
        if not txs: break
        for tx in txs:
            decimals = int(tx.get("tokenDecimal", 18))
            all_tx.append({
                "from": tx.get("from",""), "to": tx.get("to",""),
                "amount": float(tx.get("value",0))/(10**decimals),
                "block": int(tx.get("blockNumber",0)),
                "ts": int(tx.get("timeStamp",0)),
                "dt": datetime.fromtimestamp(int(tx.get("timeStamp",0)), tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "hash": tx.get("hash",""),
            })
        if len(txs) < 1000: break
        time.sleep(0.21)
    return all_tx

# Solana (Alchemy/Helius RPC)
def get_sol_rpc():
    cfg = load_config("alchemy_key.json")
    url = cfg.get("rpc_url", "").replace("bnb-mainnet", "solana-mainnet")
    if url: return url
    cfg = load_config("helius_key.json")
    if cfg.get("api_key"): return f"https://mainnet.helius-rpc.com/?api-key={cfg['api_key']}"
    return "https://api.mainnet-beta.solana.com"

def fetch_sol(mint, wallet):
    rpc = get_sol_rpc()
    sigs, before = [], None
    for _ in range(50):
        params = {"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress","params":[wallet,{"limit":100}]}
        if before: params["params"][1]["before"] = before
        r = requests.post(rpc, json=params, timeout=15)
        results = r.json().get("result",[])
        if not results: break
        sigs.extend(results)
        before = results[-1]["signature"]
        if len(results) < 100: break
        time.sleep(0.1)
    if not sigs: return []

    transfers, ml = [], mint.lower()
    wl = wallet.lower()
    for i, sig in enumerate(sigs):
        if i > 0 and i % 50 == 0: print(f"    {i}/{len(sigs)}", file=sys.stderr)
        r = requests.post(rpc, json={"jsonrpc":"2.0","id":1,"method":"getTransaction","params":[sig["signature"],{"maxSupportedTransactionVersion":0,"encoding":"jsonParsed"}]}, timeout=15)
        tx = r.json().get("result")
        if not tx: continue
        for inst in tx.get("transaction",{}).get("message",{}).get("instructions",[]) or []:
            p = inst.get("parsed",{})
            if p.get("type") not in ("transfer","transferChecked"): continue
            info = p.get("info",{})
            if info.get("mint","").lower() != ml: continue
            fa = info.get("authority","") or info.get("source","")
            ta = info.get("destination","")
            amt = float(info.get("amount",0) or info.get("tokenAmount",{}).get("amount",0))
            dec = info.get("tokenAmount",{}).get("decimals",0)
            if dec > 0: amt /= (10**dec)
            if fa.lower() == wl or ta.lower() == wl:
                transfers.append({"from":fa,"to":ta,"amount":amt,"block":tx.get("slot",0),"ts":tx.get("blockTime",0),
                    "dt":datetime.fromtimestamp(tx.get("blockTime",0),tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if tx.get("blockTime") else "",
                    "hash":sig["signature"]})
        time.sleep(0.05)
    transfers.sort(key=lambda t: t["block"])
    return transfers

# Analysis
def analyze(wallet, transfers, price):
    buys, sells = [], []
    w = wallet.lower()
    for t in transfers:
        e = {"block":t["block"],"amount":t["amount"],"ts":t.get("ts",0),"dt":t.get("dt",""),
             "hash":t.get("hash",""),"counterparty":t["from"] if t["to"].lower()==w else t["to"]}
        if e["counterparty"].lower() in KNOWN: e["label"] = KNOWN[e["counterparty"].lower()]
        (sells if t["from"].lower() == w else buys).append(e)
    if not buys and not sells: return None
    tb = sum(b["amount"] for b in buys)
    ts = sum(s["amount"] for s in sells)
    pos = tb - ts
    if pos <= 0: bv, bd = "EXITED", "已清仓"
    elif len(buys) >= 5 and len(sells) == 0: bv, bd = "ACCUMULATING", "持续买入从未卖出"
    elif len(buys) >= 3 and len(sells) <= 1: bv, bd = "ACCUMULATING", "多数买入极少卖出"
    elif len(sells) > len(buys) and ts > tb * 0.3: bv, bd = "DISTRIBUTING", "卖出多于买入 — 派发中"
    elif len(buys) >= 3 and len(sells) >= 3 and abs(pos) < tb * 0.3: bv, bd = "ROTATING", "频繁买卖 — 波段"
    elif pos > tb * 0.7: bv, bd = "HOLDING", "大部分持仓不动"
    else: bv, bd = "HOLDING", "持有中"
    return {"wallet":wallet,"label":KNOWN.get(wallet,""),"first_buy":buys[0]["dt"] if buys else "N/A",
        "first_sell":sells[0]["dt"] if sells else "N/A","current_position":round(pos,2),
        "total_bought":round(tb,2),"total_sold":round(ts,2),"buy_count":len(buys),
        "sell_count":len(sells),"current_value_usd":round(pos*price,2) if price>0 else None,
        "behavior":bv,"behavior_desc":bd}

# Main
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--chain", default="eth", choices=["eth","polygon","sol","bsc"])
    p.add_argument("--token")
    p.add_argument("--wallets")
    p.add_argument("--days", type=int, default=180)
    p.add_argument("--json", action="store_true")
    p.add_argument("--output", "-o")
    p.add_argument("--supported-chains", action="store_true")
    args = p.parse_args()

    if args.supported_chains:
        print("eth     ✅ Etherscan V2 (free)")
        print("polygon ✅ Etherscan V2 (free)")
        print("sol     ✅ Alchemy/Helius RPC (free)")
        print("bsc     ❌ No free data source")
        return

    if not args.token or not args.wallets:
        print("Required: --token and --wallets. Use --supported-chains to list options.", file=sys.stderr)
        sys.exit(1)

    if args.chain == "bsc":
        print("BSC not supported on free tier. Use --supported-chains.", file=sys.stderr)
        sys.exit(1)

    wallets = [w.strip() for w in args.wallets.split(",")]
    price = 0
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{args.token}", timeout=10)
        if r.status_code == 200:
            pairs = r.json().get("pairs",[])
            if pairs: price = float(max(pairs, key=lambda p: p.get("liquidity",{}).get("usd",0)).get("priceUsd",0))
    except: pass

    results = []
    if args.chain in ("eth", "polygon"):
        ak = load_config("bscscan_key.json").get("api_key") or os.environ.get("ETHERSCAN_API_KEY")
        if not ak: print("Need Etherscan API key in config/bscscan_key.json", file=sys.stderr); sys.exit(1)
        print(f"[{args.chain}] Etherscan V2 | {len(wallets)} wallets | ${price:,.6f}", file=sys.stderr)
        for w in wallets:
            print(f"  {w[:10]}...", file=sys.stderr, end=" ")
            txs = fetch_evm(args.chain, args.token, w, ak)
            if not txs: results.append({"wallet":w,"behavior":"NO_ACTIVITY","current_position":0,"total_bought":0,"total_sold":0,"buy_count":0,"sell_count":0}); print("0 txs", file=sys.stderr); continue
            a = analyze(w, txs, price)
            if a: results.append(a); print(f"{a['behavior']} | B={a['buy_count']} S={a['sell_count']}", file=sys.stderr)
            time.sleep(0.22)
    elif args.chain == "sol":
        print(f"[sol] Alchemy RPC | {len(wallets)} wallets | ${price:,.6f}", file=sys.stderr)
        for w in wallets:
            print(f"  {w[:10]}...", file=sys.stderr, end=" ")
            txs = fetch_sol(args.token, w)
            if not txs: results.append({"wallet":w,"behavior":"NO_ACTIVITY","current_position":0,"total_bought":0,"total_sold":0,"buy_count":0,"sell_count":0}); print("0 txs", file=sys.stderr); continue
            a = analyze(w, txs, price)
            if a: results.append(a); print(f"{a['behavior']} | B={a['buy_count']} S={a['sell_count']} | {a['first_buy']}", file=sys.stderr)

    order = {"ACCUMULATING":0,"HOLDING":1,"DISTRIBUTING":2,"ROTATING":3,"EXITED":4,"NO_ACTIVITY":5}
    results.sort(key=lambda w: order.get(w.get("behavior",""),5))
    output = {"generated_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "chain":args.chain,"token":args.token,"price":price,"wallets":results}
    if args.output:
        with open(args.output,"w") as f: json.dump(output,f,indent=2,ensure_ascii=False)
        print(f"\n{args.output}", file=sys.stderr)
    elif args.json:
        print(json.dumps(output,indent=2,ensure_ascii=False))
    else:
        em = {"ACCUMULATING":"🔴","HOLDING":"🟢","DISTRIBUTING":"🔵","ROTATING":"🔄","EXITED":"⚫"}
        for w in results:
            print(f"\n{em.get(w['behavior'],'❓')} [{w['behavior']}] {w['wallet']}")
            if w.get("label"): print(f"   Label: {w['label']}")
            print(f"   First Buy: {w['first_buy']}  |  First Sell: {w['first_sell']}")
            print(f"   Position: {w['current_position']:,.2f}  |  Bought: {w['total_bought']:,.2f}  Sold: {w['total_sold']:,.2f}")
            print(f"   Txs: {w['buy_count']} buys / {w['sell_count']} sells")
            if w.get('current_value_usd'): print(f"   Value: \${w['current_value_usd']:,.2f}")
            print(f"   => {w['behavior_desc']}")
    acc = sum(1 for w in results if w.get("behavior")=="ACCUMULATING")
    dist = sum(1 for w in results if w.get("behavior")=="DISTRIBUTING")
    print(f"\n{acc} Acc | {sum(1 for w in results if w.get('behavior')=='HOLDING')} Hold | {dist} Dist", file=sys.stderr)

if __name__ == "__main__":
    main()
