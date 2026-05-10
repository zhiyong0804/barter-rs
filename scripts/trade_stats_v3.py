import json
import sys
from collections import defaultdict
from datetime import datetime

def process_file(file_path, use_notional=True):
    # 扩展 bucket 存储内容
    buckets = defaultdict(lambda: {
        "buy": 0.0, 
        "sell": 0.0,
        "open": None,
        "high": float('-inf'),
        "low": float('inf'),
        "close": 0.0
    })

    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                trade = json.loads(line)
                data = trade["data"]

                ts = data["trade_timestamp"] // 1000
                amount = float(data["amount"])
                price = float(data.get("price", 0))
                side = data["side"]

                value = amount * price if use_notional else amount
                
                # 统计买卖额
                if side == "Buy":
                    buckets[ts]["buy"] += value
                else:
                    buckets[ts]["sell"] += value

                # 统计 OHLC
                b = buckets[ts]
                if b["open"] is None:
                    b["open"] = price
                if price > b["high"]:
                    b["high"] = price
                if price < b["low"]:
                    b["low"] = price
                b["close"] = price

            except Exception:
                continue

    # 打印表头 (增加了 OHLC 列)
    header = (
        f"{'timestamp':<12} "
        f"{'local_time':<20} "
        f"{'buy_vol':>15} {'sell_vol':>15} {'buy%':>10}"
        f"{'sell%':>10}"
        f"{'open':>10} {'high':>10} {'low':>10} {'close':>10} "
    )
    print(header)
    print("-" * len(header))

    for sec in sorted(buckets.keys()):
        b = buckets[sec]
        total = b["buy"] + b["sell"]
        if total == 0: continue

        local_time = datetime.fromtimestamp(sec).strftime("%Y-%m-%d %H:%M:%S")
        buy_ratio = b["buy"] / total
        sell_ratio = b["sell"] / total

        print(
            f"{sec:<12} "
            f"{local_time:<20} "
            f"{b['buy']:>15,.2f} "
            f"{b['sell']:>15,.2f} "
            f"{buy_ratio:>10.4f} "
            f"{sell_ratio:>10.4f}"
            f"{b['open']:>10.2f} {b['high']:>10.2f} {b['low']:>10.2f} {b['close']:>10.2f} "
        )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python trade_stats_v2.py <input_file>")
        sys.exit(1)
    process_file(sys.argv[1])

