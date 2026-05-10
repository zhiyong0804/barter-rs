import json
import sys
from collections import defaultdict
from datetime import datetime

def process_file(file_path, use_notional=True):
    buckets = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})

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

                # 👉 是否使用成交额（推荐）
                value = amount * price if use_notional else amount

                if side == "Buy":
                    buckets[ts]["buy"] += value
                else:
                    buckets[ts]["sell"] += value

            except Exception as e:
                # 跳过脏数据
                continue

    # 表头
    print(
        f"{'timestamp':<12} "
        f"{'local_time':<20} "
        f"{'buy':>15} "
        f"{'sell':>15} "
        f"{'buy%':>10} "
        f"{'sell%':>10}"
    )

    print("-" * 70)

    # 输出
    for sec in sorted(buckets.keys()):
        b = buckets[sec]
        total = b["buy"] + b["sell"]

        if total == 0:
            continue

        buy_ratio = b["buy"] / total
        sell_ratio = b["sell"] / total

        # 本地时间
        local_time = datetime.fromtimestamp(sec).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        print(
            f"{sec:<12} "
            f"{local_time:<20} "
            f"{b['buy']:>15,.2f} "
            f"{b['sell']:>15,.2f} "
            f"{buy_ratio:>10.4f} "
            f"{sell_ratio:>10.4f}"
        )

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python trade_stats.py input_file")
        sys.exit(1)

    file_path = sys.argv[1]

    # 默认用成交额（更合理）
    process_file(file_path, use_notional=True)
