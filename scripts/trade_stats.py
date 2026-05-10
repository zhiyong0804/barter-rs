#!/usr/bin/env python3

import argparse
import json
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path

getcontext().prec = 28


def decimal_div(a: Decimal, b: Decimal) -> Decimal:
	return a / b if b else Decimal("0")


def infer_symbol_from_path(path: Path) -> str | None:
	name = path.name.lower()
	if name.endswith(".trade"):
		return name[:-6]
	return None


def parse_trade_json(line: str, symbol: str | None) -> dict | None:
	marker = '{"stream":"'
	idx = line.find(marker)
	if idx < 0:
		return None

	payload = line[idx:].strip()
	try:
		obj = json.loads(payload)
	except json.JSONDecodeError:
		return None

	stream = str(obj.get("stream", "")).lower()
	if not stream.endswith("@trade"):
		return None

	if symbol and stream != f"{symbol.lower()}@trade":
		return None

	data = obj.get("data")
	if not isinstance(data, dict):
		return None
	return data


def main() -> None:
	parser = argparse.ArgumentParser(
		description="统计 Binance trade 日志中的主动买占比与主动买/卖均价"
	)
	parser.add_argument("file", help="行情日志文件路径，例如 /path/raveusdt.trade")
	parser.add_argument(
		"--symbol",
		help="交易对（可选），例如 RAVEUSDT；不传则从文件名推断",
	)
	args = parser.parse_args()

	path = Path(args.file)
	if not path.exists():
		raise SystemExit(f"文件不存在: {path}")

	symbol = args.symbol.lower() if args.symbol else infer_symbol_from_path(path)

	count_buy = 0
	count_sell = 0
	qty_buy = Decimal("0")
	qty_sell = Decimal("0")
	notional_buy = Decimal("0")
	notional_sell = Decimal("0")

	with path.open("r", encoding="utf-8", errors="ignore") as file:
		for line in file:
			trade = parse_trade_json(line, symbol)
			if trade is None:
				continue

			try:
				price = Decimal(str(trade["p"]))
				qty = Decimal(str(trade["q"]))
				maker = bool(trade["m"])
			except (KeyError, InvalidOperation):
				continue

			if maker is False:
				count_buy += 1
				qty_buy += qty
				notional_buy += price * qty
			else:
				count_sell += 1
				qty_sell += qty
				notional_sell += price * qty

	total_count = count_buy + count_sell
	total_qty = qty_buy + qty_sell

	buy_ratio_by_count = decimal_div(Decimal(count_buy), Decimal(total_count))
	buy_ratio_by_qty = decimal_div(qty_buy, total_qty)
	buy_avg_price_vwap = decimal_div(notional_buy, qty_buy)
	sell_avg_price_vwap = decimal_div(notional_sell, qty_sell)

	print(f"file={path}")
	print(f"symbol={symbol or 'ALL'}")
	print(f"count_buy={count_buy}")
	print(f"count_sell={count_sell}")
	print(f"buy_ratio_by_count={buy_ratio_by_count}")
	print(f"buy_ratio_by_qty={buy_ratio_by_qty}")
	print(f"buy_avg_price_vwap={buy_avg_price_vwap}")
	print(f"sell_avg_price_vwap={sell_avg_price_vwap}")


if __name__ == "__main__":
	main()
