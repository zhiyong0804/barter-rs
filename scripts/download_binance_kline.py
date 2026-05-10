#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import urllib.request
import urllib.parse
import json
import time
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any
import os

# Binance Futures API基础URL
BASE_URL = "https://fapi.binance.com"

def get_klines(symbol: str, interval: str, start_time: int, end_time: int, limit: int = 1000) -> List[List[Any]]:
    """
    获取K线数据

    Args:
        symbol: 交易对符号
        interval: K线间隔 (如 '1m', '5m', '15m', '1h', '1d', etc.)
        start_time: 开始时间戳 (毫秒)
        end_time: 结束时间戳 (毫秒)
        limit: 每次请求的最大K线数量 (最大1000)

    Returns:
        K线数据列表
    """
    url = f"{BASE_URL}/fapi/v1/klines"

    params = {
        'symbol': symbol.upper(),
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': limit
    }

    # 构建查询字符串
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"

    try:
        # 发送GET请求
        request = urllib.request.Request(full_url)
        response = urllib.request.urlopen(request)

        # 读取响应数据
        data = response.read().decode('utf-8')
        return json.loads(data)
    except urllib.error.HTTPError as e:
        print(f"HTTP错误: {e.code} - {e.reason}")
        raise
    except urllib.error.URLError as e:
        print(f"URL错误: {e.reason}")
        raise
    except Exception as e:
        print(f"请求出错: {e}")
        raise

def download_klines(symbol: str, interval: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    下载指定时间段内的K线数据

    Args:
        symbol: 交易对符号
        interval: K线间隔
        start_time: 开始时间
        end_time: 结束时间

    Returns:
        K线数据列表
    """
    # 转换为毫秒时间戳
    start_ts = int(start_time.timestamp() * 1000)
    end_ts = int(end_time.timestamp() * 1000)

    all_klines = []
    current_start = start_ts

    print(f"开始下载 {symbol} 的 {interval} K线数据...")
    print(f"时间范围: {start_time.isoformat()} 到 {end_time.isoformat()}")

    while current_start < end_ts:
        try:
            klines = get_klines(symbol, interval, current_start, end_ts)

            if not klines:
                break

            # 添加到总列表
            all_klines.extend(klines)

            # 更新下一次请求的开始时间
            # 使用最后一条K线的开盘时间作为下一次请求的开始时间
            current_start = klines[-1][0] + 1

            print(f"已下载 {len(all_klines)} 条K线数据...")

            # 避免超过API速率限制
            time.sleep(0.1)

        except Exception as e:
            print(f"请求出错: {e}")
            time.sleep(1)
            continue

    print(f"总共下载了 {len(all_klines)} 条K线数据")
    return all_klines

def format_kline_data(klines: List[List[Any]]) -> List[Dict[str, Any]]:
    """
    格式化K线数据

    K线数据格式:
    [
      [
        1499040000000,      // 开盘时间
        "0.01634790",       // 开盘价
        "0.80000000",       // 最高价
        "0.01575800",       // 最低价
        "0.01577100",       // 收盘价(当前K线未结束的最新成交价)
        "148976.11427815",  // 成交量
        1499644799999,      // 收盘时间
        "2434.19055334",    // 成交额
        308,                // 成交笔数
        "1756.87402397",    // 主动买入成交量
        "28.46694368",      // 主动买入成交额
        "17928899.62484339" // 请忽略该项，用于未来功能
      ]
    ]
    """
    formatted_klines = []

    for kline in klines:
        formatted_kline = {
            'open_time': kline[0],  # 开盘时间 (毫秒)
            'open': float(kline[1]),  # 开盘价
            'high': float(kline[2]),  # 最高价
            'low': float(kline[3]),  # 最低价
            'close': float(kline[4]),  # 收盘价
            'volume': float(kline[5]),  # 成交量
            'close_time': kline[6],  # 收盘时间 (毫秒)
            'quote_asset_volume': float(kline[7]),  # 成交额
            'number_of_trades': kline[8],  # 成交笔数
            'taker_buy_base_asset_volume': float(kline[9]),  # 主动买入成交量
            'taker_buy_quote_asset_volume': float(kline[10]),  # 主动买入成交额
        }
        formatted_klines.append(formatted_kline)

    return formatted_klines

def save_klines_to_file(klines: List[Dict[str, Any]], symbol: str, start_time: datetime, end_time: datetime, interval: str = '1m'):
    """
    保存K线数据到文件

    文件名格式: symbol_$start_time_$end_time.candle_$interval
    """
    # 格式化时间戳为文件名友好的格式
    start_str = start_time.strftime('%Y%m%d_%H%M%S')
    end_str = end_time.strftime('%Y%m%d_%H%M%S')

    # 创建文件名
    filename = f"{symbol.lower()}_{start_str}_{end_str}.candle_{interval}"

    # 确保目录存在
    os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)

    # 保存到文件
    with open(filename, 'w') as f:
        for kline in klines:
            json.dump(kline, f)
            f.write('\n')

    print(f"K线数据已保存到文件: {filename}")
    return filename

def main():
    parser = argparse.ArgumentParser(description='下载Binance期货K线数据')
    parser.add_argument('symbol', help='交易对符号 (例如: BTCUSDT)')
    parser.add_argument('start_time', help='开始时间 (ISO格式, 例如: 2026-04-28T23:00:00)')
    parser.add_argument('end_time', help='结束时间 (ISO格式, 例如: 2026-04-29T23:00:00)')
    parser.add_argument('--interval', default='1m', help='K线间隔 (默认: 1m)')

    args = parser.parse_args()

    # 解析时间参数
    start_time = datetime.fromisoformat(args.start_time.replace('Z', '+00:00'))
    end_time = datetime.fromisoformat(args.end_time.replace('Z', '+00:00'))

    # 确保时间有UTC时区信息
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    # 如果时间是其他时区，转换为UTC
    start_time = start_time.astimezone(timezone.utc)
    end_time = end_time.astimezone(timezone.utc)

    # 检查时间范围是否有效
    if start_time >= end_time:
        print("错误: 开始时间必须早于结束时间")
        return

    # 下载K线数据
    raw_klines = download_klines(args.symbol, args.interval, start_time, end_time)

    if not raw_klines:
        print("没有下载到任何K线数据")
        return

    # 格式化K线数据
    formatted_klines = format_kline_data(raw_klines)

    # 保存到文件
    filename = save_klines_to_file(formatted_klines, args.symbol, start_time, end_time, args.interval)

    print(f"下载完成！共下载 {len(formatted_klines)} 条K线数据")
    print(f"数据已保存到: {filename}")


### ./download_binance_kline.py ETHUSDT 2026-04-29T00:00:00 2026-04-29T02:00:00 --interval 5m

if __name__ == '__main__':
    main()
