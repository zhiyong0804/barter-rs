#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any


def parse_datetime(date_str: str) -> datetime:
    """解析日期时间字符串，统一转换为UTC时间"""
    # 移除末尾的'Z'字符
    if date_str.endswith('Z'):
        # 替换Z为+00:00
        date_str = date_str[:-1] + '+00:00'

    # 处理不同的日期格式
    try:
        # 尝试标准格式
        dt = datetime.fromisoformat(date_str)
        # 如果没有时区信息，假设是UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        try:
            # 尝试去掉毫秒后的时区信息
            if '+' in date_str:
                date_part, tz_part = date_str.rsplit('+', 1)
                tz_part = tz_part.replace(':', '')  # 移除冒号
                if len(tz_part) == 2:
                    tz_part += '00'  # 补充分钟部分
                elif len(tz_part) == 0:
                    tz_part = '0000'

                if '.' in date_part:
                    # 只保留最多6位微秒
                    date_parts = date_part.split('.')
                    if len(date_parts) > 1:
                        ms_part = date_parts[1][:6]  # 最多6位
                        date_part = f"{date_parts[0]}.{ms_part}"

                # 重构带时区的字符串
                tz_sign = '+' if not tz_part.startswith('-') else ''
                tz_hours = int(tz_part[:2])
                tz_minutes = int(tz_part[2:4]) if len(tz_part) > 2 else 0
                tz_offset = timezone(datetime.strptime(f"{tz_sign}{tz_hours:02d}:{tz_minutes:02d}", "%z").utcoffset(None) or timezone.utc)

                dt = datetime.fromisoformat(f"{date_part}{tz_sign}{tz_hours:02d}:{tz_minutes:02d}")
                return dt.astimezone(timezone.utc)
            else:
                # 没有时区信息，假设是UTC
                if '.' in date_str:
                    date_part = date_str.rsplit('.', 1)[0]
                else:
                    date_part = date_str
                dt = datetime.fromisoformat(date_part)
                return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            # 最后尝试手动解析
            # 格式如: 2026-04-28T23:14:00.5851
            try:
                if '.' in date_str and '+' not in date_str:
                    # 只保留最多6位微秒
                    date_part, ms_part = date_str.split('.')
                    ms_part = ms_part[:6]  # 最多6位
                    date_str = f"{date_part}.{ms_part}"
                dt = datetime.fromisoformat(date_str)
                # 如果没有时区信息，假设是UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                # 最简单的处理方式：移除时区信息并假设是UTC
                if '+' in date_str:
                    date_str = date_str.split('+')[0]
                elif '-' in date_str and date_str.count('-') > 2:  # 日期中的连字符不算
                    parts = date_str.split('-')
                    if len(parts) > 3:
                        date_str = '-'.join(parts[:3])
                # 确保只保留到秒的部分
                if '.' in date_str:
                    date_str = date_str.split('.')[0]
                dt = datetime.fromisoformat(date_str)
                return dt.replace(tzinfo=timezone.utc)


def parse_kline_line(lines: List[str]) -> Dict[str, Any]:
    """解析K线数据（可能跨越多行）"""
    # 合并所有行
    combined_line = ''.join(lines)

    try:
        data = json.loads(combined_line)
        time_dt = parse_datetime(data['time'])
        close_time_dt = parse_datetime(data['data']['close_time'])

        return {
            'time': time_dt,
            'close_time': close_time_dt,
            'volume': float(data['data']['volume']),
            'kind': data['kind']
        }
    except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
        print(f"JSON解析错误: {e}")
        print(f"数据内容: {combined_line[:100]}...")
        return None


def load_klines(file_path: str) -> List[Dict[str, Any]]:
    """加载K线数据"""
    klines = []

    # 读取所有行
    with open(file_path, 'r') as f:
        lines = f.readlines()

    # 处理每一组K线数据
    current_kline_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 检查是否是新的K线开始（以 '{' 开头）
        if line.startswith('{'):
            # 如果已经有累积的行，处理它们
            if current_kline_lines:
                kline = parse_kline_line(current_kline_lines)
                if kline:
                    klines.append(kline)

            # 开始新的K线
            current_kline_lines = [line]
        else:
            # 继续累积当前K线的行
            current_kline_lines.append(line)

    # 处理最后一组K线
    if current_kline_lines:
        kline = parse_kline_line(current_kline_lines)
        if kline:
            klines.append(kline)

    # 按时间排序
    klines.sort(key=lambda x: x['time'])
    return klines


def find_klines_before_cutoff(klines: List[Dict[str, Any]], cutoff_time: datetime, count: int, kind: str) -> List[Dict[str, Any]]:
    """找到截止时间之前的指定数量的K线，按close_time排序"""
    # 确保cutoff_time有时区信息
    if cutoff_time.tzinfo is None:
        cutoff_time = cutoff_time.replace(tzinfo=timezone.utc)

    # 筛选出指定类型的K线
    filtered_klines = [k for k in klines if k['kind'] == kind]

    # 找到截止时间之前的K线（使用close_time）
    before_cutoff = [k for k in filtered_klines if k['close_time'] <= cutoff_time]

    # 按close_time排序
    before_cutoff.sort(key=lambda x: x['close_time'])

    # 取最后count根K线
    return before_cutoff[-count:] if len(before_cutoff) >= count else before_cutoff


def calculate_vol_base(minute_klines: List[Dict[str, Any]]) -> float:
    """计算vol_base：T(15)到T(N)的平均交易量，其中N是最末尾的索引"""
    if not minute_klines or len(minute_klines) < 16:
        return 0.0

    # 取T(15)到T(N)，即索引15到N（包含15，不包含N+1）
    # 但在实际数组中，T(15)是倒数第16个元素
    if len(minute_klines) >= 120:
        # 如果有120根或更多K线，取倒数第120到倒数第16根（共105根）
        selected_klines = minute_klines[-120:-15]
    else:
        # 如果少于120根，取从第15根到最后一根（不包括最后15根）
        if len(minute_klines) > 15:
            selected_klines = minute_klines[15:]
        else:
            selected_klines = []

    if not selected_klines:
        return 0.0

    total_volume = sum(k['volume'] for k in selected_klines)
    return total_volume / len(selected_klines)


def calculate_recent_5min_volumes(minute_klines: List[Dict[str, Any]]) -> tuple:
    """计算最近3根5分钟K线的交易量
    q1 = T(0) + T(1) + T(2) + T(3) + T(4)
    q2 = T(5) + T(6) + T(7) + T(8) + T(9)
    q3 = T(10) + T(11) + T(12) + T(13) + T(14)
    """
    if len(minute_klines) < 15:
        # 如果不够15根，有多少算多少
        volumes = []
        total_volume = 0
        for i in range(min(len(minute_klines) // 5, 3)):
            start_idx = i * 5
            end_idx = min((i + 1) * 5, len(minute_klines))
            group_volume = sum(minute_klines[j]['volume'] for j in range(start_idx, end_idx))
            volumes.append(group_volume)
            total_volume += group_volume
        return volumes, total_volume

    # 计算最近3根5分钟K线的交易量
    volumes = []
    for i in range(3):
        start_idx = i * 5
        end_idx = (i + 1) * 5
        group_volume = sum(minute_klines[j]['volume'] for j in range(start_idx, end_idx))
        volumes.append(group_volume)

    total_volume = sum(volumes)
    return volumes, total_volume


def main():
    parser = argparse.ArgumentParser(description='计算K线指标')
    parser.add_argument('kline_file', help='K线文件路径')
    parser.add_argument('cutoff_time', help='截止时间 (ISO格式, 例如: 2026-04-28T23:00:00)')

    args = parser.parse_args()

    # 解析截止时间
    cutoff_time_str = args.cutoff_time
    # 处理带毫秒和Z后缀的时间格式
    if cutoff_time_str.endswith('Z'):
        # 替换Z为+00:00
        cutoff_time_str = cutoff_time_str[:-1] + '+00:00'
    # 处理毫秒部分，只保留最多6位
    if '.' in cutoff_time_str:
        date_part, ms_tz_part = cutoff_time_str.split('.', 1)
        if '+' in ms_tz_part:
            ms_part, tz_part = ms_tz_part.split('+', 1)
            ms_part = ms_part[:6]  # 最多6位微秒
            cutoff_time_str = f"{date_part}.{ms_part}+{tz_part}"
        elif ms_tz_part.count('-') > 0 and not ms_tz_part.startswith('-'):
            # 处理负时区偏移
            parts = ms_tz_part.split('-', 1)
            ms_part = parts[0][:6]  # 最多6位微秒
            tz_part = parts[1]
            cutoff_time_str = f"{date_part}.{ms_part}-{tz_part}"
        else:
            # 没有时区信息
            ms_part = ms_tz_part[:6]  # 最多6位微秒
            cutoff_time_str = f"{date_part}.{ms_part}"

    cutoff_time = datetime.fromisoformat(cutoff_time_str)
    # 确保截止时间有时区信息
    if cutoff_time.tzinfo is None:
        cutoff_time = cutoff_time.replace(tzinfo=timezone.utc)

    # 加载K线数据
    klines = load_klines(args.kline_file)

    print(f"总共加载了 {len(klines)} 根K线")

    # 按类型统计
    kind_counts = {}
    for k in klines:
        kind = k['kind']
        kind_counts[kind] = kind_counts.get(kind, 0) + 1

    print("K线类型统计:")
    for kind, count in kind_counts.items():
        print(f"  {kind}: {count}")

    # 获取截止时间前的所有分钟K线（按时间排序）
    all_minute_klines = find_klines_before_cutoff(klines, cutoff_time, len(klines), 'kline_1m')

    # 获取截止时间前的15根分钟K线（用于显示）
    minute_klines = find_klines_before_cutoff(klines, cutoff_time, 15, 'kline_1m')

    # 计算vol_base：T(15)到T(119)的平均交易量
    vol_base = calculate_vol_base(all_minute_klines)

    # 获取截止时间前的5分钟K线（用于计算最近3根）
    five_min_klines = find_klines_before_cutoff(klines, cutoff_time, 10, 'kline_5m')  # 取10根以确保至少有3根

    # 计算最近3根5分钟K线的交易量（使用1分钟K线合成）
    recent_5min_volumes, total_recent_5min_volume = calculate_recent_5min_volumes(minute_klines)

    # 输出结果
    print(f"\n截止时间: {cutoff_time.isoformat()}")
    print(f"用于计算vol_base的分钟K线数量: {len(all_minute_klines)}")

    # 计算用于vol_base的K线范围
    if len(all_minute_klines) >= 120:
        # 如果有120根或更多K线，取T(15)到T(119)
        vol_base_klines = all_minute_klines[-120:-15]
        print(f"实际用于计算vol_base的K线: T(15)到T(119), 共105根")
    elif len(all_minute_klines) > 15:
        # 如果少于120根但多于15根，取T(15)到T(N)
        vol_base_klines = all_minute_klines[15:]
        count = len(vol_base_klines)
        print(f"实际用于计算vol_base的K线: T(15)到T({len(all_minute_klines)-1}), 共{count}根")
    else:
        vol_base_klines = []
        print("K线数量不足，无法计算vol_base")

    # 打印用于计算vol_base的K线信息
    if vol_base_klines:
        print("用于计算vol_base的K线:")
        # 显示所有用于计算vol_base的K线
        for i, k in enumerate(vol_base_klines):
            # 计算实际的T(i)标记
            t_index = i + 15
            print(f"  T({t_index}): {k['close_time'].isoformat()}: {k['volume']:.2f}")
    else:
        print("没有用于计算vol_base的K线")

    print(f"vol_base (T(15)到T({len(all_minute_klines)-1})的平均交易量): {vol_base:.2f}")

    if minute_klines:
        print("\n用于计算最近3根5分钟K线交易量的分钟K线:")
        # 按照close_time倒序显示，最新的在前面
        for i, k in enumerate(reversed(minute_klines[:15])):
            print(f"  T({i}): {k['close_time'].isoformat()}: {k['volume']:.2f}")

        if len(minute_klines) >= 15:
            print(f"\n最近3根5分钟K线的交易量:")
            print(f"  q1 (T(0)+...+T(4)): {recent_5min_volumes[0]:.2f}")
            print(f"  q2 (T(5)+...+T(9)): {recent_5min_volumes[1]:.2f}")
            print(f"  q3 (T(10)+...+T(14)): {recent_5min_volumes[2]:.2f}")
            print(f"  总计: {total_recent_5min_volume:.2f}")
        else:
            print(f"\n最近{len(recent_5min_volumes)}根5分钟K线的交易量: {[f'{v:.2f}' for v in recent_5min_volumes]}")
            print(f"总计: {total_recent_5min_volume:.2f}")
    else:
        print("\n没有找到分钟K线数据")

    if five_min_klines:
        print(f"\n找到 {len(five_min_klines)} 根5分钟K线")
    else:
        print("\n没有找到5分钟K线数据")


if __name__ == '__main__':
    main()
