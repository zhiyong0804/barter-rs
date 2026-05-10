import pandas as pd
import json
import sys
import os

def analyze_liquidations(file_path, target_symbol):
    if not os.path.exists(file_path):
        print(f"❌ 错误: 文件 '{file_path}' 不存在")
        return

    raw_data = []
    
    # 1. 过滤并加载数据
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                item = json.loads(line)
                if item.get('symbol') == target_symbol:
                    raw_data.append({
                        'time': pd.to_datetime(item['data']['time']),
                        'amount': float(item['data']['price']) * float(item['data']['quantity'])
                    })
            except (json.JSONDecodeError, KeyError):
                continue

    if not raw_data:
        print(f"⚠️ 找不到 {target_symbol} 的爆仓数据")
        return

    df = pd.DataFrame(raw_data).set_index('time')

    # 2. 统计逻辑
    def process(freq_str):
        # resample 聚合数据
        res = df.resample(freq_str).sum()
        # 计算环比涨跌幅 (pct_change)
        res['change_%'] = (res['amount'].pct_change() * 100).round(2)
        return res

    # 3. 输出结果
    print(f"\n🚀 分析目标: {target_symbol} | 数据来源: {file_path}")
    
    for freq in ['1min', '5min']:
        print(f"\n--- {freq} 统计 ---")
        result = process(freq)
        # 仅显示有爆仓金额的时段
        print(result[result['amount'] > 0].to_string())

if __name__ == "__main__":
    # 检查参数：脚本名 文件名 币种
    if len(sys.argv) < 3:
        print("💡 使用方法: python analyze.py <文件名> <Symbol>")
        print("   例如: python analyze.py data.json QQQUSDT")
    else:
        file_input = sys.argv[1]
        symbol_input = sys.argv[2]
        analyze_liquidations(file_input, symbol_input)

