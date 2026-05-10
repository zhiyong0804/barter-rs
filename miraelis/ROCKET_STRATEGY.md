# Rocket策略 - 突发巨量单浪行情检测

## 策略概述

Rocket策略专门用于检测加密货币期货市场中的突发巨量单浪行情。它通过分析秒级成交量和价格行为来识别强势上升浪。

## 触发条件

rocket策略触发需要同时满足以下所有条件：

### 1. 巨量成交检测（两秒需求）
- **阈值**：最近30秒内需要至少有2个秒的每秒成交量达到平均每秒成交量的**10倍以上**
- **极值要求**：这2个秒中，**至少有1个** 的成交量达到**20倍以上**
- **时间间隔**：两个巨量秒之间的最大间隔为5秒

### 2. 价格方向性（两秒都需满足）
- `close > open`：两秒的收盘价都必须高于开盘价（强势K线）
- 两秒的收盘价都必须**高于最近10分钟的最高价**

### 3. 实体比例（两秒都需满足）
- **实体比例** = `(close - open) / (high - low)` > **50%**
- 这保证了K线有充分的实体部分，而不是仅有长上影线

### 4. 递进性（第二秒约束）
- **第二个巨量秒的close > 第一个巨量秒的open**
- 这确保两浪呈现递进上升的形态

## 配置参数

在`config.json`中的`rocket_cfg`部分配置：

```json
{
  "rocket_cfg": {
    "symbols": ["ETHUSDT", "BTCUSDT"],       // 交易对列表，为空则监控exchange_info中的全部
    "excluded_symbols": ["USDC"],             // 排除列表
    "volume_multiplier_threshold": 10.0,      // 成交量倍数阈值（默认10倍）
    "volume_multiplier_extreme": 20.0,        // 极值倍数（默认20倍）
    "entity_ratio_threshold": 0.5,            // 实体比例阈值（默认50%, 取值0-1）
    "lookback_seconds": 30,                   // 回溯窗口（秒）
    "lookback_minutes": 10,                   // 计算最高价的回溯窗口（分钟）
    "cooldown_seconds": 300,                  // 同一symbol的信号冷却时间（秒）
    "max_second_gap": 5,                      // 两个巨量秒之间的最大间隔（秒）
    "order_usdt": 10.0,                       // 下单的USDT数量
    "enable": true                            // 是否启用自动下单
  }
}
```

### 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `volume_multiplier_threshold` | 10.0 | 定义"巨量"的阈值倍数。越小越敏感 |
| `volume_multiplier_extreme` | 20.0 | 极端巨量倍数。需要至少一个秒达到此倍数 |
| `entity_ratio_threshold` | 0.5 | K线实体比例阈值。范围0-1，值越大越严格 |
| `lookback_seconds` | 30 | 回溯窗口，用于计算平均成交量。增大可降低噪音 |
| `lookback_minutes` | 10 | 计算10分钟高点时的分钟数 |
| `cooldown_seconds` | 60 | 同一coin避免连续触发的等待时间 |
| `max_second_gap` | 5 | 允许的两个巨量秒间的最大间隔 |
| `order_usdt` | 10.0 | 下单的USDT数量（当enable为true时执行） |
| `enable` | false | 是否启用自动下单（false则仅检测信号不下单） |

## 数学表达

设：
- `qty_i` = 第i秒的成交量
- `avg_qty` = 最近30秒的平均成交量
- `open, close, high, low` = 秒级K线的OHLC

触发条件：
```
EXISTS i, j where j > i, (j - i) <= max_second_gap:
  1. qty_i >= 10 * avg_qty && qty_j >= 10 * avg_qty
  2. (EXISTS k in {i,j}: qty_k >= 20 * avg_qty)
  3. close_i > open_i && close_j > open_j
  4. close_i > high_10min && close_j > high_10min
  5. (close_i - open_i) / (high_i - low_i) > 0.5
  6. (close_j - open_j) / (high_j - low_j) > 0.5
  7. close_j > open_i
```

## 策略逻辑流程

```
Trade Event → 检查symbol是否启用
           ↓
        累积秒级数据
           ↓
        计算平均成交量
           ↓
        筛选10倍以上的秒
           ↓
        至少2个且有1个20倍？ → NO → 返回
           ↓
        YES
           ↓
        获取10分钟最高价
           ↓
        配对巨量秒 → 检查所有条件
           ↓
        条件全部满足 → 发送信号
```

## 使用场景

### 适用于：
- 🚀 **突发巨量的真实上升浪** - 识别强势单浪行情
- 📊 **交易所聚合单** - 检测大额买单推动的行情
- ⚡ **高波动率时段** - 特别是公告后、美联储声明等事件后

### 不适用于：
- ❌ **振荡盘整** - 成交量不足或方向混乱
- ❌ **缓慢上升** - 需要快速的突发巨量
- ❌ **长期趋势** - 只看秒级和10分钟级别

## 输出信号格式

```
Rocket detected: 
  first_qty_ratio=15.3x, 
  second_qty_ratio=22.5x,
  first_close=99.234,
  second_close=99.456,
  10min_high=99.100
```

## 集成建议

1. **配合其他策略**：在Frame策略等之前运行，避免冲突
2. **头寸管理**：由于是突发行情，建议小头寸、快进快出
3. **止损设置**：第二浪close + 5秒内高点
4. **风险控制**：限制同时rocket信号的最大仓位数

## 性能优化

- 信号已带cooldown，避免同一coin在60秒内重复触发
- 秒级数据缓存66秒窗口，内存占用极低
- 所有条件检查都在内存中完成，无外部调用

## 调试技巧

启用debug日志查看详细信息：

```bash
RUST_LOG=debug ./miraelis --config config.json
```

关键日志：
- `Rocket signal triggered` - 信号触发
- `symbol_enabled` - symbol检查结果
- `check_rocket_conditions` - 条件检查过程
