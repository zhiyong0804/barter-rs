use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info};

use crate::{
    quotation::trade_window::TradeItem,
    signal::{SignalLevel, SignalType, TradeSignalBase},
    strategy::frame::{FrameOrderType, OrderRequest, PositionSide},
};

use super::{StrategyContext, StrategyModule};

/// Rocket策略：检测突发巨量单浪行情
/// 触发条件：
/// 1. 最近30秒内，有2秒的每秒成交量是平均每秒成交量的10倍以上，其中有一秒的成交量是20倍以上
/// 2. 这两秒的close > open，close价格都大于最近10分钟的high
/// 3. 这两秒的实体部分都大于50%
/// 4. 第二个巨量成交量秒的close一定要大于第一个巨量成交量秒的open
#[derive(Debug, Clone)]
pub struct RocketSignalContext {
    pub id: u64,
    pub name: String,
    pub symbols: Vec<String>,
    pub rocket_ctxs: HashMap<String, RocketOrderContext>,
    pub started: bool,
    /// 下单的USDT数量
    pub order_usdt: f64,
    /// 是否启用自动下单
    pub enable: bool,
    include_all_symbols: bool,
    included_symbols: HashSet<String>,
    excluded_symbols: HashSet<String>,
    config: RocketModuleConfig,
}

impl Default for RocketSignalContext {
    fn default() -> Self {
        Self {
            id: 3, // module_id::ROCKET_SIGNAL
            name: "strategy.rocket".to_owned(),
            symbols: Vec::new(),
            rocket_ctxs: HashMap::new(),
            started: false,
            order_usdt: 10.0,
            enable: false,
            include_all_symbols: true,
            included_symbols: HashSet::new(),
            excluded_symbols: HashSet::new(),
            config: RocketModuleConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RocketOrderContext {
    pub symbol: String,
    pub last_signal_time: u64,
    /// 最近一次检测到的两个巨量秒
    pub last_detected_seconds: Option<DetectedRocketSeconds>,
}

#[derive(Debug, Clone, Copy)]
pub struct DetectedRocketSeconds {
    pub first_second: u64,
    pub second_second: u64,
    pub first_close: f64,
    pub second_close: f64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RocketModuleConfig {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub excluded_symbols: Vec<String>,
    /// 成交量倍数阈值（10倍）
    #[serde(default = "default_volume_multiplier_threshold")]
    pub volume_multiplier_threshold: f64,
    /// 极值倍数（20倍）
    #[serde(default = "default_volume_multiplier_extreme")]
    pub volume_multiplier_extreme: f64,
    /// 实体比例阈值（50%）
    #[serde(default = "default_entity_ratio_threshold")]
    pub entity_ratio_threshold: f64,
    /// 回溯秒数（30秒）
    #[serde(default = "default_lookback_seconds")]
    pub lookback_seconds: usize,
    /// 回溯分钟数（10分钟）
    #[serde(default = "default_lookback_minutes")]
    pub lookback_minutes: usize,
    /// 信号冷却秒数
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,
    /// 两个巨量秒之间的最大间隔秒数
    #[serde(default = "default_max_second_gap")]
    pub max_second_gap: u64,
    /// 价格上升幅度最小值（1%）
    #[serde(default = "default_min_price_increase_ratio")]
    pub min_price_increase_ratio: f64,
    /// 最大成交量秒的价格上升幅度最小值（3%）
    #[serde(default = "default_max_volume_price_increase_ratio")]
    pub max_volume_price_increase_ratio: f64,
    /// 下单的USDT数量
    #[serde(default = "default_order_usdt")]
    pub order_usdt: f64,
    /// 是否启用自动下单
    #[serde(default = "default_enable")]
    pub enable: bool,
}

fn default_volume_multiplier_threshold() -> f64 {
    10.0
}

fn default_volume_multiplier_extreme() -> f64 {
    20.0
}

fn default_entity_ratio_threshold() -> f64 {
    0.5
}

fn default_lookback_seconds() -> usize {
    45
}

fn default_lookback_minutes() -> usize {
    10
}

fn default_cooldown_seconds() -> u64 {
    60
}

fn default_max_second_gap() -> u64 {
    5
}

fn default_min_price_increase_ratio() -> f64 {
    0.01 // 1%
}

fn default_max_volume_price_increase_ratio() -> f64 {
    0.03 // 3%
}

fn default_order_usdt() -> f64 {
    10.0
}

fn default_enable() -> bool {
    false
}

pub struct RocketSignalModule {
    pub order_tx: Option<UnboundedSender<OrderRequest>>,
    pub cfg: RocketSignalContext,
    order_seq: AtomicU64,
}

impl Default for RocketSignalModule {
    fn default() -> Self {
        Self {
            order_tx: None,
            cfg: RocketSignalContext::default(),
            order_seq: AtomicU64::new(1),
        }
    }
}

impl RocketSignalModule {
    pub fn with_config(config: RocketModuleConfig) -> Self {
        let include_all_symbols = config.symbols.is_empty()
            || config
                .symbols
                .iter()
                .any(|s| s.trim().eq_ignore_ascii_case("*"));

        let included_symbols = config
            .symbols
            .iter()
            .filter(|s| !s.trim().eq_ignore_ascii_case("*"))
            .map(|s| s.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let excluded_symbols = config
            .excluded_symbols
            .iter()
            .map(|s| s.to_ascii_uppercase())
            .collect::<HashSet<_>>();

        let mut ctx = RocketSignalContext::default();
        ctx.symbols = config.symbols.clone();
        ctx.order_usdt = config.order_usdt;
        ctx.enable = config.enable;
        ctx.include_all_symbols = include_all_symbols;
        ctx.included_symbols = included_symbols;
        ctx.excluded_symbols = excluded_symbols;
        ctx.config = config;

        Self {
            cfg: ctx,
            ..Default::default()
        }
    }

    /// Attach an order channel.  Call this before starting the module.
    pub fn with_order_tx(mut self, tx: UnboundedSender<OrderRequest>) -> Self {
        self.order_tx = Some(tx);
        self
    }

    fn next_order_id(&self) -> u64 {
        self.order_seq.fetch_add(1, Ordering::Relaxed)
    }

    fn submit_order(&self, req: OrderRequest) {
        if let Some(tx) = &self.order_tx {
            if tx.send(req).is_err() {
                tracing::error!(strategy = %self.cfg.name, "failed to send order: channel closed");
            }
        } else {
            tracing::warn!(strategy = %self.cfg.name, "order_tx is not set, skip order submission");
        }
    }

    fn place_market_buy(&self, symbol: &str, qty: f64, tag: &str) {
        if qty <= 0.0 || !qty.is_finite() {
            return;
        }

        let seq = self.next_order_id();
        let cid = format!("rk_buy_{}_{}", tag, seq);
        let req = OrderRequest {
            client_order_id: cid.clone(),
            symbol: symbol.to_owned(),
            side: PositionSide::Long,
            order_type: FrameOrderType::Market,
            price: 0.0,
            stop_price: 0.0,
            qty,
            reduce_only: false,
            close_position: false,
            good_till_date: 0,
            strategy_id: self.cfg.id,
            origin_client_order_id: None,
        };

        tracing::info!(
            strategy = %self.cfg.name,
            symbol,
            qty,
            tag,
            client_order_id = %cid,
            "Rocket: market BUY"
        );
        self.submit_order(req);
    }

    fn symbol_enabled(&self, symbol: &str) -> bool {
        if self.cfg.excluded_symbols.contains(symbol) {
            return false;
        }
        if self.cfg.include_all_symbols {
            return true;
        }
        self.cfg.included_symbols.contains(symbol)
    }

    /// 检查rocket条件（不可变）
    fn check_rocket_conditions(
        &self,
        _symbol: &str,
        last_signal_time: u64,
        now_sec: u64,
        trade_window: &crate::quotation::trade_window::UhfTradeWindow,
    ) -> Option<RocketSignalInfo> {
        // 检查冷却时间
        if now_sec - last_signal_time < self.cfg.config.cooldown_seconds {
            return None;
        }

        // 检查24小时成交额是否大于2000万
        if trade_window.hours_window.tf_total_value < 20_000_000.0 {
            return None;
        }

        // 收集最近N秒的秒级数据
        let lookback_seconds = self.cfg.config.lookback_seconds;
        let mut recent_seconds = Vec::new();

        for second_item in &trade_window.seconds_window.items {
            if now_sec - second_item.second < lookback_seconds as u64 {
                recent_seconds.push(second_item.clone());
            }
        }

        if recent_seconds.len() < 2 {
            return None;
        }

        // 计算平均每秒成交量：24小时成交量 / (24 * 3600 秒)
        let avg_qty = trade_window.hours_window.tf_total_qty / (24.0 * 3600.0);

        if avg_qty <= 0.0 {
            return None;
        }

        // 找出所有满足条件的巨量秒（10倍及以上）
        let mut large_volume_seconds = Vec::new();
        for second_item in recent_seconds.iter() {
            let volume_ratio = second_item.qty / avg_qty;
            if volume_ratio >= self.cfg.config.volume_multiplier_threshold {
                large_volume_seconds.push((second_item.clone(), volume_ratio));
            }
        }

        // 需要至少2个10倍以上的成交量秒，其中至少1个是20倍以上
        if large_volume_seconds.len() < 2 {
            return None;
        }

        let has_extreme = large_volume_seconds
            .iter()
            .any(|(_, ratio)| *ratio >= self.cfg.config.volume_multiplier_extreme);

        if !has_extreme {
            return None;
        }

        // 尝试配对两个巨量秒
        for i in 0..large_volume_seconds.len() - 1 {
            for j in i + 1..large_volume_seconds.len() {
                let (first_second, first_ratio) = &large_volume_seconds[i];
                let (second_second, second_ratio) = &large_volume_seconds[j];

                // 两秒应该相对靠近（最多相隔max_second_gap秒）
                if second_second.second - first_second.second > self.cfg.config.max_second_gap {
                    continue;
                }

                // 根据第一个巨量秒所在分钟的前10分钟获取最高价格
                let ten_min_high = self.get_10min_high(trade_window, first_second.second);
                if ten_min_high.is_nan() {
                    continue;
                }

                // 检查两秒是否都满足条件
                if self.check_rocket_pair_conditions(
                    first_second,
                    second_second,
                    ten_min_high,
                    *first_ratio >= self.cfg.config.volume_multiplier_extreme,
                    *second_ratio >= self.cfg.config.volume_multiplier_extreme,
                ) {
                    // 返回信号信息
                    return Some(RocketSignalInfo {
                        signal_time: second_second.event_time,
                        first_ratio: *first_ratio,
                        second_ratio: *second_ratio,
                        first_open: first_second.open,
                        first_high: first_second.high,
                        first_low: first_second.low,
                        first_close: first_second.close,
                        second_open: second_second.open,
                        second_high: second_second.high,
                        second_low: second_second.low,
                        second_close: second_second.close,
                        ten_min_high,
                        first_second_time: first_second.second,
                        second_second_time: second_second.second,
                    });
                }
            }
        }

        None
    }

    /// 检查两个巨量秒是否满足所有rocket条件
    fn check_rocket_pair_conditions(
        &self,
        first: &crate::quotation::trade_window::SecondTradeItem,
        second: &crate::quotation::trade_window::SecondTradeItem,
        ten_min_high: f64,
        first_is_extreme: bool,
        second_is_extreme: bool,
    ) -> bool {
        use std::f64;

        // 至少有一个需要是20倍以上
        if !first_is_extreme && !second_is_extreme {
            return false;
        }

        // 条件2a: close > open
        if !(first.close > first.open) {
            return false;
        }
        if !(second.close > second.open) {
            return false;
        }

        // 条件2b: close > 10分钟最高点
        if !(first.close > ten_min_high) {
            return false;
        }
        if !(second.close > ten_min_high) {
            return false;
        }

        // 条件3: 实体部分 > 50%
        let first_entity = first.close - first.open;
        let first_height = first.high - first.low;
        if first_height > f64::EPSILON {
            let first_entity_ratio = first_entity / first_height;
            if first_entity_ratio < self.cfg.config.entity_ratio_threshold {
                return false;
            }
        } else {
            return false;
        }

        let second_entity = second.close - second.open;
        let second_height = second.high - second.low;
        if second_height > f64::EPSILON {
            let second_entity_ratio = second_entity / second_height;
            if second_entity_ratio < self.cfg.config.entity_ratio_threshold {
                return false;
            }
        } else {
            return false;
        }

        // 条件4: 第二秒close > 第一秒open
        if !(second.close > first.open) {
            return false;
        }

        // 条件5: 两秒的价格上升幅度都要大于1%
        let first_price_increase_ratio = if first.open > f64::EPSILON {
            (first.close - first.open) / first.open
        } else {
            return false;
        };

        if first_price_increase_ratio < self.cfg.config.min_price_increase_ratio {
            return false;
        }

        let second_price_increase_ratio = if second.open > f64::EPSILON {
            (second.close - second.open) / second.open
        } else {
            return false;
        };

        if second_price_increase_ratio < self.cfg.config.min_price_increase_ratio {
            return false;
        }

        // 条件6: 最大成交量秒的价格变动要大于3%
        let max_volume_price_ratio = if first.qty >= second.qty {
            first_price_increase_ratio
        } else {
            second_price_increase_ratio
        };

        if max_volume_price_ratio < self.cfg.config.max_volume_price_increase_ratio {
            return false;
        }

        true
    }

    /// 获取第一个巨量秒所在分钟的前lookback_minutes分钟的最高价格
    fn get_10min_high(
        &self,
        trade_window: &crate::quotation::trade_window::UhfTradeWindow,
        first_second_time: u64,
    ) -> f64 {
        let lookback_minutes = self.cfg.config.lookback_minutes;
        let mut max_high = f64::NEG_INFINITY;

        let minutes_count = trade_window.minutes_window.items.len();
        if minutes_count == 0 {
            return f64::NAN;
        }

        // 根据第一个巨量秒的时间戳找到对应的分钟索引
        let first_second_minute = first_second_time / 60; // 转换为分钟时间戳
        let mut reference_minute_idx = None;

        for (idx, minute_item) in trade_window.minutes_window.items.iter().enumerate() {
            // minute_item.second 也是秒级时间戳，也需要除以60转换为分钟时间戳
            let minute_time = minute_item.second / 60;
            if minute_time == first_second_minute {
                reference_minute_idx = Some(idx);
                break;
            }
        }

        // 如果找到了参考分钟，从该分钟向前查lookback_minutes分钟
        if let Some(ref_idx) = reference_minute_idx {
            let start_idx = if ref_idx >= lookback_minutes {
                ref_idx - lookback_minutes + 1 // +1是因为要包含ref_idx本身
            } else {
                0
            };

            for idx in start_idx..=ref_idx {
                if idx < minutes_count {
                    let minute_item = &trade_window.minutes_window.items[idx];
                    if !minute_item.high.is_nan() && minute_item.high > max_high {
                        max_high = minute_item.high;
                    }
                }
            }
        } else {
            return f64::NAN;
        }

        if max_high.is_infinite() {
            f64::NAN
        } else {
            max_high
        }
    }
}

/// 信号信息结构体方便返回多个值
#[derive(Debug, Clone)]
struct RocketSignalInfo {
    signal_time: u64,
    first_ratio: f64,
    second_ratio: f64,
    first_open: f64,
    first_high: f64,
    first_low: f64,
    first_close: f64,
    second_open: f64,
    second_high: f64,
    second_low: f64,
    second_close: f64,
    ten_min_high: f64,
    first_second_time: u64,
    second_second_time: u64,
}

impl StrategyModule for RocketSignalModule {
    fn id(&self) -> u64 {
        self.cfg.id
    }

    fn name(&self) -> &str {
        &self.cfg.name
    }

    fn init(
        &mut self,
        _ctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cfg.rocket_ctxs.clear();
        self.cfg.started = false;
        Ok(())
    }

    fn start(
        &mut self,
        sctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cfg.started = true;
        info!(
            strategy = self.name(),
            strategy_id = self.id(),
            symbols = %self.cfg.symbols.join(","),
            excluded_symbols = %self.cfg.excluded_symbols.iter().cloned().collect::<Vec<_>>().join(","),
            volume_multiplier_threshold = self.cfg.config.volume_multiplier_threshold,
            volume_multiplier_extreme = self.cfg.config.volume_multiplier_extreme,
            entity_ratio_threshold = self.cfg.config.entity_ratio_threshold,
            lookback_seconds = self.cfg.config.lookback_seconds,
            lookback_minutes = self.cfg.config.lookback_minutes,
            cooldown_seconds = self.cfg.config.cooldown_seconds,
            min_price_increase_ratio = self.cfg.config.min_price_increase_ratio,
            max_volume_price_increase_ratio = self.cfg.config.max_volume_price_increase_ratio,
            "rocket module started"
        );

        let mut count = 0;
        for symbol in sctx.exchange_info.keys() {
            let symbol_upper = symbol.to_ascii_uppercase();
            if !self.symbol_enabled(&symbol_upper) {
                continue;
            }

            count += 1;
            self.cfg.rocket_ctxs.insert(
                symbol.clone(),
                RocketOrderContext {
                    symbol: symbol.clone(),
                    last_signal_time: 0,
                    last_detected_seconds: None,
                },
            );
        }

        info!(
            strategy = self.name(),
            count = count,
            "rocket symbols initialized"
        );

        Ok(())
    }

    fn handle_best_bid_ask(
        &mut self,
        ctx: &mut StrategyContext,
        bba: &crate::quotation::trade_window::BestBidAskItem,
    ) {
        if !self.symbol_enabled(&bba.symbol) {
            return;
        }

        let Some(trade_window) = ctx.trades.get(&bba.symbol) else {
            return;
        };

        if !trade_window.ready {
            return;
        }

        let now_sec = trade_window.last_now_sec;
        let symbol = bba.symbol.clone();

        // 先进行条件检查（不可变）
        let last_signal_time = self
            .cfg
            .rocket_ctxs
            .get(&symbol)
            .map(|ctx| ctx.last_signal_time)
            .unwrap_or(0);

        if let Some(signal_info) =
            self.check_rocket_conditions(&symbol, last_signal_time, now_sec, trade_window)
        {
            // 然后更新状态（可变）
            if let Some(symbol_ctx) = self.cfg.rocket_ctxs.get_mut(&symbol) {
                symbol_ctx.last_signal_time = now_sec;
                symbol_ctx.last_detected_seconds = Some(DetectedRocketSeconds {
                    first_second: signal_info.first_second_time,
                    second_second: signal_info.second_second_time,
                    first_close: signal_info.first_close,
                    second_close: signal_info.second_close,
                });

                debug!(
                    strategy = %self.cfg.name,
                    symbol = %symbol,
                    trigger_time = signal_info.signal_time,
                    first_second = signal_info.first_second_time,
                    second_second = signal_info.second_second_time,
                    first_qty_ratio = signal_info.first_ratio,
                    second_qty_ratio = signal_info.second_ratio,
                    first_ohlc = %format!(
                        "{:.6}/{:.6}/{:.6}/{:.6}",
                        signal_info.first_open,
                        signal_info.first_high,
                        signal_info.first_low,
                        signal_info.first_close
                    ),
                    second_ohlc = %format!(
                        "{:.6}/{:.6}/{:.6}/{:.6}",
                        signal_info.second_open,
                        signal_info.second_high,
                        signal_info.second_low,
                        signal_info.second_close
                    ),
                    ten_min_high = signal_info.ten_min_high,
                    order_enable = self.cfg.enable,
                    order_usdt = self.cfg.order_usdt,
                    tf_total_qty = trade_window.hours_window.tf_total_qty,
                    tf_total_value = trade_window.hours_window.tf_total_value,
                    "rocket signal triggered"
                );

                let context = format!(
                    "Rocket detected: first_qty_ratio={:.1}x, second_qty_ratio={:.1}x, \
                     first_close={:.4}, second_close={:.4}, 10min_high={:.4}",
                    signal_info.first_ratio,
                    signal_info.second_ratio,
                    signal_info.first_close,
                    signal_info.second_close,
                    signal_info.ten_min_high
                );

                if self.cfg.enable {
                    let buy_price = signal_info.second_close;
                    let qty = if buy_price > 0.0 {
                        self.cfg.order_usdt / buy_price
                    } else {
                        0.0
                    };

                    if qty > 0.0 && qty.is_finite() {
                        self.place_market_buy(&symbol, qty, "rocket");
                    } else {
                        tracing::warn!(
                            strategy = %self.cfg.name,
                            symbol = %symbol,
                            order_usdt = self.cfg.order_usdt,
                            buy_price,
                            "Rocket: invalid buy qty, skip order"
                        );
                    }
                }

                // 这里可以发送信号到外部系统
                let _ = context;
            }
        }
    }
}
