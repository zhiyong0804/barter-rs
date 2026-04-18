use serde::Deserialize;
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    quotation::trade_window::{QuotationKline, TradeWindowPriceQty, UhfTradeWindow},
    signal::{HugeMomentumSignal, SignalLevel, SignalType, TradeSignalBase},
};

use super::{module_id, QuotationTicker, StrategyContext, StrategyModule};

#[derive(Debug, Clone, Default)]
pub struct HugeMomentumSymbolContext {
    pub symbol: String,
    pub last_notified: u64,
    pub breakout_timestamp: u64,
    pub breakout_price: f64,
    pub ignite_active: bool,
}

#[derive(Debug, Clone)]
pub struct HugeMomentumSignalCtx {
    pub id: u64,
    pub name: String,
    pub started: bool,
    pub started_timestamp: u64,
    pub tf_24h_qty_value_threshold: f64,
    pub price_15m_change_threshold: f64,
    pub qty_15m_times_threshold: f64,
    pub qty_5m_total_multiple: f64,
    pub required_minutes: usize,
    pub confirm_score_threshold: f64,
    pub pending_valid_seconds: u64,
    pub cooldown_seconds: u64,
    pub symbol_contexts: HashMap<String, HugeMomentumSymbolContext>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HugeMomentumModuleConfig {
    #[serde(default = "default_tf_24h_qty_value_threshold")]
    pub tf_24h_qty_value_threshold: f64,
    #[serde(default = "default_price_15m_change_threshold")]
    pub price_15m_change_threshold: f64,
    #[serde(default = "default_qty_15m_times_threshold")]
    pub qty_15m_times_threshold: f64,
    #[serde(default = "default_qty_5m_total_multiple")]
    pub qty_5m_total_multiple: f64,
    #[serde(default = "default_required_minutes")]
    pub required_minutes: usize,
    #[serde(default = "default_confirm_score_threshold")]
    pub confirm_score_threshold: f64,
    #[serde(default = "default_pending_valid_seconds")]
    pub pending_valid_seconds: u64,
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,
}

fn default_tf_24h_qty_value_threshold() -> f64 {
    10_000_000.0
}
fn default_price_15m_change_threshold() -> f64 {
    0.008
}
fn default_qty_15m_times_threshold() -> f64 {
    20.0
}
fn default_qty_5m_total_multiple() -> f64 {
    5.0
}
fn default_required_minutes() -> usize {
    120
}
fn default_confirm_score_threshold() -> f64 {
    2.2
}
fn default_pending_valid_seconds() -> u64 {
    15 * 60
}
fn default_cooldown_seconds() -> u64 {
    45 * 60
}

impl Default for HugeMomentumModuleConfig {
    fn default() -> Self {
        Self {
            tf_24h_qty_value_threshold: default_tf_24h_qty_value_threshold(),
            price_15m_change_threshold: default_price_15m_change_threshold(),
            qty_15m_times_threshold: default_qty_15m_times_threshold(),
            qty_5m_total_multiple: default_qty_5m_total_multiple(),
            required_minutes: default_required_minutes(),
            confirm_score_threshold: default_confirm_score_threshold(),
            pending_valid_seconds: default_pending_valid_seconds(),
            cooldown_seconds: default_cooldown_seconds(),
        }
    }
}

impl Default for HugeMomentumSignalCtx {
    fn default() -> Self {
        Self {
            id: module_id::HUGE_MOMENTUM_SIGNAL,
            name: "strategy.huge.momentum.signal".to_owned(),
            started: false,
            started_timestamp: 0,
            tf_24h_qty_value_threshold: 10_000_000.0,
            price_15m_change_threshold: 0.008,
            qty_15m_times_threshold: 20.0,
            qty_5m_total_multiple: 5.0,
            required_minutes: 120,
            confirm_score_threshold: 2.2,
            pending_valid_seconds: 15 * 60,
            cooldown_seconds: 45 * 60,
            symbol_contexts: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct Aggregated5mBar {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    qty: f64,
    bid_qty: f64,
    second: u64,
}

pub struct HugeMomentumSignalModule {
    pub cfg: HugeMomentumSignalCtx,
    pub last_check_second: u64,
}

impl Default for HugeMomentumSignalModule {
    fn default() -> Self {
        Self {
            cfg: HugeMomentumSignalCtx::default(),
            last_check_second: 0,
        }
    }
}

impl HugeMomentumSignalModule {
    pub fn with_config(config: HugeMomentumModuleConfig) -> Self {
        let mut ctx = HugeMomentumSignalCtx::default();
        ctx.tf_24h_qty_value_threshold = config.tf_24h_qty_value_threshold;
        ctx.price_15m_change_threshold = config.price_15m_change_threshold;
        ctx.qty_15m_times_threshold = config.qty_15m_times_threshold;
        ctx.qty_5m_total_multiple = config.qty_5m_total_multiple;
        ctx.required_minutes = config.required_minutes;
        ctx.confirm_score_threshold = config.confirm_score_threshold;
        ctx.pending_valid_seconds = config.pending_valid_seconds;
        ctx.cooldown_seconds = config.cooldown_seconds;

        Self {
            cfg: ctx,
            last_check_second: 0,
        }
    }

    fn now_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0)
    }

    fn safe_div(numerator: f64, denominator: f64) -> f64 {
        if denominator <= 0.0 || !denominator.is_finite() {
            0.0
        } else {
            numerator / denominator
        }
    }

    fn valid_kline(kline: &TradeWindowPriceQty) -> bool {
        kline.open.is_finite()
            && kline.close.is_finite()
            && kline.high.is_finite()
            && kline.low.is_finite()
            && kline.open > 0.0
            && kline.high >= kline.low
    }

    fn price_change(open: f64, close: f64) -> f64 {
        if open <= 0.0 || !open.is_finite() || !close.is_finite() {
            return 0.0;
        }
        (close - open) / open
    }

    fn reset_symbol_state(symbol_ctx: &mut HugeMomentumSymbolContext) {
        symbol_ctx.breakout_timestamp = 0;
        symbol_ctx.breakout_price = 0.0;
        symbol_ctx.ignite_active = false;
    }

    fn build_recent_5m(recent_minutes: &[TradeWindowPriceQty]) -> Vec<Aggregated5mBar> {
        let mut bars = Vec::with_capacity(recent_minutes.len() / 5);
        let mut end = recent_minutes.len() as i32;
        while end >= 5 {
            let start = end - 5;
            let start_idx = start as usize;
            let end_idx = end as usize;

            let first = &recent_minutes[start_idx];
            let last = &recent_minutes[end_idx - 1];

            let mut bar = Aggregated5mBar {
                open: first.open,
                high: first.high,
                low: first.low,
                close: last.close,
                qty: 0.0,
                bid_qty: 0.0,
                second: last.second,
            };

            for item in &recent_minutes[start_idx..end_idx] {
                bar.high = bar.high.max(item.high);
                bar.low = bar.low.min(item.low);
                bar.qty += item.qty;
                bar.bid_qty += item.bid_qty;
            }

            bars.push(bar);
            end -= 5;
        }

        bars.reverse();
        bars
    }

    pub fn check(
        &mut self,
        symbol: &str,
        tw: &UhfTradeWindow,
        current: u64,
    ) -> Option<HugeMomentumSignal> {
        if current < self.cfg.started_timestamp + 120 {
            return None;
        }

        if !Self::precheck_trade_window(tw, self.cfg.required_minutes) {
            return None;
        }

        let Some(recent_minutes) = Self::collect_recent_minutes(tw, current) else {
            return None;
        };
        if recent_minutes.len() < 3 {
            return None;
        }

        let latest_minute = *recent_minutes.last().expect("checked len");
        let recent_5m = Self::build_recent_5m(&recent_minutes);
        if recent_5m.len() < 3 {
            return None;
        }

        let vol_base_range_end = 120_usize.min(tw.minutes_window.items.len().saturating_sub(1));
        if vol_base_range_end < 15 {
            return None;
        }
        let Some(vol_base) = Self::compute_vol_base(tw, current, 15, vol_base_range_end) else {
            return None;
        };

        let qty_15m: f64 = recent_minutes.iter().map(|m| m.qty).sum();
        let _qty_15m_times = Self::safe_div(qty_15m, vol_base);
        let price_15m_change = Self::price_change(recent_minutes[0].open, latest_minute.close);

        let b1 = &recent_5m[recent_5m.len() - 3];
        let b2 = &recent_5m[recent_5m.len() - 2];
        let b3 = &recent_5m[recent_5m.len() - 1];
        let q1 = b1.qty;
        let q2 = b2.qty;
        let q3 = b3.qty;
        let qsum = q1 + q2 + q3;

        let threshold_each_5m = vol_base * self.cfg.qty_5m_total_multiple;
        let threshold_total_15m = vol_base * self.cfg.qty_15m_times_threshold;
        let three_5m_valid = q1 >= threshold_each_5m
            && q2 >= threshold_each_5m
            && q3 >= threshold_each_5m
            && qsum >= threshold_total_15m;

        let recent_105m_high = Self::compute_recent_105m_high(tw, current, 15, vol_base_range_end);
        let up_break_105m_high = recent_105m_high > 0.0 && latest_minute.close > recent_105m_high;
        let price_15m_valid = price_15m_change >= self.cfg.price_15m_change_threshold;
        let ignite_triggered = three_5m_valid && up_break_105m_high && price_15m_valid;

        tracing::debug!(
            strategy = self.name(),
            strategy_id = self.id(),
            symbol = symbol,
            ignite_triggered,
            three_5m_valid,
            up_break_105m_high,
            price_15m_valid,
            q1,
            q2,
            q3,
            qsum,
            threshold_each_5m,
            threshold_total_15m,
            vol_base,
            recent_105m_high,
            latest_close = latest_minute.close,
            price_15m_change,
            tf_24h_qty = tw.hours_window.tf_total_qty,
            tf_24h_value = tw.hours_window.tf_total_value,
            tf_24h_qty_value_threshold = self.cfg.tf_24h_qty_value_threshold,
            "huge momentum ignite evaluation"
        );

        let tf_24h_qty = tw.hours_window.tf_total_qty;
        let tf_24h_value = tw.hours_window.tf_total_value;
        if tf_24h_value < self.cfg.tf_24h_qty_value_threshold || tf_24h_qty <= 0.0 {
            return None;
        }

        let symbol_ctx = self
            .cfg
            .symbol_contexts
            .entry(symbol.to_owned())
            .or_insert_with(|| HugeMomentumSymbolContext {
                symbol: symbol.to_owned(),
                ..HugeMomentumSymbolContext::default()
            });

        if symbol_ctx.ignite_active {
            let stale_seconds = 300_u64;
            if symbol_ctx.breakout_price <= 0.0
                || latest_minute.second > symbol_ctx.breakout_timestamp + stale_seconds
                || latest_minute.close < symbol_ctx.breakout_price
            {
                Self::reset_symbol_state(symbol_ctx);
            }
        }

        let ignite_just_activated = ignite_triggered && !symbol_ctx.ignite_active;
        if ignite_just_activated {
            symbol_ctx.ignite_active = true;
            symbol_ctx.breakout_timestamp = latest_minute.second;
            symbol_ctx.breakout_price = recent_105m_high;

            let qty_5m_base = vol_base * 5.0;
            let signal = HugeMomentumSignal {
                base: TradeSignalBase::new(
                    SignalType::HugeMomentum,
                    self.cfg.name.clone(),
                    self.cfg.id,
                    SignalLevel::Normal,
                    "IGNITE",
                    latest_minute.second,
                    symbol.to_owned(),
                ),
                latest_5m_ohlc: [
                    [b1.open, b1.high, b1.low, b1.close],
                    [b2.open, b2.high, b2.low, b2.close],
                    [b3.open, b3.high, b3.low, b3.close],
                ],
                latest_5m_qty: [q1, q2, q3],
                latest_5m_qty_times: [
                    Self::safe_div(q1, qty_5m_base),
                    Self::safe_div(q2, qty_5m_base),
                    Self::safe_div(q3, qty_5m_base),
                ],
                val_base: vol_base,
                latest_24h_qty: tf_24h_qty,
                latest_24h_value: tf_24h_value,
            };

            symbol_ctx.last_notified = latest_minute.second;
            return Some(signal);
        }

        None
    }

    fn precheck_trade_window(tw: &UhfTradeWindow, required_minutes: usize) -> bool {
        tw.minutes_window.items.len() >= required_minutes
    }

    fn collect_recent_minutes(
        tw: &UhfTradeWindow,
        current: u64,
    ) -> Option<Vec<TradeWindowPriceQty>> {
        let mut recent = Vec::with_capacity(15);
        for i in (1..=15).rev() {
            let item = tw.get_target_closed_minute_price_qty(-(i as i32), current);
            if !Self::valid_kline(&item) || item.qty <= 0.0 || !item.qty.is_finite() {
                return None;
            }
            recent.push(item);
        }
        Some(recent)
    }

    fn compute_vol_base(
        tw: &UhfTradeWindow,
        current: u64,
        start: usize,
        end: usize,
    ) -> Option<f64> {
        let mut sum = 0.0;
        let mut count = 0_usize;
        for i in start..=end {
            let item = tw.get_target_closed_minute_price_qty(-(i as i32), current);
            if !Self::valid_kline(&item) || item.qty <= 0.0 || !item.qty.is_finite() {
                continue;
            }
            sum += item.qty;
            count += 1;
        }
        if count < 30 {
            return None;
        }
        let vol_base = sum / count as f64;
        if vol_base <= 0.0 || !vol_base.is_finite() {
            return None;
        }
        Some(vol_base)
    }

    fn compute_recent_105m_high(
        tw: &UhfTradeWindow,
        current: u64,
        start: usize,
        end: usize,
    ) -> f64 {
        let mut high = 0.0_f64;
        for i in start..=end {
            let item = tw.get_target_closed_minute_price_qty(-(i as i32), current);
            if !Self::valid_kline(&item) {
                continue;
            }
            high = high.max(item.high);
        }
        high
    }
}

impl StrategyModule for HugeMomentumSignalModule {
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
        self.cfg.started = false;
        self.cfg.started_timestamp = 0;
        Ok(())
    }

    fn start(
        &mut self,
        _ctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cfg.started = true;
        self.cfg.started_timestamp = Self::now_seconds();
        self.last_check_second = 0;
        tracing::info!(
            strategy = self.name(),
            strategy_id = self.id(),
            started_timestamp = self.cfg.started_timestamp,
            tf_24h_qty_value_threshold = self.cfg.tf_24h_qty_value_threshold,
            price_15m_change_threshold = self.cfg.price_15m_change_threshold,
            qty_15m_times_threshold = self.cfg.qty_15m_times_threshold,
            qty_5m_total_multiple = self.cfg.qty_5m_total_multiple,
            required_minutes = self.cfg.required_minutes,
            confirm_score_threshold = self.cfg.confirm_score_threshold,
            pending_valid_seconds = self.cfg.pending_valid_seconds,
            cooldown_seconds = self.cfg.cooldown_seconds,
            "huge momentum module started"
        );
        Ok(())
    }

    fn handle_candle_1m(&mut self, ctx: &mut StrategyContext, _candle: &QuotationKline) {
        if !self.cfg.started {
            return;
        }

        let now = Self::now_seconds();
        if self.last_check_second > 0 && now < self.last_check_second + 60 {
            return;
        }
        self.last_check_second = now;

        let Some(tw) = ctx.trades.get(&_candle.symbol) else {
            return;
        };

        if let Some(signal) = self.check(&_candle.symbol, tw, now) {
            tracing::info!(
                strategy = self.name(),
                strategy_id = self.id(),
                symbol = %signal.base.symbol,
                message = %signal.format(),
                "huge momentum signal triggered"
            );
        }
    }
}
