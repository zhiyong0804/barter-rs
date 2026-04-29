use serde::Deserialize;
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    quotation::trade_window::{QuotationKline, TradeWindowPriceQty, UhfTradeWindow},
    signal::{HugeMomentumSignal, SignalLevel, SignalType, TradeSignalBase},
};

use super::{module_id, StrategyContext, StrategyModule};

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

    pub fn check(&mut self, symbol: &str, tw: &UhfTradeWindow) -> Option<HugeMomentumSignal> {
        if !Self::precheck_trade_window(tw, self.cfg.required_minutes) {
            tracing::trace!(
                strategy = self.name(),
                strategy_id = self.id(),
                symbol,
                minutes_len = tw.minutes_window.items.len(),
                required_minutes = self.cfg.required_minutes,
                "huge momentum check precheck_trade_window failed"
            );
            return None;
        }

        let Some(recent_minutes) = Self::collect_recent_minutes(tw) else {
            tracing::trace!(
                strategy = self.name(),
                strategy_id = self.id(),
                symbol,
                minutes_len = tw.minutes_window.items.len(),
                "huge momentum check collect_recent_minutes failed"
            );
            return None;
        };
        if recent_minutes.len() < 3 {
            tracing::trace!(
                strategy = self.name(),
                strategy_id = self.id(),
                symbol,
                recent_minutes_len = recent_minutes.len(),
                "huge momentum check not enough recent minutes"
            );
            return None;
        }

        let latest_minute = *recent_minutes.last().expect("checked len");
        let recent_5m = Self::build_recent_5m(&recent_minutes);
        if recent_5m.len() < 3 {
            tracing::trace!(
                strategy = self.name(),
                strategy_id = self.id(),
                symbol,
                recent_5m_len = recent_5m.len(),
                "huge momentum check not enough recent 5m bars"
            );
            return None;
        }

        let vol_base_range_end = 120_usize.min(tw.minutes_window.items.len().saturating_sub(1));
        if vol_base_range_end < 15 {
            tracing::trace!(
                strategy = self.name(),
                strategy_id = self.id(),
                symbol,
                minutes_len = tw.minutes_window.items.len(),
                "huge momentum check not enough minutes for vol_base"
            );
            return None;
        }
        let Some(vol_base) = Self::compute_vol_base(tw, 15, vol_base_range_end) else {
            tracing::trace!(
                strategy = self.name(),
                strategy_id = self.id(),
                symbol,
                minutes_len = tw.minutes_window.items.len(),
                "huge momentum check compute_vol_base failed"
            );
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

        let threshold_each_5m = vol_base * self.cfg.qty_5m_total_multiple * 5.0;
        let threshold_total_15m = vol_base * self.cfg.qty_15m_times_threshold * 5.0;
        let three_5m_valid = q1 >= threshold_each_5m
            && q2 >= threshold_each_5m
            && q3 >= threshold_each_5m
            && qsum >= threshold_total_15m;

        let recent_105m_high = Self::compute_recent_105m_high(tw, 15, vol_base_range_end);
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

    fn collect_recent_minutes(tw: &UhfTradeWindow) -> Option<Vec<TradeWindowPriceQty>> {
        let mut recent = Vec::with_capacity(15);
        for i in (1..=15).rev() {
            let item = tw.get_target_closed_minute_price_qty(-(i as i32));
            if !Self::valid_kline(&item) || item.qty <= 0.0 || !item.qty.is_finite() {
                return None;
            }
            recent.push(item);
        }
        Some(recent)
    }

    fn compute_vol_base(tw: &UhfTradeWindow, start: usize, end: usize) -> Option<f64> {
        let mut sum = 0.0;
        let mut count = 0_usize;
        for i in start..=end {
            let item = tw.get_target_closed_minute_price_qty(-(i as i32));
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

    fn compute_recent_105m_high(tw: &UhfTradeWindow, start: usize, end: usize) -> f64 {
        let mut high = 0.0_f64;
        for i in start..=end {
            let item = tw.get_target_closed_minute_price_qty(-(i as i32));
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

    fn handle_candle_1m(&mut self, ctx: &mut StrategyContext, candle: &QuotationKline) {
        if !self.cfg.started {
            return;
        }

        if !candle.is_final {
            return;
        }

        let Some(tw) = ctx.trades.get(&candle.symbol) else {
            return;
        };

        if let Some(signal) = self.check(&candle.symbol, tw) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quotation::trade_window::UhfKlineInterval;
    use chrono::{DateTime, Utc};
    use serde::Deserialize;
    use std::{fs, path::PathBuf};

    fn init_test_tracing() -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    #[derive(Debug, Clone, Deserialize)]
    struct KlineFixture {
        start_timestamp: u64,
        open: f64,
        close: f64,
        high: f64,
        low: f64,
        volume: f64,
        bid_volume: f64,
    }

    #[derive(Debug, Clone, Deserialize)]
    struct PersistedKlineData {
        close: f64,
        close_time: DateTime<Utc>,
        high: f64,
        low: f64,
        open: f64,
        volume: f64,
        #[serde(default)]
        bid_volume: f64,
    }

    #[derive(Debug, Clone, Deserialize)]
    struct PersistedKlineEvent {
        instrument: String,
        data: PersistedKlineData,
    }

    fn load_fixture_rows(file_name: &str) -> Vec<KlineFixture> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let candidate_paths = [
            manifest_dir.join("tests").join("data").join(file_name),
            manifest_dir
                .join("miraelis")
                .join("tests")
                .join("data")
                .join(file_name),
        ];

        let path = candidate_paths
            .iter()
            .find(|candidate| candidate.exists())
            .expect("failed to find fixture file");
        let raw = fs::read_to_string(path).expect("failed to read fixture file");

        if let Ok(rows) = serde_json::from_str::<Vec<KlineFixture>>(&raw) {
            return rows;
        }

        if let Ok(events) = serde_json::from_str::<Vec<PersistedKlineEvent>>(&raw) {
            return events
                .into_iter()
                .map(convert_persisted_event)
                .collect::<Vec<_>>();
        }

        raw.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<PersistedKlineEvent>(line)
                    .map(convert_persisted_event)
                    .expect("failed to parse persisted kline line")
            })
            .collect::<Vec<_>>()
    }

    fn convert_persisted_event(event: PersistedKlineEvent) -> KlineFixture {
        let interval_ms = if event.instrument.ends_with("|kline_1h") {
            3_600_000
        } else {
            60_000
        };
        let close_ms = event.data.close_time.timestamp_millis().max(0) as u64;
        let start_timestamp = close_ms.saturating_sub(interval_ms - 1);

        KlineFixture {
            start_timestamp,
            open: event.data.open,
            close: event.data.close,
            high: event.data.high,
            low: event.data.low,
            volume: event.data.volume,
            bid_volume: event.data.bid_volume,
        }
    }

    fn build_kline(symbol: &str, interval: UhfKlineInterval, row: &KlineFixture) -> QuotationKline {
        let interval_ms = match interval {
            UhfKlineInterval::M1 => 60_000,
            UhfKlineInterval::H1 => 3_600_000,
        };

        QuotationKline {
            symbol: symbol.to_owned(),
            event_time: row.start_timestamp + interval_ms - 1,
            start_timestamp: row.start_timestamp,
            end_timestamp: row.start_timestamp + interval_ms - 1,
            interval,
            start_trade_id: 0,
            end_trade_id: 0,
            open: row.open,
            close: row.close,
            high: row.high,
            low: row.low,
            volume: row.volume,
            bid_volume: row.bid_volume,
            is_final: true,
        }
    }

    #[test]
    fn check_replay_from_1m_and_1h_files() {
        let _tracing_guard = init_test_tracing();

        let symbol = "BASEDUSDT";

        let mut module = HugeMomentumSignalModule::with_config(HugeMomentumModuleConfig {
            tf_24h_qty_value_threshold: 1_000.0,
            price_15m_change_threshold: 0.002,
            qty_15m_times_threshold: 6.0,
            qty_5m_total_multiple: 1.0,
            required_minutes: 60,
            confirm_score_threshold: 2.2,
            pending_valid_seconds: 15 * 60,
            cooldown_seconds: 45 * 60,
        });

        let rows_1h = load_fixture_rows("basedusdt_1h.candle");
        let rows_1m = load_fixture_rows("basedusdt_1m.candle");

        let first_1m_second = rows_1m
            .first()
            .expect("1m fixture should not be empty")
            .start_timestamp
            / 1000;
        module.cfg.started = true;
        module.cfg.started_timestamp = first_1m_second.saturating_sub(3_600);

        let mut trade_window = UhfTradeWindow::new(symbol);
        for row in &rows_1h {
            trade_window.update_kline(build_kline(symbol, UhfKlineInterval::H1, row));
        }
        trade_window.hours_window.tf_total_qty = rows_1h.iter().map(|item| item.volume).sum();
        trade_window.hours_window.tf_total_value =
            rows_1h.iter().map(|item| item.close * item.volume).sum();

        let mut check_calls = 0usize;
        let mut signal_hits = 0usize;
        for row in &rows_1m {
            trade_window.update_kline(build_kline(symbol, UhfKlineInterval::M1, row));

            check_calls += 1;
            if module.check(symbol, &trade_window).is_some() {
                signal_hits += 1;
            }
        }

        assert_eq!(check_calls, rows_1m.len());
        assert!(
            signal_hits > 0,
            "expected at least one huge momentum signal"
        );
    }
}

