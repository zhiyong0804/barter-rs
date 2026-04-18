use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    quotation::trade_window::{SecondTradeItem, TradeItem, UhfTradeWindow},
    signal::{SignalLevel, SignalType, TradeFrameSignal, TradeSignalBase},
};

use super::{module_id, StrategyContext, StrategyModule};

const MOMENTUM_MIN_24H_VALUE_THRESHOLD: f64 = 20_000_000.0;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PositionSide {
    Long,
    Short,
}

impl std::fmt::Display for PositionSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionSide::Long => write!(f, "LONG"),
            PositionSide::Short => write!(f, "SHORT"),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OrderRunningStatus {
    New,
    Submitting,
    Open,
    PartialFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FrameOrderType {
    Limit,
    Market,
    StopMarket,
    TakeProfitMarket,
}

#[derive(Debug, Clone)]
pub struct FrameOrderItem {
    pub new_client_order_id: String,
    pub order_id: u64,
    pub symbol: String,
    pub side: PositionSide,
    pub order_type: FrameOrderType,
    pub price: f64,
    pub stop_price: f64,
    pub qty: f64,
    pub avg_price: f64,
    pub cum_qty: f64,
    pub running_status: OrderRunningStatus,
    pub reduce_only: bool,
    pub close_position: bool,
    pub origin_client_order_id: String,
    pub origin_order_id: u64,
    pub good_till_date: u64,
    pub has_stop_close_order: bool,
    pub has_take_profit_order: bool,
    pub high_price_since_open: f64,
    pub low_price_since_open: f64,
    pub open_event_time: u64,
}

impl FrameOrderItem {
    pub fn is_open_order(&self) -> bool {
        self.origin_client_order_id.is_empty()
    }

    pub fn is_close_order(&self) -> bool {
        !self.origin_client_order_id.is_empty()
    }

    pub fn is_active_on_exchange(&self) -> bool {
        matches!(
            self.running_status,
            OrderRunningStatus::Open | OrderRunningStatus::PartialFilled
        )
    }

    pub fn is_working(&self) -> bool {
        matches!(
            self.running_status,
            OrderRunningStatus::Open
                | OrderRunningStatus::PartialFilled
                | OrderRunningStatus::Filled
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub client_order_id: String,
    pub symbol: String,
    pub side: PositionSide,
    pub order_type: FrameOrderType,
    pub price: f64,
    pub stop_price: f64,
    pub qty: f64,
    pub reduce_only: bool,
    pub close_position: bool,
    pub good_till_date: u64,
    pub strategy_id: u64,
    pub origin_client_order_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderResponse {
    pub action: OrderResponseAction,
    pub client_order_id: String,
    pub order_id: u64,
    pub symbol: String,
    pub running_status: OrderRunningStatus,
    pub avg_price: f64,
    pub cum_qty: f64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OrderResponseAction {
    Put,
    Cancel,
    Query,
}

#[derive(Debug, Clone)]
pub struct FrameOrderContext {
    pub symbol: String,
    pub trigger: u64,
    pub last_query_time: u64,
    pub open: f64,
    pub price_precision: i32,
    pub quantity_precision: i32,
    pub price_tick_size: f64,
    pub take_profit_percent: f64,
    pub reversal_price_percent: f64,
    pub stop_price_percent: f64,
    pub signal_qty_times: f64,
    pub signal_price_percent: f64,
    pub allow_long: bool,
    pub allow_short: bool,
    pub keep_position_seconds: i64,
    pub shadow_price_percent: f64,
    pub shadow_qty_times: f64,
    pub high_price_since_open: f64,
    pub low_price_since_open: f64,
    pub latest_minute_qty: f64,
    pub tf_24h_qty_value_threshold: f64,
    pub order_base_qty: f64,
    pub open_order: Option<FrameOrderItem>,
    pub orders: HashMap<String, FrameOrderItem>,
}

impl FrameOrderContext {
    fn new(symbol: String, config: &FrameModuleConfig) -> Self {
        let (allow_long, allow_short) = parse_order_direction(&config.order_direction);
        let threshold = config
            .tf_24h_qty_value_threshold
            .max(MOMENTUM_MIN_24H_VALUE_THRESHOLD);
        let take_profit_percent = normalize_percent(config.take_profit_percent);
        let reversal_price_percent = normalize_percent(config.reversal_price_percent);
        let stop_price_percent = normalize_percent(config.stop_price_percent);
        let signal_price_percent = normalize_percent(config.signal_price_percent);
        let shadow_price_percent = normalize_percent(config.shadow_price_percent);
        Self {
            symbol,
            trigger: 0,
            last_query_time: 0,
            open: 0.0,
            price_precision: 7,
            quantity_precision: 0,
            price_tick_size: 0.01,
            take_profit_percent,
            reversal_price_percent,
            stop_price_percent,
            signal_qty_times: config.signal_qty_times,
            signal_price_percent,
            allow_long,
            allow_short,
            keep_position_seconds: config.keep_position_seconds,
            shadow_price_percent,
            shadow_qty_times: config.shadow_qty_times,
            high_price_since_open: 0.0,
            low_price_since_open: 0.0,
            latest_minute_qty: 0.0,
            tf_24h_qty_value_threshold: threshold,
            order_base_qty: config.order_base_qty,
            open_order: None,
            orders: HashMap::new(),
        }
    }

    pub fn has_position(&self) -> bool {
        self.open_order
            .as_ref()
            .map(|o| {
                !matches!(
                    o.running_status,
                    OrderRunningStatus::Canceled
                        | OrderRunningStatus::Rejected
                        | OrderRunningStatus::Expired
                )
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FrameModuleConfig {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub excluded_symbols: Vec<String>,
    #[serde(default = "default_order_direction")]
    pub order_direction: String,
    #[serde(default = "default_tf_24h_qty_value_threshold")]
    pub tf_24h_qty_value_threshold: f64,
    #[serde(default = "default_take_profit_percent")]
    pub take_profit_percent: f64,
    #[serde(default = "default_reversal_price_percent")]
    pub reversal_price_percent: f64,
    #[serde(default = "default_stop_price_percent")]
    pub stop_price_percent: f64,
    #[serde(default = "default_signal_qty_times")]
    pub signal_qty_times: f64,
    #[serde(default = "default_signal_price_percent")]
    pub signal_price_percent: f64,
    #[serde(default = "default_keep_position_seconds")]
    pub keep_position_seconds: i64,
    #[serde(default = "default_shadow_price_percent")]
    pub shadow_price_percent: f64,
    #[serde(default = "default_shadow_qty_times")]
    pub shadow_qty_times: f64,
    #[serde(default = "default_order_base_qty")]
    pub order_base_qty: f64,
}

fn default_order_direction() -> String {
    "both".to_owned()
}
fn default_tf_24h_qty_value_threshold() -> f64 {
    MOMENTUM_MIN_24H_VALUE_THRESHOLD
}
fn default_take_profit_percent() -> f64 {
    0.006
}
fn default_reversal_price_percent() -> f64 {
    0.003
}
fn default_stop_price_percent() -> f64 {
    0.004
}
fn default_signal_qty_times() -> f64 {
    3.0
}
fn default_signal_price_percent() -> f64 {
    0.003
}
fn default_keep_position_seconds() -> i64 {
    6
}
fn default_shadow_price_percent() -> f64 {
    0.4
}
fn default_shadow_qty_times() -> f64 {
    3.0
}
fn default_order_base_qty() -> f64 {
    100.0
}

impl Default for FrameModuleConfig {
    fn default() -> Self {
        Self {
            symbols: Vec::new(),
            excluded_symbols: Vec::new(),
            order_direction: default_order_direction(),
            tf_24h_qty_value_threshold: default_tf_24h_qty_value_threshold(),
            take_profit_percent: default_take_profit_percent(),
            reversal_price_percent: default_reversal_price_percent(),
            stop_price_percent: default_stop_price_percent(),
            signal_qty_times: default_signal_qty_times(),
            signal_price_percent: default_signal_price_percent(),
            keep_position_seconds: default_keep_position_seconds(),
            shadow_price_percent: default_shadow_price_percent(),
            shadow_qty_times: default_shadow_qty_times(),
            order_base_qty: default_order_base_qty(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FrameSignalContext {
    pub id: u64,
    pub name: String,
    pub symbols: Vec<String>,
    pub frame_ctxs: HashMap<String, FrameOrderContext>,
    pub started: bool,
    included_symbols: HashSet<String>,
    excluded_symbols: HashSet<String>,
    config: FrameModuleConfig,
}

impl Default for FrameSignalContext {
    fn default() -> Self {
        Self {
            id: module_id::FRAME,
            name: "strategy.frame".to_owned(),
            symbols: Vec::new(),
            frame_ctxs: HashMap::new(),
            started: false,
            included_symbols: HashSet::new(),
            excluded_symbols: HashSet::new(),
            config: FrameModuleConfig::default(),
        }
    }
}

pub struct FrameSignalModule {
    pub cfg: FrameSignalContext,
    order_tx: Option<UnboundedSender<OrderRequest>>,
    order_seq: AtomicU64,
}

impl Default for FrameSignalModule {
    fn default() -> Self {
        Self {
            cfg: FrameSignalContext::default(),
            order_tx: None,
            order_seq: AtomicU64::new(1),
        }
    }
}

impl FrameSignalModule {
    pub fn with_config(config: FrameModuleConfig) -> Self {
        let included_symbols = config
            .symbols
            .iter()
            .map(|s| s.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let excluded_symbols = config
            .excluded_symbols
            .iter()
            .map(|s| s.to_ascii_lowercase())
            .collect::<HashSet<_>>();

        let mut ctx = FrameSignalContext::default();
        ctx.symbols = config.symbols.clone();
        ctx.included_symbols = included_symbols;
        ctx.excluded_symbols = excluded_symbols;
        ctx.config = config;

        Self {
            cfg: ctx,
            order_tx: None,
            order_seq: AtomicU64::new(1),
        }
    }

    pub fn with_order_tx(mut self, tx: UnboundedSender<OrderRequest>) -> Self {
        self.order_tx = Some(tx);
        self
    }

    fn safe_div(a: f64, b: f64) -> f64 {
        if b <= 0.0 || !b.is_finite() {
            0.0
        } else {
            a / b
        }
    }

    fn symbol_enabled(&self, symbol: &str) -> bool {
        let lower = symbol.to_ascii_lowercase();
        if self.cfg.excluded_symbols.contains(&lower) {
            return false;
        }
        if self.cfg.included_symbols.contains("*") {
            return true;
        }
        if self.cfg.included_symbols.is_empty() {
            return true;
        }
        self.cfg.included_symbols.contains(&lower)
    }

    fn get_recent_second(window: &UhfTradeWindow, back: usize) -> Option<&SecondTradeItem> {
        let len = window.seconds_window.items.len();
        if len <= back {
            return None;
        }
        window.seconds_window.items.get(len - 1 - back)
    }

    fn is_valid_second(item: &SecondTradeItem) -> bool {
        item.open.is_finite()
            && item.close.is_finite()
            && item.high.is_finite()
            && item.low.is_finite()
            && item.open > 0.0
            && item.high >= item.low
            && item.qty >= 0.0
    }

    fn build_entry_signal(
        strategy_name: &str,
        strategy_id: u64,
        trade: &TradeItem,
        octx: &FrameOrderContext,
        side: PositionSide,
        trigger_price: f64,
        vol_3s: f64,
        avg_sec_vol: f64,
        vol_ratio: f64,
        price_move_3s_pct: f64,
        is_accelerating: bool,
        short_signal: bool,
        long_signal: bool,
        curr_sec_qty: f64,
        prev_sec_qty: f64,
    ) -> TradeFrameSignal {
        TradeFrameSignal {
            base: TradeSignalBase::new(
                SignalType::Frame,
                strategy_name.to_owned(),
                strategy_id,
                SignalLevel::Important,
                "frame entry trigger",
                trade.event_time,
                trade.symbol.clone(),
            ),
            stage: "entry".to_owned(),
            side: side.to_string(),
            trade_id: trade.id,
            current_trade_price: trade.price,
            trigger_price,
            signal_qty_times: octx.signal_qty_times,
            signal_price_percent: octx.signal_price_percent,
            vol_3s,
            avg_sec_vol,
            vol_ratio,
            price_move_3s_pct,
            is_accelerating,
            short_signal,
            long_signal,
            stop_price_percent: octx.stop_price_percent,
            reversal_price_percent: octx.reversal_price_percent,
            shadow_price_percent: octx.shadow_price_percent,
            latest_minute_qty: octx.latest_minute_qty,
            current_second_qty: curr_sec_qty,
            prev_second_qty: prev_sec_qty,
            high_price_since_open: octx.high_price_since_open,
            low_price_since_open: octx.low_price_since_open,
            avg_price: trigger_price,
            should_stop: false,
            should_reversal: false,
            should_exhaust: false,
        }
    }

    fn build_exit_signal(
        strategy_name: &str,
        strategy_id: u64,
        trade: &TradeItem,
        octx: &FrameOrderContext,
        order: &FrameOrderItem,
        should_stop: bool,
        should_reversal: bool,
        should_exhaust: bool,
        curr_sec_qty: f64,
        prev_sec_qty: f64,
    ) -> TradeFrameSignal {
        TradeFrameSignal {
            base: TradeSignalBase::new(
                SignalType::Frame,
                strategy_name.to_owned(),
                strategy_id,
                SignalLevel::Important,
                "frame exit trigger",
                trade.event_time,
                trade.symbol.clone(),
            ),
            stage: "exit".to_owned(),
            side: order.side.to_string(),
            trade_id: trade.id,
            current_trade_price: trade.price,
            trigger_price: order.avg_price,
            signal_qty_times: octx.signal_qty_times,
            signal_price_percent: octx.signal_price_percent,
            vol_3s: 0.0,
            avg_sec_vol: 0.0,
            vol_ratio: 0.0,
            price_move_3s_pct: 0.0,
            is_accelerating: false,
            short_signal: false,
            long_signal: false,
            stop_price_percent: octx.stop_price_percent,
            reversal_price_percent: octx.reversal_price_percent,
            shadow_price_percent: octx.shadow_price_percent,
            latest_minute_qty: octx.latest_minute_qty,
            current_second_qty: curr_sec_qty,
            prev_second_qty: prev_sec_qty,
            high_price_since_open: order.high_price_since_open,
            low_price_since_open: order.low_price_since_open,
            avg_price: order.avg_price,
            should_stop,
            should_reversal,
            should_exhaust,
        }
    }

    fn submit_order(order_tx: Option<&UnboundedSender<OrderRequest>>, req: OrderRequest) {
        if let Some(tx) = order_tx {
            if let Err(error) = tx.send(req) {
                tracing::error!(%error, "frame: failed to send order request");
            }
        }
    }

    fn trigger_open_orders(
        octx: &mut FrameOrderContext,
        trade: &TradeItem,
        strategy_id: u64,
        order_seq: &AtomicU64,
        order_tx: Option<&UnboundedSender<OrderRequest>>,
        side: PositionSide,
        intercept_price: f64,
    ) {
        let price = align_to_tick_size(intercept_price, octx.price_tick_size, octx.price_precision);
        let qty = round_to_n_decimal(octx.order_base_qty / trade.price, octx.quantity_precision);
        if qty <= 0.0 {
            tracing::warn!(symbol = %trade.symbol, "frame: calculated qty <= 0, skipping");
            return;
        }

        let good_till_date = trade.event_time + 601_000;
        let seq = order_seq.fetch_add(1, Ordering::Relaxed);
        let cid = format!("strategy.frame_{}", seq);

        let running_status = if order_tx.is_none() {
            OrderRunningStatus::Filled
        } else {
            OrderRunningStatus::Submitting
        };

        let order = FrameOrderItem {
            new_client_order_id: cid.clone(),
            order_id: 0,
            symbol: trade.symbol.clone(),
            side,
            order_type: FrameOrderType::Limit,
            price,
            stop_price: 0.0,
            qty,
            avg_price: price,
            cum_qty: 0.0,
            running_status,
            reduce_only: false,
            close_position: false,
            origin_client_order_id: String::new(),
            origin_order_id: 0,
            good_till_date,
            has_stop_close_order: false,
            has_take_profit_order: false,
            high_price_since_open: trade.price,
            low_price_since_open: trade.price,
            open_event_time: trade.event_time,
        };

        octx.orders.insert(cid.clone(), order.clone());
        octx.open_order = Some(order);
        octx.trigger = trade.event_time;

        let req = OrderRequest {
            client_order_id: cid,
            symbol: trade.symbol.clone(),
            side,
            order_type: FrameOrderType::Limit,
            price,
            stop_price: 0.0,
            qty,
            reduce_only: false,
            close_position: false,
            good_till_date,
            strategy_id,
            origin_client_order_id: None,
        };
        Self::submit_order(order_tx, req);
    }

    fn reduce_position(
        octx: &mut FrameOrderContext,
        trade: &TradeItem,
        strategy_id: u64,
        order_seq: &AtomicU64,
        order_tx: Option<&UnboundedSender<OrderRequest>>,
        origin: &FrameOrderItem,
    ) {
        let close_side = match origin.side {
            PositionSide::Long => PositionSide::Short,
            PositionSide::Short => PositionSide::Long,
        };
        let raw_price = match close_side {
            PositionSide::Long => trade.price * 1.002,
            PositionSide::Short => trade.price * 0.998,
        };
        let price = align_to_tick_size(raw_price, octx.price_tick_size, octx.price_precision);
        let good_till_date = octx.trigger + 601_000;
        let seq = order_seq.fetch_add(1, Ordering::Relaxed);
        let cid = format!("c_{}_{}", origin.order_id, seq);

        let close_order = FrameOrderItem {
            new_client_order_id: cid.clone(),
            order_id: 0,
            symbol: origin.symbol.clone(),
            side: close_side,
            order_type: FrameOrderType::Market,
            price,
            stop_price: price,
            qty: origin.qty,
            avg_price: 0.0,
            cum_qty: 0.0,
            running_status: OrderRunningStatus::Submitting,
            reduce_only: true,
            close_position: true,
            origin_client_order_id: origin.new_client_order_id.clone(),
            origin_order_id: origin.order_id,
            good_till_date,
            has_stop_close_order: false,
            has_take_profit_order: false,
            high_price_since_open: 0.0,
            low_price_since_open: 0.0,
            open_event_time: 0,
        };
        octx.orders.insert(cid.clone(), close_order);

        let req = OrderRequest {
            client_order_id: cid,
            symbol: trade.symbol.clone(),
            side: close_side,
            order_type: FrameOrderType::Market,
            price,
            stop_price: price,
            qty: origin.qty,
            reduce_only: true,
            close_position: true,
            good_till_date,
            strategy_id,
            origin_client_order_id: Some(origin.new_client_order_id.clone()),
        };
        Self::submit_order(order_tx, req);

        if order_tx.is_none() {
            octx.open_order = None;
            octx.high_price_since_open = 0.0;
            octx.low_price_since_open = 0.0;
        }
    }

    fn place_stop_position(
        octx: &mut FrameOrderContext,
        trade: &TradeItem,
        strategy_id: u64,
        order_seq: &AtomicU64,
        order_tx: Option<&UnboundedSender<OrderRequest>>,
        origin: &FrameOrderItem,
        stop_price_override: Option<f64>,
    ) {
        if origin.has_stop_close_order {
            return;
        }
        let close_side = match origin.side {
            PositionSide::Long => PositionSide::Short,
            PositionSide::Short => PositionSide::Long,
        };
        let raw_stop = stop_price_override.unwrap_or_else(|| match close_side {
            PositionSide::Long => origin.avg_price * (1.0 + octx.stop_price_percent),
            PositionSide::Short => origin.avg_price * (1.0 - octx.stop_price_percent),
        });
        let stop_price = align_to_tick_size(raw_stop, octx.price_tick_size, octx.price_precision);
        let price =
            align_to_tick_size(origin.avg_price, octx.price_tick_size, octx.price_precision);
        let good_till_date = octx.trigger + 601_000;
        let seq = order_seq.fetch_add(1, Ordering::Relaxed);
        let cid = format!("cr_{}_{}", origin.order_id, seq);

        let stop_order = FrameOrderItem {
            new_client_order_id: cid.clone(),
            order_id: 0,
            symbol: origin.symbol.clone(),
            side: close_side,
            order_type: FrameOrderType::StopMarket,
            price,
            stop_price,
            qty: origin.qty,
            avg_price: 0.0,
            cum_qty: 0.0,
            running_status: OrderRunningStatus::Submitting,
            reduce_only: true,
            close_position: true,
            origin_client_order_id: origin.new_client_order_id.clone(),
            origin_order_id: origin.order_id,
            good_till_date,
            has_stop_close_order: false,
            has_take_profit_order: false,
            high_price_since_open: 0.0,
            low_price_since_open: 0.0,
            open_event_time: 0,
        };
        octx.orders.insert(cid.clone(), stop_order);

        let req = OrderRequest {
            client_order_id: cid,
            symbol: trade.symbol.clone(),
            side: close_side,
            order_type: FrameOrderType::StopMarket,
            price,
            stop_price,
            qty: origin.qty,
            reduce_only: true,
            close_position: true,
            good_till_date,
            strategy_id,
            origin_client_order_id: Some(origin.new_client_order_id.clone()),
        };
        Self::submit_order(order_tx, req);

        if let Some(oo) = octx.orders.get_mut(&origin.new_client_order_id) {
            oo.has_stop_close_order = true;
        }
        if let Some(ref mut oo) = octx.open_order {
            if oo.new_client_order_id == origin.new_client_order_id {
                oo.has_stop_close_order = true;
            }
        }
    }

    fn place_take_profit_stop_order(
        octx: &mut FrameOrderContext,
        trade: &TradeItem,
        strategy_id: u64,
        order_seq: &AtomicU64,
        order_tx: Option<&UnboundedSender<OrderRequest>>,
        origin: &FrameOrderItem,
        take_profit_percent: f64,
    ) {
        if origin.has_take_profit_order {
            return;
        }
        let close_side = match origin.side {
            PositionSide::Long => PositionSide::Short,
            PositionSide::Short => PositionSide::Long,
        };
        let raw_stop = match close_side {
            PositionSide::Long => origin.avg_price * (1.0 + take_profit_percent),
            PositionSide::Short => origin.avg_price * (1.0 - take_profit_percent),
        };
        let stop_price = align_to_tick_size(raw_stop, octx.price_tick_size, octx.price_precision);
        let price =
            align_to_tick_size(origin.avg_price, octx.price_tick_size, octx.price_precision);
        let good_till_date = octx.trigger + 601_000;
        let seq = order_seq.fetch_add(1, Ordering::Relaxed);
        let cid = format!("tp_{}_{}", origin.order_id, seq);

        let tp_order = FrameOrderItem {
            new_client_order_id: cid.clone(),
            order_id: 0,
            symbol: origin.symbol.clone(),
            side: close_side,
            order_type: FrameOrderType::TakeProfitMarket,
            price,
            stop_price,
            qty: origin.qty,
            avg_price: 0.0,
            cum_qty: 0.0,
            running_status: OrderRunningStatus::Submitting,
            reduce_only: true,
            close_position: true,
            origin_client_order_id: origin.new_client_order_id.clone(),
            origin_order_id: origin.order_id,
            good_till_date,
            has_stop_close_order: false,
            has_take_profit_order: false,
            high_price_since_open: 0.0,
            low_price_since_open: 0.0,
            open_event_time: 0,
        };
        octx.orders.insert(cid.clone(), tp_order);

        let req = OrderRequest {
            client_order_id: cid,
            symbol: trade.symbol.clone(),
            side: close_side,
            order_type: FrameOrderType::TakeProfitMarket,
            price,
            stop_price,
            qty: origin.qty,
            reduce_only: true,
            close_position: true,
            good_till_date,
            strategy_id,
            origin_client_order_id: Some(origin.new_client_order_id.clone()),
        };
        Self::submit_order(order_tx, req);

        if let Some(oo) = octx.orders.get_mut(&origin.new_client_order_id) {
            oo.has_take_profit_order = true;
        }
        if let Some(ref mut oo) = octx.open_order {
            if oo.new_client_order_id == origin.new_client_order_id {
                oo.has_take_profit_order = true;
            }
        }
    }

    pub fn handle_put_response(&mut self, response: &OrderResponse) {
        let Some(octx) = self
            .cfg
            .frame_ctxs
            .get_mut(&response.symbol.to_ascii_lowercase())
        else {
            return;
        };
        let Some(order) = octx.orders.get_mut(&response.client_order_id) else {
            return;
        };

        order.order_id = response.order_id;
        order.running_status = response.running_status;
        if order.is_open_order() {
            if let Some(ref mut oo) = octx.open_order {
                if oo.new_client_order_id == response.client_order_id {
                    oo.order_id = response.order_id;
                    oo.running_status = response.running_status;
                }
            }
        }
    }

    pub fn handle_query_response(&mut self, response: &OrderResponse) {
        let symbol_key = response.symbol.to_ascii_lowercase();
        let Some(octx) = self.cfg.frame_ctxs.get_mut(&symbol_key) else {
            return;
        };
        let cid = response.client_order_id.clone();
        let Some(order) = octx.orders.get_mut(&cid) else {
            return;
        };

        order.order_id = response.order_id;
        order.avg_price = response.avg_price;
        order.cum_qty = response.cum_qty;
        order.running_status = response.running_status;

        if order.is_close_order() && order.running_status == OrderRunningStatus::Filled {
            let origin_cid = order.origin_client_order_id.clone();
            octx.orders.remove(&origin_cid);
            if octx
                .open_order
                .as_ref()
                .map(|o| o.new_client_order_id == origin_cid)
                .unwrap_or(false)
            {
                octx.open_order = None;
                octx.high_price_since_open = 0.0;
                octx.low_price_since_open = 0.0;
            }
            octx.orders.remove(&cid);
            return;
        }

        if order.is_open_order()
            && matches!(
                order.running_status,
                OrderRunningStatus::Filled | OrderRunningStatus::PartialFilled
            )
        {
            if let Some(ref mut oo) = octx.open_order {
                if oo.new_client_order_id == cid {
                    oo.avg_price = response.avg_price;
                    oo.running_status = response.running_status;
                }
            }
        }
    }

    pub fn handle_cancel_response(&mut self, response: &OrderResponse) {
        let Some(octx) = self
            .cfg
            .frame_ctxs
            .get_mut(&response.symbol.to_ascii_lowercase())
        else {
            return;
        };
        if response.running_status == OrderRunningStatus::Canceled {
            octx.orders.remove(&response.client_order_id);
        }
    }

    pub fn handle_query_all_orders(&mut self, symbol: &str, event_time: u64) {
        let Some(octx) = self.cfg.frame_ctxs.get_mut(&symbol.to_ascii_lowercase()) else {
            return;
        };
        if event_time / 1000 == octx.last_query_time / 1000 {
            return;
        }
        let mut queried = false;
        for order in octx.orders.values() {
            if order.order_id > 0 {
                queried = true;
                tracing::debug!(
                    symbol,
                    order_id = order.order_id,
                    cid = %order.new_client_order_id,
                    "frame: query order"
                );
            }
        }
        if queried {
            octx.last_query_time = event_time;
        }
    }

    pub fn cancel_all_open_orders(&self, symbol: &str) {
        let Some(octx) = self.cfg.frame_ctxs.get(&symbol.to_ascii_lowercase()) else {
            return;
        };
        for order in octx.orders.values() {
            if order.is_active_on_exchange() {
                tracing::info!(
                    symbol,
                    order_id = order.order_id,
                    cid = %order.new_client_order_id,
                    "frame: cancel active order"
                );
            }
        }
    }

    fn check_and_emit(&mut self, sctx: &mut StrategyContext, trade: &TradeItem) {
        let strategy_id = self.cfg.id;
        let strategy_name = self.cfg.name.clone();
        let symbol = trade.symbol.to_ascii_lowercase();

        let (curr, prev1, prev2, tf_total_value, seconds_qty, seconds_count) = {
            let Some(tw) = sctx.trades.get(&symbol) else {
                return;
            };
            let Some(c) = Self::get_recent_second(tw, 0) else {
                return;
            };
            let Some(p1) = Self::get_recent_second(tw, 1) else {
                return;
            };
            let Some(p2) = Self::get_recent_second(tw, 2) else {
                return;
            };
            if !Self::is_valid_second(c) || !Self::is_valid_second(p1) || !Self::is_valid_second(p2)
            {
                return;
            }
            (
                c.clone(),
                p1.clone(),
                p2.clone(),
                tw.hours_window.tf_total_value,
                tw.seconds_window.qty,
                tw.seconds_window.items.len() as f64,
            )
        };

        if !self.symbol_enabled(&symbol) {
            return;
        }

        {
            let config = self.cfg.config.clone();
            self.cfg
                .frame_ctxs
                .entry(symbol.clone())
                .or_insert_with(|| FrameOrderContext::new(symbol.clone(), &config));
        }

        if let Some(ei) = sctx.exchange_info.get(&symbol).cloned() {
            if let Some(octx) = self.cfg.frame_ctxs.get_mut(&symbol) {
                octx.price_precision = ei.price_precision as i32;
                octx.quantity_precision = ei.qty_precision as i32;
                octx.price_tick_size = ei.tick_size;
            }
        }

        let order_seq = &self.order_seq;
        let order_tx = self.order_tx.as_ref();
        let Some(octx) = self.cfg.frame_ctxs.get_mut(&symbol) else {
            return;
        };

        if tf_total_value < octx.tf_24h_qty_value_threshold {
            return;
        }

        let vol_3s = curr.qty + prev1.qty + prev2.qty;
        let avg_sec_vol = if seconds_count > 3.0 {
            Self::safe_div((seconds_qty - vol_3s).max(0.0), seconds_count - 3.0)
        } else {
            0.0
        };
        if avg_sec_vol <= 0.0 {
            return;
        }

        let vol_ratio = Self::safe_div(vol_3s, avg_sec_vol);
        let is_accelerating = curr.qty > avg_sec_vol * octx.signal_qty_times
            && prev1.qty > avg_sec_vol * octx.signal_qty_times
            && prev2.qty > avg_sec_vol * octx.signal_qty_times;
        let price_move_3s_pct = Self::safe_div(trade.price - prev2.open, prev2.open);

        if trade.event_time > octx.trigger + 1_000 && !octx.has_position() {
            let mut short_signal = vol_3s > avg_sec_vol * 3.0 * octx.signal_qty_times
                && is_accelerating
                && price_move_3s_pct > octx.signal_price_percent;
            let mut long_signal = vol_3s > avg_sec_vol * 3.0 * octx.signal_qty_times
                && is_accelerating
                && price_move_3s_pct < -octx.signal_price_percent;
            if !octx.allow_short {
                short_signal = false;
            }
            if !octx.allow_long {
                long_signal = false;
            }

            if short_signal || long_signal {
                let (intercept_price, entry_side) = if short_signal {
                    let slope_offset = (trade.price - prev1.open) * 0.8;
                    let ip = (trade.price + slope_offset).min(trade.price * 1.015);
                    (ip, PositionSide::Short)
                } else {
                    let v_down = curr.close - prev1.close;
                    let ip = (trade.price + v_down * 0.5).max(trade.price * 0.98);
                    (ip, PositionSide::Long)
                };

                octx.open = trade.price;
                octx.latest_minute_qty = avg_sec_vol * 60.0;
                octx.high_price_since_open = trade.price;
                octx.low_price_since_open = trade.price;

                let signal = Self::build_entry_signal(
                    &strategy_name,
                    strategy_id,
                    trade,
                    octx,
                    entry_side,
                    intercept_price,
                    vol_3s,
                    avg_sec_vol,
                    vol_ratio,
                    price_move_3s_pct,
                    is_accelerating,
                    short_signal,
                    long_signal,
                    curr.qty,
                    prev1.qty,
                );

                tracing::info!(
                    strategy = strategy_name,
                    strategy_id,
                    symbol = %trade.symbol,
                    message = %signal.format(),
                    "frame signal triggered"
                );

                Self::trigger_open_orders(
                    octx,
                    trade,
                    strategy_id,
                    order_seq,
                    order_tx,
                    entry_side,
                    intercept_price,
                );
            }
        }

        let open_order_snapshot = octx.open_order.clone();
        if let Some(open_order) = open_order_snapshot {
            if matches!(
                open_order.running_status,
                OrderRunningStatus::Filled | OrderRunningStatus::PartialFilled
            ) {
                if !open_order.has_stop_close_order {
                    Self::place_stop_position(
                        octx,
                        trade,
                        strategy_id,
                        order_seq,
                        order_tx,
                        &open_order,
                        None,
                    );
                }
                if !open_order.has_take_profit_order {
                    Self::place_take_profit_stop_order(
                        octx,
                        trade,
                        strategy_id,
                        order_seq,
                        order_tx,
                        &open_order,
                        octx.take_profit_percent,
                    );
                }
            }

            if !open_order.is_working() {
                update_high_low(octx, trade.price);
                return;
            }

            let cur_p = trade.price;
            let should_stop = match open_order.side {
                PositionSide::Long => {
                    cur_p < open_order.avg_price * (1.0 - octx.stop_price_percent)
                }
                PositionSide::Short => {
                    cur_p > open_order.avg_price * (1.0 + octx.stop_price_percent)
                }
            };
            let should_reversal = match open_order.side {
                PositionSide::Long => {
                    cur_p < octx.high_price_since_open * (1.0 - octx.reversal_price_percent)
                }
                PositionSide::Short => {
                    cur_p > octx.low_price_since_open * (1.0 + octx.reversal_price_percent)
                }
            };
            let body_range = (curr.high - curr.low).abs();
            let shadow_pct = if body_range > 1e-7 {
                match open_order.side {
                    PositionSide::Long => (curr.high - cur_p) / body_range,
                    PositionSide::Short => (cur_p - curr.low) / body_range,
                }
            } else {
                0.0
            };
            let should_exhaust =
                shadow_pct > octx.shadow_price_percent && curr.qty < prev1.qty * 0.4;
            let timeout_exit = octx.keep_position_seconds > 0
                && trade.event_time
                    > open_order.open_event_time + (octx.keep_position_seconds as u64 * 1_000);

            if (should_stop || should_reversal || should_exhaust || timeout_exit)
                && !open_order.has_stop_close_order
            {
                let signal = Self::build_exit_signal(
                    &strategy_name,
                    strategy_id,
                    trade,
                    octx,
                    &open_order,
                    should_stop,
                    should_reversal,
                    should_exhaust || timeout_exit,
                    curr.qty,
                    prev1.qty,
                );
                tracing::info!(
                    strategy = strategy_name,
                    strategy_id,
                    symbol = %trade.symbol,
                    message = %signal.format(),
                    "frame signal triggered"
                );

                Self::reduce_position(octx, trade, strategy_id, order_seq, order_tx, &open_order);
                return;
            }

            update_high_low(octx, trade.price);
        }
    }
}

impl StrategyModule for FrameSignalModule {
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
        self.cfg.frame_ctxs.clear();
        self.cfg.started = false;
        Ok(())
    }

    fn start(
        &mut self,
        _ctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cfg.started = true;
        tracing::info!(
            strategy = self.name(),
            strategy_id = self.id(),
            symbols = %self.cfg.symbols.join(","),
            excluded_symbols = %self.cfg.excluded_symbols.iter().cloned().collect::<Vec<_>>().join(","),
            order_direction = %self.cfg.config.order_direction,
            tf_24h_qty_value_threshold = self.cfg.config.tf_24h_qty_value_threshold,
            signal_qty_times = self.cfg.config.signal_qty_times,
            signal_price_percent = self.cfg.config.signal_price_percent,
            stop_price_percent = self.cfg.config.stop_price_percent,
            reversal_price_percent = self.cfg.config.reversal_price_percent,
            keep_position_seconds = self.cfg.config.keep_position_seconds,
            shadow_price_percent = self.cfg.config.shadow_price_percent,
            shadow_qty_times = self.cfg.config.shadow_qty_times,
            order_base_qty = self.cfg.config.order_base_qty,
            order_execution = self.order_tx.is_some(),
            "frame module started"
        );
        Ok(())
    }

    fn handle_trade(&mut self, ctx: &mut StrategyContext, trade: &TradeItem) {
        if !self.cfg.started {
            return;
        }
        self.check_and_emit(ctx, trade);
    }

    fn handle_order_response(&mut self, _ctx: &mut StrategyContext, response: &OrderResponse) {
        match response.action {
            OrderResponseAction::Put => self.handle_put_response(response),
            OrderResponseAction::Query => self.handle_query_response(response),
            OrderResponseAction::Cancel => self.handle_cancel_response(response),
        }
    }
}

fn align_to_tick_size(price: f64, tick_size: f64, precision: i32) -> f64 {
    if tick_size <= 0.0 {
        return price;
    }
    let aligned = (price / tick_size).round() * tick_size;
    let factor = 10f64.powi(precision);
    (aligned * factor).round() / factor
}

fn round_to_n_decimal(qty: f64, precision: i32) -> f64 {
    let factor = 10f64.powi(precision);
    (qty * factor).round() / factor
}

fn update_high_low(octx: &mut FrameOrderContext, price: f64) {
    let hp = octx.high_price_since_open.max(price);
    let lp = if octx.low_price_since_open <= 0.0 {
        price
    } else {
        octx.low_price_since_open.min(price)
    };
    octx.high_price_since_open = hp;
    octx.low_price_since_open = lp;
    if let Some(ref mut oo) = octx.open_order {
        oo.high_price_since_open = hp;
        oo.low_price_since_open = lp;
    }
}

fn parse_order_direction(order_direction: &str) -> (bool, bool) {
    match order_direction.to_ascii_lowercase().as_str() {
        "long" => (true, false),
        "short" => (false, true),
        _ => (true, true),
    }
}

fn normalize_percent(value: f64) -> f64 {
    if value > 1.0 {
        value / 100.0
    } else {
        value
    }
}
