use serde::Serialize;
use std::collections::{vec_deque, VecDeque};

pub const TRADE_SECONDS_WINDOW_SIZE: usize = 66;
pub const TRADE_MINUTES_WINDOW_SIZE: usize = 120;
pub const TRADE_HOURS_WINDOW_SIZE: usize = 24;
pub const MAX_TRADES_PER_SECOND: usize = 1000;

pub fn greater_or_equal(x: f64, y: f64, eps: f64) -> bool {
    (x > y) || (x - y).abs() < eps
}

pub fn less_or_equal(x: f64, y: f64, eps: f64) -> bool {
    (x < y) || (x - y).abs() < eps
}

#[inline]
fn dmax(a: f64, b: f64) -> f64 {
    if a > b {
        a
    } else {
        b
    }
}

#[inline]
fn dmin(a: f64, b: f64) -> f64 {
    if a < b {
        a
    } else {
        b
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum UhfKlineInterval {
    M1 = 1,
    H1 = 60,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeItem {
    pub id: u64,
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    pub second: u64,
    pub event_time: u64,
    pub transact_time: u64,
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuotationKline {
    pub symbol: String,
    pub event_time: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub interval: UhfKlineInterval,
    pub start_trade_id: u64,
    pub end_trade_id: u64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub bid_volume: f64,
    pub is_final: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuotationTicker {
    pub symbol: String,
    pub event_time: u64,
    pub tf_open_price: f64,
    pub tf_high_price: f64,
    pub tf_low_price: f64,
    pub tf_total_qty: f64,
    pub tf_total_value: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BestBidAskItem {
    pub symbol: String,
    pub best_bid_price: f64,
    pub best_ask_price: f64,
    pub best_bid_qty: f64,
    pub best_ask_qty: f64,
    pub event_time: u64,
    pub transact_time: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct TradeWindowPriceQty {
    pub price: f64,
    pub qty: f64,
    pub bid_qty: f64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub second: u64,
    pub event_time: u64,
}

impl Default for TradeWindowPriceQty {
    fn default() -> Self {
        Self {
            price: f64::NAN,
            qty: f64::NAN,
            bid_qty: f64::NAN,
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            second: 0,
            event_time: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SecondTradeItem {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub price: f64,
    pub qty: f64,
    pub bid_qty: f64,
    pub second: u64,
    pub event_time: u64,
    pub items: VecDeque<TradeItem>,
}

impl Default for SecondTradeItem {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            price: f64::NAN,
            qty: 0.0,
            bid_qty: 0.0,
            second: 0,
            event_time: 0,
            items: VecDeque::with_capacity(MAX_TRADES_PER_SECOND),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeSecondsWindow {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub qty: f64,
    pub price: f64,
    pub second: u64,
    pub items: VecDeque<SecondTradeItem>,
}

// 缓存66秒逐笔行情
impl Default for TradeSecondsWindow {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            qty: 0.0,
            price: f64::NAN,
            second: 0,
            items: VecDeque::with_capacity(TRADE_SECONDS_WINDOW_SIZE),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeMinuteItem {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub price: f64,
    pub qty: f64,
    pub bid_qty: f64,
    pub second: u64,
    pub seconds: VecDeque<SecondTradeItem>,
}

impl Default for TradeMinuteItem {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            price: f64::NAN,
            qty: 0.0,
            bid_qty: 0.0,
            second: 0,
            seconds: VecDeque::with_capacity(60),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeMinutesWindow {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub price: f64,
    pub qty: f64,
    pub second: u64,
    pub items: VecDeque<TradeMinuteItem>,
}

impl Default for TradeMinutesWindow {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            price: f64::NAN,
            qty: 0.0,
            second: 0,
            items: VecDeque::with_capacity(TRADE_MINUTES_WINDOW_SIZE),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TradeHourItem {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub qty: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub is_final: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeHourWindow {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub qty: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub tf_total_qty: f64,
    pub tf_total_value: f64,
    pub tf_open_price: f64,
    pub tf_high_price: f64,
    pub tf_low_price: f64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub second: u64,
    pub items: VecDeque<TradeHourItem>,
}

impl Default for TradeHourWindow {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            qty: 0.0,
            bid_qty: 0.0,
            ask_qty: 0.0,
            tf_total_qty: 0.0,
            tf_total_value: 0.0,
            tf_open_price: f64::NAN,
            tf_high_price: f64::NAN,
            tf_low_price: f64::NAN,
            start_timestamp: 0,
            end_timestamp: 0,
            second: 0,
            items: VecDeque::with_capacity(TRADE_HOURS_WINDOW_SIZE),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct UhfTradeWindow {
    pub symbol: String,
    pub ready: bool,
    pub last_now_sec: u64,
    pub last_trade_id: u64,
    pub best_bid_ask: BestBidAskItem,
    pub seconds_window: TradeSecondsWindow,
    pub minutes_window: TradeMinutesWindow,
    pub hours_window: TradeHourWindow,
}

impl UhfTradeWindow {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            ready: false,
            last_now_sec: 0,
            last_trade_id: 0,
            best_bid_ask: BestBidAskItem::default(),
            seconds_window: TradeSecondsWindow::default(),
            minutes_window: TradeMinutesWindow::default(),
            hours_window: TradeHourWindow::default(),
        }
    }

    pub fn update(&mut self, item: TradeItem) {
        if item.id < self.last_trade_id {
            return;
        }
        self.update_second_window(&item);

        self.last_trade_id = item.id;
        self.last_now_sec = item.second;
        self.ready = true;
    }

    fn update_second_window(&mut self, item: &TradeItem) {
        let bid_qty = if item.is_buyer_maker { item.qty } else { 0.0 };

        if !self.ready || self.seconds_window.items.is_empty() {
            let mut items = VecDeque::with_capacity(MAX_TRADES_PER_SECOND);
            items.push_back(item.clone());
            let new_item = SecondTradeItem {
                open: item.price,
                close: item.price,
                high: item.price,
                low: item.price,
                price: item.price,
                qty: item.qty,
                bid_qty,
                second: item.second,
                event_time: item.event_time,
                items,
            };
            self.seconds_window.qty = item.qty;
            self.seconds_window.open = item.price;
            self.seconds_window.close = item.price;
            self.seconds_window.high = item.price;
            self.seconds_window.low = item.price;
            self.seconds_window.price = new_item.close;
            self.seconds_window.second = new_item.second;
            self.seconds_window.items.push_back(new_item.clone());
            return;
        }

        let last_second = self
            .seconds_window
            .items
            .back()
            .map(|value| value.second)
            .unwrap_or(item.second);

        if item.second == last_second {
            // We need to track the old state before modification
            if let Some(current) = self.seconds_window.items.back_mut() {
                current.close = item.price;
                current.price = item.price;
                current.qty += item.qty;
                current.bid_qty += bid_qty;
                current.high = dmax(current.high, item.price);
                current.low = dmin(current.low, item.price);
                current.items.push_back(item.clone());
                self.update_second_item_incrementally(&item);
            }
            return;
        }

        if item.second > last_second + 1 {
            for sec in (last_second + 1)..item.second {
                let placeholder = SecondTradeItem {
                    open: f64::NAN,
                    close: f64::NAN,
                    high: f64::NAN,
                    low: f64::NAN,
                    price: f64::NAN,
                    qty: 0.0,
                    bid_qty: 0.0,
                    second: sec,
                    event_time: sec * 1000,
                    ..Default::default()
                };
                self.push_second(placeholder);
            }
        }

        let mut items = VecDeque::with_capacity(MAX_TRADES_PER_SECOND);
        items.push_back(item.clone());
        let new_second = SecondTradeItem {
            open: item.price,
            close: item.price,
            high: item.price,
            low: item.price,
            price: item.price,
            qty: item.qty,
            bid_qty,
            second: item.second,
            event_time: item.event_time,
            items,
        };
        self.push_second(new_second);
    }

    fn push_second(&mut self, value: SecondTradeItem) {
        // Check if we are at capacity and need to remove the oldest item
        if self.seconds_window.items.len() == TRADE_SECONDS_WINDOW_SIZE {
            if let Some(old_item) = self.seconds_window.items.pop_front() {
                self.remove_second_item_incrementally(&old_item);
            }
        }

        self.add_second_item_incrementally(&value);
        self.seconds_window.items.push_back(value);
    }

    fn add_second_item_incrementally(&mut self, new_second_trade: &SecondTradeItem) {
        if !new_second_trade.open.is_nan() {
            self.seconds_window.qty += new_second_trade.qty;
            self.seconds_window.close = new_second_trade.close;
            self.seconds_window.price = new_second_trade.price;
            self.seconds_window.high = dmax(self.seconds_window.high, new_second_trade.high);
            self.seconds_window.low = dmin(self.seconds_window.low, new_second_trade.low);
        }
    }

    fn remove_second_item_incrementally(&mut self, old_item: &SecondTradeItem) {
        if old_item.open.is_nan() {
            return;
        }

        self.seconds_window.qty -= old_item.qty;

        // Step 2: 判断是否需要重新扫描整个窗口来更新 high/low/open
        let mut need_rescan_high = false;
        let mut need_rescan_low = false;
        let mut need_rescan_open = false;

        // Check if the removed item was contributing to the global high/low/open
        if !old_item.high.is_nan() && old_item.high == self.seconds_window.high {
            need_rescan_high = true;
        }
        if !old_item.low.is_nan() && old_item.low == self.seconds_window.low {
            need_rescan_low = true;
        }
        if !old_item.open.is_nan() && !self.seconds_window.items.is_empty() {
            if let Some(front) = self.seconds_window.items.front() {
                if front.second == old_item.second {
                    need_rescan_open = true;
                }
            }
        }

        if !(need_rescan_high || need_rescan_low || need_rescan_open) {
            return;
        }

        self.recompute_seconds_window_fields_after_removal();
    }

    fn update_second_item_incrementally(&mut self, item: &TradeItem) {
        self.seconds_window.qty += item.qty;
        self.seconds_window.high = dmax(self.seconds_window.high, item.price);
        self.seconds_window.low = dmin(self.seconds_window.low, item.price);
    }

    fn reset_seconds_window(&mut self) {
        self.seconds_window.open = f64::NAN;
        self.seconds_window.close = f64::NAN;
        self.seconds_window.high = f64::NAN;
        self.seconds_window.low = f64::NAN;
    }

    fn recompute_seconds_window_fields_after_removal(&mut self) {
        self.seconds_window.open = f64::NAN;
        self.seconds_window.close = f64::NAN;
        self.seconds_window.high = f64::NAN;
        self.seconds_window.low = f64::NAN;

        let mut first = true;
        for item in &self.seconds_window.items {
            if item.open.is_nan() || item.close.is_nan() || item.high.is_nan() || item.low.is_nan()
            {
                continue;
            }

            if first {
                self.seconds_window.open = item.open;
                self.seconds_window.close = item.close;
                self.seconds_window.high = item.high;
                self.seconds_window.low = item.low;
                first = false;
            } else {
                self.seconds_window.close = item.close;
                self.seconds_window.high = dmax(self.seconds_window.high, item.high);
                self.seconds_window.low = dmin(self.seconds_window.low, item.low);
            }
        }

        if let Some(last) = self.seconds_window.items.back() {
            self.seconds_window.price = last.close;
            self.seconds_window.second = last.second;
        }
    }

    fn push_minute(&mut self, value: TradeMinuteItem) {
        if self.minutes_window.items.len() == TRADE_MINUTES_WINDOW_SIZE {
            if let Some(old_item) = self.minutes_window.items.pop_front() {
                self.remove_minute_item_incrementally(&old_item);
            }
        }

        self.add_minute_item_incrementally(&value);
        self.minutes_window.items.push_back(value);
    }

    fn add_minute_item_incrementally(&mut self, new_item: &TradeMinuteItem) {
        if !new_item.open.is_nan() {
            self.minutes_window.close = new_item.close;
            self.minutes_window.high = dmax(self.minutes_window.high, new_item.high);
            self.minutes_window.low = dmin(self.minutes_window.low, new_item.low);
        }

        self.minutes_window.qty += new_item.qty;
        self.minutes_window.price = new_item.close;
        self.minutes_window.second = new_item.second;
    }

    fn remove_minute_item_incrementally(&mut self, old_item: &TradeMinuteItem) {
        self.minutes_window.qty -= old_item.qty;

        if self.minutes_window.items.is_empty() {
            self.reset_minutes_window();
            return;
        }

        let mut need_rescan_high = false;
        let mut need_rescan_low = false;
        let mut need_rescan_open = false;

        if !old_item.high.is_nan() && old_item.high == self.minutes_window.high {
            need_rescan_high = true;
        }
        if !old_item.low.is_nan() && old_item.low == self.minutes_window.low {
            need_rescan_low = true;
        }
        if !old_item.open.is_nan() && !self.minutes_window.items.is_empty() {
            if let Some(front) = self.minutes_window.items.front() {
                if front.second == old_item.second {
                    need_rescan_open = true;
                }
            }
        }

        if !(need_rescan_high || need_rescan_low || need_rescan_open) {
            return;
        }

        self.recompute_minutes_window_fields_after_removal();
    }

    fn reset_minutes_window(&mut self) {
        self.minutes_window.open = f64::NAN;
        self.minutes_window.close = f64::NAN;
        self.minutes_window.high = f64::NAN;
        self.minutes_window.low = f64::NAN;
    }

    fn recompute_minutes_window_fields_after_removal(&mut self) {
        self.minutes_window.open = f64::NAN;
        self.minutes_window.close = f64::NAN;
        self.minutes_window.high = f64::NAN;
        self.minutes_window.low = f64::NAN;

        let mut first = true;
        for item in &self.minutes_window.items {
            if item.open.is_nan() || item.close.is_nan() || item.high.is_nan() || item.low.is_nan()
            {
                continue;
            }

            if first {
                self.minutes_window.open = item.open;
                self.minutes_window.close = item.close;
                self.minutes_window.high = item.high;
                self.minutes_window.low = item.low;
                first = false;
            } else {
                self.minutes_window.close = item.close;
                self.minutes_window.high = dmax(self.minutes_window.high, item.high);
                self.minutes_window.low = dmin(self.minutes_window.low, item.low);
            }
        }

        if let Some(last) = self.minutes_window.items.back() {
            self.minutes_window.price = last.close;
            self.minutes_window.second = last.second;
        }
    }

    pub fn update_kline(&mut self, item: QuotationKline) {
        match item.interval {
            UhfKlineInterval::M1 => self.update_kline_1m(item),
            UhfKlineInterval::H1 => self.update_kline_1h(item),
        }
    }

    pub fn update_kline_1m(&mut self, item: QuotationKline) {
        let minute_second = item.start_timestamp / 1000;

        if let Some(current) = self.minutes_window.items.back_mut() {
            if current.second == minute_second {
                let old_qty = current.qty;
                current.open = item.open;
                current.close = item.close;
                current.high = item.high;
                current.low = item.low;
                current.price = item.close;
                current.qty = item.volume;
                current.bid_qty = item.bid_volume;

                self.minutes_window.qty += current.qty - old_qty;
                self.minutes_window.close = current.close;
                self.minutes_window.price = current.price;
                self.minutes_window.high = dmax(self.minutes_window.high, current.high);
                self.minutes_window.low = dmin(self.minutes_window.low, current.low);

                self.minutes_window.second = current.second;
                return;
            }
        }

        let new_item = TradeMinuteItem {
            open: item.open,
            close: item.close,
            high: item.high,
            low: item.low,
            price: item.close,
            qty: item.volume,
            bid_qty: item.bid_volume,
            second: minute_second,
            seconds: VecDeque::with_capacity(60),
        };

        self.push_minute(new_item.clone());
    }

    fn update_kline_1h(&mut self, item: QuotationKline) {
        let new_hour_item = TradeHourItem {
            open: item.open,
            close: item.close,
            high: item.high,
            low: item.low,
            start_timestamp: item.start_timestamp,
            end_timestamp: item.end_timestamp,
            first_trade_id: item.start_trade_id,
            last_trade_id: item.end_trade_id,
            qty: item.volume,
            bid_qty: item.bid_volume,
            ask_qty: item.volume - item.bid_volume,
            is_final: item.is_final,
        };

        if let Some(current) = self.hours_window.items.back_mut() {
            if current.start_timestamp == item.start_timestamp {
                // Store old values for incremental update
                let old_qty = current.qty;
                let old_bid_qty = current.bid_qty;
                let old_ask_qty = current.ask_qty;

                // Update the existing item
                current.close = item.close;
                current.high = item.high;
                current.low = item.low;
                current.end_timestamp = item.end_timestamp;
                current.last_trade_id = item.end_trade_id;
                current.qty = item.volume;
                current.bid_qty = item.bid_volume;
                current.ask_qty = item.volume - item.bid_volume;
                current.is_final = item.is_final;

                // Incrementally update the window statistics
                self.hours_window.qty += current.qty - old_qty;
                self.hours_window.bid_qty += current.bid_qty - old_bid_qty;
                self.hours_window.ask_qty += current.ask_qty - old_ask_qty;
                self.hours_window.close = current.close;
                self.hours_window.end_timestamp = current.end_timestamp;
                self.hours_window.second = current.start_timestamp / 1000;

                // Update high and low with dmax/dmin to handle NaN properly
                self.hours_window.high = dmax(self.hours_window.high, current.high);
                self.hours_window.low = dmin(self.hours_window.low, current.low);
                return;
            }
        }

        // Check if we need to remove the oldest item
        if self.hours_window.items.len() == TRADE_HOURS_WINDOW_SIZE {
            if let Some(old_item) = self.hours_window.items.pop_front() {
                self.remove_hour_item_incrementally(&old_item);
            };
        }

        // Push the new item and update incrementally
        self.add_hour_item_incrementally(&new_hour_item);
        self.hours_window.items.push_back(new_hour_item);
    }

    /// Incrementally update the hours window statistics when adding a new hour item
    fn add_hour_item_incrementally(&mut self, new_item: &TradeHourItem) {
        // Update cumulative quantities
        self.hours_window.qty += new_item.qty;
        self.hours_window.bid_qty += new_item.bid_qty;
        self.hours_window.ask_qty += new_item.ask_qty;

        // Handle first item initialization
        if self.hours_window.open.is_nan() {
            self.hours_window.open = new_item.open;
            self.hours_window.close = new_item.close;
            self.hours_window.high = new_item.high;
            self.hours_window.low = new_item.low;
            self.hours_window.start_timestamp = new_item.start_timestamp;
        } else {
            // Update close price
            self.hours_window.close = new_item.close;

            // Update high and low with dmax/dmin to handle NaN properly
            self.hours_window.high = dmax(self.hours_window.high, new_item.high);
            self.hours_window.low = dmin(self.hours_window.low, new_item.low);
        }

        // Update latest price and timestamp
        self.hours_window.end_timestamp = new_item.end_timestamp;
        self.hours_window.second = new_item.start_timestamp / 1000;
    }

    /// Incrementally update the hours window statistics when removing an old hour item
    fn remove_hour_item_incrementally(&mut self, old_item: &TradeHourItem) {
        // Subtract the quantities of the removed item
        self.hours_window.qty -= old_item.qty;
        self.hours_window.bid_qty -= old_item.bid_qty;
        self.hours_window.ask_qty -= old_item.ask_qty;

        if self.hours_window.items.is_empty() {
            self.reset_hours_window();
            return;
        }

        let mut need_rescan_high = false;
        let mut need_rescan_low = false;
        let mut need_rescan_open = false;

        if !old_item.high.is_nan() && old_item.high == self.hours_window.high {
            need_rescan_high = true;
        }
        if !old_item.low.is_nan() && old_item.low == self.hours_window.low {
            need_rescan_low = true;
        }
        if !old_item.open.is_nan() && !self.hours_window.items.is_empty() {
            if let Some(front) = self.hours_window.items.front() {
                if front.start_timestamp == old_item.start_timestamp {
                    need_rescan_open = true;
                }
            }
        }

        if !(need_rescan_high || need_rescan_low || need_rescan_open) {
            return;
        }

        self.recompute_hours_window_fields_after_removal();
    }

    fn reset_hours_window(&mut self) {
        self.hours_window.open = f64::NAN;
        self.hours_window.close = f64::NAN;
        self.hours_window.high = f64::NAN;
        self.hours_window.low = f64::NAN;
        self.hours_window.qty = 0.0;
        self.hours_window.bid_qty = 0.0;
        self.hours_window.ask_qty = 0.0;
        self.hours_window.start_timestamp = 0;
        self.hours_window.end_timestamp = 0;
        self.hours_window.second = 0;
    }

    // Helper function to partially recompute only necessary fields
    fn recompute_hours_window_fields_after_removal(&mut self) {
        self.hours_window.open = f64::NAN;
        self.hours_window.close = f64::NAN;
        self.hours_window.high = f64::NAN;
        self.hours_window.low = f64::NAN;
        self.hours_window.qty = 0.0;
        self.hours_window.bid_qty = 0.0;
        self.hours_window.ask_qty = 0.0;

        let mut first = true;
        for item in &self.hours_window.items {
            self.hours_window.qty += item.qty;
            self.hours_window.bid_qty += item.bid_qty;
            self.hours_window.ask_qty += item.ask_qty;

            if first {
                self.hours_window.open = item.open;
                self.hours_window.close = item.close;
                self.hours_window.high = item.high;
                self.hours_window.low = item.low;
                self.hours_window.start_timestamp = item.start_timestamp;
                first = false;
            } else {
                self.hours_window.close = item.close;
                self.hours_window.high = dmax(self.hours_window.high, item.high);
                self.hours_window.low = dmin(self.hours_window.low, item.low);
            }
        }

        // Update latest price and timestamp from the last item
        if let Some(last) = self.hours_window.items.back() {
            self.hours_window.end_timestamp = last.end_timestamp;
            self.hours_window.second = last.start_timestamp / 1000;
        }
    }

    pub fn update_ticker(&mut self, item: QuotationTicker) {
        self.hours_window.tf_total_qty = item.tf_total_qty;
        self.hours_window.tf_total_value = item.tf_total_value;
        self.hours_window.tf_open_price = item.tf_open_price;
        self.hours_window.tf_high_price = item.tf_high_price;
        self.hours_window.tf_low_price = item.tf_low_price;
    }

    pub fn update_best_bid_ask(&mut self, item: BestBidAskItem) {
        self.best_bid_ask = item;
    }

    pub fn get_second_avg_qty(&self) -> f64 {
        if self.seconds_window.items.len() <= 1 {
            return 0.0;
        }
        let current = self
            .seconds_window
            .items
            .back()
            .map(|v| v.qty)
            .unwrap_or(0.0);
        (self.seconds_window.qty - current) / ((self.seconds_window.items.len() - 1) as f64)
    }

    pub fn get_current_second_qty(&self) -> f64 {
        self.seconds_window
            .items
            .back()
            .map(|v| v.qty)
            .unwrap_or(0.0)
    }

    pub fn get_current_open_price(&self) -> f64 {
        self.seconds_window
            .items
            .back()
            .map(|v| v.open)
            .unwrap_or(f64::NAN)
    }

    pub fn get_current_close_price(&self) -> f64 {
        self.seconds_window
            .items
            .back()
            .map(|v| v.close)
            .unwrap_or(f64::NAN)
    }

    pub fn get_current_low_price(&self) -> f64 {
        self.seconds_window
            .items
            .back()
            .map(|v| v.low)
            .unwrap_or(f64::NAN)
    }

    pub fn get_current_high_price(&self) -> f64 {
        self.seconds_window
            .items
            .back()
            .map(|v| v.high)
            .unwrap_or(f64::NAN)
    }

    pub fn get_best_bid_price(&self) -> f64 {
        self.best_bid_ask.best_bid_price
    }

    pub fn get_best_ask_price(&self) -> f64 {
        self.best_bid_ask.best_ask_price
    }

    pub fn get_target_price_qty_by_idx(&self, idx: i32) -> Option<TradeWindowPriceQty> {
        if self.seconds_window.items.is_empty() {
            return None;
        }

        let len = self.seconds_window.items.len() as i32;
        let target = (len + idx).clamp(0, len - 1) as usize;
        let item = &self.seconds_window.items[target];
        Some(TradeWindowPriceQty {
            price: item.price,
            qty: item.qty,
            bid_qty: item.bid_qty,
            open: item.open,
            close: item.close,
            high: item.high,
            low: item.low,
            second: item.second,
            event_time: item.event_time,
        })
    }

    pub fn get_target_price_qty_by_second(&self, target_second: u64) -> TradeWindowPriceQty {
        for item in &self.seconds_window.items {
            if item.second == target_second {
                return TradeWindowPriceQty {
                    price: item.price,
                    qty: item.qty,
                    bid_qty: item.bid_qty,
                    open: item.open,
                    close: item.close,
                    high: item.high,
                    low: item.low,
                    second: item.second,
                    event_time: item.event_time,
                };
            }
        }
        TradeWindowPriceQty::default()
    }

    pub fn get_hour_price_qty(&self) -> TradeWindowPriceQty {
        if self.minutes_window.items.is_empty() {
            return TradeWindowPriceQty::default();
        }

        let mut out = TradeWindowPriceQty::default();
        let mut first = true;
        for item in &self.minutes_window.items {
            if item.open.is_nan() {
                continue;
            }
            if first {
                out.open = item.open;
                out.close = item.close;
                out.high = item.high;
                out.low = item.low;
                out.second = item.second;
                first = false;
            } else {
                out.close = item.close;
                out.high = dmax(out.high, item.high);
                out.low = dmin(out.low, item.low);
            }
            out.qty = if out.qty.is_nan() {
                item.qty
            } else {
                out.qty + item.qty
            };
            out.bid_qty = if out.bid_qty.is_nan() {
                item.bid_qty
            } else {
                out.bid_qty + item.bid_qty
            };
            out.price = item.price;
        }
        out
    }

    pub fn get_target_minute_price_qty(&self, idx: i32) -> TradeWindowPriceQty {
        if idx > 0 || self.minutes_window.items.is_empty() {
            return TradeWindowPriceQty::default();
        }

        let len = self.minutes_window.items.len() as i32;
        if -idx > len {
            return TradeWindowPriceQty::default();
        }

        let target = (len + idx) as usize;

        let item = &self.minutes_window.items[target];
        TradeWindowPriceQty {
            price: item.price,
            qty: item.qty,
            bid_qty: item.bid_qty,
            open: item.open,
            close: item.close,
            high: item.high,
            low: item.low,
            second: item.second,
            event_time: 0,
        }
    }

    pub fn get_target_closed_minute_price_qty(&self, idx: i32) -> TradeWindowPriceQty {
        if self.minutes_window.items.is_empty() || idx > 0 {
            return TradeWindowPriceQty::default();
        }

        self.get_target_minute_price_qty(idx)
    }

    pub fn get_latest_60_minutes_qty(&self) -> f64 {
        if self.minutes_window.items.len() < 30 {
            return 0.0;
        }
        self.minutes_window.qty
    }

    pub fn get_target_bid_ask_qty(&self, target_second: u64) -> Option<(f64, f64)> {
        None
    }

    pub fn get_target_minute_item(&self, idx: usize) -> Option<&TradeMinuteItem> {
        self.minutes_window.items.get(idx)
    }

    pub async fn dump_to_file(
        &self,
        filepath: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(parent) = std::path::Path::new(filepath).parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let payload = serde_json::to_vec_pretty(self)?;
        tokio::fs::write(filepath, payload).await?;
        Ok(())
    }

    pub fn get_24hour_price_qty(&self) -> TradeWindowPriceQty {
        if self.hours_window.items.len() < 2 {
            return TradeWindowPriceQty::default();
        }

        TradeWindowPriceQty {
            price: self.hours_window.close,
            qty: self.hours_window.qty / self.hours_window.items.len() as f64,
            bid_qty: self.hours_window.bid_qty,
            open: self.hours_window.open,
            close: self.hours_window.close,
            high: self.hours_window.high,
            low: self.hours_window.low,
            second: self.hours_window.second,
            event_time: 0,
        }
    }

    pub fn get_target_hour_price_qty(&self, idx: i32) -> TradeWindowPriceQty {
        if idx > 0 || self.hours_window.items.is_empty() {
            return TradeWindowPriceQty::default();
        }

        let len = self.hours_window.items.len() as i32;
        if -idx >= len {
            return TradeWindowPriceQty::default();
        }

        let target = (len - 1 + idx) as usize;
        let hour = &self.hours_window.items[target];
        TradeWindowPriceQty {
            price: hour.close,
            qty: hour.qty,
            bid_qty: hour.bid_qty,
            open: hour.open,
            close: hour.close,
            high: hour.high,
            low: hour.low,
            second: hour.start_timestamp / 1000,
            event_time: 0,
        }
    }

    pub fn get_recent_second(&self, idx: i32) -> Option<&SecondTradeItem> {
        if idx > 0 || self.seconds_window.items.is_empty() {
            return None;
        }

        let len = self.seconds_window.items.len() as i32;
        if -idx > len {
            return None;
        }

        let target = (len + idx) as usize;

        let item = &self.seconds_window.items[target];
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::Path;

    #[test]
    fn test_update_with_real_trade_data() {
        // Create a new UhfTradeWindow instance
        let mut window = UhfTradeWindow::new("SIRENUSDT");

        // Load trade data from file
        let file_path = Path::new("miraelis/tests/data/sirenusdt.trade");
        let file = File::open(file_path).expect("Failed to open trade data file");
        let reader = BufReader::new(file);

        // Counter to track number of trades processed
        let mut trade_count = 0;

        // Process each line in the file
        let mut trades = Vec::with_capacity(1000);
        for line in reader.lines() {
            let line = line.expect("Failed to read line");
            if line.trim().is_empty() {
                continue;
            }

            // Parse the JSON line into a TradeItem
            let trade_item = parse_trade_line(&line);

            trades.push(trade_item);
        }

        let mut total_qty = 0.0;
        window.update(trades[0].clone());
        total_qty += trades[0].qty;
        if let Ok(payload) = serde_json::to_string_pretty(&window) {
            assert_eq!(1, window.seconds_window.items.len());

            assert_eq!(4.0, window.get_current_second_qty());
            assert_eq!(0.6664, window.get_current_open_price());
            assert_eq!(0.6664, window.get_current_close_price());
            assert_eq!(0.6664, window.get_current_high_price());
            assert_eq!(0.6664, window.get_current_low_price());

            if let Some(current_second_price_qty) = window.get_target_price_qty_by_idx(-1) {
                assert_eq!(4.0, current_second_price_qty.qty);
                assert_eq!(0.0, current_second_price_qty.bid_qty);
                assert_eq!(0.6664, current_second_price_qty.open);
                assert_eq!(0.6664, current_second_price_qty.close);
                assert_eq!(0.6664, current_second_price_qty.high);
                assert_eq!(0.6664, current_second_price_qty.low);
                assert_eq!(0.6664, current_second_price_qty.price);
            }
        }

        for i in 1..10 {
            window.update(trades[i].clone());
            total_qty += trades[i].qty;
        }
        // if let Ok(payload) = serde_json::to_string_pretty(&window) {
        //     println!("update first 10 trades, window: {}", payload);
        // }
        assert_eq!(0.6664, window.seconds_window.open);
        assert_eq!(0.6666, window.seconds_window.close);
        assert_eq!(0.6666, window.seconds_window.high);
        assert_eq!(0.6664, window.seconds_window.low);
        assert_eq!(262.0, window.seconds_window.qty);
        assert_eq!(7, window.seconds_window.items.len());

        if let Some(current_second_price_qty) = window.get_target_price_qty_by_idx(-1) {
            assert_eq!(0.6666, current_second_price_qty.open);
            assert_eq!(0.6666, current_second_price_qty.close);
            assert_eq!(0.6666, current_second_price_qty.high);
            assert_eq!(0.6666, current_second_price_qty.low);
            assert_eq!(40.0, current_second_price_qty.qty);
            assert_eq!(0.0, current_second_price_qty.bid_qty);
        }

        if let Some(pre_second_price_qty) = window.get_target_price_qty_by_idx(-2) {
            assert!(pre_second_price_qty.open.is_nan());
            assert!(pre_second_price_qty.close.is_nan());
            assert!(pre_second_price_qty.high.is_nan());
            assert!(pre_second_price_qty.low.is_nan());
            assert_eq!(0.0, pre_second_price_qty.qty);
            assert_eq!(0.0, pre_second_price_qty.bid_qty);
        }

        if let Some(pre_pre_second_price_qty) = window.get_target_price_qty_by_idx(-3) {
            assert_eq!(0.6666, pre_pre_second_price_qty.open);
            assert_eq!(0.6666, pre_pre_second_price_qty.close);
            assert_eq!(0.6666, pre_pre_second_price_qty.high);
            assert_eq!(0.6666, pre_pre_second_price_qty.low);
            assert_eq!(8.0, pre_pre_second_price_qty.qty);
            assert_eq!(8.0, pre_pre_second_price_qty.bid_qty);
        }

        for i in 11..228 {
            window.update(trades[i].clone());
            total_qty += trades[i].qty;
        }
        if let Ok(payload) = serde_json::to_string_pretty(&window) {
            println!("update first 200 trades, window: {}", payload);
        }

        assert_eq!(471010082, window.last_trade_id);
        assert_eq!(TRADE_SECONDS_WINDOW_SIZE, window.seconds_window.items.len());
        assert_eq!(15698.0, window.seconds_window.qty);

        if let Some(oldest_second_price_qty) = window.get_target_price_qty_by_idx(-66) {
            assert!(oldest_second_price_qty.open.is_nan());
            assert!(oldest_second_price_qty.close.is_nan());
            assert!(oldest_second_price_qty.high.is_nan());
            assert!(oldest_second_price_qty.low.is_nan());
            assert_eq!(0.0, oldest_second_price_qty.qty);
            assert_eq!(0.0, oldest_second_price_qty.bid_qty);
            assert_eq!(1777575895, oldest_second_price_qty.second);
        }

        // Print final state for verification
        println!("Final state after processing {} trades:", trade_count);
        println!("Ready: {}", window.ready);
        println!("Last trade ID: {}", window.last_trade_id);
        println!("Last now sec: {}", window.last_now_sec);
        println!("total qty: {}", total_qty);

        // Verify that the window is ready after processing trades
        assert!(
            window.ready,
            "Window should be ready after processing trades"
        );

        // Verify that seconds window has items
        assert!(
            !window.seconds_window.items.is_empty(),
            "Seconds window should have items"
        );

        // Verify that the last trade ID matches the last item in the file
        // Note: This assumes the file is sorted by trade ID
        println!(
            "Seconds window items count: {}",
            window.seconds_window.items.len()
        );
    }

    fn parse_trade_line(line: &str) -> TradeItem {
        use serde_json::Value;

        // Parse the JSON line
        let v: Value = serde_json::from_str(line).expect("Failed to parse JSON line");

        // Extract fields from the JSON
        let data = &v["data"];

        // Convert timestamp to seconds
        let trade_timestamp = data["trade_timestamp"].as_u64().unwrap_or(0);
        let second = trade_timestamp / 1000; // Convert milliseconds to seconds

        // Create TradeItem
        TradeItem {
            id: data["id"].as_str().unwrap_or("0").parse().unwrap_or(0),
            symbol: v["symbol"].as_str().unwrap_or("").to_string(),
            price: data["price"].as_f64().unwrap_or(0.0),
            qty: data["amount"].as_f64().unwrap_or(0.0),
            second,
            event_time: trade_timestamp,
            transact_time: trade_timestamp,
            is_buyer_maker: data["side"].as_str().map(|s| s == "Buy").unwrap_or(false),
        }
    }
}

