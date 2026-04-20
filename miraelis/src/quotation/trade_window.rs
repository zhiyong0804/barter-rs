use serde::Serialize;
use std::collections::VecDeque;

pub const TRADE_MILLI_SECONDS_WINDOW_SIZE: usize = 6;
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct MilliSecondTradeItem {
    pub price: f64,
    pub qty: f64,
    pub second: u64,
    pub event_time: u64,
    pub transact_time: u64,
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SingleSecondTradeItems {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub price: f64,
    pub qty: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub max_qty: f64,
    pub second: u64,
    pub event_time: u64,
    pub items: VecDeque<MilliSecondTradeItem>,
}

impl Default for SingleSecondTradeItems {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            price: f64::NAN,
            qty: 0.0,
            bid_qty: 0.0,
            ask_qty: 0.0,
            max_qty: 0.0,
            second: 0,
            event_time: 0,
            items: VecDeque::with_capacity(MAX_TRADES_PER_SECOND),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeMilliSecondsWindow {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub price: f64,
    pub qty: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub items: VecDeque<SingleSecondTradeItems>,
}

impl Default for TradeMilliSecondsWindow {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            price: f64::NAN,
            qty: 0.0,
            bid_qty: 0.0,
            ask_qty: 0.0,
            items: VecDeque::with_capacity(TRADE_MILLI_SECONDS_WINDOW_SIZE),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SecondTradeItem {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub price: f64,
    pub qty: f64,
    pub second: u64,
    pub event_time: u64,
    pub transact_time: u64,
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeSecondsWindow {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub qty: f64,
    pub items: VecDeque<SecondTradeItem>,
}

impl Default for TradeSecondsWindow {
    fn default() -> Self {
        Self {
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            qty: 0.0,
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
    pub milli_seconds_window: TradeMilliSecondsWindow,
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
            milli_seconds_window: TradeMilliSecondsWindow::default(),
            seconds_window: TradeSecondsWindow::default(),
            minutes_window: TradeMinutesWindow::default(),
            hours_window: TradeHourWindow::default(),
        }
    }

    pub fn update(&mut self, item: TradeItem) {
        self.update_milli_seconds_window(&item);
        self.update_second_window(&item);
        self.update_minute_window(&item);

        self.last_trade_id = item.id;
        self.last_now_sec = item.second;
        self.ready = true;
    }

    fn update_milli_seconds_window(&mut self, item: &TradeItem) {
        if !self.ready || self.milli_seconds_window.items.is_empty() {
            let second = Self::new_single_second(item);
            self.milli_seconds_window.items.push_back(second.clone());
            self.milli_seconds_window.qty = second.qty;
            self.milli_seconds_window.bid_qty = second.bid_qty;
            self.milli_seconds_window.ask_qty = second.ask_qty;
            self.milli_seconds_window.open = second.open;
            self.milli_seconds_window.close = second.close;
            self.milli_seconds_window.high = second.high;
            self.milli_seconds_window.low = second.low;
            self.milli_seconds_window.price = second.close;
            return;
        }

        let last_second = self
            .milli_seconds_window
            .items
            .back()
            .map(|value| value.second)
            .unwrap_or(item.second);

        if item.second == last_second {
            if let Some(current) = self.milli_seconds_window.items.back_mut() {
                Self::append_milli_trade(current, item);
            }

            self.milli_seconds_window.qty += item.qty;
            if item.is_buyer_maker {
                self.milli_seconds_window.bid_qty += item.qty;
            } else {
                self.milli_seconds_window.ask_qty += item.qty;
            }

            self.milli_seconds_window.close = item.price;
            self.milli_seconds_window.price = item.price;
            if self.milli_seconds_window.high.is_nan() {
                self.milli_seconds_window.high = item.price;
            } else {
                self.milli_seconds_window.high = dmax(self.milli_seconds_window.high, item.price);
            }
            if self.milli_seconds_window.low.is_nan() {
                self.milli_seconds_window.low = item.price;
            } else {
                self.milli_seconds_window.low = dmin(self.milli_seconds_window.low, item.price);
            }
            if self.milli_seconds_window.open.is_nan() {
                self.milli_seconds_window.open = item.price;
            }
            return;
        }

        let mut ohlc_needs_recompute = false;

        if item.second > last_second + 1 {
            for sec in (last_second + 1)..item.second {
                let placeholder = SingleSecondTradeItems {
                    second: sec,
                    ..SingleSecondTradeItems::default()
                };
                if let Some(removed) = self.push_single_second(placeholder) {
                    self.milli_seconds_window.qty -= removed.qty;
                    self.milli_seconds_window.bid_qty -= removed.bid_qty;
                    self.milli_seconds_window.ask_qty -= removed.ask_qty;
                    if !removed.open.is_nan() {
                        ohlc_needs_recompute = true;
                    }
                }
            }
        }

        let new_second = Self::new_single_second(item);
        self.milli_seconds_window.qty += new_second.qty;
        self.milli_seconds_window.bid_qty += new_second.bid_qty;
        self.milli_seconds_window.ask_qty += new_second.ask_qty;

        if let Some(removed) = self.push_single_second(new_second.clone()) {
            self.milli_seconds_window.qty -= removed.qty;
            self.milli_seconds_window.bid_qty -= removed.bid_qty;
            self.milli_seconds_window.ask_qty -= removed.ask_qty;
            if !removed.open.is_nan() {
                ohlc_needs_recompute = true;
            }
        }

        if ohlc_needs_recompute {
            self.recompute_milli_window_ohlc();
        } else {
            if self.milli_seconds_window.open.is_nan() {
                self.milli_seconds_window.open = new_second.open;
                self.milli_seconds_window.high = new_second.high;
                self.milli_seconds_window.low = new_second.low;
            } else {
                self.milli_seconds_window.high =
                    dmax(self.milli_seconds_window.high, new_second.high);
                self.milli_seconds_window.low = dmin(self.milli_seconds_window.low, new_second.low);
            }
            self.milli_seconds_window.close = new_second.close;
            self.milli_seconds_window.price = new_second.close;
        }

        if self.milli_seconds_window.qty < 0.0 {
            self.milli_seconds_window.qty = 0.0;
        }
        if self.milli_seconds_window.bid_qty < 0.0 {
            self.milli_seconds_window.bid_qty = 0.0;
        }
        if self.milli_seconds_window.ask_qty < 0.0 {
            self.milli_seconds_window.ask_qty = 0.0;
        }
    }

    fn push_single_second(
        &mut self,
        value: SingleSecondTradeItems,
    ) -> Option<SingleSecondTradeItems> {
        self.milli_seconds_window.items.push_back(value);
        let mut removed = None;
        while self.milli_seconds_window.items.len() > TRADE_MILLI_SECONDS_WINDOW_SIZE {
            removed = self.milli_seconds_window.items.pop_front();
        }
        removed
    }

    fn new_single_second(item: &TradeItem) -> SingleSecondTradeItems {
        let mut second = SingleSecondTradeItems {
            open: item.price,
            close: item.price,
            high: item.price,
            low: item.price,
            price: item.price,
            qty: item.qty,
            bid_qty: if item.is_buyer_maker { item.qty } else { 0.0 },
            ask_qty: if item.is_buyer_maker { 0.0 } else { item.qty },
            max_qty: item.qty,
            second: item.second,
            event_time: item.event_time,
            items: VecDeque::with_capacity(MAX_TRADES_PER_SECOND),
        };
        second.items.push_back(MilliSecondTradeItem {
            price: item.price,
            qty: item.qty,
            second: item.second,
            event_time: item.event_time,
            transact_time: item.transact_time,
            is_buyer_maker: item.is_buyer_maker,
        });
        second
    }

    fn append_milli_trade(current: &mut SingleSecondTradeItems, item: &TradeItem) {
        current.price = item.price;
        current.close = item.price;
        current.high = dmax(current.high, item.price);
        current.low = dmin(current.low, item.price);
        current.qty += item.qty;
        current.max_qty = dmax(current.max_qty, item.qty);
        current.event_time = item.event_time;
        if item.is_buyer_maker {
            current.bid_qty += item.qty;
        } else {
            current.ask_qty += item.qty;
        }

        current.items.push_back(MilliSecondTradeItem {
            price: item.price,
            qty: item.qty,
            second: item.second,
            event_time: item.event_time,
            transact_time: item.transact_time,
            is_buyer_maker: item.is_buyer_maker,
        });
        while current.items.len() > MAX_TRADES_PER_SECOND {
            current.items.pop_front();
        }
    }

    fn recompute_milli_window_ohlc(&mut self) {
        let mut first = true;
        self.milli_seconds_window.open = f64::NAN;
        self.milli_seconds_window.close = f64::NAN;
        self.milli_seconds_window.high = f64::NAN;
        self.milli_seconds_window.low = f64::NAN;

        for item in &self.milli_seconds_window.items {
            if item.open.is_nan() || item.close.is_nan() || item.high.is_nan() || item.low.is_nan()
            {
                continue;
            }
            if first {
                self.milli_seconds_window.open = item.open;
                self.milli_seconds_window.close = item.close;
                self.milli_seconds_window.high = item.high;
                self.milli_seconds_window.low = item.low;
                first = false;
            } else {
                self.milli_seconds_window.close = item.close;
                self.milli_seconds_window.high = dmax(self.milli_seconds_window.high, item.high);
                self.milli_seconds_window.low = dmin(self.milli_seconds_window.low, item.low);
            }
        }
        self.milli_seconds_window.price = self.milli_seconds_window.close;
    }

    fn update_second_window(&mut self, item: &TradeItem) {
        if !self.ready || self.seconds_window.items.is_empty() {
            self.seconds_window.items.push_back(SecondTradeItem {
                open: item.price,
                close: item.price,
                high: item.price,
                low: item.price,
                price: item.price,
                qty: item.qty,
                second: item.second,
                event_time: item.event_time,
                transact_time: item.transact_time,
                is_buyer_maker: item.is_buyer_maker,
            });
            self.recompute_seconds_window();
            return;
        }

        let last_second = self
            .seconds_window
            .items
            .back()
            .map(|value| value.second)
            .unwrap_or(item.second);

        if item.second == last_second {
            if let Some(current) = self.seconds_window.items.back_mut() {
                current.close = item.price;
                current.price = item.price;
                current.qty += item.qty;
                current.high = dmax(current.high, item.price);
                current.low = dmin(current.low, item.price);
                current.event_time = item.event_time;
                current.transact_time = item.transact_time;
                current.is_buyer_maker = item.is_buyer_maker;
            }
            self.recompute_seconds_window();
            return;
        }

        if item.second > last_second + 1 {
            for sec in (last_second + 1)..item.second {
                self.push_second(SecondTradeItem {
                    open: f64::NAN,
                    close: f64::NAN,
                    high: f64::NAN,
                    low: f64::NAN,
                    price: f64::NAN,
                    qty: 0.0,
                    second: sec,
                    event_time: 0,
                    transact_time: 0,
                    is_buyer_maker: false,
                });
            }
        }

        self.push_second(SecondTradeItem {
            open: item.price,
            close: item.price,
            high: item.price,
            low: item.price,
            price: item.price,
            qty: item.qty,
            second: item.second,
            event_time: item.event_time,
            transact_time: item.transact_time,
            is_buyer_maker: item.is_buyer_maker,
        });
        self.recompute_seconds_window();
    }

    fn push_second(&mut self, value: SecondTradeItem) {
        self.seconds_window.items.push_back(value);
        while self.seconds_window.items.len() > TRADE_SECONDS_WINDOW_SIZE {
            self.seconds_window.items.pop_front();
        }
    }

    fn recompute_seconds_window(&mut self) {
        self.seconds_window.qty = 0.0;
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

            self.seconds_window.qty += item.qty;
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
    }

    fn update_minute_window(&mut self, item: &TradeItem) {
        let minute_second = (item.second / 60) * 60;

        if !self.ready || self.minutes_window.items.is_empty() {
            let mut minute = TradeMinuteItem {
                open: item.price,
                close: item.price,
                high: item.price,
                low: item.price,
                price: item.price,
                qty: item.qty,
                bid_qty: if item.is_buyer_maker { item.qty } else { 0.0 },
                second: minute_second,
                seconds: VecDeque::with_capacity(60),
            };
            minute.seconds.push_back(SecondTradeItem {
                open: item.price,
                close: item.price,
                high: item.price,
                low: item.price,
                price: item.price,
                qty: item.qty,
                second: item.second,
                event_time: item.event_time,
                transact_time: item.transact_time,
                is_buyer_maker: item.is_buyer_maker,
            });
            self.minutes_window.items.push_back(minute);
            self.recompute_minutes_window();
            return;
        }

        let current_minute_sec = self
            .minutes_window
            .items
            .back()
            .map(|value| value.second)
            .unwrap_or(minute_second);

        if minute_second == current_minute_sec {
            if let Some(minute) = self.minutes_window.items.back_mut() {
                Self::append_trade_to_minute(minute, item);
            }
            self.recompute_minutes_window();
            return;
        }

        if minute_second > current_minute_sec + 60 {
            let mut sec = current_minute_sec + 60;
            while sec < minute_second {
                self.push_minute(TradeMinuteItem {
                    second: sec,
                    ..TradeMinuteItem::default()
                });
                sec += 60;
            }
        }

        let mut minute = TradeMinuteItem {
            open: item.price,
            close: item.price,
            high: item.price,
            low: item.price,
            price: item.price,
            qty: item.qty,
            bid_qty: if item.is_buyer_maker { item.qty } else { 0.0 },
            second: minute_second,
            seconds: VecDeque::with_capacity(60),
        };
        minute.seconds.push_back(SecondTradeItem {
            open: item.price,
            close: item.price,
            high: item.price,
            low: item.price,
            price: item.price,
            qty: item.qty,
            second: item.second,
            event_time: item.event_time,
            transact_time: item.transact_time,
            is_buyer_maker: item.is_buyer_maker,
        });
        self.push_minute(minute);
        self.recompute_minutes_window();
    }

    fn append_trade_to_minute(minute: &mut TradeMinuteItem, item: &TradeItem) {
        if let Some(last_sec) = minute.seconds.back_mut() {
            if last_sec.second == item.second {
                last_sec.close = item.price;
                last_sec.price = item.price;
                last_sec.qty += item.qty;
                last_sec.high = dmax(last_sec.high, item.price);
                last_sec.low = dmin(last_sec.low, item.price);
                last_sec.event_time = item.event_time;
                last_sec.transact_time = item.transact_time;
                last_sec.is_buyer_maker = item.is_buyer_maker;
            } else {
                minute.seconds.push_back(SecondTradeItem {
                    open: item.price,
                    close: item.price,
                    high: item.price,
                    low: item.price,
                    price: item.price,
                    qty: item.qty,
                    second: item.second,
                    event_time: item.event_time,
                    transact_time: item.transact_time,
                    is_buyer_maker: item.is_buyer_maker,
                });
                while minute.seconds.len() > 60 {
                    minute.seconds.pop_front();
                }
            }
        } else {
            minute.seconds.push_back(SecondTradeItem {
                open: item.price,
                close: item.price,
                high: item.price,
                low: item.price,
                price: item.price,
                qty: item.qty,
                second: item.second,
                event_time: item.event_time,
                transact_time: item.transact_time,
                is_buyer_maker: item.is_buyer_maker,
            });
        }

        minute.price = item.price;
        minute.close = item.price;
        minute.high = dmax(minute.high, item.price);
        minute.low = dmin(minute.low, item.price);
        minute.qty += item.qty;
        if item.is_buyer_maker {
            minute.bid_qty += item.qty;
        }
    }

    fn push_minute(&mut self, value: TradeMinuteItem) {
        self.minutes_window.items.push_back(value);
        while self.minutes_window.items.len() > TRADE_MINUTES_WINDOW_SIZE {
            self.minutes_window.items.pop_front();
        }
    }

    fn recompute_minutes_window(&mut self) {
        self.minutes_window.open = f64::NAN;
        self.minutes_window.close = f64::NAN;
        self.minutes_window.high = f64::NAN;
        self.minutes_window.low = f64::NAN;
        self.minutes_window.qty = 0.0;

        let mut first = true;
        for minute in &self.minutes_window.items {
            if minute.open.is_nan()
                || minute.close.is_nan()
                || minute.high.is_nan()
                || minute.low.is_nan()
            {
                continue;
            }

            self.minutes_window.qty += minute.qty;
            if first {
                self.minutes_window.open = minute.open;
                self.minutes_window.close = minute.close;
                self.minutes_window.high = minute.high;
                self.minutes_window.low = minute.low;
                first = false;
            } else {
                self.minutes_window.close = minute.close;
                self.minutes_window.high = dmax(self.minutes_window.high, minute.high);
                self.minutes_window.low = dmin(self.minutes_window.low, minute.low);
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
                current.open = item.open;
                current.close = item.close;
                current.high = item.high;
                current.low = item.low;
                current.price = item.close;
                current.qty = item.volume;
                current.bid_qty = item.bid_volume;
                self.recompute_minutes_window();
                return;
            }
        }

        self.push_minute(TradeMinuteItem {
            open: item.open,
            close: item.close,
            high: item.high,
            low: item.low,
            price: item.close,
            qty: item.volume,
            bid_qty: item.bid_volume,
            second: minute_second,
            seconds: VecDeque::with_capacity(60),
        });
        self.recompute_minutes_window();
    }

    fn update_kline_1h(&mut self, item: QuotationKline) {
        if let Some(current) = self.hours_window.items.back_mut() {
            if current.start_timestamp == item.start_timestamp {
                current.close = item.close;
                current.high = item.high;
                current.low = item.low;
                current.end_timestamp = item.end_timestamp;
                current.last_trade_id = item.end_trade_id;
                current.qty = item.volume;
                current.bid_qty = item.bid_volume;
                current.ask_qty = item.volume - item.bid_volume;
                current.is_final = item.is_final;
                self.recompute_hours_window();
                return;
            }
        }

        self.hours_window.items.push_back(TradeHourItem {
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
        });
        while self.hours_window.items.len() > TRADE_HOURS_WINDOW_SIZE {
            self.hours_window.items.pop_front();
        }
        self.recompute_hours_window();
    }

    fn recompute_hours_window(&mut self) {
        self.hours_window.open = f64::NAN;
        self.hours_window.close = f64::NAN;
        self.hours_window.high = f64::NAN;
        self.hours_window.low = f64::NAN;
        self.hours_window.qty = 0.0;
        self.hours_window.bid_qty = 0.0;
        self.hours_window.ask_qty = 0.0;

        let mut first = true;
        for hour in &self.hours_window.items {
            self.hours_window.qty += hour.qty;
            self.hours_window.bid_qty += hour.bid_qty;
            self.hours_window.ask_qty += hour.ask_qty;
            if first {
                self.hours_window.open = hour.open;
                self.hours_window.close = hour.close;
                self.hours_window.high = hour.high;
                self.hours_window.low = hour.low;
                self.hours_window.start_timestamp = hour.start_timestamp;
                first = false;
            } else {
                self.hours_window.close = hour.close;
                self.hours_window.high = dmax(self.hours_window.high, hour.high);
                self.hours_window.low = dmin(self.hours_window.low, hour.low);
            }
            self.hours_window.end_timestamp = hour.end_timestamp;
            self.hours_window.second = hour.start_timestamp / 1000;
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
        self.milli_seconds_window
            .items
            .back()
            .map(|v| v.qty)
            .unwrap_or(0.0)
    }

    pub fn get_current_open_price(&self) -> f64 {
        self.milli_seconds_window
            .items
            .back()
            .map(|v| v.open)
            .unwrap_or(f64::NAN)
    }

    pub fn get_current_low_price(&self) -> f64 {
        self.milli_seconds_window
            .items
            .back()
            .map(|v| v.low)
            .unwrap_or(f64::NAN)
    }

    pub fn get_current_high_price(&self) -> f64 {
        self.milli_seconds_window
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

    pub fn get_target_price_qty_by_idx(&self, idx: i32) -> TradeWindowPriceQty {
        if self.milli_seconds_window.items.is_empty() {
            return TradeWindowPriceQty::default();
        }

        let len = self.milli_seconds_window.items.len() as i32;
        let target = (len - 1 + idx).clamp(0, len - 1) as usize;
        let item = &self.milli_seconds_window.items[target];
        TradeWindowPriceQty {
            price: item.price,
            qty: item.qty,
            bid_qty: item.bid_qty,
            open: item.open,
            close: item.close,
            high: item.high,
            low: item.low,
            second: item.second,
            event_time: item.event_time,
        }
    }

    pub fn get_target_price_qty_by_second(&self, target_second: u64) -> TradeWindowPriceQty {
        for item in &self.milli_seconds_window.items {
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
        if -idx >= len {
            return TradeWindowPriceQty::default();
        }

        let target = (len - 1 + idx) as usize;
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

    pub fn get_target_closed_minute_price_qty(
        &self,
        idx: i32,
        now_sec: u64,
    ) -> TradeWindowPriceQty {
        if self.minutes_window.items.is_empty() || idx > 0 {
            return TradeWindowPriceQty::default();
        }

        let mut shift = 0;
        if let Some(current) = self.minutes_window.items.back() {
            if current.second > 0 && now_sec > 0 && (current.second / 60) == (now_sec / 60) {
                shift = 1;
            }
        }

        self.get_target_minute_price_qty(idx - shift)
    }

    pub fn get_latest_60_minutes_qty(&self) -> f64 {
        if self.minutes_window.items.len() < 30 {
            return 0.0;
        }
        self.minutes_window.qty
    }

    pub fn get_target_bid_ask_qty(&self, target_second: u64) -> Option<(f64, f64)> {
        for item in &self.milli_seconds_window.items {
            if item.second != target_second {
                continue;
            }

            let mut bid_qty = 0.0;
            let mut ask_qty = 0.0;
            let mut bid_seen = false;
            let mut ask_seen = false;
            for mill in &item.items {
                if mill.is_buyer_maker {
                    bid_qty += mill.qty;
                    bid_seen = true;
                } else {
                    ask_qty += mill.qty;
                    ask_seen = true;
                }
            }

            return Some((
                if bid_seen { bid_qty } else { f64::NAN },
                if ask_seen { ask_qty } else { f64::NAN },
            ));
        }
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
}
