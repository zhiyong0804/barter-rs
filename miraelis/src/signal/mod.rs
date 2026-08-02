use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SignalLevel {
    Urgent,
    Important,
    Normal,
    Fyi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SignalType {
    Momentum,
    Rocket,
    Frame,
    SystemStatus,
    HugeMomentum,
    Pullback,
    OrderNotify,
}

pub const ORDER_NOTIFY_STRATEGY_ID: u64 = 10_001;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SignalTypeChatIds {
    #[serde(default)]
    pub momentum: Option<String>,
    #[serde(default)]
    pub rocket: Option<String>,
    #[serde(default)]
    pub frame: Option<String>,
    #[serde(default)]
    pub system_status: Option<String>,
    #[serde(default)]
    pub huge_momentum: Option<String>,
    #[serde(default)]
    pub pullback: Option<String>,
    #[serde(default)]
    pub order_notify: Option<String>,
}

impl SignalTypeChatIds {
    pub fn has_any(&self) -> bool {
        [
            self.momentum.as_deref(),
            self.rocket.as_deref(),
            self.frame.as_deref(),
            self.system_status.as_deref(),
            self.huge_momentum.as_deref(),
            self.pullback.as_deref(),
            self.order_notify.as_deref(),
        ]
        .iter()
        .any(|value| value.is_some_and(|v| !v.trim().is_empty()))
    }

    pub fn get(&self, signal_type: SignalType) -> Option<&str> {
        let raw = match signal_type {
            SignalType::Momentum => self.momentum.as_deref(),
            SignalType::Rocket => self.rocket.as_deref(),
            SignalType::Frame => self.frame.as_deref(),
            SignalType::SystemStatus => self.system_status.as_deref(),
            SignalType::HugeMomentum => self.huge_momentum.as_deref(),
            SignalType::Pullback => self.pullback.as_deref(),
            SignalType::OrderNotify => self.order_notify.as_deref(),
        }?;

        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    }
}

pub fn signal_level_to_string(level: SignalLevel) -> &'static str {
    match level {
        SignalLevel::Urgent => "urgent",
        SignalLevel::Important => "important",
        SignalLevel::Normal => "normal",
        SignalLevel::Fyi => "fyi",
    }
}

pub fn signal_level_to_visible_string(level: SignalLevel) -> &'static str {
    match level {
        SignalLevel::Urgent => "🌟🌟🌟🌟🌟",
        SignalLevel::Important => "🌟🌟🌟🌟",
        SignalLevel::Normal => "🌟🌟🌟",
        SignalLevel::Fyi => "🌟🌟",
    }
}

pub fn signal_format_qty(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.2}k", value / 1_000.0)
    } else {
        format!("{:.0}", value)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SignalBase {
    pub signal_type: SignalType,
    pub strategy_id: u64,
    pub strategy_name: String,
}

impl SignalBase {
    pub fn new(
        signal_type: SignalType,
        strategy_id: u64,
        strategy_name: impl Into<String>,
    ) -> Self {
        Self {
            signal_type,
            strategy_id,
            strategy_name: strategy_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeSignalBase {
    pub base: SignalBase,
    pub signal_level: SignalLevel,
    pub msg: String,
    pub trigger: u64,
    pub symbol: String,
}

impl TradeSignalBase {
    pub fn new(
        signal_type: SignalType,
        strategy_name: impl Into<String>,
        strategy_id: u64,
        signal_level: SignalLevel,
        msg: impl Into<String>,
        trigger: u64,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            base: SignalBase::new(signal_type, strategy_id, strategy_name),
            signal_level,
            msg: msg.into(),
            trigger,
            symbol: symbol.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct WindowPrice {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
}

fn format_trigger_seconds(seconds: u64) -> String {
    DateTime::<Utc>::from_timestamp(seconds as i64, 0)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "invalid-time".to_owned())
}

fn format_trigger_millis(millis: u64) -> String {
    DateTime::<Utc>::from_timestamp_millis(millis as i64)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "invalid-time".to_owned())
}

#[derive(Debug, Clone, Serialize)]
pub struct MomentumSignal {
    pub base: TradeSignalBase,
    pub latest_second_price: WindowPrice,
    pub latest_minute_price: WindowPrice,
    pub latest_hour_price: WindowPrice,
    pub latest_hour_avg_qty: f64,
    pub latest_minute_qty: [f64; 6],
    pub prev_hour_qty: f64,
    pub latest_24hour_qty: f64,
    pub latest_24hour_value: f64,
    pub strong_up_count: i32,
    pub strong_down_count: i32,
    pub weak_up_count: i32,
    pub weak_down_count: i32,
    pub latest_6_minutes_price_amplitude: f64,
    pub latest_6_minutes_qty_times: f64,
    pub latest_hour_price_amplitude: f64,
    pub latest_hour_qty_times: f64,
}

impl MomentumSignal {
    pub fn format(&self) -> String {
        let mut text = format!(
            "signal level: {}\nsymbol: {}\n",
            signal_level_to_visible_string(self.base.signal_level),
            self.base.symbol
        );

        text.push_str(&format!(
            "strong up count: {}, weak up count: {}\n",
            self.strong_up_count, self.weak_up_count
        ));
        text.push_str(&format!(
            "latest 6 minutes price amplitude: {:.2}, latest 6 minutes qty times: {:.2}\n",
            self.latest_6_minutes_price_amplitude, self.latest_6_minutes_qty_times
        ));
        text.push_str(&format!(
            "latest hour price amplitude: {:.2}, latest hour qty times: {:.2}\n",
            self.latest_hour_price_amplitude, self.latest_hour_qty_times
        ));
        text.push_str(&format!(
            "latest second price: open: {:.7}, close:{:.7}, high:{:.7}, low:{:.7}\n",
            self.latest_second_price.open,
            self.latest_second_price.close,
            self.latest_second_price.high,
            self.latest_second_price.low
        ));
        text.push_str(&format!(
            "latest minute price: open: {:.7}, close:{:.7}, high:{:.7}, low:{:.7}\n",
            self.latest_minute_price.open,
            self.latest_minute_price.close,
            self.latest_minute_price.high,
            self.latest_minute_price.low
        ));
        text.push_str(&format!(
            "latest hour price: open: {:.7}, close:{:.7}, high:{:.7}, low:{:.7}\n",
            self.latest_hour_price.open,
            self.latest_hour_price.close,
            self.latest_hour_price.high,
            self.latest_hour_price.low
        ));
        text.push_str(&format!(
            "latest 5 minutes qty: {}, {}, {}, {}, {}\n",
            signal_format_qty(self.latest_minute_qty[0]),
            signal_format_qty(self.latest_minute_qty[1]),
            signal_format_qty(self.latest_minute_qty[2]),
            signal_format_qty(self.latest_minute_qty[3]),
            signal_format_qty(self.latest_minute_qty[4])
        ));
        text.push_str(&format!(
            "latest hour avg qty: {}\n",
            signal_format_qty(self.latest_hour_avg_qty)
        ));
        text.push_str(&format!(
            "prev hour qty: {}\n",
            signal_format_qty(self.prev_hour_qty)
        ));
        text.push_str(&format!(
            "latest 24hour qty: {}\n",
            signal_format_qty(self.latest_24hour_qty)
        ));
        text.push_str(&format!(
            "latest 24hour value: {}\n",
            signal_format_qty(self.latest_24hour_value)
        ));
        text.push_str(&format!("signal msg: {}\n", self.base.msg));
        text.push_str(&format!(
            "strategy name: {}\n",
            self.base.base.strategy_name
        ));
        text.push_str(&format!("strategy id: {}\n", self.base.base.strategy_id));
        text.push_str(&format!(
            "trigger: {}\n",
            format_trigger_seconds(self.base.trigger)
        ));
        text
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeRocketSignal {
    pub base: TradeSignalBase,
    pub first_second_bid_qty_percent: f64,
    pub first_second_ask_qty_percent: f64,
    pub second_second_bid_qty_percent: f64,
    pub second_second_ask_qty_percent: f64,
    pub first_second_price: WindowPrice,
    pub second_second_price: WindowPrice,
    pub first_second_bid_qty: f64,
    pub first_second_ask_qty: f64,
    pub second_second_bid_qty: f64,
    pub second_second_ask_qty: f64,
    pub latest_minute_qty: f64,
    pub latest_6s_bid_qty: f64,
    pub latest_6s_ask_qty: f64,
    pub latest_6s_high_price: f64,
    pub latest_6s_low_price: f64,
}

impl TradeRocketSignal {
    pub fn format(&self) -> String {
        let mut text = format!(
            "signal level:{}\nsymbol: {}\n",
            signal_level_to_visible_string(self.base.signal_level),
            self.base.symbol
        );
        text.push_str(&format!(
            "first second bid qty percent: {:.2}\n",
            self.first_second_bid_qty_percent
        ));
        text.push_str(&format!(
            "first second ask qty percent: {:.2}\n",
            self.first_second_ask_qty_percent
        ));
        text.push_str(&format!(
            "second second bid qty percent: {:.2}\n",
            self.second_second_bid_qty_percent
        ));
        text.push_str(&format!(
            "second second ask qty percent: {:.2}\n",
            self.second_second_ask_qty_percent
        ));
        text.push_str(&format!(
            "first second bid qty: {}\n",
            signal_format_qty(self.first_second_bid_qty)
        ));
        text.push_str(&format!(
            "first second ask qty: {}\n",
            signal_format_qty(self.first_second_ask_qty)
        ));
        text.push_str(&format!(
            "second second bid qty: {}\n",
            signal_format_qty(self.second_second_bid_qty)
        ));
        text.push_str(&format!(
            "second second ask qty: {}\n",
            signal_format_qty(self.second_second_ask_qty)
        ));
        text.push_str(&format!(
            "latest minute qty: {}\n",
            signal_format_qty(self.latest_minute_qty)
        ));
        text.push_str(&format!(
            "latest 6s bid qty: {}\n",
            signal_format_qty(self.latest_6s_bid_qty)
        ));
        text.push_str(&format!(
            "latest 6s ask qty: {}\n",
            signal_format_qty(self.latest_6s_ask_qty)
        ));
        text.push_str(&format!(
            "latest 6s high price: {:.7}\nlatest 6s low price: {:.7}\n",
            self.latest_6s_high_price, self.latest_6s_low_price
        ));
        text.push_str(&format!("signal msg: {}\n", self.base.msg));
        text.push_str(&format!(
            "strategy name: {}\n",
            self.base.base.strategy_name
        ));
        text.push_str(&format!("strategy id: {}\n", self.base.base.strategy_id));
        text.push_str(&format!(
            "trigger: {}\n",
            format_trigger_millis(self.base.trigger)
        ));
        text
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeFrameSignal {
    pub base: TradeSignalBase,
    pub stage: String,
    pub side: String,
    pub trade_id: u64,
    pub current_trade_price: f64,
    pub trigger_price: f64,
    pub signal_qty_times: f64,
    pub signal_price_percent: f64,
    pub vol_3s: f64,
    pub avg_sec_vol: f64,
    pub vol_ratio: f64,
    pub price_move_3s_pct: f64,
    pub is_accelerating: bool,
    pub short_signal: bool,
    pub long_signal: bool,
    pub stop_price_percent: f64,
    pub reversal_price_percent: f64,
    pub shadow_price_percent: f64,
    pub latest_minute_qty: f64,
    pub current_second_qty: f64,
    pub prev_second_qty: f64,
    pub high_price_since_open: f64,
    pub low_price_since_open: f64,
    pub avg_price: f64,
    pub should_stop: bool,
    pub should_reversal: bool,
    pub should_exhaust: bool,
}

impl TradeFrameSignal {
    pub fn format(&self) -> String {
        let mut text = format!(
            "signal level:{}\nsymbol: {}\nstage: {}\nside: {}\ntrade id: {}\n",
            signal_level_to_visible_string(self.base.signal_level),
            self.base.symbol,
            self.stage,
            self.side,
            self.trade_id
        );
        text.push_str(&format!(
            "current trade price: {:.7}\ntrigger price: {:.7}\n",
            self.current_trade_price, self.trigger_price
        ));
        text.push_str(&format!(
            "signal qty times: {:.4}\nsignal price percent: {:.4}\n",
            self.signal_qty_times, self.signal_price_percent
        ));
        text.push_str(&format!(
            "vol 3s: {}\navg sec vol: {}\nvol ratio: {:.4}\n",
            signal_format_qty(self.vol_3s),
            signal_format_qty(self.avg_sec_vol),
            self.vol_ratio
        ));
        text.push_str(&format!(
            "price move 3s pct: {:.6}\nis accelerating: {}\nshort signal: {}\nlong signal: {}\n",
            self.price_move_3s_pct, self.is_accelerating, self.short_signal, self.long_signal
        ));
        text.push_str(&format!(
            "stop price percent: {:.4}\nreversal price percent: {:.4}\nshadow price percent: {:.4}\n",
            self.stop_price_percent, self.reversal_price_percent, self.shadow_price_percent
        ));
        text.push_str(&format!(
            "latest minute qty: {}\ncurrent second qty: {}\nprev second qty: {}\n",
            signal_format_qty(self.latest_minute_qty),
            signal_format_qty(self.current_second_qty),
            signal_format_qty(self.prev_second_qty)
        ));
        text.push_str(&format!(
            "high price since open: {:.7}\nlow price since open: {:.7}\navg price: {:.7}\n",
            self.high_price_since_open, self.low_price_since_open, self.avg_price
        ));
        text.push_str(&format!(
            "should stop: {}\nshould reversal: {}\nshould exhaust: {}\n",
            self.should_stop, self.should_reversal, self.should_exhaust
        ));
        text.push_str(&format!("signal msg: {}\n", self.base.msg));
        text.push_str(&format!(
            "strategy name: {}\n",
            self.base.base.strategy_name
        ));
        text.push_str(&format!("strategy id: {}\n", self.base.base.strategy_id));
        text.push_str(&format!(
            "trigger: {}\n",
            format_trigger_millis(self.base.trigger)
        ));
        text
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PullbackSignal {
    pub base: TradeSignalBase,
    pub latest_minute_high_price: f64,
    pub latest_minute_low_price: f64,
    pub current_trade_price: f64,
    pub pullback_percent: f64,
    pub current_trade_id: u64,
}

impl PullbackSignal {
    pub fn format(&self) -> String {
        let mut text = format!(
            "symbol: {}\nsignal level:{}\n",
            self.base.symbol,
            signal_level_to_visible_string(self.base.signal_level)
        );
        text.push_str(&format!(
            "latest minute high price: {:.7}\nlatest minute low price: {:.7}\n",
            self.latest_minute_high_price, self.latest_minute_low_price
        ));
        text.push_str(&format!(
            "current trade price: {:.7}\ncurrent trade id: {}\npullback percent: {:.2}\n",
            self.current_trade_price, self.current_trade_id, self.pullback_percent
        ));
        text.push_str(&format!("signal msg: {}\n", self.base.msg));
        text.push_str(&format!(
            "strategy name: {}\n",
            self.base.base.strategy_name
        ));
        text.push_str(&format!("strategy id: {}\n", self.base.base.strategy_id));
        text.push_str(&format!(
            "trigger: {}\n",
            format_trigger_seconds(self.base.trigger)
        ));
        text
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderNotifySignal {
    pub base: SignalBase,
    pub phase: String,
    pub trigger: u64,
    pub origin_strategy_id: u64,
    pub symbol: String,
    pub new_client_order_id: String,
    pub order_id: u64,
    pub side: String,
    pub order_type: String,
    pub price: f64,
    pub stop_price: f64,
    pub avg_price: f64,
    pub qty: f64,
    pub executed_qty: f64,
    pub cum_qty: f64,
    pub cum_quote: f64,
    pub reduce_only: bool,
    pub close_position: bool,
    pub status: String,
    pub running_status: String,
    pub action_status: String,
    pub has_realized_pnl: bool,
    pub open_price: f64,
    pub close_price: f64,
    pub closed_qty: f64,
    pub realized_pnl: f64,
    pub realized_pnl_percent: f64,
}

impl Default for OrderNotifySignal {
    fn default() -> Self {
        Self {
            base: SignalBase::new(
                SignalType::OrderNotify,
                ORDER_NOTIFY_STRATEGY_ID,
                "order.notify",
            ),
            phase: String::new(),
            trigger: 0,
            origin_strategy_id: 0,
            symbol: String::new(),
            new_client_order_id: String::new(),
            order_id: 0,
            side: "BUY".to_owned(),
            order_type: "unknown".to_owned(),
            price: 0.0,
            stop_price: 0.0,
            avg_price: 0.0,
            qty: 0.0,
            executed_qty: 0.0,
            cum_qty: 0.0,
            cum_quote: 0.0,
            reduce_only: false,
            close_position: false,
            status: "unknown".to_owned(),
            running_status: "new".to_owned(),
            action_status: "unknown".to_owned(),
            has_realized_pnl: false,
            open_price: 0.0,
            close_price: 0.0,
            closed_qty: 0.0,
            realized_pnl: 0.0,
            realized_pnl_percent: 0.0,
        }
    }
}

impl OrderNotifySignal {
    pub fn format(&self) -> String {
        let mut text = String::from("type: order notify\n");
        text.push_str(&format!("phase: {}\n", self.phase));
        text.push_str(&format!(
            "origin strategy id: {}\n",
            self.origin_strategy_id
        ));
        text.push_str(&format!("symbol: {}\n", self.symbol));
        text.push_str(&format!(
            "new client order id: {}\n",
            self.new_client_order_id
        ));
        text.push_str(&format!("order id: {}\n", self.order_id));
        text.push_str(&format!("side: {}\n", self.side));
        text.push_str(&format!("order type: {}\n", self.order_type));
        text.push_str(&format!(
            "price: {:.7}, stop price: {:.7}, avg price: {:.7}\n",
            self.price, self.stop_price, self.avg_price
        ));
        text.push_str(&format!(
            "qty: {}, executed qty: {}, cum qty: {}, cum quote: {}\n",
            signal_format_qty(self.qty),
            signal_format_qty(self.executed_qty),
            signal_format_qty(self.cum_qty),
            signal_format_qty(self.cum_quote)
        ));
        text.push_str(&format!(
            "reduce only: {}, close position: {}\n",
            self.reduce_only, self.close_position
        ));
        text.push_str(&format!("status: {}\n", self.status));
        text.push_str(&format!("running status: {}\n", self.running_status));
        text.push_str(&format!("action status: {}\n", self.action_status));
        if self.has_realized_pnl {
            text.push_str(&format!(
                "close pnl: {:.4} USDT, pnl ratio: {:.2}%\n",
                self.realized_pnl, self.realized_pnl_percent
            ));
            text.push_str(&format!(
                "open price: {:.7}, close price: {:.7}, closed qty: {}, closed quote(usdt): {:.4}\n",
                self.open_price,
                self.close_price,
                signal_format_qty(self.closed_qty),
                self.close_price * self.closed_qty
            ));
        }
        text.push_str(&format!(
            "trigger: {}\n",
            format_trigger_millis(self.trigger)
        ));
        text
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemRunningStatusSignal {
    pub base: SignalBase,
    pub cpu_usage: u32,
    pub mem_usage: u32,
    pub disk_usage: u32,
    pub network_usage: u32,
    pub io_usage: u32,
}

impl SystemRunningStatusSignal {
    pub fn new(cpu: u32, mem: u32, disk: u32, net: u32, io: u32) -> Self {
        Self {
            base: SignalBase::new(SignalType::SystemStatus, 100, "system_status"),
            cpu_usage: cpu,
            mem_usage: mem,
            disk_usage: disk,
            network_usage: net,
            io_usage: io,
        }
    }

    pub fn format(&self) -> String {
        format!(
            "system is running:\ncpu usage: {}\nmem usage: {}\ndisk usage: {}\nnetwork usage: {}\nio usage: {}",
            self.cpu_usage, self.mem_usage, self.disk_usage, self.network_usage, self.io_usage
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HugeMomentumSignal {
    pub base: TradeSignalBase,
    pub latest_5m_ohlc: [[f64; 4]; 3],
    pub latest_5m_qty: [f64; 3],
    pub latest_5m_qty_times: [f64; 3],
    pub val_base: f64,
    pub latest_24h_qty: f64,
    pub latest_24h_value: f64,
}

impl HugeMomentumSignal {
    pub fn format(&self) -> String {
        let trigger_time = DateTime::<Utc>::from_timestamp(self.base.trigger as i64, 0)
            .map(|time| time.format("%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "invalid-time".to_owned());

        let mut text = format!(
            "[{}] {} @ {}\ntype: {}\n",
            signal_level_to_visible_string(self.base.signal_level),
            self.base.symbol,
            trigger_time,
            self.base.msg
        );

        for idx in 0..3 {
            let [open, high, low, close] = self.latest_5m_ohlc[idx];
            text.push_str(&format!(
                "latest_5m_price[{idx}]: o:{open:.7} h:{high:.7} l:{low:.7} c:{close:.7}\n"
            ));
            text.push_str(&format!(
                "latest_5m_qty[{idx}]: {}\n",
                signal_format_qty(self.latest_5m_qty[idx])
            ));
            text.push_str(&format!(
                "latest_5m_qty_times[{idx}]: {:.2}\n",
                self.latest_5m_qty_times[idx]
            ));
        }

        text.push_str(&format!("val_base: {}\n", signal_format_qty(self.val_base)));
        text.push_str(&format!(
            "latest_24h_qty: {}\n",
            signal_format_qty(self.latest_24h_qty)
        ));
        text.push_str(&format!(
            "latest_24h_value: {}",
            signal_format_qty(self.latest_24h_value)
        ));
        text
    }
}

#[derive(Debug, Clone)]
pub struct TelegramNotifier {
    client: Client,
    bot_token: String,
    default_chat_id: Option<String>,
    signal_chat_ids: SignalTypeChatIds,
}

impl TelegramNotifier {
    pub fn new(bot_token: impl Into<String>, chat_id: impl Into<String>) -> Self {
        let default_chat_id = chat_id.into();
        Self {
            client: Client::new(),
            bot_token: bot_token.into(),
            default_chat_id: if default_chat_id.trim().is_empty() {
                None
            } else {
                Some(default_chat_id)
            },
            signal_chat_ids: SignalTypeChatIds::default(),
        }
    }

    pub fn with_signal_routes(
        bot_token: impl Into<String>,
        default_chat_id: Option<String>,
        signal_chat_ids: SignalTypeChatIds,
    ) -> Self {
        Self {
            client: Client::new(),
            bot_token: bot_token.into(),
            default_chat_id: default_chat_id.and_then(|value| {
                let trimmed = value.trim().to_owned();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            }),
            signal_chat_ids,
        }
    }

    async fn send_text_to_chat(
        &self,
        chat_id: &str,
        text: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);

        self.client
            .post(endpoint)
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": true,
            }))
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn send_for_signal_type(
        &self,
        signal_type: SignalType,
        text: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_id = self
            .signal_chat_ids
            .get(signal_type)
            .or(self.default_chat_id.as_deref())
            .ok_or_else(|| "telegram chat_id is not configured".to_owned())?;

        self.send_text_to_chat(chat_id, text).await
    }

    pub async fn send_text(
        &self,
        text: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_id = self
            .default_chat_id
            .as_deref()
            .ok_or_else(|| "telegram default chat_id is not configured".to_owned())?;
        self.send_text_to_chat(chat_id, text).await
    }

    /// Fire-and-forget async send for use in synchronous context (signal handlers).
    /// Spawns a tokio task to send the notification without blocking the caller.
    pub fn send_signal_async(self: &std::sync::Arc<Self>, signal_type: SignalType, text: String) {
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(error) = this.send_for_signal_type(signal_type, &text).await {
                tracing::warn!(?error, ?signal_type, "send telegram signal notification failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn telegram_notifier_with_signal_routes_empty_default_chat_id() {
        let notifier = TelegramNotifier::with_signal_routes(
            "test_token",
            Some("".to_owned()),
            SignalTypeChatIds::default(),
        );
        // default_chat_id should be None when empty string is provided
        assert!(notifier.default_chat_id.is_none());
    }

    #[test]
    fn telegram_notifier_signal_chat_ids_routing() {
        let mut chat_ids = SignalTypeChatIds::default();
        chat_ids.frame = Some("-123456".to_owned());
        chat_ids.huge_momentum = Some("-789012".to_owned());

        // Frame → gets the configured chat_id
        assert_eq!(chat_ids.get(SignalType::Frame), Some("-123456"));
        // HugeMomentum → gets the configured chat_id
        assert_eq!(chat_ids.get(SignalType::HugeMomentum), Some("-789012"));
        // Momentum → not configured, returns None
        assert_eq!(chat_ids.get(SignalType::Momentum), None);
    }

    #[tokio::test]
    async fn telegram_notifier_send_signal_async_does_not_panic() {
        // Create a notifier with no valid chat IDs — the spawned task will fail
        // gracefully (logged as warning), but the call itself must not panic.
        let notifier = Arc::new(TelegramNotifier::with_signal_routes(
            "test_bot_token",
            None,
            SignalTypeChatIds::default(),
        ));
        // This should not panic, even though the Telegram API call will fail
        notifier.send_signal_async(SignalType::Frame, "test signal message".to_owned());

        // Give the spawned task a moment to complete (it will fail, which is fine)
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    #[test]
    fn signal_chat_ids_empty_string_treated_as_none() {
        let chat_ids = SignalTypeChatIds {
            frame: Some("".to_owned()),
            ..Default::default()
        };
        assert_eq!(chat_ids.get(SignalType::Frame), None);
    }

    #[test]
    fn trade_frame_signal_format_contains_key_fields() {
        let signal = TradeFrameSignal {
            base: TradeSignalBase::new(
                SignalType::Frame,
                "test_strategy",
                1,
                SignalLevel::Important,
                "entry trigger",
                1700000000,
                "BTCUSDT",
            ),
            stage: "entry".to_owned(),
            side: "SHORT".to_owned(),
            trade_id: 12345,
            current_trade_price: 50000.0,
            trigger_price: 49900.0,
            signal_qty_times: 2.5,
            signal_price_percent: 0.045,
            vol_3s: 1500.0,
            avg_sec_vol: 300.0,
            vol_ratio: 5.0,
            price_move_3s_pct: 0.002,
            is_accelerating: true,
            short_signal: true,
            long_signal: false,
            stop_price_percent: 0.01,
            reversal_price_percent: 0.008,
            shadow_price_percent: 0.33,
            latest_minute_qty: 50000.0,
            current_second_qty: 800.0,
            prev_second_qty: 200.0,
            high_price_since_open: 50100.0,
            low_price_since_open: 49800.0,
            avg_price: 49900.0,
            should_stop: false,
            should_reversal: false,
            should_exhaust: false,
        };
        let formatted = signal.format();
        assert!(formatted.contains("BTCUSDT"));
        assert!(formatted.contains("entry"));
        assert!(formatted.contains("SHORT"));
        assert!(formatted.contains("12345"));
        assert!(formatted.contains("50000"));
    }

    #[test]
    fn huge_momentum_signal_format_contains_key_fields() {
        let signal = HugeMomentumSignal {
            base: TradeSignalBase::new(
                SignalType::HugeMomentum,
                "huge_momentum",
                6,
                SignalLevel::Normal,
                "IGNITE",
                1700000060,
                "ETHUSDT",
            ),
            latest_5m_ohlc: [[3000.0, 3010.0, 2995.0, 3008.0]; 3],
            latest_5m_qty: [10000.0, 12000.0, 15000.0],
            latest_5m_qty_times: [1.5, 1.8, 2.2],
            val_base: 5000.0,
            latest_24h_qty: 500000.0,
            latest_24h_value: 1500000000.0,
        };
        let formatted = signal.format();
        assert!(formatted.contains("ETHUSDT"));
        assert!(formatted.contains("IGNITE"));
        // signal_format_qty formats 500000 as "500.00k"
        assert!(formatted.contains("500.00k"));
        // signal_format_qty formats 1500000000 as "1500.00M"
        assert!(formatted.contains("1500.00M"));
    }
}
