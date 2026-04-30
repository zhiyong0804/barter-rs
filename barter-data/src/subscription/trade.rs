use super::SubscriptionKind;
use barter_instrument::Side;
use barter_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`PublicTrade`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct PublicTrades;

impl SubscriptionKind for PublicTrades {
    type Event = PublicTrade;

    fn as_str(&self) -> &'static str {
        "public_trades"
    }
}

impl std::fmt::Display for PublicTrades {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`PublicTrade`] model.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct PublicTrade {
    pub id: String,
    pub price: f64,
    pub amount: f64,
    pub side: Side,
    /// 事件时间戳 (E)
    #[serde(default)]
    pub event_timestamp: u64,
    /// 交易时间戳 (T)
    #[serde(default)]
    pub trade_timestamp: u64,
    /// 交易对符号 (s)
    #[serde(default)]
    pub symbol: String,
    /// 交易类型 (X)
    #[serde(default)]
    pub trade_type: Option<String>,
    /// 交易时间（解析后的DateTime）
    #[serde(default = "default_datetime")]
    pub time: DateTime<Utc>,
}

/// 默认时间函数
fn default_datetime() -> DateTime<Utc> {
    Utc::now()
}

