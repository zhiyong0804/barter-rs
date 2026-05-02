use crate::subscription::SubscriptionKind;
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 24hrTicker subscription type.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct Tickers24hr;

impl SubscriptionKind for Tickers24hr {
    type Event = Ticker;

    fn as_str(&self) -> &'static str {
        "tickers_24hr"
    }
}

/// 24 hour rolling window statistics ticker.
///
/// See Binance docs: <https://binance-docs.github.io/apidocs/futures/en/#24hr-ticker-price-change-statistics>
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Ticker {
    /// Event type
    #[serde(alias = "e")]
    pub event_type: String,

    /// Event time
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub event_time: DateTime<Utc>,

    /// Symbol
    #[serde(alias = "s")]
    pub symbol: String,

    /// Price change
    #[serde(
        alias = "p",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub price_change: f64,

    /// Price change percent
    #[serde(
        alias = "P",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub price_change_percent: f64,

    /// Weighted average price
    #[serde(
        alias = "w",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub weighted_avg_price: f64,

    /// First trade (opening) price
    #[serde(
        alias = "o",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub open_price: f64,

    /// Highest price
    #[serde(
        alias = "h",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub high_price: f64,

    /// Lowest price
    #[serde(
        alias = "l",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub low_price: f64,

    /// Latest price
    #[serde(
        alias = "c",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub close_price: f64,

    /// Last quantity
    #[serde(
        alias = "Q",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub last_qty: f64,

    /// Total traded base asset volume
    #[serde(
        alias = "v",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub base_volume: f64,

    /// Total traded quote asset volume
    #[serde(
        alias = "q",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub quote_volume: f64,
}

impl std::fmt::Display for Tickers24hr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<Tickers24hr> for MarketDataInstrumentKind {
    fn from(_: Tickers24hr) -> Self {
        // For now we'll map this to Spot, but it might need its own variant
        MarketDataInstrumentKind::Spot
    }
}

