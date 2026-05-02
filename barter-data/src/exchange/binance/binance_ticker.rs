use super::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    subscription::ticker::{Ticker, Tickers24hr},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// [`Binance`] 24hr ticker message.
///
/// ### Raw Payload Examples
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#24hr-ticker-price-change-statistics>
/// ```json
/// {
///   "e": "24hrTicker",
///   "E": 123456789,
///   "s": "BTCUSDT",
///   "p": "0.0015",
///   "P": "250.00",
///   "w": "0.0018",
///   "c": "0.0025",
///   "Q": "10",
///   "o": "0.0010",
///   "h": "0.0025",
///   "l": "0.0010",
///   "v": "10000",
///   "q": "18",
///   "O": 0,
///   "C": 86400000,
///   "F": 0,
///   "L": 18150,
///   "n": 18151
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceTicker {
    /// Event type
    #[serde(alias = "e")]
    pub event_type: String,

    /// Event time
    #[serde(alias = "E")]
    pub event_time: u64,

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
    pub first_price: f64,

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
    pub last_price: f64,

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

    /// Statistics open time
    #[serde(alias = "O")]
    pub stats_open_time: u64,

    /// Statistics close time
    #[serde(alias = "C")]
    pub stats_close_time: u64,

    /// First trade ID
    #[serde(alias = "F")]
    pub first_trade_id: u64,

    /// Last trade ID
    #[serde(alias = "L")]
    pub last_trade_id: u64,

    /// Total number of trades
    #[serde(alias = "n")]
    pub trade_count: u64,
}

impl Identifier<Option<SubscriptionId>> for BinanceTicker {
    fn id(&self) -> Option<SubscriptionId> {
        Some(SubscriptionId::from(format!(
            "{}|{}",
            BinanceChannel::TICKER_24HR.0,
            self.symbol
        )))
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceTicker)>
    for MarketIter<InstrumentKey, Ticker>
{
    fn from((exchange_id, instrument, ticker): (ExchangeId, InstrumentKey, BinanceTicker)) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: DateTime::<Utc>::from_timestamp_millis(ticker.event_time as i64)
                .unwrap_or_else(|| Utc::now()),
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Ticker {
                event_type: ticker.event_type,
                event_time: DateTime::<Utc>::from_timestamp_millis(ticker.event_time as i64)
                    .unwrap_or_else(|| Utc::now()),
                symbol: ticker.symbol,
                price_change: ticker.price_change,
                price_change_percent: ticker.price_change_percent,
                weighted_avg_price: ticker.weighted_avg_price,
                open_price: ticker.first_price,
                high_price: ticker.high_price,
                low_price: ticker.low_price,
                close_price: ticker.last_price,
                last_qty: ticker.last_qty,
                base_volume: ticker.base_volume,
                quote_volume: ticker.quote_volume,
            },
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_binance_ticker() {
            let input = r#"
            {
              "e": "24hrTicker",
              "E": 123456789,
              "s": "BTCUSDT",
              "p": "0.0015",
              "P": "250.00",
              "w": "0.0018",
              "c": "0.0025",
              "Q": "10",
              "o": "0.0010",
              "h": "0.0025",
              "l": "0.0010",
              "v": "10000",
              "q": "18",
              "O": 0,
              "C": 86400000,
              "F": 0,
              "L": 18150,
              "n": 18151
            }
            "#;

            assert_eq!(
                serde_json::from_str::<BinanceTicker>(input).unwrap(),
                BinanceTicker {
                    event_type: "24hrTicker".to_string(),
                    event_time: 123456789,
                    symbol: "BTCUSDT".to_string(),
                    price_change: 0.0015,
                    price_change_percent: 250.00,
                    weighted_avg_price: 0.0018,
                    first_price: 0.0010,
                    high_price: 0.0025,
                    low_price: 0.0010,
                    last_price: 0.0025,
                    last_qty: 10.0,
                    base_volume: 10000.0,
                    quote_volume: 18.0,
                    stats_open_time: 0,
                    stats_close_time: 86400000,
                    first_trade_id: 0,
                    last_trade_id: 18150,
                    trade_count: 18151,
                }
            );
        }
    }
}

