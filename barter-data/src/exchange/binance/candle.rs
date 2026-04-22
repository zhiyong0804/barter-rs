use super::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceCandle1m {
    #[serde(alias = "s", deserialize_with = "de_candle_1m_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(alias = "k")]
    pub kline: BinanceKline,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceCandle1h {
    #[serde(alias = "s", deserialize_with = "de_candle_1h_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(alias = "k")]
    pub kline: BinanceKline,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceKline {
    #[serde(
        alias = "T",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub close_time: DateTime<Utc>,
    #[serde(
        alias = "o",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub open: f64,
    #[serde(
        alias = "h",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub high: f64,
    #[serde(
        alias = "l",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub low: f64,
    #[serde(
        alias = "c",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub close: f64,
    #[serde(
        alias = "v",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub volume: f64,
    #[serde(
        alias = "V",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub bid_volume: f64,
    #[serde(
        alias = "Q",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub bid_quote_asset_volume: f64,
    #[serde(alias = "n")]
    pub trade_count: u64,
    #[serde(alias = "x")]
    pub is_final: bool,
}

impl Identifier<Option<SubscriptionId>> for BinanceCandle1m {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl Identifier<Option<SubscriptionId>> for BinanceCandle1h {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceCandle1m)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, candle): (ExchangeId, InstrumentKey, BinanceCandle1m),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: candle.kline.close_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Candle {
                close_time: candle.kline.close_time,
                open: candle.kline.open,
                high: candle.kline.high,
                low: candle.kline.low,
                close: candle.kline.close,
                volume: candle.kline.volume,
                bid_volume: candle.kline.bid_volume,
                bid_quote_asset_volume: candle.kline.bid_quote_asset_volume,
                trade_count: candle.kline.trade_count,
                is_final: candle.kline.is_final,
            },
        })])
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceCandle1h)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, candle): (ExchangeId, InstrumentKey, BinanceCandle1h),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: candle.kline.close_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Candle {
                close_time: candle.kline.close_time,
                open: candle.kline.open,
                high: candle.kline.high,
                low: candle.kline.low,
                close: candle.kline.close,
                volume: candle.kline.volume,
                bid_volume: candle.kline.bid_volume,
                bid_quote_asset_volume: candle.kline.bid_quote_asset_volume,
                trade_count: candle.kline.trade_count,
                is_final: candle.kline.is_final,
            },
        })])
    }
}

pub fn de_candle_1m_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BinanceChannel::KLINE_1M, market)).id())
}

pub fn de_candle_1h_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BinanceChannel::KLINE_1H, market)).id())
}
