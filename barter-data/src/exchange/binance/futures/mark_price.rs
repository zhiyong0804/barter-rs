use super::super::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    subscription::mark_price::MarkPrice,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// [`BinanceFuturesUsd`](super::BinanceFuturesUsd) Mark Price message.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream>
///
/// ### Raw Payload Examples
/// ```json
/// {
///     "e": "markPriceUpdate",
///     "E": 1649839266000,
///     "s": "BTCUSDT",
///     "p": "38500.00",
///     "i": "37000.00",
///     "r": "0.00005",
///     "T": 1649839260000
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceMarkPrice {
    #[serde(alias = "s", deserialize_with = "de_mark_price_subscription_id")]
    pub subscription_id: SubscriptionId,
    /// Mark price
    #[serde(
        alias = "p",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub mark_price: f64,
    /// Index price (spot price index)
    #[serde(
        alias = "i",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub index_price: f64,
    /// Funding rate
    #[serde(
        alias = "r",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub funding_rate: f64,
    /// Estimated liquidation price (optional in some versions)
    #[serde(alias = "L", default, deserialize_with = "de_liquidation_price")]
    pub liquidation_price: Option<f64>,
    /// Event timestamp
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub event_time: DateTime<Utc>,
}

impl Identifier<Option<SubscriptionId>> for BinanceMarkPrice {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceMarkPrice)>
    for MarketIter<InstrumentKey, MarkPrice>
{
    fn from(
        (exchange_id, instrument, mark_price): (ExchangeId, InstrumentKey, BinanceMarkPrice),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: mark_price.event_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: MarkPrice {
                mark_price: mark_price.mark_price,
                liquidation_price: mark_price.liquidation_price,
                funding_rate: Some(mark_price.funding_rate),
            },
        })])
    }
}

/// Deserialize a [`BinanceMarkPrice`] "s" (eg/ "BTCUSDT") as the associated [`SubscriptionId`]
/// (eg/ "@markPrice|BTCUSDT").
pub fn de_mark_price_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer).map(|market: &str| {
        SubscriptionId::from(format!("{}|{}", BinanceChannel::MARK_PRICE.0, market))
    })
}

/// Deserialize an optional liquidation price field.
fn de_liquidation_price<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let s = <Option<&str> as Deserialize>::deserialize(deserializer)?;
    Ok(s.and_then(|v| v.parse::<f64>().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_binance_mark_price() {
        let input = r#"
        {
            "e": "markPriceUpdate",
            "E": 1665523974222,
            "s": "BTCUSDT",
            "p": "18917.15",
            "i": "18915.00",
            "r": "0.00003",
            "T": 1665523974222
        }
        "#;

        let result: Result<BinanceMarkPrice, _> = serde_json::from_str(input);
        assert!(result.is_ok());

        let mark_price = result.unwrap();
        assert_eq!(
            mark_price.subscription_id,
            SubscriptionId::from("@markPrice|BTCUSDT")
        );
        assert_eq!(mark_price.mark_price, 18917.15);
        assert_eq!(mark_price.index_price, 18915.00);
        assert_eq!(mark_price.funding_rate, 0.00003);
        assert_eq!(
            mark_price.event_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1665523974222))
        );
    }
}
