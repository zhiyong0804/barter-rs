use super::super::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    subscription::open_interest::OpenInterest,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// [`BinanceFuturesUsd`](super::BinanceFuturesUsd) Open Interest message.
///
/// See docs: <https://developers.binance.com/docs/derivatives/usds-m-futures/websocket-market-streams/Open-Interest-Stream>
///
/// ### Raw Payload Example
/// ```json
/// {
///     "e": "openInterest",
///     "E": 1785683827210,
///     "s": "BTCUSDT",
///     "oI": "108872.642"
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceOpenInterest {
    #[serde(alias = "s", deserialize_with = "de_open_interest_subscription_id")]
    pub subscription_id: SubscriptionId,
    /// Event timestamp
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub event_time: DateTime<Utc>,
    /// Open interest in base asset
    #[serde(
        alias = "oI",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub open_interest: f64,
}

impl Identifier<Option<SubscriptionId>> for BinanceOpenInterest {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceOpenInterest)>
    for MarketIter<InstrumentKey, OpenInterest>
{
    fn from(
        (exchange_id, instrument, oi): (ExchangeId, InstrumentKey, BinanceOpenInterest),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: oi.event_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: OpenInterest {
                open_interest: oi.open_interest,
            },
        })])
    }
}

pub fn de_open_interest_subscription_id<'de, D>(
    deserializer: D,
) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer).map(|market: &str| {
        SubscriptionId::from(format!("{}|{}", BinanceChannel::OPEN_INTEREST.0, market))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binance_open_interest() {
        let input = r#"
        {
            "e": "openInterest",
            "E": 1785683827210,
            "s": "BTCUSDT",
            "oI": "108872.642"
        }
        "#;

        let oi: BinanceOpenInterest = serde_json::from_str(input).unwrap();
        assert_eq!(
            oi.subscription_id,
            SubscriptionId::from("@openInterest|BTCUSDT")
        );
        assert_eq!(oi.open_interest, 108872.642);
    }
}
