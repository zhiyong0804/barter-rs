use super::SubscriptionKind;
use barter_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`MarkPrice`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
///
/// Only supported for Binance Futures exchanges.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct MarkPrices;

impl SubscriptionKind for MarkPrices {
    type Event = MarkPrice;

    fn as_str(&self) -> &'static str {
        "mark_prices"
    }
}

impl std::fmt::Display for MarkPrices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`MarkPrice`] model.
///
/// Mark price is the settlement price used for liquidations and funding rates
/// in perpetual futures contracts. It's more stable than the last trade price
/// as it's based on spot price index + funding rate.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct MarkPrice {
    /// The mark price value
    pub mark_price: f64,
    /// Estimated liquidation price
    pub liquidation_price: Option<f64>,
    /// Funding rate
    pub funding_rate: Option<f64>,
}
