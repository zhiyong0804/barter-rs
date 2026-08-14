use super::{SubscriptionKind, candle::Candle};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields 4-hour
/// [`Candle`] [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct Candles4h;

impl SubscriptionKind for Candles4h {
    type Event = Candle;

    fn as_str(&self) -> &'static str {
        "candles_4h"
    }
}

impl std::fmt::Display for Candles4h {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
