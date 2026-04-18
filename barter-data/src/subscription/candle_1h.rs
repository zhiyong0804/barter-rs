use super::{SubscriptionKind, candle::Candle};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields 1-hour
/// [`Candle`] [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct Candles1h;

impl SubscriptionKind for Candles1h {
    type Event = Candle;

    fn as_str(&self) -> &'static str {
        "candles_1h"
    }
}

impl std::fmt::Display for Candles1h {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
