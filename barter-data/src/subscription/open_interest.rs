use super::SubscriptionKind;
use barter_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`OpenInterest`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct OpenInterests;

impl SubscriptionKind for OpenInterests {
    type Event = OpenInterest;

    fn as_str(&self) -> &'static str {
        "open_interests"
    }
}

impl std::fmt::Display for OpenInterests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`OpenInterest`] model. Pushed every 3s by Binance.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OpenInterest {
    /// Open interest quantity in base asset
    pub open_interest: f64,
}
