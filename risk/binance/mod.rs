mod credentials;
mod account;
mod position_risk;
mod rest_client;
pub(crate) mod model;

pub use account::FuturesAccountInformation;
pub use credentials::BinanceCredentials;
pub use model::{
    AccountUpdateSummary, BinanceError, BookTickerEvent, HedgeOrderRequest, ListenKeyResponse,
    MarkPriceEvent, OrderTradeUpdate, RawBookTickerEvent, RawMarkPriceEvent, UserDataEvent,
};
pub use position_risk::FuturesPositionRisk;
pub use rest_client::BinanceFuturesRestClient;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BinancePositionSide {
    Both,
    Long,
    Short,
}

impl BinancePositionSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Both => "BOTH",
            Self::Long => "LONG",
            Self::Short => "SHORT",
        }
    }

    pub(crate) fn from_exchange_str(value: &str) -> Result<Self, BinanceError> {
        match value {
            "BOTH" => Ok(Self::Both),
            "LONG" => Ok(Self::Long),
            "SHORT" => Ok(Self::Short),
            other => Err(BinanceError::InvalidResponse(format!(
                "unknown position side: {other}"
            ))),
        }
    }
}