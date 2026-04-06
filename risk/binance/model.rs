use crate::binance::BinancePositionSide;
use barter_instrument::Side;
use reqwest::StatusCode;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use sha2::Sha256;
use std::str::FromStr;
use thiserror::Error;

pub(crate) type HmacSha256 = hmac::Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct HedgeOrderRequest {
    pub symbol: String,
    pub side: Side,
    pub position_side: Option<BinancePositionSide>,
    pub quantity: Decimal,
    pub client_order_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "e")]
pub enum UserDataEvent {
    #[serde(rename = "ACCOUNT_UPDATE")]
    AccountUpdate {
        #[serde(rename = "E")]
        event_time: i64,
        #[serde(rename = "T")]
        _transaction_time: i64,
        #[serde(rename = "a")]
        account: RawAccountUpdateData,
    },
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate {
        #[serde(rename = "E")]
        _event_time: i64,
        #[serde(rename = "T")]
        _transaction_time: i64,
        #[serde(rename = "o")]
        order: RawOrderTradeUpdate,
    },
    #[serde(rename = "listenKeyExpired")]
    ListenKeyExpired {
        #[serde(rename = "E")]
        _event_time: i64,
        #[serde(rename = "listenKey")]
        listen_key: String,
    },
    /// Catch-all for unrecognised event types (e.g. TRADE_LITE, MARGIN_CALL, etc.)
    #[serde(other)]
    Ignored,
}

#[derive(Debug, Clone, Default)]
pub struct AccountUpdateSummary {
    pub total_wallet_balance: Decimal,
    pub total_unrealized_profit: Decimal,
    pub total_margin_balance: Decimal,
    pub available_balance: Decimal,
    pub positions: Vec<PositionUpdate>,
}

impl TryFrom<RawAccountUpdateData> for AccountUpdateSummary {
    type Error = BinanceError;

    fn try_from(value: RawAccountUpdateData) -> Result<Self, Self::Error> {
        let balances = value
            .balances
            .into_iter()
            .map(AccountBalanceUpdate::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let positions = value
            .positions
            .into_iter()
            .map(PositionUpdate::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let total_wallet_balance = balances
            .iter()
            .fold(Decimal::ZERO, |acc, balance| acc + balance.wallet_balance);
        let available_balance = balances.iter().fold(Decimal::ZERO, |acc, balance| {
            acc + balance.cross_wallet_balance
        });
        let total_unrealized_profit = positions.iter().fold(Decimal::ZERO, |acc, position| {
            acc + position.unrealized_profit
        });
        let total_margin_balance = total_wallet_balance + total_unrealized_profit;

        Ok(Self {
            total_wallet_balance,
            total_unrealized_profit,
            total_margin_balance,
            available_balance,
            positions,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AccountBalanceUpdate {
    pub wallet_balance: Decimal,
    pub cross_wallet_balance: Decimal,
}

impl TryFrom<RawAccountBalanceUpdate> for AccountBalanceUpdate {
    type Error = BinanceError;

    fn try_from(value: RawAccountBalanceUpdate) -> Result<Self, Self::Error> {
        Ok(Self {
            wallet_balance: parse_decimal("wb", &value.wallet_balance)?,
            cross_wallet_balance: parse_decimal("cw", &value.cross_wallet_balance)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PositionUpdate {
    pub symbol: String,
    pub position_amt: Decimal,
    pub entry_price: Decimal,
    pub unrealized_profit: Decimal,
    pub margin_type: String,
    pub position_side: BinancePositionSide,
}

impl TryFrom<RawAccountPositionUpdate> for PositionUpdate {
    type Error = BinanceError;

    fn try_from(value: RawAccountPositionUpdate) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: value.symbol,
            position_amt: parse_decimal("pa", &value.position_amt)?,
            entry_price: parse_decimal("ep", &value.entry_price)?,
            unrealized_profit: parse_decimal("up", &value.unrealized_profit)?,
            margin_type: value.margin_type,
            position_side: BinancePositionSide::from_exchange_str(&value.position_side)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OrderTradeUpdate {
    pub symbol: String,
    pub side: Side,
    pub position_side: BinancePositionSide,
    pub order_status: String,
    pub execution_type: String,
    pub quantity: Decimal,
    pub filled_accumulated_quantity: Decimal,
    pub last_filled_price: Decimal,
    pub client_order_id: String,
}

impl TryFrom<RawOrderTradeUpdate> for OrderTradeUpdate {
    type Error = BinanceError;

    fn try_from(value: RawOrderTradeUpdate) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: value.symbol,
            side: side_from_binance(&value.side)?,
            position_side: BinancePositionSide::from_exchange_str(&value.position_side)?,
            order_status: value.order_status,
            execution_type: value.execution_type,
            quantity: parse_decimal("q", &value.quantity)?,
            filled_accumulated_quantity: parse_decimal("z", &value.filled_accumulated_quantity)?,
            last_filled_price: parse_decimal("L", &value.last_filled_price)?,
            client_order_id: value.client_order_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BookTickerEvent {
    pub symbol: String,
    pub best_bid_price: Decimal,
    pub best_ask_price: Decimal,
}

impl BookTickerEvent {
    pub fn mid_price(&self) -> Decimal {
        (self.best_bid_price + self.best_ask_price) / Decimal::from(2)
    }
}

impl TryFrom<RawBookTickerEvent> for BookTickerEvent {
    type Error = BinanceError;

    fn try_from(value: RawBookTickerEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: value.symbol,
            best_bid_price: parse_decimal("b", &value.best_bid_price)?,
            best_ask_price: parse_decimal("a", &value.best_ask_price)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MarkPriceEvent {
    pub symbol: String,
    pub mark_price: Decimal,
    pub event_time: i64,
}

impl TryFrom<RawMarkPriceEvent> for MarkPriceEvent {
    type Error = BinanceError;

    fn try_from(value: RawMarkPriceEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: value.symbol,
            mark_price: parse_decimal("p", &value.mark_price)?,
            event_time: value.event_time,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderAck {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: u64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawFuturesAccountInformation {
    #[serde(rename = "totalWalletBalance")]
    pub(crate) total_wallet_balance: String,
    #[serde(rename = "totalUnrealizedProfit")]
    pub(crate) total_unrealized_profit: String,
    #[serde(rename = "totalMarginBalance")]
    pub(crate) total_margin_balance: String,
    #[serde(rename = "availableBalance")]
    pub(crate) available_balance: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawFuturesPositionRisk {
    pub(crate) symbol: String,
    #[serde(rename = "positionAmt")]
    pub(crate) position_amt: String,
    #[serde(rename = "entryPrice")]
    pub(crate) entry_price: String,
    #[serde(rename = "markPrice")]
    pub(crate) mark_price: String,
    #[serde(rename = "liquidationPrice")]
    pub(crate) liquidation_price: String,
    #[serde(rename = "unRealizedProfit")]
    pub(crate) un_realized_profit: String,
    pub(crate) notional: String,
    pub(crate) leverage: String,
    #[serde(rename = "marginType")]
    pub(crate) margin_type: String,
    #[serde(rename = "positionSide")]
    pub(crate) position_side: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RawAccountUpdateData {
    #[serde(rename = "B")]
    balances: Vec<RawAccountBalanceUpdate>,
    #[serde(rename = "P")]
    positions: Vec<RawAccountPositionUpdate>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RawAccountBalanceUpdate {
    #[serde(rename = "a")]
    _asset: String,
    #[serde(rename = "wb")]
    wallet_balance: String,
    #[serde(rename = "cw")]
    cross_wallet_balance: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RawAccountPositionUpdate {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "pa")]
    position_amt: String,
    #[serde(rename = "ep")]
    entry_price: String,
    #[serde(rename = "up")]
    unrealized_profit: String,
    #[serde(rename = "mt")]
    margin_type: String,
    #[serde(rename = "ps")]
    position_side: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RawOrderTradeUpdate {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "ps")]
    position_side: String,
    #[serde(rename = "X")]
    order_status: String,
    #[serde(rename = "x")]
    execution_type: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "z")]
    filled_accumulated_quantity: String,
    #[serde(rename = "L")]
    last_filled_price: String,
    #[serde(rename = "c")]
    client_order_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawBookTickerEvent {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub best_bid_price: String,
    #[serde(rename = "a")]
    pub best_ask_price: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawMarkPriceEvent {
    #[serde(rename = "e")]
    pub _event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: String,
}

#[derive(Debug, Error)]
pub enum BinanceError {
    #[error("environment variable not set: {name}")]
    MissingEnvVar { name: String },
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("signing error: {0}")]
    Signing(String),
    #[error("binance api error {status}: {body}")]
    Api { status: StatusCode, body: String },
    #[error("invalid decimal for {field}: {value}")]
    InvalidDecimal { field: &'static str, value: String },
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

pub(crate) fn parse_decimal(field: &'static str, value: &str) -> Result<Decimal, BinanceError> {
    Decimal::from_str(value).map_err(|_| BinanceError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

pub(crate) fn decimal_to_string(value: Decimal) -> String {
    value.normalize().to_string()
}

pub(crate) fn form_body(params: Vec<(&str, String)>) -> String {
    params
        .into_iter()
        .map(|(key, value)| format!("{}={}", key, value))
        .collect::<Vec<_>>()
        .join("&")
}

pub(crate) fn side_to_binance(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

pub(crate) fn side_from_binance(side: &str) -> Result<Side, BinanceError> {
    match side {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other => Err(BinanceError::InvalidResponse(format!(
            "unknown order side: {other}"
        ))),
    }
}

pub(crate) async fn parse_response<T>(response: reqwest::Response) -> Result<T, BinanceError>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        return Err(BinanceError::Api { status, body });
    }

    serde_json::from_str(&body).map_err(|error| {
        BinanceError::InvalidResponse(format!(
            "failed to decode response body: {error}; body={body}"
        ))
    })
}
