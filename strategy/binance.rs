use crate::command::CommandSide;
use crate::config::BinanceConfig;
use chrono::Utc;
use hmac::Mac;
use reqwest::{header::CONTENT_TYPE, Client};
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::Sha256;

#[derive(Debug, thiserror::Error)]
pub enum BinanceError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("binance api error {status}: {body}")]
    Api {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

type HmacSha256 = hmac::Hmac<Sha256>;

#[derive(Debug, Clone, Copy)]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    fn as_str(&self) -> &'static str {
        match self {
            PositionSide::Long => "LONG",
            PositionSide::Short => "SHORT",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PositionRisk {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: Decimal,
    pub notional: Decimal,
    #[serde(rename = "positionSide")]
    pub position_side: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrder {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    pub side: String,
    #[serde(rename = "positionSide")]
    pub position_side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub status: String,
    pub price: Decimal,
    #[serde(rename = "stopPrice")]
    pub stop_price: Decimal,
    #[serde(rename = "origQty")]
    pub orig_qty: Decimal,
    #[serde(rename = "executedQty")]
    pub executed_qty: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BookTicker {
    #[serde(rename = "bidPrice")]
    pub bid_price: Decimal,
    #[serde(rename = "askPrice")]
    pub ask_price: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderInfo {
    pub status: String,
    #[serde(rename = "origQty")]
    pub orig_qty: Decimal,
    #[serde(rename = "executedQty")]
    pub executed_qty: Decimal,
}

#[derive(Debug, Clone)]
pub struct BinanceClient {
    http: Client,
    rest_base_url: String,
    recv_window_ms: u64,
    api_key: String,
    api_secret: String,
}

impl BinanceClient {
    pub fn from_env(
        config: &BinanceConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let api_key = std::env::var(&config.api_key_env)?;
        let api_secret = std::env::var(&config.api_secret_env)?;
        Ok(Self {
            http: Client::new(),
            rest_base_url: config.rest_base_url.trim_end_matches('/').to_owned(),
            recv_window_ms: config.recv_window_ms,
            api_key,
            api_secret,
        })
    }

    pub async fn fetch_price(&self, symbol: &str) -> Result<Decimal, BinanceError> {
        #[derive(Deserialize)]
        struct PriceResp {
            price: Decimal,
        }

        let url = format!("{}/fapi/v1/ticker/price", self.rest_base_url);
        let response = self
            .http
            .get(url)
            .query(&[("symbol", symbol)])
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(BinanceError::Api { status, body });
        }

        serde_json::from_str::<PriceResp>(&body)
            .map(|item| item.price)
            .map_err(|err| BinanceError::InvalidResponse(err.to_string()))
    }

    pub async fn fetch_book_ticker(&self, symbol: &str) -> Result<BookTicker, BinanceError> {
        let url = format!("{}/fapi/v1/ticker/bookTicker", self.rest_base_url);
        let response = self
            .http
            .get(url)
            .query(&[("symbol", symbol)])
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(BinanceError::Api { status, body });
        }

        serde_json::from_str::<BookTicker>(&body)
            .map_err(|err| BinanceError::InvalidResponse(err.to_string()))
    }

    pub async fn place_market(
        &self,
        symbol: &str,
        side: CommandSide,
        position_side: PositionSide,
        quantity: Decimal,
        client_order_id: &str,
    ) -> Result<(), BinanceError> {
        let mut params = vec![
            ("symbol", symbol.to_owned()),
            ("side", side_to_str(side).to_owned()),
            ("positionSide", position_side.as_str().to_owned()),
            ("type", "MARKET".to_owned()),
            ("quantity", decimal_to_string(quantity)),
            ("newClientOrderId", client_order_id.to_owned()),
        ];
        self.signed_post("/fapi/v1/order", &mut params).await?;
        Ok(())
    }

    pub async fn place_limit(
        &self,
        symbol: &str,
        side: CommandSide,
        position_side: PositionSide,
        quantity: Decimal,
        price: Decimal,
        client_order_id: &str,
    ) -> Result<(), BinanceError> {
        let mut params = vec![
            ("symbol", symbol.to_owned()),
            ("side", side_to_str(side).to_owned()),
            ("positionSide", position_side.as_str().to_owned()),
            ("type", "LIMIT".to_owned()),
            ("timeInForce", "GTC".to_owned()),
            ("quantity", decimal_to_string(quantity)),
            ("price", decimal_to_string(price)),
            ("newClientOrderId", client_order_id.to_owned()),
        ];
        self.signed_post("/fapi/v1/order", &mut params).await?;
        Ok(())
    }

    pub async fn place_stop_market(
        &self,
        symbol: &str,
        side: CommandSide,
        position_side: PositionSide,
        quantity: Decimal,
        stop_price: Decimal,
        client_order_id: &str,
    ) -> Result<(), BinanceError> {
        let mut params = vec![
            ("symbol", symbol.to_owned()),
            ("side", side_to_str(side).to_owned()),
            ("positionSide", position_side.as_str().to_owned()),
            ("type", "STOP_MARKET".to_owned()),
            ("workingType", "MARK_PRICE".to_owned()),
            ("stopPrice", decimal_to_string(stop_price)),
            ("quantity", decimal_to_string(quantity)),
            ("newClientOrderId", client_order_id.to_owned()),
        ];
        self.signed_post("/fapi/v1/order", &mut params).await?;
        Ok(())
    }

    pub async fn fetch_positions(&self, symbol: &str) -> Result<Vec<PositionRisk>, BinanceError> {
        let params = vec![("symbol", symbol.to_owned())];
        let query = self.signed_query(params)?;
        let url = format!("{}/fapi/v2/positionRisk?{}", self.rest_base_url, query);

        let response = self
            .http
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(BinanceError::Api { status, body });
        }

        serde_json::from_str::<Vec<PositionRisk>>(&body)
            .map_err(|err| BinanceError::InvalidResponse(err.to_string()))
    }

    pub async fn fetch_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<OpenOrder>, BinanceError> {
        let mut params = Vec::new();
        if let Some(symbol) = symbol {
            params.push(("symbol", symbol.to_owned()));
        }

        let query = self.signed_query(params)?;
        let url = format!("{}/fapi/v1/openOrders?{}", self.rest_base_url, query);

        let response = self
            .http
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(BinanceError::Api { status, body });
        }

        serde_json::from_str::<Vec<OpenOrder>>(&body)
            .map_err(|err| BinanceError::InvalidResponse(err.to_string()))
    }

    pub async fn fetch_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<OrderInfo, BinanceError> {
        let params = vec![
            ("symbol", symbol.to_owned()),
            ("origClientOrderId", client_order_id.to_owned()),
        ];
        let query = self.signed_query(params)?;
        let url = format!("{}/fapi/v1/order?{}", self.rest_base_url, query);

        let response = self
            .http
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(BinanceError::Api { status, body });
        }

        serde_json::from_str::<OrderInfo>(&body)
            .map_err(|err| BinanceError::InvalidResponse(err.to_string()))
    }

    pub async fn cancel_order_by_client_id(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<(), BinanceError> {
        let params = vec![
            ("symbol", symbol.to_owned()),
            ("origClientOrderId", client_order_id.to_owned()),
        ];
        let query = self.signed_query(params)?;
        let url = format!("{}/fapi/v1/order?{}", self.rest_base_url, query);

        let response = self
            .http
            .delete(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(BinanceError::Api { status, body });
        }

        Ok(())
    }

    async fn signed_post(
        &self,
        path: &str,
        params: &mut Vec<(&str, String)>,
    ) -> Result<(), BinanceError> {
        let body = self.signed_query(params.clone())?;
        let url = format!("{}{}", self.rest_base_url, path);

        let response = self
            .http
            .post(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(BinanceError::Api { status, body });
        }

        Ok(())
    }

    fn signed_query(&self, mut params: Vec<(&str, String)>) -> Result<String, BinanceError> {
        params.push(("recvWindow", self.recv_window_ms.to_string()));
        params.push(("timestamp", Utc::now().timestamp_millis().to_string()));

        let query = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|err| BinanceError::InvalidResponse(err.to_string()))?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        Ok(format!("{}&signature={}", query, signature))
    }
}

fn side_to_str(side: CommandSide) -> &'static str {
    match side {
        CommandSide::Buy => "BUY",
        CommandSide::Sell => "SELL",
    }
}

fn decimal_to_string(value: Decimal) -> String {
    value.normalize().to_string()
}
