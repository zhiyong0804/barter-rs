use crate::binance::{
    model::{
        decimal_to_string, form_body, parse_response, side_to_binance, HmacSha256, OrderAck,
        RawFuturesAccountInformation, RawFuturesPositionRisk,
    },
    BinanceCredentials, BinanceError, FuturesAccountInformation, FuturesPositionRisk,
    HedgeOrderRequest, ListenKeyResponse,
};
use crate::config::BinanceConfig;
use chrono::Utc;
use hmac::Mac;
use reqwest::{header::CONTENT_TYPE, Client};
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct BinanceFuturesRestClient {
    http: Client,
    rest_base_url: String,
    recv_window_ms: u64,
    credentials: BinanceCredentials,
}

impl BinanceFuturesRestClient {
    pub fn new(
        config: &BinanceConfig,
        credentials: BinanceCredentials,
    ) -> Result<Self, BinanceError> {
        let http = Client::builder()
            .user_agent("barter-rs-binance-futures-risk-manager")
            .build()?;

        Ok(Self {
            http,
            rest_base_url: config.rest_base_url.trim_end_matches('/').to_owned(),
            recv_window_ms: config.recv_window_ms,
            credentials,
        })
    }

    pub async fn fetch_account_information(
        &self,
    ) -> Result<FuturesAccountInformation, BinanceError> {
        let raw = self
            .signed_get::<RawFuturesAccountInformation>("/fapi/v2/account", Vec::new())
            .await?;
        FuturesAccountInformation::try_from(raw)
    }

    pub async fn fetch_position_risks(&self) -> Result<Vec<FuturesPositionRisk>, BinanceError> {
        let raw: Vec<RawFuturesPositionRisk> = self
            .signed_get::<Vec<RawFuturesPositionRisk>>("/fapi/v2/positionRisk", Vec::new())
            .await?;

        raw.into_iter()
            .map(FuturesPositionRisk::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn place_market_hedge(
        &self,
        request: HedgeOrderRequest,
    ) -> Result<OrderAck, BinanceError> {
        let mut params = vec![
            ("symbol", request.symbol),
            ("side", side_to_binance(request.side).to_owned()),
            ("type", "MARKET".to_owned()),
            ("quantity", decimal_to_string(request.quantity)),
            ("newClientOrderId", request.client_order_id),
        ];

        if let Some(position_side) = request.position_side {
            params.push(("positionSide", position_side.as_str().to_owned()));
        }

        self.signed_post("/fapi/v1/order", params).await
    }

    pub async fn start_user_data_stream(&self) -> Result<String, BinanceError> {
        let response = self
            .api_key_post::<ListenKeyResponse>("/fapi/v1/listenKey", Vec::new())
            .await?;
        Ok(response.listen_key)
    }

    pub async fn keepalive_user_data_stream(&self, listen_key: &str) -> Result<(), BinanceError> {
        let _: ListenKeyResponse = self
            .api_key_put(
                "/fapi/v1/listenKey",
                vec![("listenKey", listen_key.to_owned())],
            )
            .await?;
        Ok(())
    }

    pub async fn close_user_data_stream(&self, listen_key: &str) -> Result<(), BinanceError> {
        let _: serde_json::Value = self
            .api_key_delete(
                "/fapi/v1/listenKey",
                vec![("listenKey", listen_key.to_owned())],
            )
            .await?;
        Ok(())
    }

    async fn signed_get<T>(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<T, BinanceError>
    where
        T: DeserializeOwned,
    {
        let query = self.signed_query(params)?;
        let url = format!("{}{}?{}", self.rest_base_url, path, query);

        let response = self
            .http
            .get(url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?;

        parse_response(response).await
    }

    async fn signed_post<T>(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<T, BinanceError>
    where
        T: DeserializeOwned,
    {
        let body = self.signed_query(params)?;
        let url = format!("{}{}", self.rest_base_url, path);

        let response = self
            .http
            .post(url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        parse_response(response).await
    }

    async fn api_key_post<T>(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<T, BinanceError>
    where
        T: DeserializeOwned,
    {
        let body = form_body(params);
        let url = format!("{}{}", self.rest_base_url, path);

        let response = self
            .http
            .post(url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        parse_response(response).await
    }

    async fn api_key_put<T>(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<T, BinanceError>
    where
        T: DeserializeOwned,
    {
        let body = form_body(params);
        let url = format!("{}{}", self.rest_base_url, path);

        let response = self
            .http
            .put(url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        parse_response(response).await
    }

    async fn api_key_delete<T>(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<T, BinanceError>
    where
        T: DeserializeOwned,
    {
        let body = form_body(params);
        let url = format!("{}{}", self.rest_base_url, path);

        let response = self
            .http
            .delete(url)
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        parse_response(response).await
    }

    fn signed_query(&self, mut params: Vec<(&str, String)>) -> Result<String, BinanceError> {
        params.push(("recvWindow", self.recv_window_ms.to_string()));
        params.push(("timestamp", Utc::now().timestamp_millis().to_string()));

        let query = params
            .iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect::<Vec<_>>()
            .join("&");

        let mut mac = HmacSha256::new_from_slice(self.credentials.api_secret.as_bytes()).map_err(
            |error: hmac::digest::InvalidLength| BinanceError::Signing(error.to_string()),
        )?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        Ok(format!("{}&signature={}", query, signature))
    }
}
