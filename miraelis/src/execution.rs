use futures_util::{SinkExt, StreamExt};
use hex::encode as hex_encode;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::{BTreeMap, HashMap},
    env,
    time::Duration,
};
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::signal::{SignalType, TelegramNotifier};
use crate::strategy::frame::{
    OrderRequest, OrderResponse, OrderResponseAction, OrderRunningStatus, PositionSide,
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_api_secret_env")]
    pub api_secret_env: String,
    #[serde(default = "default_ws_trade_url")]
    pub ws_trade_url: String,
    #[serde(default = "default_ws_user_data_base_url")]
    pub ws_user_data_base_url: String,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default = "default_listen_key_keepalive_secs")]
    pub listen_key_keepalive_secs: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key_env: default_api_key_env(),
            api_secret_env: default_api_secret_env(),
            ws_trade_url: default_ws_trade_url(),
            ws_user_data_base_url: default_ws_user_data_base_url(),
            recv_window_ms: default_recv_window_ms(),
            listen_key_keepalive_secs: default_listen_key_keepalive_secs(),
        }
    }
}

fn default_api_key_env() -> String {
    "BINANCE_API_KEY".to_owned()
}
fn default_api_secret_env() -> String {
    "BINANCE_API_SECRET".to_owned()
}
fn default_ws_trade_url() -> String {
    "wss://ws-fapi.binance.com/ws-fapi/v1".to_owned()
}
fn default_ws_user_data_base_url() -> String {
    "wss://fstream.binance.com/ws".to_owned()
}
fn default_recv_window_ms() -> u64 {
    5000
}
fn default_listen_key_keepalive_secs() -> u64 {
    1800
}

#[derive(Debug, Clone)]
struct BinanceCredentials {
    api_key: String,
    api_secret: String,
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("missing env var: {0}")]
    MissingEnvVar(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("websocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("hmac error: {0}")]
    Hmac(String),
    #[error("invalid order: {0}")]
    InvalidOrder(String),
}

#[derive(Debug, Clone, Copy)]
pub struct SymbolTradingRule {
    pub price_precision: i32,
    pub qty_precision: i32,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_qty: f64,
}

impl BinanceCredentials {
    fn from_env(config: &ExecutionConfig) -> Result<Self, ExecutionError> {
        let api_key = env::var(&config.api_key_env)
            .map_err(|_| ExecutionError::MissingEnvVar(config.api_key_env.clone()))?;
        let api_secret = env::var(&config.api_secret_env)
            .map_err(|_| ExecutionError::MissingEnvVar(config.api_secret_env.clone()))?;

        Ok(Self {
            api_key,
            api_secret,
        })
    }
}

pub async fn start_execution_tasks(
    config: ExecutionConfig,
    rest_base_url: String,
    order_rx: UnboundedReceiver<OrderRequest>,
    response_tx: UnboundedSender<OrderResponse>,
    symbol_rules: HashMap<String, SymbolTradingRule>,
    telegram_notifier: Option<TelegramNotifier>,
) -> Result<(), ExecutionError> {
    if !config.enabled {
        return Ok(());
    }

    let credentials = BinanceCredentials::from_env(&config)?;

    let trade_config = config.clone();
    let trade_credentials = credentials.clone();
    let trade_tx = response_tx.clone();
    let trade_symbol_rules = symbol_rules.clone();
    let trade_notifier = telegram_notifier.clone();
    tokio::spawn(async move {
        run_trade_ws_loop(
            trade_config,
            trade_credentials,
            order_rx,
            trade_tx,
            trade_symbol_rules,
            trade_notifier,
        )
        .await;
    });

    let listen_key = create_listen_key(&rest_base_url, &credentials.api_key).await?;

    let keepalive_config = config.clone();
    let keepalive_credentials = credentials.clone();
    let keepalive_rest = rest_base_url.clone();
    let keepalive_key = listen_key.clone();
    tokio::spawn(async move {
        run_listen_key_keepalive_loop(
            keepalive_rest,
            keepalive_config,
            keepalive_credentials,
            keepalive_key,
        )
        .await;
    });

    let user_config = config.clone();
    let user_tx = response_tx;
    let user_notifier = telegram_notifier;
    tokio::spawn(async move {
        run_user_data_ws_loop(user_config, listen_key, user_tx, user_notifier).await;
    });

    info!("execution tasks started");
    Ok(())
}

async fn run_trade_ws_loop(
    config: ExecutionConfig,
    credentials: BinanceCredentials,
    mut order_rx: UnboundedReceiver<OrderRequest>,
    response_tx: UnboundedSender<OrderResponse>,
    symbol_rules: HashMap<String, SymbolTradingRule>,
    telegram_notifier: Option<TelegramNotifier>,
) {
    let mut req_seq: u64 = 1;

    loop {
        let ws_connect = connect_async(&config.ws_trade_url).await;
        let (ws_stream, _) = match ws_connect {
            Ok(ok) => ok,
            Err(error) => {
                warn!(?error, "trade ws connect failed, retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        info!(url = %config.ws_trade_url, "trade ws connected");

        let (mut ws_write, mut ws_read) = ws_stream.split();

        loop {
            tokio::select! {
                maybe_order = order_rx.recv() => {
                    let Some(order) = maybe_order else {
                        info!("order request channel closed, stopping trade ws loop");
                        return;
                    };

                    let normalized = match normalize_order_request(order, &symbol_rules) {
                        Ok(order) => order,
                        Err(error) => {
                            error!(?error, "normalize order request failed");
                            let text = format!("❌ order normalize failed: {error}");
                            send_order_notify(&telegram_notifier, &text).await;
                            continue;
                        }
                    };

                    req_seq = req_seq.saturating_add(1);
                    let request_id = format!("frame-{}-{}", normalized.client_order_id, req_seq);

                    let payload = match build_ws_order_payload(
                        &request_id,
                        &normalized,
                        &credentials,
                        config.recv_window_ms,
                    ) {
                        Ok(payload) => payload,
                        Err(error) => {
                            error!(?error, cid = %normalized.client_order_id, "build ws order payload failed");
                            let text = format!(
                                "❌ order payload build failed\nsymbol: {}\ncid: {}\nerror: {}",
                                normalized.symbol, normalized.client_order_id, error
                            );
                            send_order_notify(&telegram_notifier, &text).await;
                            let _ = response_tx.send(OrderResponse {
                                action: OrderResponseAction::Put,
                                client_order_id: normalized.client_order_id,
                                order_id: 0,
                                symbol: normalized.symbol,
                                running_status: OrderRunningStatus::Rejected,
                                avg_price: 0.0,
                                cum_qty: 0.0,
                            });
                            continue;
                        }
                    };

                    if let Err(error) = ws_write.send(Message::Text(payload.into())).await {
                        error!(?error, "send ws order payload failed");
                        let text = format!("❌ order ws send failed: {error}");
                        send_order_notify(&telegram_notifier, &text).await;
                        break;
                    }
                }

                incoming = ws_read.next() => {
                    let Some(msg_res) = incoming else {
                        warn!("trade ws closed by peer");
                        break;
                    };

                    let msg = match msg_res {
                        Ok(msg) => msg,
                        Err(error) => {
                            warn!(?error, "trade ws read error");
                            break;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            if let Some(parsed) = parse_trade_ws_response(&text) {
                                match parsed {
                                    Ok(resp) => {
                                        let notify = format_order_ack_notify(&resp);
                                        send_order_notify(&telegram_notifier, &notify).await;
                                        let _ = response_tx.send(resp);
                                    }
                                    Err(reason) => {
                                        let notify = format!("❌ order rejected\n{}", reason);
                                        send_order_notify(&telegram_notifier, &notify).await;
                                    }
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            if let Err(error) = ws_write.send(Message::Pong(payload)).await {
                                warn!(?error, "failed to pong trade ws");
                                break;
                            }
                        }
                        Message::Close(frame) => {
                            warn!(?frame, "trade ws closed");
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_user_data_ws_loop(
    config: ExecutionConfig,
    listen_key: String,
    response_tx: UnboundedSender<OrderResponse>,
    telegram_notifier: Option<TelegramNotifier>,
) {
    let ws_url = format!(
        "{}/{}",
        config.ws_user_data_base_url.trim_end_matches('/'),
        listen_key
    );

    loop {
        let connect = connect_async(&ws_url).await;
        let (ws_stream, _) = match connect {
            Ok(ok) => ok,
            Err(error) => {
                warn!(?error, "user data ws connect failed, retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        info!(url = %ws_url, "user data ws connected");

        let (mut ws_write, mut ws_read) = ws_stream.split();

        while let Some(incoming) = ws_read.next().await {
            let msg = match incoming {
                Ok(msg) => msg,
                Err(error) => {
                    warn!(?error, "user data ws read error");
                    break;
                }
            };

            match msg {
                Message::Text(text) => {
                    if let Some(resp) = parse_user_data_order_update(&text) {
                        let notify = format_order_status_notify(&resp);
                        send_order_notify(&telegram_notifier, &notify).await;
                        let _ = response_tx.send(resp);
                    }
                }
                Message::Ping(payload) => {
                    if let Err(error) = ws_write.send(Message::Pong(payload)).await {
                        warn!(?error, "failed to pong user data ws");
                        break;
                    }
                }
                Message::Close(frame) => {
                    warn!(?frame, "user data ws closed");
                    break;
                }
                _ => {}
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_listen_key_keepalive_loop(
    rest_base_url: String,
    config: ExecutionConfig,
    credentials: BinanceCredentials,
    listen_key: String,
) {
    let client = reqwest::Client::new();
    let url = format!("{}/fapi/v1/listenKey", rest_base_url.trim_end_matches('/'));
    let mut ticker = tokio::time::interval(Duration::from_secs(
        config.listen_key_keepalive_secs.max(60),
    ));
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let res = client
            .put(&url)
            .header("X-MBX-APIKEY", &credentials.api_key)
            .query(&[("listenKey", listen_key.as_str())])
            .send()
            .await;
        match res {
            Ok(resp) if resp.status().is_success() => {
                info!("listen key keepalive success");
            }
            Ok(resp) => {
                warn!(status = %resp.status(), "listen key keepalive failed");
            }
            Err(error) => {
                warn!(?error, "listen key keepalive request failed");
            }
        }
    }
}

async fn create_listen_key(rest_base_url: &str, api_key: &str) -> Result<String, ExecutionError> {
    let client = reqwest::Client::new();
    let url = format!("{}/fapi/v1/listenKey", rest_base_url.trim_end_matches('/'));
    let response = client
        .post(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?;

    let value: Value = response.json().await?;
    let key = value
        .get("listenKey")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            ExecutionError::Json(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "listenKey missing in response",
            )))
        })?;
    Ok(key.to_owned())
}

fn build_ws_order_payload(
    request_id: &str,
    order: &OrderRequest,
    credentials: &BinanceCredentials,
    recv_window_ms: u64,
) -> Result<String, ExecutionError> {
    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut params = BTreeMap::new();
    params.insert("apiKey".to_owned(), credentials.api_key.clone());
    params.insert("symbol".to_owned(), order.symbol.to_uppercase());
    params.insert(
        "side".to_owned(),
        match order.side {
            PositionSide::Long => "BUY".to_owned(),
            PositionSide::Short => "SELL".to_owned(),
        },
    );

    let order_type = match order.order_type {
        crate::strategy::frame::FrameOrderType::Limit => "LIMIT",
        crate::strategy::frame::FrameOrderType::Market => "MARKET",
        crate::strategy::frame::FrameOrderType::StopMarket => "STOP_MARKET",
        crate::strategy::frame::FrameOrderType::TakeProfitMarket => "TAKE_PROFIT_MARKET",
    }
    .to_owned();
    params.insert("type".to_owned(), order_type.clone());
    params.insert("quantity".to_owned(), order.qty.to_string());
    params.insert("newClientOrderId".to_owned(), order.client_order_id.clone());
    params.insert("recvWindow".to_owned(), recv_window_ms.to_string());
    params.insert("timestamp".to_owned(), timestamp.to_string());

    if matches!(
        order.order_type,
        crate::strategy::frame::FrameOrderType::Limit
    ) {
        params.insert("timeInForce".to_owned(), "GTC".to_owned());
        params.insert("price".to_owned(), order.price.to_string());
    }

    if matches!(
        order.order_type,
        crate::strategy::frame::FrameOrderType::StopMarket
            | crate::strategy::frame::FrameOrderType::TakeProfitMarket
    ) {
        params.insert("stopPrice".to_owned(), order.stop_price.to_string());
    }

    if order.reduce_only {
        params.insert("reduceOnly".to_owned(), "true".to_owned());
    }

    let signature = sign_map(&params, &credentials.api_secret)?;

    let mut ws_params = serde_json::Map::new();
    ws_params.insert(
        "apiKey".to_owned(),
        Value::String(params.get("apiKey").cloned().unwrap_or_default()),
    );
    ws_params.insert(
        "symbol".to_owned(),
        Value::String(params.get("symbol").cloned().unwrap_or_default()),
    );
    ws_params.insert(
        "side".to_owned(),
        Value::String(params.get("side").cloned().unwrap_or_default()),
    );
    ws_params.insert(
        "type".to_owned(),
        Value::String(params.get("type").cloned().unwrap_or_default()),
    );
    ws_params.insert(
        "quantity".to_owned(),
        Value::String(params.get("quantity").cloned().unwrap_or_default()),
    );
    ws_params.insert(
        "newClientOrderId".to_owned(),
        Value::String(params.get("newClientOrderId").cloned().unwrap_or_default()),
    );
    ws_params.insert(
        "recvWindow".to_owned(),
        Value::String(params.get("recvWindow").cloned().unwrap_or_default()),
    );
    ws_params.insert(
        "timestamp".to_owned(),
        Value::String(params.get("timestamp").cloned().unwrap_or_default()),
    );
    ws_params.insert("signature".to_owned(), Value::String(signature));

    if let Some(v) = params.get("timeInForce") {
        ws_params.insert("timeInForce".to_owned(), Value::String(v.clone()));
    }
    if let Some(v) = params.get("price") {
        ws_params.insert("price".to_owned(), Value::String(v.clone()));
    }
    if let Some(v) = params.get("stopPrice") {
        ws_params.insert("stopPrice".to_owned(), Value::String(v.clone()));
    }
    if let Some(v) = params.get("reduceOnly") {
        ws_params.insert("reduceOnly".to_owned(), Value::String(v.clone()));
    }

    let payload = serde_json::json!({
        "id": request_id,
        "method": "order.place",
        "params": ws_params,
    });

    Ok(payload.to_string())
}

fn sign_map(params: &BTreeMap<String, String>, secret: &str) -> Result<String, ExecutionError> {
    let query = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| ExecutionError::Hmac(error.to_string()))?;
    mac.update(query.as_bytes());
    let bytes = mac.finalize().into_bytes();
    Ok(hex_encode(bytes))
}

fn parse_trade_ws_response(text: &str) -> Option<Result<OrderResponse, String>> {
    let value: Value = serde_json::from_str(text).ok()?;

    let status_code = value.get("status").and_then(|v| v.as_i64());
    let result = value.get("result");

    if status_code == Some(200) {
        let result = result?;
        let symbol = result.get("symbol")?.as_str()?.to_owned();
        let cid = result.get("clientOrderId")?.as_str()?.to_owned();
        let oid = result.get("orderId").and_then(|v| v.as_u64()).unwrap_or(0);
        let status = result
            .get("status")
            .and_then(|v| v.as_str())
            .map(map_order_status)
            .unwrap_or(OrderRunningStatus::Submitting);

        return Some(Ok(OrderResponse {
            action: OrderResponseAction::Put,
            client_order_id: cid,
            order_id: oid,
            symbol,
            running_status: status,
            avg_price: 0.0,
            cum_qty: 0.0,
        }));
    }

    if status_code.is_some() {
        let detail = value
            .get("error")
            .map(|v| v.to_string())
            .unwrap_or_else(|| value.to_string());
        warn!(detail = %detail, "order ws request rejected");
        return Some(Err(detail));
    }

    None
}

fn parse_user_data_order_update(text: &str) -> Option<OrderResponse> {
    let value: Value = serde_json::from_str(text).ok()?;
    let event_type = value.get("e")?.as_str()?;
    if event_type != "ORDER_TRADE_UPDATE" {
        return None;
    }

    let order = value.get("o")?;
    let symbol = order.get("s")?.as_str()?.to_owned();
    let cid = order.get("c")?.as_str()?.to_owned();
    let oid = order.get("i").and_then(|v| v.as_u64()).unwrap_or(0);
    let status = order
        .get("X")
        .and_then(|v| v.as_str())
        .map(map_order_status)
        .unwrap_or(OrderRunningStatus::Submitting);
    let avg_price = order
        .get("ap")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);
    let cum_qty = order
        .get("z")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);

    let action = if status == OrderRunningStatus::Canceled {
        OrderResponseAction::Cancel
    } else {
        OrderResponseAction::Query
    };

    Some(OrderResponse {
        action,
        client_order_id: cid,
        order_id: oid,
        symbol,
        running_status: status,
        avg_price,
        cum_qty,
    })
}

fn map_order_status(status: &str) -> OrderRunningStatus {
    match status {
        "NEW" => OrderRunningStatus::Open,
        "PARTIALLY_FILLED" => OrderRunningStatus::PartialFilled,
        "FILLED" => OrderRunningStatus::Filled,
        "CANCELED" => OrderRunningStatus::Canceled,
        "REJECTED" => OrderRunningStatus::Rejected,
        "EXPIRED" => OrderRunningStatus::Expired,
        _ => OrderRunningStatus::Submitting,
    }
}

fn format_order_ack_notify(resp: &OrderResponse) -> String {
    format!(
        "✅ order ack\nsymbol: {}\ncid: {}\noid: {}\nstatus: {:?}",
        resp.symbol, resp.client_order_id, resp.order_id, resp.running_status
    )
}

fn format_order_status_notify(resp: &OrderResponse) -> String {
    format!(
        "📌 order update\nsymbol: {}\ncid: {}\noid: {}\nstatus: {:?}\navg_price: {}\ncum_qty: {}",
        resp.symbol,
        resp.client_order_id,
        resp.order_id,
        resp.running_status,
        resp.avg_price,
        resp.cum_qty
    )
}

async fn send_order_notify(notifier: &Option<TelegramNotifier>, text: &str) {
    if let Some(notifier) = notifier {
        if let Err(error) = notifier
            .send_for_signal_type(SignalType::OrderNotify, text)
            .await
        {
            warn!(?error, "send telegram order notify failed");
        }
    }
}

fn normalize_order_request(
    mut order: OrderRequest,
    symbol_rules: &HashMap<String, SymbolTradingRule>,
) -> Result<OrderRequest, ExecutionError> {
    let key = order.symbol.to_ascii_lowercase();
    let Some(rule) = symbol_rules.get(&key) else {
        warn!(symbol = %order.symbol, "missing symbol precision rule, use order values as-is");
        return Ok(order);
    };

    order.qty = normalize_qty(order.qty, rule.step_size, rule.qty_precision);
    if order.qty <= 0.0 {
        return Err(ExecutionError::InvalidOrder(format!(
            "normalized qty <= 0 for {}",
            order.symbol
        )));
    }

    if rule.min_qty > 0.0 && order.qty + 1e-12 < rule.min_qty {
        return Err(ExecutionError::InvalidOrder(format!(
            "qty {} below min_qty {} for {}",
            order.qty, rule.min_qty, order.symbol
        )));
    }

    if matches!(
        order.order_type,
        crate::strategy::frame::FrameOrderType::Limit
    ) {
        order.price = normalize_price(order.price, rule.tick_size, rule.price_precision);
    }

    if matches!(
        order.order_type,
        crate::strategy::frame::FrameOrderType::StopMarket
            | crate::strategy::frame::FrameOrderType::TakeProfitMarket
    ) {
        order.stop_price = normalize_price(order.stop_price, rule.tick_size, rule.price_precision);
        if order.price > 0.0 {
            order.price = normalize_price(order.price, rule.tick_size, rule.price_precision);
        }
    }

    Ok(order)
}

fn normalize_price(price: f64, tick_size: f64, precision: i32) -> f64 {
    if tick_size <= 0.0 || !price.is_finite() {
        return price;
    }
    let aligned = (price / tick_size).round() * tick_size;
    let factor = 10f64.powi(precision.max(0));
    (aligned * factor).round() / factor
}

fn normalize_qty(qty: f64, step_size: f64, precision: i32) -> f64 {
    if !qty.is_finite() || qty <= 0.0 {
        return 0.0;
    }

    let stepped = if step_size > 0.0 {
        (qty / step_size).floor() * step_size
    } else {
        qty
    };

    let factor = 10f64.powi(precision.max(0));
    (stepped * factor).floor() / factor
}
