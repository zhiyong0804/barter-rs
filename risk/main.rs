mod app;
mod binance;
mod config;
mod market_stream;
mod state;
mod supervisor;
mod user_data_stream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    app::run().await
}
/*
mod app;
mod binance;
mod config;
use crate::{
    binance::{
        AccountUpdateSummary, BinanceCredentials, BinanceFuturesRestClient, BinancePositionSide,
        BookTickerEvent, FuturesAccountInformation, FuturesPositionRisk, HedgeOrderRequest,
        MarkPriceEvent, OrderTradeUpdate, RawBookTickerEvent, RawMarkPriceEvent, UserDataEvent,
    },
    config::{AppConfig, HedgeOrderType, MarginMode, PositionMode, load_config},
};
use barter::logging::init_logging;
use barter_instrument::Side;
use chrono::{DateTime, TimeZone, Utc};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Default)]
struct MarketSnapshot {
    reference_price: Option<Decimal>,
type SharedMarketState = Arc<RwLock<HashMap<String, MarketSnapshot>>>;
type SharedRiskState = Arc<RwLock<RiskState>>;

#[derive(Debug, Clone)]
struct RiskState {
    account: FuturesAccountInformation,
    positions: HashMap<String, FuturesPositionRisk>,
    last_orders: HashMap<String, OrderTradeUpdate>,
}

#[derive(Debug, Clone)]
struct HedgeDecision {
    symbol: String,
    side: Side,
    position_side: Option<BinancePositionSide>,
    quantity: Decimal,
    reason: String,
}

#[derive(Debug, Clone)]
enum RiskTrigger {
    All,
    Symbol(String),
}

struct PositionSupervisor {
    config: AppConfig,
    client: BinanceFuturesRestClient,
    market_state: SharedMarketState,
    risk_state: SharedRiskState,
}

impl PositionSupervisor {
    fn new(
        config: AppConfig,
        client: BinanceFuturesRestClient,
        market_state: SharedMarketState,
        risk_state: SharedRiskState,
    ) -> Self {
        Self {
            config,
            client,
            market_state,
            risk_state,
        }
    }

    async fn run(self, mut trigger_rx: mpsc::UnboundedReceiver<RiskTrigger>) {
    let mut cooldowns: HashMap<String, Instant> = HashMap::new();

    info!(
            dry_run = self.config.monitor.dry_run,
            debounce_ms = self.config.monitor.poll_interval_ms,
            "Event-driven position supervisor started"
    );

    while let Some(trigger) = trigger_rx.recv().await {
            tokio::time::sleep(Duration::from_millis(self.config.monitor.poll_interval_ms)).await;

            let mut symbols = HashSet::new();
            let mut full_scan = matches!(trigger, RiskTrigger::All);
            if let RiskTrigger::Symbol(symbol) = trigger {
                symbols.insert(symbol);
            while let Ok(trigger) = trigger_rx.try_recv() {
                match trigger {
                    RiskTrigger::All => {
                        full_scan = true;
                        symbols.clear();
                    }
                    RiskTrigger::Symbol(symbol) if !full_scan => {
                        symbols.insert(symbol);
                    }
                    RiskTrigger::Symbol(_) => {}
                }
            }
            if let Err(error) = self
                .evaluate_and_hedge(&mut cooldowns, full_scan.then_some(()), symbols)
                .await
            {
                error!(?error, "risk evaluation cycle failed");
            }
        }
    }

    async fn evaluate_and_hedge(
    &self,
mod app;
mod binance;
mod config;
use crate::{
    binance::{
        AccountUpdateSummary, BinanceCredentials, BinanceFuturesRestClient, BinancePositionSide,
        BookTickerEvent, FuturesAccountInformation, FuturesPositionRisk, HedgeOrderRequest,
        MarkPriceEvent, OrderTradeUpdate, RawBookTickerEvent, RawMarkPriceEvent, UserDataEvent,
    },
    config::{AppConfig, HedgeOrderType, MarginMode, PositionMode, load_config},
};
use barter::logging::init_logging;
use barter_instrument::Side;
use chrono::{DateTime, TimeZone, Utc};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Default)]
struct MarketSnapshot {
    reference_price: Option<Decimal>,
    last_update: Option<DateTime<Utc>>,
}

type SharedMarketState = Arc<RwLock<HashMap<String, MarketSnapshot>>>;
type SharedRiskState = Arc<RwLock<RiskState>>;

#[derive(Debug, Clone)]
struct RiskState {
    account: FuturesAccountInformation,
    positions: HashMap<String, FuturesPositionRisk>,
    last_orders: HashMap<String, OrderTradeUpdate>,
}

#[derive(Debug, Clone)]
struct HedgeDecision {
    symbol: String,
    side: Side,
    position_side: Option<BinancePositionSide>,
    quantity: Decimal,
    reason: String,
}
#[derive(Debug, Clone)]
enum RiskTrigger {
    All,
    Symbol(String),
}

struct PositionSupervisor {
    config: AppConfig,
    client: BinanceFuturesRestClient,
    market_state: SharedMarketState,
    risk_state: SharedRiskState,
}

impl PositionSupervisor {
    fn new(
        config: AppConfig,
        client: BinanceFuturesRestClient,
        market_state: SharedMarketState,
        risk_state: SharedRiskState,
    ) -> Self {
        Self {
            config,
            client,
            market_state,
            risk_state,
        }
    }

    async fn run(self, mut trigger_rx: mpsc::UnboundedReceiver<RiskTrigger>) {
    let mut cooldowns: HashMap<String, Instant> = HashMap::new();

    info!(
            dry_run = self.config.monitor.dry_run,
            debounce_ms = self.config.monitor.poll_interval_ms,
            "Event-driven position supervisor started"
    );

    while let Some(trigger) = trigger_rx.recv().await {
            tokio::time::sleep(Duration::from_millis(self.config.monitor.poll_interval_ms)).await;

            let mut symbols = HashSet::new();
            let mut full_scan = matches!(trigger, RiskTrigger::All);
            if let RiskTrigger::Symbol(symbol) = trigger {
                symbols.insert(symbol);
            while let Ok(trigger) = trigger_rx.try_recv() {
                match trigger {
                    RiskTrigger::All => {
                        full_scan = true;
                        symbols.clear();
                    }
                    RiskTrigger::Symbol(symbol) if !full_scan => {
                        symbols.insert(symbol);
                    }
                    RiskTrigger::Symbol(_) => {}
                }
            }
            if let Err(error) = self
                .evaluate_and_hedge(&mut cooldowns, full_scan.then_some(()), symbols)
                .await
            {
                error!(?error, "risk evaluation cycle failed");
            }
        }
    }

    async fn evaluate_and_hedge(
    &self,
    cooldowns: &mut HashMap<String, Instant>,
    full_scan: Option<()>,
        symbols: HashSet<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let risk_state = self.risk_state.read().await;
    let market_state = self.market_state.read().await;

    let account = risk_state.account.clone();
        let account_loss_ratio = negative_pnl_ratio(
            account.total_unrealized_profit,
            account.total_wallet_balance,
    );

    let positions = risk_state
            .positions
            .values()
            .filter(|position| position.is_open())
            .filter(|position| full_scan.is_some() || symbols.contains(&position.symbol))
            .cloned()
            .collect::<Vec<_>>();
        for position in positions {
            if self.config.monitor.margin_mode == MarginMode::Cross && !position.is_cross() {
                continue;
            let Some(decision) = evaluate_hedge_decision(
                &self.config,
                &account,
                &position,
                market_state.get(&position.symbol),
                risk_state.last_orders.get(&position.symbol),
                account_loss_ratio,
            ) else {
                continue;
            };

            let cooldown_key = format!("{}:{}", position.symbol, position.position_side.as_str());
            if cooldown_active(cooldowns, &cooldown_key, self.config.hedge.cooldown_secs) {
                continue;
            }
            if self.config.monitor.dry_run {
                info!(
                    symbol = %decision.symbol,
                    side = %decision.side,
                    quantity = %decision.quantity,
                    reason = %decision.reason,
                    "Dry-run hedge triggered"
                );
            } else {
                let client_order_id = format!(
                    "barter_hedge_{}_{}",
                    decision.symbol.to_lowercase(),
                    Utc::now().timestamp_millis()
                );
                let ack = self
                    .client
                    .place_market_hedge(HedgeOrderRequest {
                        symbol: decision.symbol.clone(),
                        side: decision.side,
                        position_side: decision.position_side,
                        quantity: decision.quantity,
                        client_order_id,
                    })
                    .await?;
                info!(
                    symbol = %ack.symbol,
                    order_id = ack.order_id,
                    client_order_id = %ack.client_order_id,
                    status = ?ack.status,
                    reason = %decision.reason,
                    "Hedge order placed"
                );
            }
            cooldowns.insert(cooldown_key, Instant::now());
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let config = load_config("risk/config.json")?;
    validate_config(&config)?;

    let credentials = BinanceCredentials::from_env_names(
        &config.binance.api_key_env,
        &config.binance.api_secret_env,
    )?;
    let client = BinanceFuturesRestClient::new(&config.binance, credentials)?;

    let initial_account = client.fetch_account_information().await?;
    let initial_positions = client.fetch_position_risks().await?;

    let risk_state: SharedRiskState = Arc::new(RwLock::new(RiskState {
        account: initial_account,
        positions: initial_positions
            .into_iter()
            .map(|position| (position_key(&position.symbol, position.position_side), position))
            .collect(),
        last_orders: HashMap::new(),
    }));
    let market_state: SharedMarketState = Arc::new(RwLock::new(HashMap::new()));
    let (trigger_tx, trigger_rx) = mpsc::unbounded_channel();

    let book_ticker_task = {
        let market_state = Arc::clone(&market_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = config.binance.ws_base_url.clone();
        tokio::spawn(async move {
            if let Err(error) = run_all_book_ticker_stream(&ws_base_url, market_state, trigger_tx).await {
                error!(?error, "all-market book ticker task failed");
            }
        })
    };

    let mark_price_task = {
        let market_state = Arc::clone(&market_state);
        let risk_state = Arc::clone(&risk_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = config.binance.ws_base_url.clone();
        tokio::spawn(async move {
            if let Err(error) = run_all_mark_price_stream(&ws_base_url, market_state, risk_state, trigger_tx).await {
                error!(?error, "all-market mark price task failed");
            }
        })
    };

    let user_stream_task = {
        let client = client.clone();
        let risk_state = Arc::clone(&risk_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = config.binance.ws_base_url.clone();
        let keepalive_secs = config.binance.listen_key_keepalive_secs;
        tokio::spawn(async move {
            if let Err(error) = run_user_data_stream(
                client,
                ws_base_url,
                keepalive_secs,
                risk_state,
                trigger_tx,
            )
            .await
            {
                error!(?error, "user data stream task failed");
            }
        })
    };

    let _ = trigger_tx.send(RiskTrigger::All);

    let supervisor = PositionSupervisor::new(
        config,
        client,
        Arc::clone(&market_state),
        Arc::clone(&risk_state),
    );

    tokio::select! {
        _ = supervisor.run(trigger_rx) => {}
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received, shutting down");
        }
    }

    book_ticker_task.abort();
    mark_price_task.abort();
    user_stream_task.abort();

    Ok(())
}

fn validate_config(config: &AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !config.system.instruments.is_empty() {
        warn!(
            configured_instruments = config.system.instruments.len(),
            "system.instruments is ignored in dynamic all-symbol market mode"
        );
    }

    if config.monitor.margin_mode != MarginMode::Cross {
        warn!("Current implementation is tuned for cross margin supervision");
    }

    Ok(())
}

*/
/*
mod binance;
mod config;

use crate::{
    binance::{
        AccountUpdateSummary, BinanceCredentials, BinanceFuturesRestClient, BinancePositionSide,
        BookTickerEvent, FuturesAccountInformation, FuturesPositionRisk, HedgeOrderRequest,
        MarkPriceEvent, OrderTradeUpdate, RawBookTickerEvent, RawMarkPriceEvent, UserDataEvent,
    },
    config::{AppConfig, HedgeOrderType, MarginMode, PositionMode, load_config},
};
use barter::logging::init_logging;
use barter_instrument::Side;
use chrono::{DateTime, TimeZone, Utc};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Default)]
struct MarketSnapshot {
    reference_price: Option<Decimal>,
    last_update: Option<DateTime<Utc>>,
}

type SharedMarketState = Arc<RwLock<HashMap<String, MarketSnapshot>>>;
type SharedRiskState = Arc<RwLock<RiskState>>;

#[derive(Debug, Clone)]
struct RiskState {
    account: FuturesAccountInformation,
    positions: HashMap<String, FuturesPositionRisk>,
    last_orders: HashMap<String, OrderTradeUpdate>,
}

#[derive(Debug, Clone)]
struct HedgeDecision {
    symbol: String,
    side: Side,
    position_side: Option<BinancePositionSide>,
    quantity: Decimal,
    reason: String,
}

#[derive(Debug, Clone)]
enum RiskTrigger {
    All,
    Symbol(String),
}

struct PositionSupervisor {
    config: AppConfig,
    client: BinanceFuturesRestClient,
    market_state: SharedMarketState,
    risk_state: SharedRiskState,
}

impl PositionSupervisor {
    fn new(
        config: AppConfig,
        client: BinanceFuturesRestClient,
        market_state: SharedMarketState,
        risk_state: SharedRiskState,
    ) -> Self {
        Self {
            config,
            client,
            market_state,
            risk_state,
        }
    }

    async fn run(self, mut trigger_rx: mpsc::UnboundedReceiver<RiskTrigger>) {
        let mut cooldowns: HashMap<String, Instant> = HashMap::new();

        info!(
            dry_run = self.config.monitor.dry_run,
            debounce_ms = self.config.monitor.poll_interval_ms,
            "Event-driven position supervisor started"
        );

        while let Some(trigger) = trigger_rx.recv().await {
            tokio::time::sleep(Duration::from_millis(self.config.monitor.poll_interval_ms)).await;

            let mut symbols = HashSet::new();
            let mut full_scan = matches!(trigger, RiskTrigger::All);
            if let RiskTrigger::Symbol(symbol) = trigger {
                symbols.insert(symbol);
            }

            while let Ok(trigger) = trigger_rx.try_recv() {
                match trigger {
                    RiskTrigger::All => {
                        full_scan = true;
                        symbols.clear();
                    }
                    RiskTrigger::Symbol(symbol) if !full_scan => {
                        symbols.insert(symbol);
                    }
                    RiskTrigger::Symbol(_) => {}
                }
            }

            if let Err(error) = self
                .evaluate_and_hedge(&mut cooldowns, full_scan.then_some(()), symbols)
                .await
            {
                error!(?error, "risk evaluation cycle failed");
            }
        }
    }

    async fn evaluate_and_hedge(
        &self,
        cooldowns: &mut HashMap<String, Instant>,
        full_scan: Option<()>,
        symbols: HashSet<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let risk_state = self.risk_state.read().await;
        let market_state = self.market_state.read().await;

        let account = risk_state.account.clone();
        let account_loss_ratio = negative_pnl_ratio(
            account.total_unrealized_profit,
            account.total_wallet_balance,
        );

        let positions = risk_state
            .positions
            .values()
            .filter(|position| position.is_open())
            .filter(|position| full_scan.is_some() || symbols.contains(&position.symbol))
            .cloned()
            .collect::<Vec<_>>();

        for position in positions {
            if self.config.monitor.margin_mode == MarginMode::Cross && !position.is_cross() {
                continue;
            }

            let Some(decision) = evaluate_hedge_decision(
                &self.config,
                &account,
                &position,
                market_state.get(&position.symbol),
                risk_state.last_orders.get(&position.symbol),
                account_loss_ratio,
            ) else {
                continue;
            };

            let cooldown_key = format!("{}:{}", position.symbol, position.position_side.as_str());
            if cooldown_active(cooldowns, &cooldown_key, self.config.hedge.cooldown_secs) {
                continue;
            }

            if self.config.monitor.dry_run {
                info!(
                    symbol = %decision.symbol,
                    side = %decision.side,
                    quantity = %decision.quantity,
                    reason = %decision.reason,
                    "Dry-run hedge triggered"
                );
            } else {
                let client_order_id = format!(
                    "barter_hedge_{}_{}",
                    decision.symbol.to_lowercase(),
                    Utc::now().timestamp_millis()
                );
                let ack = self
                    .client
                    .place_market_hedge(HedgeOrderRequest {
                        symbol: decision.symbol.clone(),
                        side: decision.side,
                        position_side: decision.position_side,
                        quantity: decision.quantity,
                        client_order_id,
                    })
                    .await?;

                info!(
                    symbol = %ack.symbol,
                    order_id = ack.order_id,
                    client_order_id = %ack.client_order_id,
                    status = ?ack.status,
                    reason = %decision.reason,
                    "Hedge order placed"
                );
            }

            cooldowns.insert(cooldown_key, Instant::now());
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let config = load_config("risk/config.json")?;
    validate_config(&config)?;

    let credentials = BinanceCredentials::from_env_names(
        &config.binance.api_key_env,
        &config.binance.api_secret_env,
    )?;
    let client = BinanceFuturesRestClient::new(&config.binance, credentials)?;

    let initial_account = client.fetch_account_information().await?;
    let initial_positions = client.fetch_position_risks().await?;

    let risk_state = Arc::new(RwLock::new(RiskState {
        account: initial_account,
        positions: initial_positions
            .into_iter()
            .map(|position| (position_key(&position.symbol, position.position_side), position))
            .collect(),
        last_orders: HashMap::new(),
    }));
    let market_state: SharedMarketState = Arc::new(RwLock::new(HashMap::new()));
    let (trigger_tx, trigger_rx) = mpsc::unbounded_channel();

    let book_ticker_task = {
        let market_state = Arc::clone(&market_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = config.binance.ws_base_url.clone();
        tokio::spawn(async move {
            if let Err(error) = run_all_book_ticker_stream(&ws_base_url, market_state, trigger_tx).await {
                error!(?error, "all-market book ticker task failed");
            }
        })
    };

    let mark_price_task = {
        let market_state = Arc::clone(&market_state);
        let risk_state = Arc::clone(&risk_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = config.binance.ws_base_url.clone();
        tokio::spawn(async move {
            if let Err(error) = run_all_mark_price_stream(&ws_base_url, market_state, risk_state, trigger_tx).await {
                error!(?error, "all-market mark price task failed");
            }
        })
    };

    let user_stream_task = {
        let client = client.clone();
        let risk_state = Arc::clone(&risk_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = config.binance.ws_base_url.clone();
        let keepalive_secs = config.binance.listen_key_keepalive_secs;
        tokio::spawn(async move {
            if let Err(error) = run_user_data_stream(
                client,
                ws_base_url,
                keepalive_secs,
                risk_state,
                trigger_tx,
            )
            .await
            {
                error!(?error, "user data stream task failed");
            }
        })
    };

    let _ = trigger_tx.send(RiskTrigger::All);

    let supervisor = PositionSupervisor::new(
        config,
        client,
        Arc::clone(&market_state),
        Arc::clone(&risk_state),
    );

    tokio::select! {
        _ = supervisor.run(trigger_rx) => {}
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received, shutting down");
        }
    }

    book_ticker_task.abort();
    mark_price_task.abort();
    user_stream_task.abort();

    Ok(())
}

*/

/*

async fn run_all_book_ticker_stream(
    ws_base_url: &str,
    market_state: SharedMarketState,
    trigger_tx: mpsc::UnboundedSender<RiskTrigger>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let ws_url = format!("{}/ws/!bookTicker", ws_base_url.trim_end_matches('/'));
        let (ws_stream, _) = connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        while let Some(message) = read.next().await {
            match message? {
                Message::Text(text) => {
                    let event = BookTickerEvent::try_from(serde_json::from_str::<RawBookTickerEvent>(&text)?)?;
                    let symbol = event.symbol.clone();

                    let mut markets = market_state.write().await;
                    let snapshot = markets.entry(symbol.clone()).or_default();
                    snapshot.reference_price = Some(event.mid_price());
                    snapshot.last_update = Some(Utc::now());

                    let _ = trigger_tx.send(RiskTrigger::Symbol(symbol));
                }
                Message::Ping(payload) => {
                    write.send(Message::Pong(payload)).await?;
                }
                Message::Close(frame) => {
                    warn!(?frame, "all-market book ticker websocket closed, reconnecting");
                    break;
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_all_mark_price_stream(
    ws_base_url: &str,
    market_state: SharedMarketState,
    risk_state: SharedRiskState,
    trigger_tx: mpsc::UnboundedSender<RiskTrigger>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let ws_url = format!("{}/ws/!markPrice@arr@1s", ws_base_url.trim_end_matches('/'));
        let (ws_stream, _) = connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        while let Some(message) = read.next().await {
            match message? {
                Message::Text(text) => {
                    let events = serde_json::from_str::<Vec<RawMarkPriceEvent>>(&text)?;
                    for raw in events {
                        let event = MarkPriceEvent::try_from(raw)?;
                        let symbol = event.symbol.clone();

                        {
                            let mut markets = market_state.write().await;
                            let snapshot = markets.entry(symbol.clone()).or_default();
                            snapshot.reference_price = Some(event.mark_price);
                            snapshot.last_update = Utc
                                .timestamp_millis_opt(event.event_time)
                                .single()
                                .or_else(|| Some(Utc::now()));
                        }

                        {
                            let mut risk = risk_state.write().await;
                            for position in risk.positions.values_mut().filter(|p| p.symbol == symbol) {
                                position.mark_price = event.mark_price;
                                position.notional = position.position_amt * event.mark_price;
                            }
                        }

                        let _ = trigger_tx.send(RiskTrigger::Symbol(symbol));
                    }
                }
                Message::Ping(payload) => {
                    write.send(Message::Pong(payload)).await?;
                }
                Message::Close(frame) => {
                    warn!(?frame, "all-market mark price websocket closed, reconnecting");
                    break;
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_user_data_stream(
    client: BinanceFuturesRestClient,
    ws_base_url: String,
    keepalive_secs: u64,
    risk_state: SharedRiskState,
    trigger_tx: mpsc::UnboundedSender<RiskTrigger>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let listen_key = client.start_user_data_stream().await?;
        info!(%listen_key, "Binance user data stream created");

        let keepalive_handle = {
            let client = client.clone();
            let listen_key = listen_key.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(keepalive_secs));
                loop {
                    interval.tick().await;
                    if let Err(error) = client.keepalive_user_data_stream(&listen_key).await {
                        error!(?error, %listen_key, "listenKey keepalive failed");
                        break;
                    }
                }
            })
        };

        let ws_url = format!("{}/ws/{}", ws_base_url.trim_end_matches('/'), listen_key);
        let stream_result = consume_user_stream(&ws_url, &risk_state, &trigger_tx).await;

        keepalive_handle.abort();
        let _ = client.close_user_data_stream(&listen_key).await;

        match stream_result {
            Ok(()) => warn!("user data websocket ended normally, reconnecting"),
            Err(error) => warn!(?error, "user data websocket failed, reconnecting"),
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn consume_user_stream(
    ws_url: &str,
    risk_state: &SharedRiskState,
    trigger_tx: &mpsc::UnboundedSender<RiskTrigger>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match message? {
            Message::Text(text) => {
                let event = serde_json::from_str::<UserDataEvent>(&text)?;
                match event {
                    UserDataEvent::AccountUpdate {
                        event_time,
                        _transaction_time: _,
                        account,
                    } => {
                        let summary = AccountUpdateSummary::try_from(account)?;
                        apply_account_update(risk_state, summary, event_time).await;
                        let _ = trigger_tx.send(RiskTrigger::All);
                    }
                    UserDataEvent::OrderTradeUpdate {
                        _event_time: _,
                        _transaction_time: _,
                        order,
                    } => {
                        let order = OrderTradeUpdate::try_from(order)?;
                        let symbol = order.symbol.clone();
                        apply_order_update(risk_state, order).await;
                        let _ = trigger_tx.send(RiskTrigger::Symbol(symbol));
                    }
                    UserDataEvent::ListenKeyExpired {
                        _event_time: _,
                        listen_key,
                    } => {
                        warn!(%listen_key, "listenKey expired, reconnecting websocket");
                        break;
                    }
                }
            }
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await?;
            }
            Message::Close(frame) => {
                warn!(?frame, "user data websocket closed by peer");
                break;
            }
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    Ok(())
}

*/

/*

async fn apply_account_update(
    risk_state: &SharedRiskState,
    summary: AccountUpdateSummary,
    event_time: i64,
) {
    let mut state = risk_state.write().await;
    state.account.total_wallet_balance = summary.total_wallet_balance;
    state.account.total_unrealized_profit = summary.total_unrealized_profit;
    state.account.total_margin_balance = summary.total_margin_balance;
    state.account.available_balance = summary.available_balance;

    for position_update in summary.positions {
        let key = position_key(&position_update.symbol, position_update.position_side);
        let position = state.positions.entry(key).or_insert_with(|| FuturesPositionRisk {
            symbol: position_update.symbol.clone(),
            position_amt: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            mark_price: Decimal::ZERO,
            liquidation_price: Decimal::ZERO,
            unrealized_profit: Decimal::ZERO,
            notional: Decimal::ZERO,
            margin_type: position_update.margin_type.clone(),
            position_side: position_update.position_side,
        });

        position.symbol = position_update.symbol;
        position.position_amt = position_update.position_amt;
        position.entry_price = position_update.entry_price;
        position.unrealized_profit = position_update.unrealized_profit;
        position.margin_type = position_update.margin_type;
        position.position_side = position_update.position_side;
        if let Some(event_time) = Utc.timestamp_millis_opt(event_time).single() {
            let _ = event_time;
        }
    }
}

async fn apply_order_update(risk_state: &SharedRiskState, order: OrderTradeUpdate) {
    let mut state = risk_state.write().await;
    state.last_orders.insert(order.symbol.clone(), order);
}

fn evaluate_hedge_decision(
    config: &AppConfig,
    account: &FuturesAccountInformation,
    position: &FuturesPositionRisk,
    market: Option<&MarketSnapshot>,
    last_order: Option<&OrderTradeUpdate>,
    account_loss_ratio: Decimal,
) -> Option<HedgeDecision> {
    if config.monitor.position_mode == PositionMode::Hedge
        && position.position_side == BinancePositionSide::Both
    {
        warn!(
            symbol = %position.symbol,
            "Configured hedge mode but Binance returned BOTH position side; check account position mode"
        );
        return None;
    }

    let market_reference = if position.mark_price > Decimal::ZERO {
        position.mark_price
    } else {
        market.and_then(|state| state.reference_price).unwrap_or(Decimal::ZERO)
    };

    let position_notional = if position.notional.abs() > Decimal::ZERO {
        position.notional.abs()
    } else {
        position.position_amt.abs() * market_reference
    };

    if position_notional < config.risk.min_position_notional_usdt {
        return None;
    }

    let liquidation_buffer_ratio = liquidation_buffer_ratio(market_reference, position.liquidation_price);
    let symbol_loss_ratio = negative_pnl_ratio(position.unrealized_profit, position_notional);

    let mut triggers = Vec::new();
    if let Some(buffer_ratio) = liquidation_buffer_ratio {
        if buffer_ratio <= config.risk.min_liquidation_buffer_ratio {
            triggers.push(format!(
                "liq_buffer={} <= {}",
                buffer_ratio, config.risk.min_liquidation_buffer_ratio
            ));
        }
    }

    if symbol_loss_ratio >= config.risk.max_symbol_loss_ratio {
        triggers.push(format!(
            "symbol_loss_ratio={} >= {}",
            symbol_loss_ratio, config.risk.max_symbol_loss_ratio
        ));
    }

    if account_loss_ratio >= config.risk.max_account_loss_ratio {
        triggers.push(format!(
            "account_loss_ratio={} >= {}",
            account_loss_ratio, config.risk.max_account_loss_ratio
        ));
    }

    if triggers.is_empty() {
        return None;
    }

    if config.hedge.order_type != HedgeOrderType::Market {
        return None;
    }

    let mut quantity = position.position_amt.abs() * config.hedge.hedge_ratio;
    if let Some(max_hedge_notional_usdt) = config.hedge.max_hedge_notional_usdt {
        if market_reference > Decimal::ZERO {
            let max_qty = max_hedge_notional_usdt / market_reference;
            if quantity > max_qty {
                quantity = max_qty;
            }
        }
    }

    if quantity <= Decimal::ZERO {
        return None;
    }

    let (side, position_side) = match config.monitor.position_mode {
        PositionMode::OneWay => {
            if position.position_amt > Decimal::ZERO {
                (Side::Sell, None)
            } else {
                (Side::Buy, None)
            }
        }
        PositionMode::Hedge => {
            if position.position_amt > Decimal::ZERO {
                (Side::Sell, Some(BinancePositionSide::Short))
            } else {
                (Side::Buy, Some(BinancePositionSide::Long))
            }
        }
    };

    let mut reason = format!(
        "entry_price={}, wallet_balance={}, margin_balance={}, available_balance={}, {}",
        position.entry_price,
        account.total_wallet_balance,
        account.total_margin_balance,
        account.available_balance,
        triggers.join("; ")
    );

    if let Some(order) = last_order {
        reason.push_str(&format!(
            "; last_order_status={}, last_exec_type={}, last_order_side={}, last_order_position_side={}, last_order_qty={}, last_order_filled_qty={}, last_filled_price={}, last_order_client_id={}",
            order.order_status,
            order.execution_type,
            order.side,
            order.position_side.as_str(),
            order.quantity,
            order.filled_accumulated_quantity,
            order.last_filled_price,
            order.client_order_id,
        ));
    }

    Some(HedgeDecision {
        symbol: position.symbol.clone(),
        side,
        position_side,
        quantity,
        reason,
    })
}

fn negative_pnl_ratio(pnl: Decimal, base: Decimal) -> Decimal {
    if pnl >= Decimal::ZERO || base <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (-pnl) / base
    }
}

fn liquidation_buffer_ratio(mark_price: Decimal, liquidation_price: Decimal) -> Option<Decimal> {
    if mark_price <= Decimal::ZERO || liquidation_price <= Decimal::ZERO {
        return None;
    }

    Some((mark_price - liquidation_price).abs() / mark_price)
}

fn cooldown_active(
    cooldowns: &HashMap<String, Instant>,
    key: &str,
    cooldown_secs: u64,
) -> bool {
    cooldowns
        .get(key)
        .is_some_and(|last_hit| last_hit.elapsed() < Duration::from_secs(cooldown_secs))
}

fn position_key(symbol: &str, position_side: BinancePositionSide) -> String {
    format!("{}:{}", symbol, position_side.as_str())
}

fn validate_config(config: &AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !config.system.instruments.is_empty() {
        warn!(
            configured_instruments = config.system.instruments.len(),
            "system.instruments is ignored in dynamic all-symbol market mode"
        );
    }

    if config.monitor.margin_mode != MarginMode::Cross {
        warn!("Current implementation is tuned for cross margin supervision");
    }

    Ok(())
}

*/
