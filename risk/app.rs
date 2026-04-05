use crate::{
    binance::{BinanceCredentials, BinanceFuturesRestClient},
    config::{load_config, AppConfig, MarginMode},
    market_stream::{run_all_book_ticker_stream, run_all_mark_price_stream},
    state::{position_key, RiskState, RiskTrigger, SharedMarketState, SharedRiskState},
    supervisor::PositionSupervisor,
    user_data_stream::run_user_data_stream,
};
use barter::logging::init_logging;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            .map(|position| {
                (
                    position_key(&position.symbol, position.position_side),
                    position,
                )
            })
            .collect(),
        last_orders: HashMap::new(),
    }));
    let market_state: SharedMarketState = Arc::new(RwLock::new(HashMap::new()));
    let (trigger_tx, trigger_rx) = mpsc::unbounded_channel();

    let ws_base_url = config.binance.ws_base_url.clone();
    let keepalive_secs = config.binance.listen_key_keepalive_secs;

    let book_ticker_task = {
        let market_state = Arc::clone(&market_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = ws_base_url.clone();
        tokio::spawn(async move {
            if let Err(error) =
                run_all_book_ticker_stream(&ws_base_url, market_state, trigger_tx).await
            {
                error!(?error, "all-market book ticker task failed");
            }
        })
    };

    let mark_price_task = {
        let market_state = Arc::clone(&market_state);
        let risk_state = Arc::clone(&risk_state);
        let trigger_tx = trigger_tx.clone();
        let ws_base_url = ws_base_url.clone();
        tokio::spawn(async move {
            if let Err(error) =
                run_all_mark_price_stream(&ws_base_url, market_state, risk_state, trigger_tx).await
            {
                error!(?error, "all-market mark price task failed");
            }
        })
    };

    let user_stream_task = {
        let client = client.clone();
        let risk_state = Arc::clone(&risk_state);
        let trigger_tx = trigger_tx.clone();
        tokio::spawn(async move {
            if let Err(error) =
                run_user_data_stream(client, ws_base_url, keepalive_secs, risk_state, trigger_tx)
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
