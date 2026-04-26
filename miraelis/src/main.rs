use barter::logging::init_logging_with_prefix;
use barter_integration::{
    error::SocketError,
    protocol::http::{public::PublicNoHeaders, rest::client::RestClient},
};
use std::{fs, path::Path, sync::Arc, time::Duration};

use tracing::*;

mod config;
mod execution;
mod quotation;
mod signal;
mod strategy;
use crate::quotation::writer::AsyncRollbackWriter;
use crate::quotation::FutureQuotation;
use execution::start_execution_tasks;
use signal::{SignalType, TelegramNotifier};
use strategy::frame::FrameSignalModule;
use strategy::huge_momentum::HugeMomentumSignalModule;
use strategy::StrategyEngine;

use crate::config::AppConfig;

async fn ensure_parent_dir(path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

async fn sync_exchange_info(
    rest_client: &RestClient<'_, PublicNoHeaders, config::BinancePublicParser>,
    output_file: &str,
) -> Result<config::FuturesExchangeInfo, Box<dyn std::error::Error + Send + Sync>> {
    let (exchange_info, metric) = rest_client
        .execute(config::FetchBinanceFuturesExchangeInfo)
        .await?;
    ensure_parent_dir(output_file).await?;

    let payload = serde_json::to_vec_pretty(&exchange_info)?;
    fs::write(output_file, payload)?;

    info!(
        symbols = exchange_info.symbols.len(),
        duration_ms = metric
            .fields
            .iter()
            .find(|field| field.key == "duration")
            .map(|field| format!("{:?}", field.value))
            .unwrap_or_else(|| "n/a".to_owned()),
        output_file = %output_file,
        "binance futures exchangeInfo synced"
    );

    Ok(exchange_info)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging_with_prefix("miraelis.log");

    info!(
        "
    "
    );

    let config_override = config::parse_config_arg()?;
    let (config, _config_path, _project_root) =
        config::load_runtime_config(config_override).await?;
    if !config.subscribe_all && config.symbols.is_empty() {
        return Err("config.symbols cannot be empty".into());
    }

    let rest_client = RestClient::new(
        config.binance_rest_base_url.clone(),
        PublicNoHeaders,
        config::BinancePublicParser,
    );

    let exchange_info = sync_exchange_info(&rest_client, &config.exchange_info_output).await?;
    let symbol_specs = config::build_symbol_specs(&config, &exchange_info)?;
    let telegram_notifier = build_telegram_notifier(&config);

    let sync_client = RestClient::new(
        config.binance_rest_base_url.clone(),
        PublicNoHeaders,
        config::BinancePublicParser,
    );
    let sync_output = config.exchange_info_output.clone();
    let sync_interval = config.exchange_info_sync_interval_secs;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(sync_interval.max(60)));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(error) = sync_exchange_info(&sync_client, &sync_output).await {
                error!(?error, "scheduled exchange info sync failed");
            }
        }
    });

    let mut engine = StrategyEngine::new();

    let (writer, _writer_task) = AsyncRollbackWriter::start(
        20480,
        config.market_data_shard_bytes,
        config.market_data_output_dir.as_str(),
    )
    .await;
    engine.ctx.exchange_info = config::build_strategy_exchange_info_map(&exchange_info);
    let execution_symbol_rules = config::build_execution_symbol_rules(&exchange_info);

    // -- 预热：下载历史 k 线，让策略（如 HugeMomentum）在直播流开始前就有足够数据 --
    let warmup_symbols: Vec<String> = symbol_specs
        .iter()
        .map(|s| s.symbol.to_ascii_uppercase())
        .collect();
    let warmup_rest_base = config.binance_rest_base_url.clone();
    let writer = Arc::new(writer);
    let writer_clone = writer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        FutureQuotation::warm_up_trade_windows(&warmup_rest_base, &warmup_symbols, writer_clone)
            .await;
    });

    let (order_tx, order_rx) = tokio::sync::mpsc::unbounded_channel();
    let (order_response_tx, mut order_response_rx) = tokio::sync::mpsc::unbounded_channel();

    let frame_module = if config.execution_cfg.enabled {
        FrameSignalModule::with_config(config.frame_cfg.clone()).with_order_tx(order_tx)
    } else {
        FrameSignalModule::with_config(config.frame_cfg.clone())
    };

    engine.register(Box::new(frame_module));
    engine.register(Box::new(HugeMomentumSignalModule::with_config(
        config.huge_momentum_cfg.clone(),
    )));
    engine.init_all()?;
    engine.start_all()?;

    if config.execution_cfg.enabled {
        start_execution_tasks(
            config.execution_cfg.clone(),
            config.binance_rest_base_url.clone(),
            order_rx,
            order_response_tx,
            execution_symbol_rules,
            telegram_notifier.clone(),
        )
        .await?;
    }

    if let Some(notifier) = &telegram_notifier {
        if let Err(error) = notifier
            .send_for_signal_type(
                SignalType::SystemStatus,
                &format!(
                    "miraelis-market-ingest started, symbols: {}",
                    symbol_specs.len()
                ),
            )
            .await
        {
            warn!(?error, "failed to send telegram startup notification");
        }
    }

    FutureQuotation::run_market_streams(
        symbol_specs,
        config.subscribe_all,
        config.debug_trade_window_symbol.as_deref(),
        config.debug_trade_window_interval_secs,
        &mut engine,
        writer.clone(),
        telegram_notifier.as_ref(),
        if config.execution_cfg.enabled {
            Some(&mut order_response_rx)
        } else {
            None
        },
    )
    .await?;
    Ok(())
}

fn build_telegram_notifier(config: &AppConfig) -> Option<TelegramNotifier> {
    let token = config.telegram_bot_token.as_ref()?.trim().to_owned();
    if token.is_empty() {
        return None;
    }

    let default_chat_id = config.telegram_chat_id.as_ref().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    });

    if default_chat_id.is_none() && !config.telegram_signal_chat_ids.has_any() {
        return None;
    }

    Some(TelegramNotifier::with_signal_routes(
        token,
        default_chat_id,
        config.telegram_signal_chat_ids.clone(),
    ))
}

