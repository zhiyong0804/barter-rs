use barter_integration::{
    error::SocketError,
    protocol::http::{
        public::PublicNoHeaders,
        rest::{client::RestClient, RestRequest},
        HttpParser,
    },
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, path::Path, time::Duration};
use thiserror::Error;
use tokio::fs;
use tracing::{error, info, warn};

mod quotation;
mod signal;
mod strategy;
use signal::{SignalType, SignalTypeChatIds, TelegramNotifier};
use strategy::huge_momentum::{HugeMomentumModuleConfig, HugeMomentumSignalModule};
use strategy::StrategyEngine;

#[derive(Debug, Clone, Deserialize)]
struct AppConfig {
    binance_rest_base_url: String,
    exchange_info_output: String,
    market_data_output_dir: String,
    exchange_info_sync_interval_secs: u64,
    #[serde(default)]
    subscribe_all: bool,
    #[serde(default)]
    except: Vec<String>,
    #[serde(default)]
    symbols: Vec<String>,
    #[serde(default)]
    telegram_bot_token: Option<String>,
    #[serde(default)]
    telegram_chat_id: Option<String>,
    #[serde(default)]
    telegram_signal_chat_ids: SignalTypeChatIds,
    #[serde(default)]
    huge_momentum_cfg: HugeMomentumModuleConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FuturesExchangeInfo {
    symbols: Vec<FuturesSymbol>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FuturesSymbol {
    symbol: String,
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "contractType")]
    contract_type: String,
}

#[derive(Debug, Error)]
enum HttpClientError {
    #[error("socket error: {0}")]
    Socket(#[from] SocketError),
    #[error("api response parse error: {0}")]
    Parse(#[from] serde_json::Error),
}

struct BinancePublicParser;

impl HttpParser for BinancePublicParser {
    type ApiError = serde_json::Value;
    type OutputError = HttpClientError;

    fn parse_api_error(&self, status: StatusCode, api_error: Self::ApiError) -> Self::OutputError {
        HttpClientError::Socket(SocketError::HttpResponse(status, api_error.to_string()))
    }
}

struct FetchBinanceFuturesExchangeInfo;

impl RestRequest for FetchBinanceFuturesExchangeInfo {
    type Response = FuturesExchangeInfo;
    type QueryParams = ();
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/fapi/v1/exchangeInfo")
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let config = load_config("miraelis/config.json").await?;
    if !config.subscribe_all && config.symbols.is_empty() {
        return Err("config.symbols cannot be empty".into());
    }

    let rest_client = RestClient::new(
        config.binance_rest_base_url.clone(),
        PublicNoHeaders,
        BinancePublicParser,
    );

    let exchange_info = sync_exchange_info(&rest_client, &config.exchange_info_output).await?;
    let symbol_specs = build_symbol_specs(&config, &exchange_info)?;
    let telegram_notifier = build_telegram_notifier(&config);

    let sync_client = RestClient::new(
        config.binance_rest_base_url.clone(),
        PublicNoHeaders,
        BinancePublicParser,
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
    engine.register(Box::new(HugeMomentumSignalModule::with_config(
        config.huge_momentum_cfg.clone(),
    )));
    engine.init_all()?;
    engine.start_all()?;

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

    quotation::run_market_streams(
        &config.market_data_output_dir,
        symbol_specs,
        &mut engine,
        telegram_notifier.as_ref(),
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

fn build_symbol_specs(
    config: &AppConfig,
    exchange_info: &FuturesExchangeInfo,
) -> Result<Vec<quotation::SymbolSpec>, Box<dyn std::error::Error + Send + Sync>> {
    let except_set = config
        .except
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();

    let selected = if config.subscribe_all {
        if !config.symbols.is_empty() {
            info!(
                symbols_configured = config.symbols.len(),
                "subscribe_all=true, symbols configuration will be ignored"
            );
        }

        exchange_info
            .symbols
            .iter()
            .filter(|symbol| symbol.status == "TRADING")
            .filter(|symbol| !except_set.contains(&symbol.symbol.to_ascii_lowercase()))
            .collect::<Vec<_>>()
    } else {
        let mut selected = Vec::with_capacity(config.symbols.len());
        for symbol in &config.symbols {
            let maybe = exchange_info
                .symbols
                .iter()
                .find(|item| item.symbol.eq_ignore_ascii_case(symbol));

            let Some(spec) = maybe else {
                return Err(
                    format!("symbol not found in Binance Futures exchangeInfo: {symbol}").into(),
                );
            };

            if except_set.contains(&spec.symbol.to_ascii_lowercase()) {
                continue;
            }

            if spec.status != "TRADING" {
                warn!(symbol = %spec.symbol, status = %spec.status, "symbol is not TRADING");
            }

            selected.push(spec);
        }
        selected
    };

    let mut out = Vec::with_capacity(selected.len());
    for spec in selected {
        out.push(quotation::SymbolSpec {
            symbol: spec.symbol.clone(),
            base: spec.base_asset.to_lowercase(),
            quote: spec.quote_asset.to_lowercase(),
        });
    }

    if out.is_empty() {
        return Err("symbol selection resulted in an empty set, please check subscribe_all/symbols/except configuration".into());
    }

    info!(
        subscribe_all = config.subscribe_all,
        symbols_selected = out.len(),
        symbols_excluded = config.except.len(),
        "symbol selection resolved"
    );

    Ok(out)
}

async fn ensure_parent_dir(path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    Ok(())
}

async fn sync_exchange_info(
    rest_client: &RestClient<'_, PublicNoHeaders, BinancePublicParser>,
    output_file: &str,
) -> Result<FuturesExchangeInfo, Box<dyn std::error::Error + Send + Sync>> {
    let (exchange_info, metric) = rest_client.execute(FetchBinanceFuturesExchangeInfo).await?;
    ensure_parent_dir(output_file).await?;

    let payload = serde_json::to_vec_pretty(&exchange_info)?;
    fs::write(output_file, payload).await?;

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

async fn load_config(path: &str) -> Result<AppConfig, Box<dyn std::error::Error + Send + Sync>> {
    let content = fs::read_to_string(path).await?;
    let config = serde_json::from_str::<AppConfig>(&content)?;
    Ok(config)
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .json()
        .init();
}
