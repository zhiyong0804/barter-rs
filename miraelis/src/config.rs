use barter_integration::{
    error::SocketError,
    protocol::http::{HttpParser, rest::RestRequest},
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::execution::SymbolTradingRule;
use crate::quotation::config::SymbolSpec;
use crate::strategy::SymbolExchangeInfo;

use crate::execution::ExecutionConfig;
use crate::signal::SignalTypeChatIds;
use crate::strategy::frame::FrameModuleConfig;
use crate::strategy::huge_momentum::HugeMomentumModuleConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub binance_rest_base_url: String,
    pub exchange_info_output: String,
    pub market_data_output_dir: String,
    #[serde(default = "default_market_data_shard_bytes")]
    pub market_data_shard_bytes: u64,
    #[serde(default)]
    pub debug_trade_window_symbol: Option<String>,
    #[serde(default = "default_trade_window_debug_interval_secs")]
    pub debug_trade_window_interval_secs: u64,
    pub exchange_info_sync_interval_secs: u64,
    #[serde(default)]
    pub subscribe_all: bool,
    #[serde(default)]
    pub except: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub telegram_bot_token: Option<String>,
    #[serde(default)]
    pub telegram_chat_id: Option<String>,
    #[serde(default)]
    pub telegram_signal_chat_ids: SignalTypeChatIds,
    #[serde(default)]
    pub huge_momentum_cfg: HugeMomentumModuleConfig,
    #[serde(default)]
    pub frame_cfg: FrameModuleConfig,
    #[serde(default)]
    pub execution_cfg: ExecutionConfig,
}

fn default_market_data_shard_bytes() -> u64 {
    // 默认单个分片最大 200 MiB
    200 * 1024 * 1024
}

fn default_trade_window_debug_interval_secs() -> u64 {
    60 * 60
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuturesExchangeInfo {
    pub symbols: Vec<FuturesSymbol>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuturesSymbol {
    pub symbol: String,
    pub status: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "contractType")]
    pub contract_type: String,
    #[serde(rename = "pricePrecision", default)]
    pub price_precision: u32,
    #[serde(rename = "quantityPrecision", default)]
    pub quantity_precision: u32,
    #[serde(default)]
    pub filters: Vec<FuturesSymbolFilter>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuturesSymbolFilter {
    #[serde(rename = "filterType")]
    pub filter_type: String,
    #[serde(rename = "tickSize")]
    pub tick_size: Option<String>,
    #[serde(rename = "stepSize")]
    pub step_size: Option<String>,
    #[serde(rename = "minQty")]
    pub min_qty: Option<String>,
}

#[derive(Debug, Error)]
pub enum HttpClientError {
    #[error("socket error: {0}")]
    Socket(#[from] SocketError),
    #[error("api response parse error: {0}")]
    Parse(#[from] serde_json::Error),
}

pub struct BinancePublicParser;

impl HttpParser for BinancePublicParser {
    type ApiError = serde_json::Value;
    type OutputError = HttpClientError;

    fn parse_api_error(&self, status: StatusCode, api_error: Self::ApiError) -> Self::OutputError {
        HttpClientError::Socket(SocketError::HttpResponse(status, api_error.to_string()))
    }
}

pub struct FetchBinanceFuturesExchangeInfo;

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

pub fn build_symbol_specs(
    config: &AppConfig,
    exchange_info: &FuturesExchangeInfo,
) -> Result<Vec<SymbolSpec>, Box<dyn std::error::Error + Send + Sync>> {
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
        out.push(SymbolSpec {
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

pub fn build_strategy_exchange_info_map(
    exchange_info: &FuturesExchangeInfo,
) -> std::collections::HashMap<String, SymbolExchangeInfo> {
    let mut map = std::collections::HashMap::new();
    for symbol in &exchange_info.symbols {
        let tick_size = symbol
            .filters
            .iter()
            .find(|f| f.filter_type == "PRICE_FILTER")
            .and_then(|f| f.tick_size.as_deref())
            .and_then(parse_filter_float)
            .unwrap_or(0.01);

        let step_size = symbol
            .filters
            .iter()
            .find(|f| f.filter_type == "MARKET_LOT_SIZE")
            .or_else(|| symbol.filters.iter().find(|f| f.filter_type == "LOT_SIZE"))
            .and_then(|f| f.step_size.as_deref())
            .and_then(parse_filter_float)
            .unwrap_or(1.0);

        map.insert(
            symbol.symbol.to_ascii_lowercase(),
            SymbolExchangeInfo {
                symbol: symbol.symbol.clone(),
                price_precision: symbol.price_precision,
                qty_precision: symbol.quantity_precision,
                tick_size,
                step_size,
            },
        );
    }
    map
}

pub fn build_execution_symbol_rules(
    exchange_info: &FuturesExchangeInfo,
) -> std::collections::HashMap<String, SymbolTradingRule> {
    let mut map = std::collections::HashMap::new();
    for symbol in &exchange_info.symbols {
        let tick_size = symbol
            .filters
            .iter()
            .find(|f| f.filter_type == "PRICE_FILTER")
            .and_then(|f| f.tick_size.as_deref())
            .and_then(parse_filter_float)
            .unwrap_or(0.01);

        let qty_filter = symbol
            .filters
            .iter()
            .find(|f| f.filter_type == "MARKET_LOT_SIZE")
            .or_else(|| symbol.filters.iter().find(|f| f.filter_type == "LOT_SIZE"));

        let step_size = qty_filter
            .and_then(|f| f.step_size.as_deref())
            .and_then(parse_filter_float)
            .unwrap_or(1.0);

        let min_qty = qty_filter
            .and_then(|f| f.min_qty.as_deref())
            .and_then(parse_filter_float)
            .unwrap_or(0.0);

        map.insert(
            symbol.symbol.to_ascii_lowercase(),
            SymbolTradingRule {
                price_precision: symbol.price_precision as i32,
                qty_precision: symbol.quantity_precision as i32,
                tick_size,
                step_size,
                min_qty,
            },
        );
    }
    map
}

fn parse_filter_float(raw: &str) -> Option<f64> {
    let value = raw.parse::<f64>().ok()?;
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

pub async fn load_config(
    path: &str,
) -> Result<AppConfig, Box<dyn std::error::Error + Send + Sync>> {
    let content = tokio::fs::read_to_string(path).await?;
    let config = serde_json::from_str::<AppConfig>(&content)?;
    Ok(config)
}

pub async fn load_runtime_config(
    config_override: Option<PathBuf>,
) -> Result<(AppConfig, PathBuf, PathBuf), Box<dyn std::error::Error + Send + Sync>> {
    let (config_path, project_root) = resolve_config_path(config_override)?;
    let config_path_str = config_path
        .to_str()
        .ok_or_else(|| format!("invalid utf-8 config path: {}", config_path.display()))?;

    let mut config = load_config(config_path_str).await?;
    config.exchange_info_output = resolve_project_path(&project_root, &config.exchange_info_output)
        .to_string_lossy()
        .into_owned();
    config.market_data_output_dir =
        resolve_project_path(&project_root, &config.market_data_output_dir)
            .to_string_lossy()
            .into_owned();

    Ok((config, config_path, project_root))
}

pub fn parse_config_arg() -> Result<Option<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args_os().skip(1);
    let mut config_path = None;

    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "-c" {
            let value = args
                .next()
                .ok_or("missing value for --config/-c argument")?;
            config_path = Some(PathBuf::from(value));
            continue;
        }

        if let Some(value) = arg
            .to_str()
            .and_then(|value| value.strip_prefix("--config="))
        {
            config_path = Some(PathBuf::from(value));
            continue;
        }
    }

    Ok(config_path)
}

fn resolve_config_path(
    config_override: Option<PathBuf>,
) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(config_path) = config_override {
        let config_path = if config_path.is_absolute() {
            config_path
        } else {
            std::env::current_dir()?.join(config_path)
        };

        if !config_path.is_file() {
            return Err(format!("config file not found: {}", config_path.display()).into());
        }

        let project_root = config_path
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .unwrap_or_else(|| {
                config_path
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| PathBuf::from("."))
            });

        return Ok((config_path, project_root));
    }

    let mut search_roots = Vec::new();
    search_roots.push(std::env::current_dir()?);
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            search_roots.push(parent.to_path_buf());
        }
    }

    for root in search_roots {
        for ancestor in root.ancestors() {
            let candidate = ancestor.join("miraelis").join("config.json");
            if candidate.is_file() {
                return Ok((candidate, ancestor.to_path_buf()));
            }

            let candidate = ancestor.join("config.json");
            if candidate.is_file() && ancestor.file_name().is_some_and(|name| name == "miraelis") {
                let project_root = ancestor
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| ancestor.to_path_buf());
                return Ok((candidate, project_root));
            }
        }
    }

    Err("unable to locate miraelis/config.json from current directory or executable path".into())
}

fn resolve_project_path(project_root: &Path, value: &str) -> PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        project_root.join(path)
    }
}

