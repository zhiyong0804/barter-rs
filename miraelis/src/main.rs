use barter::logging::init_logging_with_prefix;
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
use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{fs, sync::Semaphore, time::sleep};
use tracing::{error, info, warn};

mod execution;
mod quotation;
mod signal;
mod strategy;
use execution::{start_execution_tasks, ExecutionConfig};
use signal::{SignalType, SignalTypeChatIds, TelegramNotifier};
use std::collections::HashMap;
use strategy::frame::{FrameModuleConfig, FrameSignalModule};
use strategy::huge_momentum::{HugeMomentumModuleConfig, HugeMomentumSignalModule};
use strategy::{StrategyEngine, SymbolExchangeInfo};

#[derive(Debug, Clone, Deserialize)]
struct AppConfig {
    binance_rest_base_url: String,
    exchange_info_output: String,
    market_data_output_dir: String,
    #[serde(default = "default_market_data_shard_bytes")]
    market_data_shard_bytes: u64,
    #[serde(default)]
    debug_trade_window_symbol: Option<String>,
    #[serde(default = "default_trade_window_debug_interval_secs")]
    debug_trade_window_interval_secs: u64,
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
    #[serde(default)]
    frame_cfg: FrameModuleConfig,
    #[serde(default)]
    execution_cfg: ExecutionConfig,
}

fn default_market_data_shard_bytes() -> u64 {
    // 默认单个分片最大 200 MiB
    200 * 1024 * 1024
}

fn default_trade_window_debug_interval_secs() -> u64 {
    60 * 60
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
    #[serde(rename = "pricePrecision", default)]
    price_precision: u32,
    #[serde(rename = "quantityPrecision", default)]
    quantity_precision: u32,
    #[serde(default)]
    filters: Vec<FuturesSymbolFilter>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FuturesSymbolFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
    #[serde(rename = "minQty")]
    min_qty: Option<String>,
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
    init_logging_with_prefix("miraelis.log");

    let config_override = parse_config_arg()?;
    let (config, _config_path, _project_root) = load_runtime_config(config_override).await?;
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

    engine.ctx.exchange_info = build_strategy_exchange_info_map(&exchange_info);
    let execution_symbol_rules = build_execution_symbol_rules(&exchange_info);

    // -- 预热：下载历史 k 线，让策略（如 HugeMomentum）在直播流开始前就有足够数据 --
    let warmup_symbols: Vec<String> = symbol_specs
        .iter()
        .map(|s| s.symbol.to_ascii_uppercase())
        .collect();
    warm_up_trade_windows(
        &config.binance_rest_base_url,
        &warmup_symbols,
        &config.market_data_output_dir,
        &mut engine,
    )
    .await;

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

    quotation::run_market_streams(
        &config.market_data_output_dir,
        symbol_specs,
        config.subscribe_all,
        config.market_data_shard_bytes,
        config.debug_trade_window_symbol.as_deref(),
        config.debug_trade_window_interval_secs,
        &mut engine,
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

async fn load_runtime_config(
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

fn parse_config_arg() -> Result<Option<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
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

fn build_strategy_exchange_info_map(
    exchange_info: &FuturesExchangeInfo,
) -> HashMap<String, SymbolExchangeInfo> {
    let mut map = HashMap::new();
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

fn build_execution_symbol_rules(
    exchange_info: &FuturesExchangeInfo,
) -> HashMap<String, execution::SymbolTradingRule> {
    let mut map = HashMap::new();
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
            execution::SymbolTradingRule {
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

/// 令牌桶限速器。
/// Binance Futures REST 上限 2400 weight/min，klines weight=1，
/// 保守取 35 req/s（留约 12% 余量）。
struct RateLimiter {
    interval: Duration,
    last: tokio::sync::Mutex<tokio::time::Instant>,
}

impl RateLimiter {
    fn new(rate_per_sec: u32) -> Arc<Self> {
        Arc::new(Self {
            interval: Duration::from_nanos(1_000_000_000 / rate_per_sec.max(1) as u64),
            last: tokio::sync::Mutex::new(tokio::time::Instant::now()),
        })
    }

    async fn acquire(&self) {
        let mut last = self.last.lock().await;
        let now = tokio::time::Instant::now();
        let elapsed = now.duration_since(*last);
        if elapsed < self.interval {
            sleep(self.interval - elapsed).await;
        }
        *last = tokio::time::Instant::now();
    }
}

async fn warm_up_trade_windows(
    rest_base: &str,
    symbols: &[String],
    output_dir: &str,
    engine: &mut strategy::StrategyEngine,
) {
    use barter_data::subscription::candle::Candle;
    use chrono::Utc;
    use quotation::trade_window::{QuotationKline, UhfKlineInterval, UhfTradeWindow};
    use quotation::{persist_candle, AsyncRollbackWriter};

    #[derive(Debug)]
    struct RawKline {
        open_time: u64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        close_time: u64,
    }

    fn parse_klines(json: &serde_json::Value) -> Vec<RawKline> {
        let Some(arr) = json.as_array() else {
            return Vec::new();
        };
        arr.iter()
            .filter_map(|row| {
                let row = row.as_array()?;
                let open_time = row.first()?.as_u64()?;
                let open = row.get(1)?.as_str()?.parse::<f64>().ok()?;
                let high = row.get(2)?.as_str()?.parse::<f64>().ok()?;
                let low = row.get(3)?.as_str()?.parse::<f64>().ok()?;
                let close = row.get(4)?.as_str()?.parse::<f64>().ok()?;
                let volume = row.get(5)?.as_str()?.parse::<f64>().ok()?;
                let close_time = row.get(6)?.as_u64()?;
                Some(RawKline {
                    open_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    close_time,
                })
            })
            .collect()
    }

    // 带退避重试的单次请求：遇到 429/418 读取 Retry-After，其余错误指数退避
    async fn fetch_klines(
        client: &reqwest::Client,
        rl: &RateLimiter,
        url: &str,
    ) -> Option<serde_json::Value> {
        const MAX_RETRIES: u32 = 3;
        let mut backoff = Duration::from_secs(2);
        for attempt in 0..=MAX_RETRIES {
            rl.acquire().await;
            let resp = match client.get(url).send().await {
                Ok(r) => r,
                Err(err) => {
                    warn!(?err, url, attempt, "warm_up: request error");
                    sleep(backoff).await;
                    backoff *= 2;
                    continue;
                }
            };
            let status = resp.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 418 {
                let wait = resp
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_secs)
                    .unwrap_or(Duration::from_secs(60));
                warn!(
                    url,
                    secs = wait.as_secs(),
                    "warm_up: rate limited, sleeping"
                );
                sleep(wait).await;
                backoff = Duration::from_secs(2);
                continue;
            }
            if !status.is_success() {
                warn!(url, status = status.as_u16(), attempt, "warm_up: non-2xx");
                if attempt < MAX_RETRIES {
                    sleep(backoff).await;
                    backoff *= 2;
                }
                continue;
            }
            match resp.json::<serde_json::Value>().await {
                Ok(json) => return Some(json),
                Err(err) => {
                    warn!(?err, url, "warm_up: JSON parse error");
                    return None;
                }
            }
        }
        None
    }

    // Binance Futures klines weight=1，保守 35 req/s；最大并发 10
    let rate_limiter = RateLimiter::new(35);
    let sem = Arc::new(Semaphore::new(10));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap_or_default();
    let base = rest_base.trim_end_matches('/').to_owned();

    info!(
        symbols = symbols.len(),
        rate_per_sec = 35,
        "warm_up: fetching historical 1h + 1m klines"
    );

    let mut tasks = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let symbol = symbol.clone();
        let client = client.clone();
        let base = base.clone();
        let rl = rate_limiter.clone();
        let sem = sem.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.ok()?;
            // 1h k线 — 最近 24 根，初始化 hours_window / tf_total_value
            let url_1h = format!("{base}/fapi/v1/klines?symbol={symbol}&interval=1h&limit=24");
            let json_1h = fetch_klines(&client, &rl, &url_1h).await?;
            let klines_1h = parse_klines(&json_1h);
            // 1m k线 — 最近 240 根 (4h)，满足 required_minutes=120 + vol_base 余量
            let url_1m = format!("{base}/fapi/v1/klines?symbol={symbol}&interval=1m&limit=240");
            let json_1m = fetch_klines(&client, &rl, &url_1m).await?;
            let klines_1m = parse_klines(&json_1m);
            Some((symbol, klines_1h, klines_1m))
        }));
    }

    let (writer, writer_task) = AsyncRollbackWriter::start(2048, 0);

    let mut loaded = 0usize;
    let mut failed = 0usize;
    for task in tasks {
        let Some(Some((symbol, klines_1h, klines_1m))) = task.await.ok() else {
            failed += 1;
            continue;
        };
        let klines_1h_count = klines_1h.len();
        let klines_1m_count = klines_1m.len();
        let sym_lower = symbol.to_ascii_lowercase();
        let window = engine
            .ctx
            .trades
            .entry(sym_lower.clone())
            .or_insert_with(|| UhfTradeWindow::new(sym_lower.clone()));

        let now = Utc::now().to_rfc3339();
        let exchange = barter_instrument::exchange::ExchangeId::BinanceFuturesUsd;

        for k in &klines_1h {
            window.update_kline(QuotationKline {
                symbol: sym_lower.clone(),
                event_time: k.close_time,
                start_timestamp: k.open_time,
                end_timestamp: k.close_time,
                interval: UhfKlineInterval::H1,
                start_trade_id: 0,
                end_trade_id: 0,
                open: k.open,
                close: k.close,
                high: k.high,
                low: k.low,
                volume: k.volume,
                bid_volume: 0.0,
                is_final: true,
            });

            let candle = Candle {
                close_time: chrono::DateTime::<Utc>::from_timestamp(
                    (k.close_time / 1000) as i64,
                    0,
                )
                .unwrap_or_else(|| Utc::now()),
                open: k.open,
                close: k.close,
                high: k.high,
                low: k.low,
                volume: k.volume,
                bid_volume: 0.0,
                bid_quote_asset_volume: 0.0,
                trade_count: 0,
                is_final: true,
            };
            let instrument = format!("{}|kline_1h", symbol);
            if let Err(e) =
                persist_candle(output_dir, &instrument, exchange, candle, &now, &writer).await
            {
                warn!(instrument, ?e, "failed to persist historical 1h kline");
            }
        }
        for k in &klines_1m {
            window.update_kline(QuotationKline {
                symbol: sym_lower.clone(),
                event_time: k.close_time,
                start_timestamp: k.open_time,
                end_timestamp: k.close_time,
                interval: UhfKlineInterval::M1,
                start_trade_id: 0,
                end_trade_id: 0,
                open: k.open,
                close: k.close,
                high: k.high,
                low: k.low,
                volume: k.volume,
                bid_volume: 0.0,
                is_final: true,
            });

            let candle = Candle {
                close_time: chrono::DateTime::<Utc>::from_timestamp(
                    (k.close_time / 1000) as i64,
                    0,
                )
                .unwrap_or_else(|| Utc::now()),
                open: k.open,
                close: k.close,
                high: k.high,
                low: k.low,
                volume: k.volume,
                bid_volume: 0.0,
                bid_quote_asset_volume: 0.0,
                trade_count: 0,
                is_final: true,
            };
            let instrument = format!("{}|kline_1m", symbol);
            if let Err(e) =
                persist_candle(output_dir, &instrument, exchange, candle, &now, &writer).await
            {
                warn!(instrument, ?e, "failed to persist historical 1m kline");
            }
        }

        info!(
            symbol = %symbol,
            klines_1h = klines_1h_count,
            klines_1m = klines_1m_count,
            "warm_up: symbol loaded"
        );

        loaded += 1;
    }

    drop(writer);
    if let Err(e) = writer_task.await {
        warn!(?e, "warm_up writer task failed");
    }

    info!(
        loaded,
        failed, "warm_up: historical klines loaded into trade windows and persisted"
    );
}
