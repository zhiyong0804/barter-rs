use barter_data::{
    event::DataKind,
    exchange::binance::futures::BinanceFuturesUsd,
    streams::reconnect,
    streams::{consumer::MarketStreamResult, reconnect::stream::ReconnectingStream, Streams},
    subscription::{
        book::OrderBooksL1, candle::Candle, candle_1h::Candles1h, candle_1m::Candles1m,
        trade::PublicTrades,
    },
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use chrono::Utc;
use crossbeam_channel::{self, TryRecvError};
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::mpsc::UnboundedReceiver,
    time::{interval, sleep, Duration},
};
use tracing::{info, warn};

use crate::signal::{SignalType, TelegramNotifier};
use crate::strategy::frame::OrderResponse;
use crate::strategy::{MarketEvent, StrategyEngine};

pub mod trade_window;
use trade_window::{
    BestBidAskItem, QuotationKline, QuotationTicker, TradeItem, UhfKlineInterval, UhfTradeWindow,
};

#[derive(Debug, Clone)]
pub struct SymbolSpec {
    pub symbol: String,
    pub base: String,
    pub quote: String,
}

/// 每一个逻辑文件路径（如 `{dir}/trades.jsonl`）对应的分片状态。
/// 物理路径格式：`{dir}/{stem}_{date}_{idx:04}{ext}`
struct ShardState {
    /// 当前物理路径
    physical_path: String,
    /// 当前活跃日期字符串 ("YYYY-MM-DD")
    date_str: String,
    /// 当前分片序号（从 1 开始）
    shard_idx: u32,
    /// 当前分片已写入字节数
    current_bytes: u64,
    data_file: fs::File,
    rollback_file: fs::File,
}

/// 将逻辑路径 + 日期 + 序号 拼成物理路径。
/// 例：`/data/trades.jsonl` + `2026-04-19` + 2 => `/data/trades_2026-04-19_0002.jsonl`
fn make_shard_path(logical: &str, date_str: &str, shard_idx: u32) -> String {
    let path = PathBuf::from(logical);
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let dir = path
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    if dir.is_empty() {
        format!("{stem}_{date_str}_{shard_idx:04}{ext}")
    } else {
        format!("{dir}/{stem}_{date_str}_{shard_idx:04}{ext}")
    }
}

/// 为当天已有分片选择恢复写入的物理路径。
/// - 若当天不存在分片：返回 `_0001`
/// - 若当天最新分片未达到大小上限：继续写该分片
/// - 若当天最新分片已达到大小上限：创建下一个分片
fn select_resume_shard_path(logical: &str, date_str: &str, max_shard_bytes: u64) -> (String, u32) {
    let logical_path = PathBuf::from(logical);
    let stem = logical_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let ext = logical_path
        .extension()
        .map(|value| format!(".{}", value.to_string_lossy()))
        .unwrap_or_default();
    let prefix = format!("{stem}_{date_str}_");

    let search_dir = logical_path.parent().unwrap_or_else(|| Path::new("."));
    let mut latest_idx = 0u32;
    let mut latest_path = None::<String>;

    if let Ok(entries) = std::fs::read_dir(search_dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();

            if !file_name.starts_with(&prefix) || !file_name.ends_with(&ext) {
                continue;
            }

            let idx_part = &file_name[prefix.len()..file_name.len().saturating_sub(ext.len())];
            let Ok(idx) = idx_part.parse::<u32>() else {
                continue;
            };

            if idx > latest_idx {
                latest_idx = idx;
                latest_path = Some(entry.path().to_string_lossy().into_owned());
            }
        }
    }

    if let Some(existing_path) = latest_path {
        let current_size = std::fs::metadata(&existing_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        if max_shard_bytes == 0 || current_size < max_shard_bytes {
            return (existing_path, latest_idx);
        }

        let next_idx = latest_idx.saturating_add(1).max(1);
        return (make_shard_path(logical, date_str, next_idx), next_idx);
    }

    (make_shard_path(logical, date_str, 1), 1)
}

struct WriteRequest {
    /// 逻辑路径，不含日期/序号后缀
    path: String,
    line: Vec<u8>,
}

#[derive(Clone)]
pub struct AsyncRollbackWriter {
    tx: crossbeam_channel::Sender<WriteRequest>,
}

impl AsyncRollbackWriter {
    /// `max_shard_bytes`: 单个分片文件的最大字节数，0 表示不限大小（仅按日期分片）。
    pub fn start(buffer: usize, max_shard_bytes: u64) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, rx) = crossbeam_channel::bounded::<WriteRequest>(buffer);

        let task = tokio::spawn(async move {
            // key = logical path
            let mut shards: HashMap<String, ShardState> = HashMap::new();

            loop {
                let req = match rx.try_recv() {
                    Ok(req) => req,
                    Err(TryRecvError::Empty) => {
                        sleep(Duration::from_millis(2)).await;
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
                };

                write_sharded(&req, &mut shards, max_shard_bytes).await;
            }

            // 刷写剩余
            while let Ok(req) = rx.try_recv() {
                write_sharded(&req, &mut shards, max_shard_bytes).await;
            }
        });

        (Self { tx }, task)
    }

    fn append_json_line(
        &self,
        path: &str,
        value: &serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let line = serde_json::to_vec(value)?;
        self.tx
            .send(WriteRequest {
                path: path.to_owned(),
                line,
            })
            .map_err(|error| format!("async rollback writer channel closed: {error}").into())
    }
}

/// 将一条 line 写入正确的分片，自动处理日期轮转和大小分片。
async fn write_sharded(
    req: &WriteRequest,
    shards: &mut HashMap<String, ShardState>,
    max_shard_bytes: u64,
) {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let need_new_shard = match shards.get(&req.path) {
        None => true,
        Some(s) => {
            s.date_str != today || (max_shard_bytes > 0 && s.current_bytes >= max_shard_bytes)
        }
    };

    if need_new_shard {
        // 启动后的首次写入要恢复到当天最新分片，而不是总是回到 `_0001`。
        let (physical, new_date, new_idx) = match shards.get(&req.path) {
            None => {
                let (physical, idx) = select_resume_shard_path(&req.path, &today, max_shard_bytes);
                (physical, today.clone(), idx)
            }
            Some(s) if s.date_str != today => {
                let physical = make_shard_path(&req.path, &today, 1u32);
                (physical, today.clone(), 1u32)
            }
            Some(s) => {
                let idx = s.shard_idx + 1;
                let physical = make_shard_path(&req.path, &today, idx);
                (physical, today.clone(), idx)
            }
        };
        match open_rollback_pair(&physical).await {
            Ok((data_file, rollback_file)) => {
                let current_bytes = data_file.metadata().await.map(|m| m.len()).unwrap_or(0);
                info!(
                    logical = %req.path,
                    physical = %physical,
                    shard_idx = new_idx,
                    "opened new shard file"
                );
                shards.insert(
                    req.path.clone(),
                    ShardState {
                        physical_path: physical,
                        date_str: new_date,
                        shard_idx: new_idx,
                        current_bytes,
                        data_file,
                        rollback_file,
                    },
                );
            }
            Err(error) => {
                warn!(path = %req.path, ?error, "open shard rollback writer failed");
                return;
            }
        }
    }

    let Some(shard) = shards.get_mut(&req.path) else {
        return;
    };
    match append_with_rollback(&mut shard.data_file, &mut shard.rollback_file, &req.line).await {
        Ok(()) => {
            shard.current_bytes += req.line.len() as u64 + 1; // +1 for newline
        }
        Err(error) => {
            warn!(path = %shard.physical_path, ?error, "append with rollback failed");
        }
    }
}

async fn open_rollback_pair(
    path: &str,
) -> Result<(fs::File, fs::File), Box<dyn std::error::Error + Send + Sync>> {
    ensure_parent_dir(path).await?;

    let rollback_path = format!("{path}.rollback");
    ensure_parent_dir(&rollback_path).await?;

    let mut data_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(path)
        .await?;

    let mut rollback_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&rollback_path)
        .await?;

    let rollback_content = fs::read_to_string(&rollback_path).await.unwrap_or_default();
    if let Some(offset) = rollback_content
        .strip_prefix("B ")
        .and_then(|value| value.trim().parse::<u64>().ok())
    {
        data_file.set_len(offset).await?;
        data_file.seek(std::io::SeekFrom::End(0)).await?;
    }

    let current_len = data_file.metadata().await?.len();
    rollback_file.set_len(0).await?;
    rollback_file.seek(std::io::SeekFrom::Start(0)).await?;
    rollback_file
        .write_all(format!("C {current_len}\n").as_bytes())
        .await?;
    rollback_file.sync_data().await?;

    Ok((data_file, rollback_file))
}

async fn append_with_rollback(
    data_file: &mut fs::File,
    rollback_file: &mut fs::File,
    line: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start_offset = data_file.metadata().await?.len();

    rollback_file.set_len(0).await?;
    rollback_file.seek(std::io::SeekFrom::Start(0)).await?;
    rollback_file
        .write_all(format!("B {start_offset}\n").as_bytes())
        .await?;
    rollback_file.sync_data().await?;

    data_file.write_all(line).await?;
    data_file.write_all(b"\n").await?;
    data_file.sync_data().await?;

    let committed_offset = start_offset + line.len() as u64 + 1;
    rollback_file.set_len(0).await?;
    rollback_file.seek(std::io::SeekFrom::Start(0)).await?;
    rollback_file
        .write_all(format!("C {committed_offset}\n").as_bytes())
        .await?;
    rollback_file.sync_data().await?;

    Ok(())
}

pub async fn run_market_streams(
    output_dir: &str,
    symbols: Vec<SymbolSpec>,
    subscribe_all: bool,
    max_shard_bytes: u64,
    debug_trade_window_symbol: Option<&str>,
    debug_trade_window_interval_secs: u64,
    engine: &mut StrategyEngine,
    telegram_notifier: Option<&TelegramNotifier>,
    mut order_response_rx: Option<&mut UnboundedReceiver<OrderResponse>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    fs::create_dir_all(output_dir).await?;
    let (writer, writer_task) = AsyncRollbackWriter::start(2048, max_shard_bytes);
    let debug_trade_window_symbol = debug_trade_window_symbol
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    let mut debug_interval = interval(Duration::from_secs(
        debug_trade_window_interval_secs.max(60),
    ));
    debug_interval.tick().await;

    let route_count = if subscribe_all { 8 } else { 1 };
    let symbol_groups = split_symbols_into_routes(&symbols, route_count);

    info!(
        subscribe_all,
        symbols_total = symbols.len(),
        routes = symbol_groups.len(),
        "initialising market stream subscriptions"
    );

    let mut builder = Streams::builder_multi();
    for group in &symbol_groups {
        let trade_subs = group
            .iter()
            .map(|item| {
                (
                    format!("{}|trade", item.symbol),
                    BinanceFuturesUsd::default(),
                    item.base.clone(),
                    item.quote.clone(),
                    MarketDataInstrumentKind::Perpetual,
                    PublicTrades,
                )
            })
            .collect::<Vec<_>>();

        let candle_1m_subs = group
            .iter()
            .map(|item| {
                (
                    format!("{}|kline_1m", item.symbol),
                    BinanceFuturesUsd::default(),
                    item.base.clone(),
                    item.quote.clone(),
                    MarketDataInstrumentKind::Perpetual,
                    Candles1m,
                )
            })
            .collect::<Vec<_>>();

        let candle_1h_subs = group
            .iter()
            .map(|item| {
                (
                    format!("{}|kline_1h", item.symbol),
                    BinanceFuturesUsd::default(),
                    item.base.clone(),
                    item.quote.clone(),
                    MarketDataInstrumentKind::Perpetual,
                    Candles1h,
                )
            })
            .collect::<Vec<_>>();

        let l1_subs = group
            .iter()
            .map(|item| {
                (
                    format!("{}|book_ticker", item.symbol),
                    BinanceFuturesUsd::default(),
                    item.base.clone(),
                    item.quote.clone(),
                    MarketDataInstrumentKind::Perpetual,
                    OrderBooksL1,
                )
            })
            .collect::<Vec<_>>();

        builder = builder
            .add(Streams::<PublicTrades>::builder().subscribe(trade_subs))
            .add(Streams::<Candles1m>::builder().subscribe(candle_1m_subs))
            .add(Streams::<Candles1h>::builder().subscribe(candle_1h_subs))
            .add(Streams::<OrderBooksL1>::builder().subscribe(l1_subs));
    }

    let streams: Streams<MarketStreamResult<String, DataKind>> = builder.init().await?;

    info!(
        symbols = %symbols.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>().join(","),
        "binance futures market streams initialised"
    );

    let mut merged = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "market stream generated error"));

    if let Some(rx) = order_response_rx.as_mut() {
        loop {
            tokio::select! {
                _ = debug_interval.tick() => {
                    dump_trade_window_debug(engine, debug_trade_window_symbol.as_deref());
                }
                maybe_market = merged.next() => {
                    let Some(event) = maybe_market else { break; };
                    match event {
                        reconnect::Event::Reconnecting(exchange) => {
                            warn!(exchange = %exchange, "market stream reconnecting");
                            if let Some(notifier) = telegram_notifier {
                                let text = format!("market stream reconnecting: {}", exchange);
                                if let Err(error) = notifier
                                    .send_for_signal_type(SignalType::SystemStatus, &text)
                                    .await
                                {
                                    warn!(?error, "failed to send telegram reconnect notification");
                                }
                            }
                        }
                        reconnect::Event::Item(event) => {
                            apply_event_to_strategy_context(engine, &event);
                            persist_event(output_dir, event, &writer).await?;
                        }
                    }
                }
                maybe_order_resp = rx.recv() => {
                    if let Some(order_resp) = maybe_order_resp {
                        engine.dispatch_order_response(order_resp);
                    }
                }
            }
        }
    } else {
        loop {
            tokio::select! {
                _ = debug_interval.tick() => {
                    dump_trade_window_debug(engine, debug_trade_window_symbol.as_deref());
                }
                maybe_market = merged.next() => {
                    let Some(event) = maybe_market else { break; };
                    match event {
                        reconnect::Event::Reconnecting(exchange) => {
                            warn!(exchange = %exchange, "market stream reconnecting");
                            if let Some(notifier) = telegram_notifier {
                                let text = format!("market stream reconnecting: {}", exchange);
                                if let Err(error) = notifier
                                    .send_for_signal_type(SignalType::SystemStatus, &text)
                                    .await
                                {
                                    warn!(?error, "failed to send telegram reconnect notification");
                                }
                            }
                        }
                        reconnect::Event::Item(event) => {
                            apply_event_to_strategy_context(engine, &event);
                            persist_event(output_dir, event, &writer).await?;
                        }
                    }
                }
            }
        }
    }

    drop(writer);
    if let Err(error) = writer_task.await {
        warn!(?error, "async rollback writer task join failed");
    }

    Ok(())
}

fn dump_trade_window_debug(engine: &StrategyEngine, symbol: Option<&str>) {
    let Some(symbol) = symbol else {
        return;
    };

    let Some(window) = engine.ctx.trades.get(symbol) else {
        info!(
            symbol,
            "trade_window debug dump skipped: symbol cache not found"
        );
        return;
    };

    match serde_json::to_string_pretty(window) {
        Ok(payload) => {
            info!(
                symbol,
                payload = %payload,
                "trade_window hourly debug dump"
            );
        }
        Err(error) => {
            warn!(
                symbol,
                ?error,
                "trade_window debug dump serialization failed"
            );
        }
    }
}

fn split_symbols_into_routes(symbols: &[SymbolSpec], route_count: usize) -> Vec<Vec<SymbolSpec>> {
    let route_count = route_count.max(1);
    if symbols.is_empty() {
        return Vec::new();
    }

    let mut routes = vec![Vec::new(); route_count.min(symbols.len())];
    let routes_len = routes.len();
    for (index, symbol) in symbols.iter().cloned().enumerate() {
        routes[index % routes_len].push(symbol);
    }

    routes
}

async fn persist_event(
    output_dir: &str,
    event: barter_data::event::MarketEvent<String, DataKind>,
    writer: &AsyncRollbackWriter,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = Utc::now().to_rfc3339();
    let symbol = extract_symbol(&event.instrument);

    match event.kind {
        DataKind::Trade(trade) => {
            let payload = serde_json::json!({
                "time": now,
                "instrument": event.instrument,
                "symbol": symbol,
                "exchange": event.exchange,
                "kind": "trade",
                "data": trade,
            });
            writer.append_json_line(&format!("{output_dir}/trades.jsonl"), &payload)?;
        }
        DataKind::Candle(candle) => {
            if candle.is_final {
                persist_candle(
                    output_dir,
                    &event.instrument,
                    event.exchange,
                    candle,
                    &now,
                    writer,
                )
                .await?;
            }
        }
        _ => {}
    }

    Ok(())
}

pub async fn persist_candle(
    output_dir: &str,
    instrument: &str,
    exchange: barter_instrument::exchange::ExchangeId,
    candle: Candle,
    now: &str,
    writer: &AsyncRollbackWriter,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (kind, file_name) = if instrument.ends_with("|kline_1m") {
        ("kline_1m", "candles_1m.jsonl")
    } else if instrument.ends_with("|kline_1h") {
        ("kline_1h", "candles_1h.jsonl")
    } else {
        ("candle", "candles.jsonl")
    };

    let payload = serde_json::json!({
        "time": now,
        "instrument": instrument,
        "symbol": extract_symbol(instrument),
        "exchange": exchange,
        "kind": kind,
        "data": candle,
    });
    writer.append_json_line(&format!("{output_dir}/{file_name}"), &payload)?;
    Ok(())
}

fn apply_event_to_strategy_context(
    engine: &mut StrategyEngine,
    event: &barter_data::event::MarketEvent<String, DataKind>,
) {
    let symbol = extract_symbol(&event.instrument);

    match &event.kind {
        DataKind::Trade(trade) => {
            let event_ms = event.time_exchange.timestamp_millis().max(0) as u64;
            let trade_id = trade.id.parse::<u64>().unwrap_or(0);
            engine.dispatch(MarketEvent::Trade(TradeItem {
                id: trade_id,
                symbol,
                price: trade.price,
                qty: trade.amount,
                second: event_ms / 1000,
                event_time: event_ms,
                transact_time: event_ms,
                is_buyer_maker: matches!(trade.side, barter_instrument::Side::Sell),
            }));
        }
        DataKind::Candle(candle) => {
            let interval = if event.instrument.ends_with("|kline_1h") {
                UhfKlineInterval::H1
            } else {
                UhfKlineInterval::M1
            };

            let close_ms = candle.close_time.timestamp_millis().max(0) as u64;
            let span_ms = match interval {
                UhfKlineInterval::M1 => 60_000,
                UhfKlineInterval::H1 => 3_600_000,
            };

            let kline = QuotationKline {
                symbol,
                event_time: close_ms,
                start_timestamp: close_ms.saturating_sub(span_ms),
                end_timestamp: close_ms,
                interval,
                start_trade_id: 0,
                end_trade_id: candle.trade_count,
                open: candle.open,
                close: candle.close,
                high: candle.high,
                low: candle.low,
                volume: candle.volume,
                bid_volume: candle.bid_volume,
                is_final: candle.is_final,
            };

            match interval {
                UhfKlineInterval::M1 => engine.dispatch(MarketEvent::Candle1m(kline.clone())),
                UhfKlineInterval::H1 => {
                    let window = engine
                        .ctx
                        .trades
                        .entry(kline.symbol.clone())
                        .or_insert_with(|| UhfTradeWindow::new(kline.symbol.clone()));
                    window.update_kline(kline.clone());
                }
            }

            let ticker = QuotationTicker {
                symbol: extract_symbol(&event.instrument),
                event_time: close_ms,
                tf_open_price: candle.open,
                tf_high_price: candle.high,
                tf_low_price: candle.low,
                tf_total_qty: candle.volume,
                tf_total_value: candle.volume * candle.close,
            };
            engine.dispatch(MarketEvent::Ticker(ticker));
        }
        DataKind::OrderBookL1(l1) => {
            let event_ms = event.time_exchange.timestamp_millis().max(0) as u64;

            let best_bid_price = l1
                .best_bid
                .map(|level| level.price.to_string().parse::<f64>().unwrap_or(0.0))
                .unwrap_or(0.0);
            let best_bid_qty = l1
                .best_bid
                .map(|level| level.amount.to_string().parse::<f64>().unwrap_or(0.0))
                .unwrap_or(0.0);
            let best_ask_price = l1
                .best_ask
                .map(|level| level.price.to_string().parse::<f64>().unwrap_or(0.0))
                .unwrap_or(0.0);
            let best_ask_qty = l1
                .best_ask
                .map(|level| level.amount.to_string().parse::<f64>().unwrap_or(0.0))
                .unwrap_or(0.0);

            engine.dispatch(MarketEvent::BestBidAsk(BestBidAskItem {
                symbol,
                best_bid_price,
                best_ask_price,
                best_bid_qty,
                best_ask_qty,
                event_time: event_ms,
                transact_time: event_ms,
            }));
        }
        _ => {}
    }
}

fn extract_symbol(instrument: &str) -> String {
    instrument
        .split('|')
        .next()
        .unwrap_or(instrument)
        .to_owned()
}

async fn ensure_parent_dir(path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = PathBuf::from(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    Ok(())
}
