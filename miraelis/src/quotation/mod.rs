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
use std::{collections::HashMap, path::PathBuf};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::mpsc::UnboundedReceiver,
    time::{sleep, Duration},
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

struct WriteRequest {
    path: String,
    line: Vec<u8>,
}

#[derive(Clone)]
struct AsyncRollbackWriter {
    tx: crossbeam_channel::Sender<WriteRequest>,
}

impl AsyncRollbackWriter {
    fn start(buffer: usize) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, rx) = crossbeam_channel::bounded::<WriteRequest>(buffer);

        let task = tokio::spawn(async move {
            let mut files: HashMap<String, (fs::File, fs::File)> = HashMap::new();

            loop {
                let req = match rx.try_recv() {
                    Ok(req) => req,
                    Err(TryRecvError::Empty) => {
                        sleep(Duration::from_millis(2)).await;
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
                };

                let opened = if let Some(opened) = files.get_mut(&req.path) {
                    Some(opened)
                } else {
                    match open_rollback_pair(&req.path).await {
                        Ok(pair) => {
                            files.insert(req.path.clone(), pair);
                            files.get_mut(&req.path)
                        }
                        Err(error) => {
                            warn!(path = %req.path, ?error, "open rollback writer failed");
                            None
                        }
                    }
                };

                let Some((data_file, rollback_file)) = opened else {
                    continue;
                };

                if let Err(error) = append_with_rollback(data_file, rollback_file, &req.line).await
                {
                    warn!(path = %req.path, ?error, "append with rollback failed");
                }
            }

            while let Ok(req) = rx.try_recv() {
                let opened = if let Some(opened) = files.get_mut(&req.path) {
                    Some(opened)
                } else {
                    match open_rollback_pair(&req.path).await {
                        Ok(pair) => {
                            files.insert(req.path.clone(), pair);
                            files.get_mut(&req.path)
                        }
                        Err(error) => {
                            warn!(path = %req.path, ?error, "open rollback writer failed");
                            None
                        }
                    }
                };

                let Some((data_file, rollback_file)) = opened else {
                    continue;
                };

                if let Err(error) = append_with_rollback(data_file, rollback_file, &req.line).await
                {
                    warn!(path = %req.path, ?error, "append with rollback failed");
                }
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
    engine: &mut StrategyEngine,
    telegram_notifier: Option<&TelegramNotifier>,
    mut order_response_rx: Option<&mut UnboundedReceiver<OrderResponse>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    fs::create_dir_all(output_dir).await?;
    let (writer, writer_task) = AsyncRollbackWriter::start(2048);

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
                            persist_event(output_dir, event, &engine.ctx.trades, &writer).await?;
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
        while let Some(event) = merged.next().await {
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
                    persist_event(output_dir, event, &engine.ctx.trades, &writer).await?;
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
    windows: &HashMap<String, UhfTradeWindow>,
    writer: &AsyncRollbackWriter,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = Utc::now().to_rfc3339();
    let symbol = extract_symbol(&event.instrument);
    let window = windows.get(symbol.as_str());

    match event.kind {
        DataKind::Trade(trade) => {
            let payload = serde_json::json!({
                "time": now,
                "instrument": event.instrument,
                "symbol": symbol,
                "exchange": event.exchange,
                "kind": "trade",
                "data": trade,
                "window": window.map(|value| serde_json::json!({
                    "current_second_qty": value.get_current_second_qty(),
                    "second_avg_qty": value.get_second_avg_qty(),
                    "current_open": value.get_current_open_price(),
                    "current_high": value.get_current_high_price(),
                    "current_low": value.get_current_low_price(),
                    "best_bid": value.get_best_bid_price(),
                    "best_ask": value.get_best_ask_price(),
                })),
            });
            writer.append_json_line(&format!("{output_dir}/trades.jsonl"), &payload)?;
        }
        DataKind::Candle(candle) => {
            persist_candle(
                output_dir,
                &event.instrument,
                event.exchange,
                candle,
                &now,
                window,
                writer,
            )
            .await?;
        }
        _ => {}
    }

    Ok(())
}

async fn persist_candle(
    output_dir: &str,
    instrument: &str,
    exchange: barter_instrument::exchange::ExchangeId,
    candle: Candle,
    now: &str,
    window: Option<&UhfTradeWindow>,
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
        "window": window.map(|value| serde_json::json!({
            "latest_60m_qty": value.get_latest_60_minutes_qty(),
            "hour": value.get_hour_price_qty(),
            "h24": value.get_24hour_price_qty(),
        })),
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
                bid_volume: 0.0,
                is_final: true,
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
