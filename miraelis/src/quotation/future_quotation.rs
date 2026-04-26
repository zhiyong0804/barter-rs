use barter_data::{
    event::DataKind,
    exchange::binance::futures::BinanceFuturesUsd,
    streams::reconnect,
    streams::{consumer::MarketStreamResult, reconnect::stream::ReconnectingStream, Streams},
    subscription::{
        book::OrderBooksL1, candle_1h::Candles1h, candle_1m::Candles1m, trade::PublicTrades,
    },
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures_util::StreamExt;
use tokio::{
    sync::mpsc::UnboundedReceiver,
    time::{interval, Duration},
};
use tracing::{info, warn};

use crate::signal::{SignalType, TelegramNotifier};
use crate::strategy::frame::OrderResponse;
use crate::strategy::{MarketEvent, StrategyEngine};

use super::rate_limiter::RateLimiter;
use super::trade_window::{
    BestBidAskItem, QuotationKline, QuotationTicker, TradeItem, UhfKlineInterval, UhfTradeWindow,
};

use super::writer::AsyncRollbackWriter;
use super::SymbolSpec;

pub struct FutureQuotation;

impl FutureQuotation {
    pub async fn run_market_streams(
        symbols: Vec<SymbolSpec>,
        subscribe_all: bool,
        debug_trade_window_symbol: Option<&str>,
        debug_trade_window_interval_secs: u64,
        engine: &mut StrategyEngine,
        writer: &AsyncRollbackWriter,
        telegram_notifier: Option<&TelegramNotifier>,
        mut order_response_rx: Option<&mut UnboundedReceiver<OrderResponse>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let debug_trade_window_symbol = debug_trade_window_symbol
            .map(|value| value.trim().to_ascii_uppercase())
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
                                // Add logging for K-line events
                                if let DataKind::Candle(ref candle) = event.kind {
                                    let symbol = extract_symbol(&event.instrument);
                                    if debug_trade_window_symbol.as_ref().map_or(false, |s| s == &symbol) {
                                        tracing::debug!(
                                            symbol = %symbol,
                                            interval = ?if event.instrument.ends_with("|kline_1h") {
                                                "1h"
                                            } else {
                                                "1m"
                                            },
                                            open = candle.open,
                                            close = candle.close,
                                            high = candle.high,
                                            low = candle.low,
                                            volume = candle.volume,
                                            is_final = candle.is_final,
                                            "Received K-line event"
                                        );
                                    }

                                    if candle.is_final {
                                        writer.write_market_event(event.clone())?;
                                    }
                                }

                                apply_event_to_strategy_context(engine, &event, debug_trade_window_symbol.clone());

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
                                // Add logging for K-line events
                                if let DataKind::Candle(ref candle) = event.kind {
                                    let symbol = extract_symbol(&event.instrument);
                                    if debug_trade_window_symbol.as_ref().map_or(false, |s| s == &symbol) {
                                        tracing::debug!(
                                            symbol = %symbol,
                                            interval = ?if event.instrument.ends_with("|kline_1h") {
                                                "1h"
                                            } else {
                                                "1m"
                                            },
                                            open = candle.open,
                                            close = candle.close,
                                            high = candle.high,
                                            low = candle.low,
                                            volume = candle.volume,
                                            is_final = candle.is_final,
                                            "Received K-line event"
                                        );
                                    }

                                    if candle.is_final {
                                        writer.write_market_event(event.clone())?;
                                    }
                                }

                                apply_event_to_strategy_context(engine, &event, debug_trade_window_symbol.clone());

                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn warm_up_trade_windows(
        rest_base: &str,
        symbols: &[String],
        writer: &AsyncRollbackWriter,
    ) {
        use barter_data::subscription::candle::Candle;
        use chrono::Utc;

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
            let mut backoff = std::time::Duration::from_secs(2);
            for attempt in 0..=MAX_RETRIES {
                rl.acquire().await;
                let resp = match client.get(url).send().await {
                    Ok(r) => r,
                    Err(err) => {
                        tracing::warn!(?err, url, attempt, "warm_up: request error");
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        continue;
                    }
                };
                let status = resp.status();
                // tracing::debug!("fetch_klines return: {:#?}", resp);
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 418 {
                    let wait = resp
                        .headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(std::time::Duration::from_secs)
                        .unwrap_or(std::time::Duration::from_secs(60));
                    tracing::warn!(
                        url,
                        secs = wait.as_secs(),
                        "warm_up: rate limited, sleeping"
                    );
                    tokio::time::sleep(wait).await;
                    backoff = std::time::Duration::from_secs(2);
                    continue;
                }
                if !status.is_success() {
                    tracing::warn!(url, status = status.as_u16(), attempt, "warm_up: non-2xx");
                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                    continue;
                }
                match resp.json::<serde_json::Value>().await {
                    Ok(json) => return Some(json),
                    Err(err) => {
                        tracing::warn!(?err, url, "warm_up: JSON parse error");
                        return None;
                    }
                }
            }
            None
        }

        // Binance Futures klines weight=1，保守 35 req/s；最大并发 10
        let rate_limiter = RateLimiter::new(35);
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(10));
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_default();
        let base = rest_base.trim_end_matches('/').to_owned();

        tracing::info!(
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

        let total_symbols = tasks.len();
        let mut processed = 0usize;
        let mut loaded = 0usize;
        let mut failed = 0usize;
        for task in tasks {
            let Some(Some((symbol, klines_1h, klines_1m))) = task.await.ok() else {
                processed += 1;
                failed += 1;
                let progress_pct = if total_symbols == 0 {
                    100.0
                } else {
                    (processed as f64 / total_symbols as f64) * 100.0
                };
                tracing::info!(
                    processed,
                    total = total_symbols,
                    progress_pct,
                    loaded,
                    failed,
                    "warm_up: progress"
                );
                continue;
            };
            let klines_1h_count = klines_1h.len();
            let klines_1m_count = klines_1m.len();
            let exchange = barter_instrument::exchange::ExchangeId::BinanceFuturesUsd;

            for k in &klines_1h {
                let candle = Candle {
                    close_time: chrono::DateTime::<Utc>::from_timestamp_millis(k.close_time as i64)
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
                let event = barter_data::event::MarketEvent {
                    time_exchange: candle.close_time,
                    time_received: Utc::now(),
                    exchange,
                    instrument,
                    kind: barter_data::event::DataKind::Candle(candle),
                };
                if let Err(error) = writer.write_market_event(event) {
                    tracing::warn!(
                        ?error,
                        "warm_up: event receiver dropped while sending 1h kline"
                    );
                    return;
                }
            }
            for k in &klines_1m {
                let candle = Candle {
                    close_time: chrono::DateTime::<Utc>::from_timestamp_millis(k.close_time as i64)
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
                let event = barter_data::event::MarketEvent {
                    time_exchange: candle.close_time,
                    time_received: Utc::now(),
                    exchange,
                    instrument,
                    kind: barter_data::event::DataKind::Candle(candle),
                };
                // tracing::debug!("successed fetch_klines 1m symbol={}", symbol);
                if let Err(error) = writer.write_market_event(event) {
                    tracing::warn!(
                        ?error,
                        "warm_up: event receiver dropped while sending 1m kline"
                    );
                    return;
                }
            }

            tracing::info!(
                symbol = %symbol,
                klines_1h = klines_1h_count,
                klines_1m = klines_1m_count,
                "warm_up: symbol loaded"
            );

            processed += 1;
            loaded += 1;
            let progress_pct = if total_symbols == 0 {
                100.0
            } else {
                (processed as f64 / total_symbols as f64) * 100.0
            };
            tracing::info!(
                processed,
                total = total_symbols,
                progress_pct,
                loaded,
                failed,
                "warm_up: progress"
            );
        }

        tracing::info!(
            loaded,
            failed,
            "warm_up: historical klines loaded into trade windows and persisted"
        );
    }
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

fn apply_event_to_strategy_context(
    engine: &mut StrategyEngine,
    event: &barter_data::event::MarketEvent<String, DataKind>,
    debug_trade_window_symbol: Option<String>,
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
                symbol: symbol.clone(),
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

            // Add logging for K-line processing
            if debug_trade_window_symbol
                .as_ref()
                .map_or(false, |s| s == &symbol)
            {
                tracing::debug!(
                    symbol = %symbol,
                    interval = ?interval,
                    open = kline.open,
                    close = kline.close,
                    high = kline.high,
                    low = kline.low,
                    volume = kline.volume,
                    is_final = kline.is_final,
                    "Processing K-line for strategy context"
                );
            }

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

