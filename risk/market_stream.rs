use crate::{
    binance::{BookTickerEvent, MarkPriceEvent, RawBookTickerEvent, RawMarkPriceEvent},
    state::{RiskTrigger, SharedMarketState, SharedRiskState},
};
use chrono::{TimeZone, Utc};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;

pub async fn run_all_book_ticker_stream(
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

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

pub async fn run_all_mark_price_stream(
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

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
