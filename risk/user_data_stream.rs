use crate::{
    binance::{
        AccountUpdateSummary, BinanceFuturesRestClient, FuturesPositionRisk, OrderTradeUpdate,
        UserDataEvent,
    },
    state::{position_key, RiskTrigger, SharedRiskState},
};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

pub async fn run_user_data_stream(
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
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(keepalive_secs));
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

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
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
                    UserDataEvent::Ignored => {}
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

async fn apply_account_update(
    risk_state: &SharedRiskState,
    summary: AccountUpdateSummary,
    _event_time: i64,
) {
    let mut state = risk_state.write().await;
    state.account.total_wallet_balance = summary.total_wallet_balance;
    state.account.total_unrealized_profit = summary.total_unrealized_profit;
    state.account.total_margin_balance = summary.total_margin_balance;
    state.account.available_balance = summary.available_balance;

    for position_update in summary.positions {
        let key = position_key(&position_update.symbol, position_update.position_side);
        let position = state
            .positions
            .entry(key)
            .or_insert_with(|| FuturesPositionRisk {
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
    }
}

async fn apply_order_update(risk_state: &SharedRiskState, order: OrderTradeUpdate) {
    let mut state = risk_state.write().await;
    state.last_orders.insert(order.symbol.clone(), order);
}
