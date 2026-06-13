use barter_data::{
    exchange::binance::futures::BinanceFuturesUsd,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::trade::AggregatePublicTrades,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures_util::StreamExt;
use tracing::{info, warn};

/// Example demonstrating how to use Binance Aggregate Trade Streams (aggTrade)
/// 
/// AggregatePublicTrades provides a lower bandwidth alternative to PublicTrades
/// by aggregating individual trades. This is useful for high-volume symbols where
/// reducing data flow is important.
///
/// See: https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber
    init_logging();

    // Initialise AggregatePublicTrades Streams for BinanceFuturesUsd
    // This uses the @aggTrade stream for lower bandwidth data consumption
    let streams = Streams::<AggregatePublicTrades>::builder()

        // Separate WebSocket connection for BTC_USDT stream (high volume - benefits from aggTrade)
        .subscribe([
            (BinanceFuturesUsd::default(), "btc", "usdt", MarketDataInstrumentKind::Perpetual, AggregatePublicTrades),
        ])

        // Separate WebSocket connection for ETH_USDT stream (high volume - benefits from aggTrade)
        .subscribe([
            (BinanceFuturesUsd::default(), "eth", "usdt", MarketDataInstrumentKind::Perpetual, AggregatePublicTrades),
        ])

        // Lower volume instruments can share a WebSocket connection
        .subscribe([
            (BinanceFuturesUsd::default(), "xrp", "usdt", MarketDataInstrumentKind::Perpetual, AggregatePublicTrades),
            (BinanceFuturesUsd::default(), "sol", "usdt", MarketDataInstrumentKind::Perpetual, AggregatePublicTrades),
            (BinanceFuturesUsd::default(), "avax", "usdt", MarketDataInstrumentKind::Perpetual, AggregatePublicTrades),
            (BinanceFuturesUsd::default(), "ltc", "usdt", MarketDataInstrumentKind::Perpetual, AggregatePublicTrades),
        ])
        .init()
        .await
        .unwrap();

    // Select and merge every exchange Stream using futures_util::stream::select_all
    // Note: use `Streams.select(ExchangeId)` to interact with individual exchange streams!
    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    while let Some(event) = joined_stream.next().await {
        info!("{event:?}");
    }
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        // Filter messages based on the INFO
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
}
