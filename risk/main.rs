mod app;
mod binance;
mod config;
mod market_stream;
mod state;
mod supervisor;
mod user_data_stream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    app::run().await
}
