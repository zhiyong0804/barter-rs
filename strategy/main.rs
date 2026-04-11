mod app;
mod binance;
mod command;
mod config;
mod telegram;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    app::run().await
}
