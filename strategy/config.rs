use rust_decimal::Decimal;
use serde::Deserialize;
use std::{fs::File, io::BufReader, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub telegram: TelegramConfig,
    pub binance: BinanceConfig,
    pub strategy: StrategyConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub bot_token: String,
    #[serde(default)]
    pub allowed_chat_ids: Vec<i64>,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceConfig {
    pub rest_base_url: String,
    pub api_key_env: String,
    pub api_secret_env: String,
    pub recv_window_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub default_profit_points: Decimal,
    pub default_stop_points: Decimal,
    pub default_qty_usdt: Decimal,
    #[serde(default = "default_entry_offset_bps")]
    pub entry_offset_bps: Decimal,
    #[serde(default = "default_entry_timeout_ms")]
    pub entry_timeout_ms: u64,
    pub max_dual_side_notional_usdt: Decimal,
    pub reduce_fraction_when_over_limit: Decimal,
    pub cooldown_ms: u64,
    #[serde(default)]
    pub dry_run: bool,
}

fn default_entry_offset_bps() -> Decimal {
    Decimal::new(50, 0)
}

fn default_entry_timeout_ms() -> u64 {
    10_000
}

pub fn load_config(
    path: impl AsRef<Path>,
) -> Result<AppConfig, Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    Ok(serde_json::from_reader(reader)?)
}
