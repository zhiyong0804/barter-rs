use barter::system::config::SystemConfig;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::BufReader, path::Path};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub system: SystemConfig,
    pub binance: BinanceConfig,
    pub monitor: MonitorConfig,
    pub risk: RiskConfig,
    pub hedge: HedgeConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceConfig {
    #[serde(default = "default_rest_base_url")]
    pub rest_base_url: String,
    #[serde(default = "default_ws_base_url")]
    pub ws_base_url: String,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_api_secret_env")]
    pub api_secret_env: String,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default = "default_listen_key_keepalive_secs")]
    pub listen_key_keepalive_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorConfig {
    pub poll_interval_ms: u64,
    #[serde(default)]
    pub dry_run: bool,
    pub margin_mode: MarginMode,
    pub position_mode: PositionMode,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RiskConfig {
    pub min_liquidation_buffer_ratio: Decimal,
    pub max_symbol_loss_ratio: Decimal,
    pub max_account_loss_ratio: Decimal,
    #[serde(default = "default_max_position_to_funds_ratio")]
    pub max_position_to_funds_ratio: Decimal,
    #[serde(default)]
    pub min_position_notional_usdt: Decimal,
}

fn default_max_position_to_funds_ratio() -> Decimal {
    Decimal::new(30, 2)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HedgeConfig {
    pub hedge_ratio: Decimal,
    pub cooldown_secs: u64,
    pub order_type: HedgeOrderType,
    pub max_hedge_notional_usdt: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MarginMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PositionMode {
    OneWay,
    Hedge,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HedgeOrderType {
    Market,
}

pub fn load_config(
    path: impl AsRef<Path>,
) -> Result<AppConfig, Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)?;
    Ok(config)
}

fn default_rest_base_url() -> String {
    "https://fapi.binance.com".to_owned()
}

fn default_ws_base_url() -> String {
    "wss://fstream.binance.com".to_owned()
}

fn default_api_key_env() -> String {
    "BINANCE_API_KEY".to_owned()
}

fn default_api_secret_env() -> String {
    "BINANCE_API_SECRET".to_owned()
}

fn default_recv_window_ms() -> u64 {
    5000
}

fn default_listen_key_keepalive_secs() -> u64 {
    1800
}
