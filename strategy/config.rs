use rust_decimal::Decimal;
use serde::Deserialize;
use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

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
    #[serde(default = "default_local_exit_poll_interval_ms")]
    pub local_exit_poll_interval_ms: u64,
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

fn default_local_exit_poll_interval_ms() -> u64 {
    500
}

pub fn load_config(
    path: impl AsRef<Path>,
) -> Result<AppConfig, Box<dyn std::error::Error + Send + Sync>> {
    let resolved = resolve_config_path(path.as_ref())?;
    let file = File::open(&resolved)?;
    let reader = BufReader::new(file);
    Ok(serde_json::from_reader(reader)?)
}

fn resolve_config_path(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if path.is_absolute() && path.is_file() {
        return Ok(path.to_path_buf());
    }

    if path.is_file() {
        return Ok(path.to_path_buf());
    }

    let relative = path;
    let mut candidates = Vec::new();

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join(relative));
        for ancestor in cwd.ancestors() {
            candidates.push(ancestor.join(relative));
            if let Some(file_name) = relative.file_name() {
                candidates.push(ancestor.join(file_name));
                candidates.push(ancestor.join("bin").join(file_name));
            }
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            candidates.push(parent.join(relative));
            for ancestor in parent.ancestors() {
                candidates.push(ancestor.join(relative));
                if let Some(file_name) = relative.file_name() {
                    candidates.push(ancestor.join(file_name));
                    candidates.push(ancestor.join("strategy").join(file_name));
                    candidates.push(ancestor.join("bin").join(file_name));
                }
            }
        }
    }

    if let Some(found) = candidates.into_iter().find(|candidate| candidate.is_file()) {
        return Ok(found);
    }

    Err(format!("config file not found: {}", path.display()).into())
}
