//! Pump Scanner — 基于五指标框架的暴涨前置扫描器
//!
//! 五指标（按优先级）：
//!   ① Volume Ratio (30%) — 成交量/7日均量
//!   ② OI 增长率 (25%) — 多周期 OI Ratio + 4h Taker 买卖量
//!   ③ Spot/Futures 同步 (20%) — 期现量价验证
//!   ④ Funding Rate (15%) — 费率地板占比
//!   ⑤ 链上大额 (10%) — 占位
//!
//! P/V/O 矩阵：
//!   P↑ V↑ OI↑ → 🟢 趋势启动
//!   P→ V↑ OI↑ → 🔥 隐藏吸筹（最优前置信号）
//!   P↑ V↑ OI↓ → 🟡 空头回补
//!   P↓ V↑ OI↑ → 🔴 新增空头
//!   P↓ V↓ OI↓ → ⚪ 市场冷却
//!
//! ## 集成方式
//!
//! 实现 [`StrategyModule`] trait，通过 [`handle_candle_1h`] 触发定期扫描。
//! 行情数据优先使用 [`UhfTradeWindow`]（价格、成交量、24h 变化），
//! OI / Funding Rate / 日K 等数据通过 Binance REST API 拉取。

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    quotation::trade_window::{MarkPriceItem, QuotationKline, UhfTradeWindow},
    signal::{SignalType, TelegramNotifier},
};

use super::{module_id, StrategyContext, StrategyModule};

// All data now comes from WebSocket. No REST calls needed.

// ════════════════════════════════════════════════════════════
// Constants
// ════════════════════════════════════════════════════════════

/// 7 days of 4h bars = 7 × 6 = 42 bars
const VOLUME_LOOKBACK_4H: usize = 42;

// ════════════════════════════════════════════════════════════
// Internal Data Types
// ════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
struct KlineBar {
    ts_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    quote_vol: f64,
    taker_buy_quote_vol: f64,
    taker_sell_quote_vol: f64,
}

// ════════════════════════════════════════════════════════════
// Pump Scan Result (public output)
// ════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpScanResult {
    pub symbol: String,
    pub timestamp: String,

    // Price
    pub current_price: f64,
    pub price_chg_24h_pct: f64,

    // ① Volume Ratio
    pub vol_ratio_latest: f64,
    pub vol_ratio_signal: String,
    pub vol_ratio_score: u32,

    // ② OI
    pub oi_ratio_24h: f64,
    pub oi_ratio_tier: String,
    pub oi_ratio_score: u32,
    pub oi_current_qty: f64,
    pub oi_change_24h_pct: f64,
    pub oi_acceleration: f64,
    pub oi_trend_7d: String,

    // ②-b Taker
    pub total_buy_vol_7d: f64,
    pub total_sell_vol_7d: f64,
    pub cumulative_ls_ratio: Option<f64>,

    // ③ Spot/Futures
    pub has_spot: bool,
    pub spot_futures_signal: String,
    pub spot_futures_score: u32,

    // ④ Funding
    pub funding_current_pct: f64,
    pub funding_floor_ratio: f64,
    pub funding_signal: String,
    pub funding_score: u32,

    // ⑤ On-chain
    pub onchain_signal: String,
    pub onchain_score: u32,

    // P/V/O
    pub pvo_signal: String,
    pub pvo_score: u32,

    // Hidden accumulation
    pub hidden_score: u32,
    pub hidden_verdict: String,

    // Composite
    pub composite_score: f64,
    pub composite_verdict: String,
}

// ════════════════════════════════════════════════════════════
// Module Config
// ════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Deserialize)]
pub struct PumpScannerModuleConfig {
    /// Min hours between full scans for the same symbol.
    #[serde(default = "default_scan_interval_hours")]
    pub scan_interval_hours: u64,

    /// Only notify when composite score >= this threshold.
    #[serde(default = "default_composite_score_threshold")]
    pub composite_score_threshold: f64,
}

fn default_scan_interval_hours() -> u64 {
    4
}
fn default_composite_score_threshold() -> f64 {
    5.0
}

impl Default for PumpScannerModuleConfig {
    fn default() -> Self {
        Self {
            scan_interval_hours: default_scan_interval_hours(),
            composite_score_threshold: default_composite_score_threshold(),
        }
    }
}

// ════════════════════════════════════════════════════════════
// Per-Symbol Context
// ════════════════════════════════════════════════════════════

/// Max funding rate samples to accumulate from WebSocket @markPrice stream.
const MAX_FUNDING_RATE_SAMPLES: usize = 200;
/// Max OI snapshots (hourly × 30 days).
const MAX_OI_SNAPSHOTS: usize = 720;

#[derive(Debug, Clone)]
struct PumpSymbolContext {
    symbol: String,
    last_scan_time: u64,
    last_oi_snapshot_time: u64,
    latest_result: Option<PumpScanResult>,
    /// Accumulated funding rates from @markPrice WebSocket (most recent last).
    funding_rates: Vec<f64>,
    /// OI snapshots from @openInterest WebSocket: (timestamp_secs, oi_qty).
    oi_snapshots: VecDeque<(u64, f64)>,
}

impl PumpSymbolContext {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            last_scan_time: 0,
            last_oi_snapshot_time: 0,
            latest_result: None,
            funding_rates: Vec::with_capacity(MAX_FUNDING_RATE_SAMPLES),
            oi_snapshots: VecDeque::with_capacity(MAX_OI_SNAPSHOTS),
        }
    }

    fn push_funding_rate(&mut self, rate: f64) {
        if self.funding_rates.last().map_or(true, |last| (*last - rate).abs() > 1e-12) {
            if self.funding_rates.len() >= MAX_FUNDING_RATE_SAMPLES {
                self.funding_rates.remove(0);
            }
            self.funding_rates.push(rate);
        }
    }

    /// Take an OI snapshot. Only stores if at least ~1h has passed since last.
    fn snapshot_oi(&mut self, now_sec: u64, oi_qty: f64) {
        if oi_qty <= 0.0 {
            return;
        }
        if self.last_oi_snapshot_time > 0
            && now_sec < self.last_oi_snapshot_time.saturating_add(3600)
        {
            return;
        }
        self.last_oi_snapshot_time = now_sec;
        if self.oi_snapshots.len() >= MAX_OI_SNAPSHOTS {
            self.oi_snapshots.pop_front();
        }
        self.oi_snapshots.push_back((now_sec, oi_qty));
    }
}

// ════════════════════════════════════════════════════════════
// Module State
// ════════════════════════════════════════════════════════════

pub struct PumpScannerCtx {
    pub id: u64,
    pub name: String,
    pub started: bool,
    pub started_timestamp: u64,

    // Config
    pub scan_interval_hours: u64,
    pub composite_score_threshold: f64,

    // Per-symbol state
    pub symbol_contexts: HashMap<String, PumpSymbolContext>,
}

impl Default for PumpScannerCtx {
    fn default() -> Self {
        Self {
            id: module_id::PUMP_SCANNER,
            name: "strategy.pump.scanner".to_owned(),
            started: false,
            started_timestamp: 0,
            scan_interval_hours: default_scan_interval_hours(),
            composite_score_threshold: default_composite_score_threshold(),
            symbol_contexts: HashMap::new(),
        }
    }
}

pub struct PumpScannerModule {
    pub cfg: PumpScannerCtx,
    telegram_notifier: Option<Arc<TelegramNotifier>>,
}

impl Default for PumpScannerModule {
    fn default() -> Self {
        Self {
            cfg: PumpScannerCtx::default(),
            telegram_notifier: None,
        }
    }
}

impl PumpScannerModule {
    pub fn with_config(config: PumpScannerModuleConfig) -> Self {
        let mut ctx = PumpScannerCtx::default();
        ctx.scan_interval_hours = config.scan_interval_hours;
        ctx.composite_score_threshold = config.composite_score_threshold;
        Self {
            cfg: ctx,
            telegram_notifier: None,
        }
    }

    pub fn with_telegram_notifier(mut self, notifier: Arc<TelegramNotifier>) -> Self {
        self.telegram_notifier = Some(notifier);
        self
    }

    // ── helpers ──

    fn now_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    // ── ① Volume Ratio (4h bars, 7-day lookback = 42 bars) ──

    fn analyze_volume_ratio(klines: &[KlineBar]) -> (f64, String, u32) {
        if klines.len() < VOLUME_LOOKBACK_4H + 1 {
            return (0.0, "⏳ 数据不足".to_string(), 0);
        }

        // Latest 4h bar volume vs 7-day (42-bar) average
        let latest_vol = klines.last().unwrap().quote_vol;
        let avg_7d: f64 =
            klines[klines.len() - 1 - VOLUME_LOOKBACK_4H..klines.len() - 1]
                .iter()
                .map(|k| k.quote_vol)
                .sum::<f64>()
                / VOLUME_LOOKBACK_4H as f64;

        let ratio = if avg_7d > 0.0 { latest_vol / avg_7d } else { 0.0 };

        let (signal, score) = if ratio >= 10.0 {
            ("🔥 极端爆量".to_string(), 9)
        } else if ratio > 3.0 {
            ("🟢 显著放量".to_string(), 7)
        } else if ratio > 1.5 {
            ("📈 温和放量".to_string(), 5)
        } else if ratio < 0.4 {
            ("❄️ 极致地量 — 弹簧压缩".to_string(), 9)
        } else if ratio < 0.7 {
            ("📉 缩量".to_string(), 6)
        } else {
            ("➖ 正常".to_string(), 3)
        };

        (ratio, signal, score)
    }

    // ── ② OI Analysis ──

    // ── ② OI Analysis (from @openInterest WebSocket snapshots) ──

    fn analyze_oi_from_snapshots(
        snapshots: &[(u64, f64)],
    ) -> (f64, String, u32, f64, f64, f64, String) {
        // (ratio_24h, tier, tier_score, acceleration, oi_qty, oi_chg_pct, oi_trend)
        if snapshots.len() < 2 {
            return (1.0, "⏳ 等待数据积累...".to_string(), 0, 0.0, 0.0, 0.0, "?".to_string());
        }

        let (_, current_oi) = snapshots.last().unwrap();

        // OI Ratio 24h: compare current to OI from ~24h ago
        let target_ts = snapshots.last().unwrap().0.saturating_sub(24 * 3600);
        let oi_24h_ago = snapshots
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= target_ts)
            .map(|(_, oi)| *oi)
            .unwrap_or(*current_oi);
        let ratio_24h = if oi_24h_ago > 0.0 {
            current_oi / oi_24h_ago
        } else {
            1.0
        };

        // OI Ratio 1h
        let target_ts_1h = snapshots.last().unwrap().0.saturating_sub(3600);
        let oi_1h_ago = snapshots
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= target_ts_1h)
            .map(|(_, oi)| *oi)
            .unwrap_or(*current_oi);
        let ratio_1h = if oi_1h_ago > 0.0 {
            current_oi / oi_1h_ago
        } else {
            1.0
        };

        let (tier, tier_score) = if ratio_24h >= 2.0 {
            ("🔥 Tier 1 — 极强异动".to_string(), 10)
        } else if ratio_24h >= 1.5 {
            ("🟠 Tier 2 — 重点监控".to_string(), 7)
        } else if ratio_24h >= 1.2 {
            ("🟡 Tier 3 — 值得注意".to_string(), 4)
        } else if ratio_24h >= 1.1 {
            ("➖ Tier 4 — 轻微流入".to_string(), 2)
        } else {
            ("⚪ Tier 5 — 正常/流出".to_string(), 0)
        };

        let hourly_pace = ratio_1h - 1.0;
        let daily_pace = (ratio_24h - 1.0) / 24.0;
        let acceleration = if daily_pace > 0.001 {
            hourly_pace / daily_pace
        } else {
            0.0
        };

        let oi_change_24h = (ratio_24h - 1.0) * 100.0;

        // OI 7d trend from snapshots
        let half = snapshots.len() / 2;
        let oi_trend = if snapshots.len() >= 12 {
            let first_avg = snapshots[..half].iter().map(|(_, oi)| oi).sum::<f64>() / half as f64;
            let second_avg =
                snapshots[half..].iter().map(|(_, oi)| oi).sum::<f64>() / (snapshots.len() - half) as f64;
            if second_avg > first_avg * 1.1 {
                "📈 扩张"
            } else if second_avg < first_avg * 0.9 {
                "📉 收缩"
            } else {
                "➖ 横盘"
            }
        } else {
            "?"
        };

        (
            ratio_24h,
            tier,
            tier_score,
            acceleration,
            *current_oi,
            oi_change_24h,
            oi_trend.to_string(),
        )
    }

    // ── ③ Spot/Futures ──

    async fn check_spot_exists(symbol: &str) -> bool {
        let url = format!(
            "https://api.binance.com/api/v3/ticker/24hr?symbol={}",
            symbol
        );
        match reqwest::get(&url).await {
            Ok(r) => r.status().is_success(),
            Err(_) => false,
        }
    }

    // ── ④ Funding Rate (from WebSocket @markPrice) ──

    /// Compute funding rate analysis from accumulated WebSocket @markPrice data.
    /// `rates` are raw funding rates (not percentages), most recent last.
    fn analyze_funding_from_buffer(rates: &[f64]) -> (f64, f64, String, u32) {
        if rates.is_empty() {
            return (0.0, 0.0, "⏳ 等待数据积累...".to_string(), 0);
        }

        let current = rates.last().unwrap() * 100.0;
        let lookback = rates.len().min(50);
        let recent = &rates[rates.len() - lookback..];
        let floor_count = recent
            .iter()
            .filter(|r| **r * 100.0 <= 0.01)
            .count();
        let floor_ratio = floor_count as f64 / lookback as f64;

        let (status, score) = if floor_ratio > 0.8 && current < 0.02 {
            ("✅ 完美 — 费率长期地板，多头完全未拥挤".to_string(), 10)
        } else if floor_ratio > 0.5 {
            ("🟢 健康 — 大部分时间费率很低".to_string(), 7)
        } else if current < 0.05 {
            ("🟡 温和 — 费率有所抬头但未过热".to_string(), 4)
        } else if current < 0.10 {
            ("🟠 偏高 — 多头开始拥挤".to_string(), 2)
        } else {
            ("🔴 过热 — 费率极高，回调风险大".to_string(), 0)
        };

        (current, floor_ratio, status, score)
    }

    // ── P/V/O Matrix ──

    fn pvo_matrix(price_chg: f64, vol_ratio: f64, oi_ratio: f64) -> (String, u32, String) {
        let price_up = price_chg > 2.0;
        let price_down = price_chg < -2.0;
        let price_flat = !price_up && !price_down;
        let vol_up = vol_ratio > 1.5;
        let oi_up = oi_ratio > 1.15;
        let oi_down = oi_ratio < 0.90;

        if price_flat && oi_up && vol_up {
            (
                "🔥 隐藏吸筹".to_string(),
                10,
                "OI 大涨但价格不动 — 大资金在压价建仓。最强的暴涨前置信号。".to_string(),
            )
        } else if price_up && vol_up && oi_up {
            (
                "🟢 趋势启动".to_string(),
                8,
                "价量齐升 + OI 增长 — 健康的多头趋势，最佳追入窗口".to_string(),
            )
        } else if price_up && vol_up && oi_down {
            (
                "🟡 空头回补".to_string(),
                4,
                "价格上涨但 OI 下降 — 空头被迫平仓推动的上涨，不具备持续性".to_string(),
            )
        } else if price_down && vol_up && oi_up {
            (
                "🔴 新增空头".to_string(),
                1,
                "价格下跌 + 成交量放大 + OI 上升 — 有空头大规模进场".to_string(),
            )
        } else if price_down && !vol_up {
            ("⚪ 市场冷却".to_string(), 0, "价跌量缩 — 市场关注度下降".to_string())
        } else {
            ("➖ 信号混合".to_string(), 3, "各维度信号不一致".to_string())
        }
    }

    // ── Hidden Accumulation Detection ──

    fn detect_hidden_accumulation(
        oi_ratio: f64,
        price_chg: f64,
        funding_pct: f64,
        vol_ratio: f64,
    ) -> (u32, String) {
        let mut score: u32 = 0;

        if oi_ratio >= 2.0 {
            score += 3;
        } else if oi_ratio >= 1.5 {
            score += 2;
        } else if oi_ratio >= 1.2 {
            score += 1;
        }

        if price_chg.abs() < 3.0 {
            score += 3;
        } else if price_chg.abs() < 5.0 {
            score += 2;
        } else if price_chg.abs() < 10.0 {
            score += 1;
        }

        if funding_pct < 0.01 {
            score += 2;
        } else if funding_pct < 0.03 {
            score += 1;
        }

        if vol_ratio > 2.0 {
            score += 2;
        } else if vol_ratio > 1.3 {
            score += 1;
        }

        let verdict = if score >= 8 {
            "🔥 极强隐藏吸筹信号 — 暴涨前夜概率极高"
        } else if score >= 5 {
            "🟠 中等吸筹信号 — 值得加入重点监控"
        } else if score >= 3 {
            "🟡 轻微吸筹信号 — 持续观察"
        } else {
            "⚪ 无明显吸筹信号"
        };

        (score, verdict.to_string())
    }

    // ── Composite Score ──

    fn composite(hidden_score: u32, pvo_score: u32, funding_score: u32) -> (f64, String) {
        let composite =
            hidden_score as f64 * 0.5 + pvo_score as f64 * 0.3 + funding_score as f64 * 0.2;
        let verdict = if composite >= 7.0 {
            "🟢 强烈关注 — 多维度确认资金涌入，大概率即将变盘"
        } else if composite >= 5.0 {
            "🟡 保持监控 — 有异常但未完全确认"
        } else if composite >= 3.0 {
            "🟠 轻度关注 — 个别指标有信号"
        } else {
            "⚪ 当前无显著异常"
        };
        (composite, verdict.to_string())
    }

    // ── Main Scan (async, called from spawned task) ──

    async fn run_scan(
        symbol: &str,
        price_chg_24h: f64,
        current_price: f64,
        funding_rates: Vec<f64>,
        klines_4h: Vec<KlineBar>,
        oi_snapshots: Vec<(u64, f64)>,
    ) -> PumpScanResult {
        // ① Volume Ratio — 4h bars, 42-bar (7-day) lookback
        let (vol_ratio, vol_signal, vol_score) =
            Self::analyze_volume_ratio(&klines_4h);

        // ② OI — from @openInterest WebSocket snapshots
        let (oi_ratio, oi_tier, oi_score, oi_accel, oi_qty, oi_chg_pct, oi_trend) =
            Self::analyze_oi_from_snapshots(&oi_snapshots);

        // ②-b Taker buy/sell from 4h klines bid_volume
        let total_buy: f64 = klines_4h.iter().map(|k| k.taker_buy_quote_vol).sum();
        let total_sell: f64 = klines_4h.iter().map(|k| k.taker_sell_quote_vol).sum();
        let cumulative_ls = if total_sell > 0.0 {
            Some(total_buy / total_sell)
        } else {
            None
        };

        // ③ Spot/Futures
        let has_spot = Self::check_spot_exists(symbol).await;
        let (spot_sig, spot_score) = if has_spot {
            ("✅ 期现可验证".to_string(), 8)
        } else {
            ("⚠️ 不适用 — 纯期货驱动代币".to_string(), 0)
        };

        // ④ Funding — from WebSocket @markPrice accumulated data (no REST call)
        let (funding_pct, floor_ratio, funding_sig, funding_score) =
            Self::analyze_funding_from_buffer(&funding_rates);

        // P/V/O
        let (pvo_sig, pvo_score, _pvo_desc) =
            Self::pvo_matrix(price_chg_24h, vol_ratio, oi_ratio);

        // Hidden accumulation
        let (hidden_score, hidden_verdict) =
            Self::detect_hidden_accumulation(oi_ratio, price_chg_24h, funding_pct, vol_ratio);

        // Composite
        let (composite_score, composite_verdict) =
            Self::composite(hidden_score, pvo_score, funding_score);

        PumpScanResult {
            symbol: symbol.to_string(),
            timestamp: Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            current_price,
            price_chg_24h_pct: price_chg_24h,

            vol_ratio_latest: vol_ratio,
            vol_ratio_signal: vol_signal,
            vol_ratio_score: vol_score,

            oi_ratio_24h: oi_ratio,
            oi_ratio_tier: oi_tier,
            oi_ratio_score: oi_score,
            oi_current_qty: oi_qty,
            oi_change_24h_pct: oi_chg_pct,
            oi_acceleration: oi_accel,
            oi_trend_7d: oi_trend.to_string(),

            total_buy_vol_7d: total_buy,
            total_sell_vol_7d: total_sell,
            cumulative_ls_ratio: cumulative_ls,

            has_spot,
            spot_futures_signal: spot_sig,
            spot_futures_score: spot_score,

            funding_current_pct: funding_pct,
            funding_floor_ratio: floor_ratio,
            funding_signal: funding_sig,
            funding_score,

            onchain_signal: "⚪ 数据不可得".to_string(),
            onchain_score: 0,

            pvo_signal: pvo_sig,
            pvo_score,

            hidden_score,
            hidden_verdict,

            composite_score,
            composite_verdict,
        }
    }
}

// ════════════════════════════════════════════════════════════
// StrategyModule Implementation
// ════════════════════════════════════════════════════════════

impl StrategyModule for PumpScannerModule {
    fn id(&self) -> u64 {
        self.cfg.id
    }

    fn name(&self) -> &str {
        &self.cfg.name
    }

    fn init(
        &mut self,
        _ctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cfg.started = false;
        self.cfg.started_timestamp = 0;
        Ok(())
    }

    fn start(
        &mut self,
        _ctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cfg.started = true;
        self.cfg.started_timestamp = Self::now_seconds();
        tracing::info!(
            strategy = self.name(),
            strategy_id = self.id(),
            started_timestamp = self.cfg.started_timestamp,
            scan_interval_hours = self.cfg.scan_interval_hours,
            composite_score_threshold = self.cfg.composite_score_threshold,
            "pump scanner module started"
        );
        Ok(())
    }

    /// On each finalized 1h kline, check if it's time to scan.
    /// If the scan interval has passed, spawn an async task to fetch
    /// REST data and compute all five indicators.
    fn handle_candle_1h(&mut self, ctx: &mut StrategyContext, candle: &QuotationKline) {
        if !self.cfg.started {
            return;
        }
        if !candle.is_final {
            return;
        }

        // Get per-symbol context
        let sym_ctx = self
            .cfg
            .symbol_contexts
            .entry(candle.symbol.clone())
            .or_insert_with(|| PumpSymbolContext::new(candle.symbol.clone()));

        let now_sec = candle.end_timestamp / 1000;
        let interval_secs = self.cfg.scan_interval_hours * 3600;

        // Respect scan interval
        if sym_ctx.last_scan_time > 0
            && now_sec < sym_ctx.last_scan_time.saturating_add(interval_secs)
        {
            return;
        }
        sym_ctx.last_scan_time = now_sec;

        // ── Gather data from UhfTradeWindow ──
        let tw = match ctx.trades.get(&candle.symbol) {
            Some(tw) => tw,
            None => {
                tracing::debug!(
                    strategy = self.name(),
                    symbol = %candle.symbol,
                    "pump scan skipped: no trade window"
                );
                return;
            }
        };

        let current_price = if tw.best_bid_ask.best_bid_price > 0.0
            && tw.best_bid_ask.best_ask_price > 0.0
        {
            (tw.best_bid_ask.best_bid_price + tw.best_bid_ask.best_ask_price) / 2.0
        } else if tw.hours_window.tf_open_price.is_finite() && tw.hours_window.tf_open_price > 0.0 {
            tw.hours_window.tf_open_price
        } else {
            // Fallback: use candle close
            candle.close
        };

        // 24h price change from ticker (tf_open_price = 24h ago open)
        let price_chg_24h = if tw.hours_window.tf_open_price.is_finite()
            && tw.hours_window.tf_open_price > 0.0
        {
            (current_price - tw.hours_window.tf_open_price) / tw.hours_window.tf_open_price * 100.0
        } else {
            0.0
        };

        // ── Snapshot OI from @openInterest WebSocket ──
        sym_ctx.snapshot_oi(now_sec, tw.latest_oi);

        // ── Clone accumulated WebSocket data for the async task ──
        let funding_rates = sym_ctx.funding_rates.clone();
        let oi_snapshots = sym_ctx
            .oi_snapshots
            .iter()
            .map(|(ts, oi)| (*ts, *oi))
            .collect::<Vec<_>>();

        // Convert 4h klines from UhfTradeWindow to internal KlineBar format
        let klines_4h: Vec<KlineBar> = tw
            .four_hour_klines
            .iter()
            .map(|k| KlineBar {
                ts_ms: k.start_timestamp as i64,
                open: k.open,
                high: k.high,
                low: k.low,
                close: k.close,
                quote_vol: k.volume * k.close, // approximate: qty × price
                taker_buy_quote_vol: k.bid_volume * k.close,
                taker_sell_quote_vol: (k.volume - k.bid_volume) * k.close,
            })
            .collect();

        // ── Spawn async scan ──
        let symbol = candle.symbol.clone();
        let threshold = self.cfg.composite_score_threshold;
        let notifier = self.telegram_notifier.clone();
        let strategy_name = self.cfg.name.clone();

        tokio::spawn(async move {
            let result = Self::run_scan(
                &symbol,
                price_chg_24h,
                current_price,
                funding_rates,
                klines_4h,
                oi_snapshots,
            )
            .await;

            tracing::info!(
                strategy = %strategy_name,
                symbol = %symbol,
                composite_score = result.composite_score,
                composite_verdict = %result.composite_verdict,
                pvo_signal = %result.pvo_signal,
                hidden_score = result.hidden_score,
                vol_ratio = result.vol_ratio_latest,
                oi_ratio_24h = result.oi_ratio_24h,
                funding_pct = result.funding_current_pct,
                "pump scan completed"
            );

            // Report if score meets threshold
            if result.composite_score >= threshold {
                if let Some(notifier) = &notifier {
                    let report = format_pump_report(&result);
                    notifier.send_signal_async(SignalType::PumpScanner, report);
                }
            }
        });
    }

    /// Accumulate funding rate from @markPrice WebSocket stream.
    /// The stream pushes every 3s but funding rate only changes every 8h;
    /// we deduplicate in `PumpSymbolContext::push_funding_rate`.
    fn handle_mark_price(&mut self, _ctx: &mut StrategyContext, mp: &MarkPriceItem) {
        if !self.cfg.started {
            return;
        }
        if mp.funding_rate == 0.0 {
            return;
        }

        let sym_ctx = self
            .cfg
            .symbol_contexts
            .entry(mp.symbol.clone())
            .or_insert_with(|| PumpSymbolContext::new(mp.symbol.clone()));

        sym_ctx.push_funding_rate(mp.funding_rate);
    }
}

// ════════════════════════════════════════════════════════════
// Report Formatting
// ════════════════════════════════════════════════════════════

/// Format a human-readable pump scan report, suitable for Telegram.
pub fn format_pump_report(result: &PumpScanResult) -> String {
    let mut lines = Vec::new();
    let sym = &result.symbol;

    lines.push(format!("🔭 Pump Scanner — {}", sym));
    lines.push(format!("  时间: {} UTC", result.timestamp));
    lines.push(format!(
        "  价格: ${:.6} | 24h: {:+.2}%",
        result.current_price, result.price_chg_24h_pct
    ));
    lines.push("".to_string());

    // ① Volume Ratio
    lines.push(format!(
        "① Volume Ratio:   {} | {}",
        score_bar(result.vol_ratio_score),
        result.vol_ratio_signal
    ));
    lines.push(format!(
        "   量比: {:.2}x",
        result.vol_ratio_latest
    ));
    lines.push("".to_string());

    // ② OI
    lines.push(format!(
        "② OI 增长率:      {} | {}",
        score_bar(result.oi_ratio_score),
        result.oi_ratio_tier
    ));
    lines.push(format!(
        "   OI Ratio 24h: {:.2}x | 加速度: {:.1}x | 趋势: {}",
        result.oi_ratio_24h, result.oi_acceleration, result.oi_trend_7d
    ));
    lines.push(format!(
        "   OI 当前: {:.0} | 24h Δ: {:+.1}%",
        result.oi_current_qty, result.oi_change_24h_pct
    ));
    if let Some(cls) = result.cumulative_ls_ratio {
        lines.push(format!(
            "   7天累计多空比: {:.2} (买入: ${:.0} / 卖出: ${:.0})",
            cls, result.total_buy_vol_7d, result.total_sell_vol_7d
        ));
    }
    lines.push("".to_string());

    // ③ Spot/Futures
    lines.push(format!(
        "③ Spot/Futures:   {} | {}",
        score_bar(result.spot_futures_score),
        result.spot_futures_signal
    ));
    lines.push("".to_string());

    // ④ Funding
    lines.push(format!(
        "④ Funding Rate:   {} | {}",
        score_bar(result.funding_score),
        result.funding_signal
    ));
    lines.push(format!(
        "   当前: {:+.4}% | 地板占比: {:.0}%",
        result.funding_current_pct,
        result.funding_floor_ratio * 100.0
    ));
    lines.push("".to_string());

    // ⑤ On-chain
    lines.push(format!(
        "⑤ 链上大额:       {} | {}",
        score_bar(result.onchain_score),
        result.onchain_signal
    ));
    lines.push("".to_string());

    // P/V/O
    lines.push(format!(
        "P/V/O: {} (得分 {})",
        result.pvo_signal, result.pvo_score
    ));

    // Hidden accumulation
    lines.push(format!(
        "隐藏吸筹: {}/10 → {}",
        result.hidden_score, result.hidden_verdict
    ));
    lines.push("".to_string());

    // Composite verdict
    lines.push(format!(
        "综合判定: {:.1}/10 → {}",
        result.composite_score, result.composite_verdict
    ));

    lines.join("\n")
}

fn score_bar(score: u32) -> String {
    match score {
        9..=10 => "🔥🔥".to_string(),
        7..=8 => "🔥  ".to_string(),
        5..=6 => "🟢  ".to_string(),
        3..=4 => "🟡  ".to_string(),
        1..=2 => "🟠  ".to_string(),
        _ => "⚪  ".to_string(),
    }
}

// ════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── Volume Ratio ──

    #[test]
    fn test_volume_ratio_4h_calculation() {
        let total_bars = super::VOLUME_LOOKBACK_4H + 2; // 44 bars
        let bars: Vec<KlineBar> = (0..total_bars)
            .map(|i| KlineBar {
                ts_ms: 1720000000000 + i as i64 * 4 * 3600 * 1000, // 4h spacing
                open: 0.008,
                high: 0.009,
                low: 0.007,
                close: 0.0085,
                quote_vol: if i as usize == total_bars - 1 { 50_000_000.0 } else { 5_000_000.0 },
                taker_buy_quote_vol: 2_500_000.0,
                taker_sell_quote_vol: 2_500_000.0,
            })
            .collect();

        let (latest, _signal, score) =
            PumpScannerModule::analyze_volume_ratio(&bars);

        // Last bar: 50M volume, previous 42 bars avg = 5M → ratio = 10.0x
        assert!((latest - 10.0).abs() < 0.01, "Expected 10.0x, got {:.2}x", latest);
        assert_eq!(score, 9);
    }

    // ── OI Ratio ──

    #[test]
    fn test_oi_ratio_from_snapshots() {
        // 2 snapshots 24h apart: OI doubled → ratio = 2.0
        let now = 1720000000u64;
        let snapshots = vec![
            (now - 24 * 3600, 1000.0),
            (now, 2000.0),
        ];
        let (ratio, _tier, _score, _accel, _qty, _chg, _trend) =
            PumpScannerModule::analyze_oi_from_snapshots(&snapshots);
        assert!((ratio - 2.0).abs() < 0.01, "Expected 2.0x, got {:.3}x", ratio);
    }

    // ── PVO Matrix ──

    #[test]
    fn test_pvo_matrix_hidden_accumulation() {
        // Price flat, volume up, OI up → hidden accumulation (score 10)
        let (sig, score, _desc) = PumpScannerModule::pvo_matrix(1.0, 2.0, 1.5);
        assert_eq!(sig, "🔥 隐藏吸筹");
        assert_eq!(score, 10);
    }

    #[test]
    fn test_pvo_matrix_trend_start() {
        // Price up, volume up, OI up → trend start (score 8)
        let (sig, score, _desc) = PumpScannerModule::pvo_matrix(5.0, 3.0, 1.3);
        assert_eq!(sig, "🟢 趋势启动");
        assert_eq!(score, 8);
    }

    // ── Report formatting ──

    #[test]
    fn test_format_pump_report() {
        let result = PumpScanResult {
            symbol: "TESTUSDT".to_string(),
            timestamp: "2026-01-01 00:00:00".to_string(),
            current_price: 1.234,
            price_chg_24h_pct: 2.5,
            vol_ratio_latest: 3.5,
            vol_ratio_signal: "🟢 显著放量".to_string(),
            vol_ratio_score: 7,
            oi_ratio_24h: 1.6,
            oi_ratio_tier: "🟠 Tier 2 — 重点监控".to_string(),
            oi_ratio_score: 7,
            oi_current_qty: 500_000_000.0,
            oi_change_24h_pct: 60.0,
            oi_acceleration: 2.5,
            oi_trend_7d: "📈 扩张".to_string(),
            total_buy_vol_7d: 10_000_000.0,
            total_sell_vol_7d: 7_000_000.0,
            cumulative_ls_ratio: Some(1.43),
            has_spot: true,
            spot_futures_signal: "✅ 期现可验证".to_string(),
            spot_futures_score: 8,
            funding_current_pct: 0.005,
            funding_floor_ratio: 0.85,
            funding_signal: "✅ 完美".to_string(),
            funding_score: 10,
            onchain_signal: "⚪ 数据不可得".to_string(),
            onchain_score: 0,
            pvo_signal: "🔥 隐藏吸筹".to_string(),
            pvo_score: 10,
            hidden_score: 8,
            hidden_verdict: "🔥 极强隐藏吸筹信号".to_string(),
            composite_score: 7.5,
            composite_verdict: "🟢 强烈关注".to_string(),
        };

        let report = format_pump_report(&result);
        assert!(report.contains("TESTUSDT"));
        assert!(report.contains("1.234"));
        assert!(report.contains("隐藏吸筹"));
        assert!(report.contains("🟢 强烈关注"));
    }
}
