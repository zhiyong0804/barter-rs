use crate::binance::{
    BinancePositionSide, FuturesAccountInformation, FuturesPositionRisk, OrderTradeUpdate,
};
use barter_instrument::Side;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Default)]
pub struct MarketSnapshot {
    pub reference_price: Option<Decimal>,
    pub last_update: Option<DateTime<Utc>>,
}

pub type SharedMarketState = Arc<RwLock<HashMap<String, MarketSnapshot>>>;
pub type SharedRiskState = Arc<RwLock<RiskState>>;

#[derive(Debug, Clone)]
pub struct RiskState {
    pub account: FuturesAccountInformation,
    pub positions: HashMap<String, FuturesPositionRisk>,
    pub last_orders: HashMap<String, OrderTradeUpdate>,
    pub latest_order: Option<OrderTradeUpdate>,
}

#[derive(Debug, Clone)]
pub struct HedgeDecision {
    pub symbol: String,
    pub side: Side,
    pub position_side: Option<BinancePositionSide>,
    pub quantity: Decimal,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub enum RiskTrigger {
    All,
    Symbol(String),
}

pub fn position_key(symbol: &str, position_side: BinancePositionSide) -> String {
    format!("{}:{}", symbol, position_side.as_str())
}
