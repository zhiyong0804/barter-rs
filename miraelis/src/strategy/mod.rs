use std::collections::HashMap;

use super::quotation::trade_window::{
    BestBidAskItem, QuotationKline, QuotationTicker, TradeItem, UhfTradeWindow,
};
use crate::strategy::frame::OrderResponse;

pub mod frame;
pub mod huge_momentum;

// ---------------------------------------------------------------------------
// Market event — typed payload passed to dispatch()
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Trade(TradeItem),
    SpotTrade(TradeItem),
    Candle1m(QuotationKline),
    BestBidAsk(BestBidAskItem),
    Ticker(QuotationTicker),
    // Order / Depth can be added here as their types are defined
}

// ---------------------------------------------------------------------------
// Exchange info (mirrors uhf_symbol_exchange_info_t)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct SymbolExchangeInfo {
    pub symbol: String,
    pub price_precision: u32,
    pub qty_precision: u32,
    pub tick_size: f64,
    pub step_size: f64,
}

// ---------------------------------------------------------------------------
// Strategy context (mirrors uhf_strategy_s, without tidx/pidx/main_ctx/mutex)
// ---------------------------------------------------------------------------

pub struct StrategyContext {
    /// Spot trade windows keyed by symbol
    pub spot_trades: HashMap<String, UhfTradeWindow>,
    /// Futures trade windows keyed by symbol
    pub trades: HashMap<String, UhfTradeWindow>,
    /// Exchange info keyed by symbol
    pub exchange_info: HashMap<String, SymbolExchangeInfo>,
    /// Whether the strategy engine has been started
    pub started: bool,
    /// Per-module contexts keyed by module id (u32)
    pub handler_ctxs: HashMap<u32, Box<dyn std::any::Any + Send>>,
}

impl StrategyContext {
    pub fn new() -> Self {
        Self {
            spot_trades: HashMap::new(),
            trades: HashMap::new(),
            exchange_info: HashMap::new(),
            started: false,
            handler_ctxs: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Strategy module trait (replaces C++ function-pointer fields)
// ---------------------------------------------------------------------------

pub trait StrategyModule: Send + Sync {
    fn id(&self) -> u64;
    fn name(&self) -> &str;

    // Lifecycle
    fn init(
        &mut self,
        ctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn start(
        &mut self,
        ctx: &mut StrategyContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // Event handlers — all optional, default to no-op.
    // The event data has already been saved into `ctx` before these are called.
    fn handle_trade(&mut self, _ctx: &mut StrategyContext, _trade: &TradeItem) {}
    fn handle_spot_trade(&mut self, _ctx: &mut StrategyContext, _trade: &TradeItem) {}
    fn handle_candle_1m(&mut self, _ctx: &mut StrategyContext, _candle: &QuotationKline) {}
    fn handle_best_bid_ask(&mut self, _ctx: &mut StrategyContext, _bba: &BestBidAskItem) {}
    fn handle_ticker(&mut self, _ctx: &mut StrategyContext, _ticker: &QuotationTicker) {}
    fn handle_order_response(&mut self, _ctx: &mut StrategyContext, _response: &OrderResponse) {}
}

// ---------------------------------------------------------------------------
// Strategy engine — owns all modules and the shared context
// ---------------------------------------------------------------------------

pub struct StrategyEngine {
    pub ctx: StrategyContext,
    modules: Vec<Box<dyn StrategyModule>>,
}

impl StrategyEngine {
    pub fn new() -> Self {
        Self {
            ctx: StrategyContext::new(),
            modules: Vec::new(),
        }
    }

    pub fn register(&mut self, module: Box<dyn StrategyModule>) {
        self.modules.push(module);
    }

    pub fn init_all(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut modules = std::mem::take(&mut self.modules);
        for m in &mut modules {
            m.init(&mut self.ctx)?;
        }
        self.modules = modules;
        Ok(())
    }

    pub fn start_all(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut modules = std::mem::take(&mut self.modules);
        for m in &mut modules {
            m.start(&mut self.ctx)?;
        }
        self.ctx.started = true;
        self.modules = modules;
        Ok(())
    }

    /// Save the event into `ctx` trade windows first, then call every module's handler.
    pub fn dispatch(&mut self, event: MarketEvent) {
        // --- Step 1: persist market event into trade windows ---
        match &event {
            MarketEvent::Trade(t) => {
                let symbol = t.symbol.clone();
                let window = self
                    .ctx
                    .trades
                    .entry(symbol.clone())
                    .or_insert_with(|| UhfTradeWindow::new(symbol.clone()));
                window.update(t.clone());
            }
            MarketEvent::SpotTrade(t) => {
                let symbol = t.symbol.clone();
                let window = self
                    .ctx
                    .spot_trades
                    .entry(symbol.clone())
                    .or_insert_with(|| UhfTradeWindow::new(symbol.clone()));
                window.update(t.clone());
            }
            MarketEvent::Candle1m(candle) => {
                let symbol = candle.symbol.clone();
                let window = self
                    .ctx
                    .trades
                    .entry(symbol.clone())
                    .or_insert_with(|| UhfTradeWindow::new(symbol.clone()));
                window.update_kline(candle.clone());
            }
            MarketEvent::BestBidAsk(bba) => {
                let symbol = bba.symbol.clone();
                let window = self
                    .ctx
                    .trades
                    .entry(symbol.clone())
                    .or_insert_with(|| UhfTradeWindow::new(symbol.clone()));
                window.update_best_bid_ask(bba.clone());
            }
            MarketEvent::Ticker(tk) => {
                let symbol = tk.symbol.clone();
                let window = self
                    .ctx
                    .trades
                    .entry(symbol.clone())
                    .or_insert_with(|| UhfTradeWindow::new(symbol.clone()));
                window.update_ticker(tk.clone());
            }
        }

        // --- Step 2: dispatch to all modules ---
        let mut modules = std::mem::take(&mut self.modules);
        for m in &mut modules {
            match &event {
                MarketEvent::Trade(t) => m.handle_trade(&mut self.ctx, t),
                MarketEvent::SpotTrade(t) => m.handle_spot_trade(&mut self.ctx, t),
                MarketEvent::Candle1m(candle) => m.handle_candle_1m(&mut self.ctx, candle),
                MarketEvent::BestBidAsk(bba) => m.handle_best_bid_ask(&mut self.ctx, bba),
                MarketEvent::Ticker(tk) => m.handle_ticker(&mut self.ctx, tk),
            }
        }
        self.modules = modules;
    }

    pub fn dispatch_order_response(&mut self, response: OrderResponse) {
        let mut modules = std::mem::take(&mut self.modules);
        for module in &mut modules {
            module.handle_order_response(&mut self.ctx, &response);
        }
        self.modules = modules;
    }
}

// ---------------------------------------------------------------------------
// Module IDs — mirrors the C++ enum / constant ordering
// ---------------------------------------------------------------------------

pub mod module_id {
    pub const FRAME: u64 = 1;
    pub const ROCKET: u64 = 2;
    pub const ROCKET_SIGNAL: u64 = 3;
    pub const MOMENTUM_SIGNAL: u64 = 4;
    pub const PULLBACK_SIGNAL: u64 = 5;
    pub const HUGE_MOMENTUM_SIGNAL: u64 = 6;
    pub const FRAME_NEEDLE: u64 = 7;
}
