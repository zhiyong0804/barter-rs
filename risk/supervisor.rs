use crate::{
    binance::{
        BinanceFuturesRestClient, BinancePositionSide, FuturesAccountInformation,
        FuturesPositionRisk, HedgeOrderRequest, OrderTradeUpdate,
    },
    config::{AppConfig, HedgeOrderType, MarginMode, PositionMode},
    state::{HedgeDecision, MarketSnapshot, RiskTrigger, SharedMarketState, SharedRiskState},
};
use barter_instrument::Side;
use rust_decimal::Decimal;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct PositionSupervisor {
    config: AppConfig,
    client: BinanceFuturesRestClient,
    market_state: SharedMarketState,
    risk_state: SharedRiskState,
}

impl PositionSupervisor {
    pub fn new(
        config: AppConfig,
        client: BinanceFuturesRestClient,
        market_state: SharedMarketState,
        risk_state: SharedRiskState,
    ) -> Self {
        Self {
            config,
            client,
            market_state,
            risk_state,
        }
    }

    pub async fn run(self, mut trigger_rx: mpsc::UnboundedReceiver<RiskTrigger>) {
        let mut cooldowns: HashMap<String, Instant> = HashMap::new();

        info!(
            dry_run = self.config.monitor.dry_run,
            debounce_ms = self.config.monitor.poll_interval_ms,
            "Event-driven position supervisor started"
        );

        while let Some(trigger) = trigger_rx.recv().await {
            tokio::time::sleep(Duration::from_millis(self.config.monitor.poll_interval_ms)).await;

            let mut symbols = HashSet::new();
            let mut full_scan = matches!(trigger, RiskTrigger::All);
            if let RiskTrigger::Symbol(symbol) = trigger {
                symbols.insert(symbol);
            }

            while let Ok(trigger) = trigger_rx.try_recv() {
                match trigger {
                    RiskTrigger::All => {
                        full_scan = true;
                        symbols.clear();
                    }
                    RiskTrigger::Symbol(symbol) if !full_scan => {
                        symbols.insert(symbol);
                    }
                    RiskTrigger::Symbol(_) => {}
                }
            }

            if let Err(error) = self
                .evaluate_and_hedge(&mut cooldowns, full_scan.then_some(()), symbols)
                .await
            {
                error!(?error, "risk evaluation cycle failed");
            }
        }
    }

    async fn evaluate_and_hedge(
        &self,
        cooldowns: &mut HashMap<String, Instant>,
        full_scan: Option<()>,
        symbols: HashSet<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let risk_state = self.risk_state.read().await;
        let market_state = self.market_state.read().await;

        let account = risk_state.account.clone();
        let account_loss_ratio = negative_pnl_ratio(
            account.total_unrealized_profit,
            account.total_wallet_balance,
        );

        let positions = risk_state
            .positions
            .values()
            .filter(|position| position.is_open())
            .filter(|position| full_scan.is_some() || symbols.contains(&position.symbol))
            .cloned()
            .collect::<Vec<_>>();

        for position in positions {
            if self.config.monitor.margin_mode == MarginMode::Cross && !position.is_cross() {
                continue;
            }

            let Some(decision) = evaluate_hedge_decision(
                &self.config,
                &account,
                &position,
                market_state.get(&position.symbol),
                risk_state.last_orders.get(&position.symbol),
                account_loss_ratio,
            ) else {
                continue;
            };

            let cooldown_key = format!("{}:{}", position.symbol, position.position_side.as_str());
            if cooldown_active(cooldowns, &cooldown_key, self.config.hedge.cooldown_secs) {
                continue;
            }

            if self.config.monitor.dry_run {
                info!(
                    symbol = %decision.symbol,
                    side = ?decision.side,
                    quantity = %decision.quantity,
                    reason = %decision.reason,
                    "Dry-run hedge triggered"
                );
            } else {
                let client_order_id = format!(
                    "barter_hedge_{}_{}",
                    decision.symbol.to_lowercase(),
                    chrono::Utc::now().timestamp_millis()
                );
                let ack = self
                    .client
                    .place_market_hedge(HedgeOrderRequest {
                        symbol: decision.symbol.clone(),
                        side: decision.side,
                        position_side: decision.position_side,
                        quantity: decision.quantity,
                        client_order_id,
                    })
                    .await?;

                info!(
                    symbol = %ack.symbol,
                    order_id = ack.order_id,
                    client_order_id = %ack.client_order_id,
                    status = ?ack.status,
                    reason = %decision.reason,
                    "Hedge order placed"
                );
            }

            cooldowns.insert(cooldown_key, Instant::now());
        }

        Ok(())
    }
}

fn evaluate_hedge_decision(
    config: &AppConfig,
    account: &FuturesAccountInformation,
    position: &FuturesPositionRisk,
    market: Option<&MarketSnapshot>,
    last_order: Option<&OrderTradeUpdate>,
    account_loss_ratio: Decimal,
) -> Option<HedgeDecision> {
    if config.monitor.position_mode == PositionMode::Hedge
        && position.position_side == BinancePositionSide::Both
    {
        warn!(
            symbol = %position.symbol,
            "Configured hedge mode but Binance returned BOTH position side; check account position mode"
        );
        return None;
    }

    let market_reference = if position.mark_price > Decimal::ZERO {
        position.mark_price
    } else {
        market
            .and_then(|state| state.reference_price)
            .unwrap_or(Decimal::ZERO)
    };

    let position_notional = if position.notional.abs() > Decimal::ZERO {
        position.notional.abs()
    } else {
        position.position_amt.abs() * market_reference
    };

    if position_notional < config.risk.min_position_notional_usdt {
        return None;
    }

    let liquidation_buffer_ratio =
        liquidation_buffer_ratio(market_reference, position.liquidation_price);
    let symbol_loss_ratio = negative_pnl_ratio(position.unrealized_profit, position_notional);

    let mut triggers = Vec::new();
    if let Some(buffer_ratio) = liquidation_buffer_ratio {
        if buffer_ratio <= config.risk.min_liquidation_buffer_ratio {
            triggers.push(format!(
                "liq_buffer={} <= {}",
                buffer_ratio, config.risk.min_liquidation_buffer_ratio
            ));
        }
    }

    if symbol_loss_ratio >= config.risk.max_symbol_loss_ratio {
        triggers.push(format!(
            "symbol_loss_ratio={} >= {}",
            symbol_loss_ratio, config.risk.max_symbol_loss_ratio
        ));
    }

    if account_loss_ratio >= config.risk.max_account_loss_ratio {
        triggers.push(format!(
            "account_loss_ratio={} >= {}",
            account_loss_ratio, config.risk.max_account_loss_ratio
        ));
    }

    if triggers.is_empty() {
        return None;
    }

    if config.hedge.order_type != HedgeOrderType::Market {
        return None;
    }

    let mut quantity = position.position_amt.abs() * config.hedge.hedge_ratio;
    if let Some(max_hedge_notional_usdt) = config.hedge.max_hedge_notional_usdt {
        if market_reference > Decimal::ZERO {
            let max_qty = max_hedge_notional_usdt / market_reference;
            if quantity > max_qty {
                quantity = max_qty;
            }
        }
    }

    if quantity <= Decimal::ZERO {
        return None;
    }

    // Reduce hedge quantity until available balance is sufficient
    // Assume 10x leverage = 10% margin requirement per 1% of notional
    let mut hedge_notional = quantity * market_reference;
    let mut estimated_margin = hedge_notional / Decimal::from(10);
    let min_reduction_factor = Decimal::from_str_exact("0.01").unwrap_or(Decimal::ZERO); // 0.01 minimum
    let mut reduction_factor = Decimal::ONE;

    while estimated_margin > account.available_balance && reduction_factor > min_reduction_factor {
        reduction_factor /= Decimal::from(2); // Halve the reduction factor each iteration
        quantity *= reduction_factor;
        hedge_notional = quantity * market_reference;
        estimated_margin = hedge_notional / Decimal::from(10);
    }

    if quantity <= Decimal::ZERO || estimated_margin > account.available_balance {
        return None; // Cannot hedge even with minimal quantity
    }

    let (side, position_side) = match config.monitor.position_mode {
        PositionMode::OneWay => {
            if position.position_amt > Decimal::ZERO {
                (Side::Sell, None)
            } else {
                (Side::Buy, None)
            }
        }
        PositionMode::Hedge => {
            if position.position_amt > Decimal::ZERO {
                (Side::Sell, Some(BinancePositionSide::Short))
            } else {
                (Side::Buy, Some(BinancePositionSide::Long))
            }
        }
    };

    let mut reason = format!(
        "entry_price={}, wallet_balance={}, margin_balance={}, available_balance={}, {}",
        position.entry_price,
        account.total_wallet_balance,
        account.total_margin_balance,
        account.available_balance,
        triggers.join("; ")
    );

    if let Some(order) = last_order {
        reason.push_str(&format!(
            "; last_order_status={}, last_exec_type={}, last_order_side={:?}, last_order_position_side={}, last_order_qty={}, last_order_filled_qty={}, last_filled_price={}, last_order_client_id={}",
            order.order_status,
            order.execution_type,
            order.side,
            order.position_side.as_str(),
            order.quantity,
            order.filled_accumulated_quantity,
            order.last_filled_price,
            order.client_order_id,
        ));
    }

    Some(HedgeDecision {
        symbol: position.symbol.clone(),
        side,
        position_side,
        quantity,
        reason,
    })
}

fn negative_pnl_ratio(pnl: Decimal, base: Decimal) -> Decimal {
    if pnl >= Decimal::ZERO || base <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (-pnl) / base
    }
}

fn liquidation_buffer_ratio(mark_price: Decimal, liquidation_price: Decimal) -> Option<Decimal> {
    if mark_price <= Decimal::ZERO || liquidation_price <= Decimal::ZERO {
        return None;
    }

    Some((mark_price - liquidation_price).abs() / mark_price)
}

fn cooldown_active(cooldowns: &HashMap<String, Instant>, key: &str, cooldown_secs: u64) -> bool {
    cooldowns
        .get(key)
        .is_some_and(|last_hit| last_hit.elapsed() < Duration::from_secs(cooldown_secs))
}
