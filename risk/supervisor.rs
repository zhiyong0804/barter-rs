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
use tracing::{error, info};

#[derive(Debug, Clone)]
struct SymbolExposure {
    symbol: String,
    market_reference: Decimal,
    net_qty: Decimal,
    net_notional: Decimal,
    margin_exposure: Decimal,
    net_unrealized_profit: Decimal,
    entry_price: Decimal,
    liquidation_price: Decimal,
}

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
            .filter(|position| {
                self.config.monitor.margin_mode != MarginMode::Cross || position.is_cross()
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut positions_by_symbol: HashMap<String, Vec<FuturesPositionRisk>> = HashMap::new();
        for position in positions {
            positions_by_symbol
                .entry(position.symbol.clone())
                .or_default()
                .push(position);
        }

        let mut exposures_by_symbol: HashMap<String, SymbolExposure> = HashMap::new();
        for (symbol, symbol_positions) in &positions_by_symbol {
            if let Some(exposure) =
                build_symbol_exposure(symbol, symbol_positions, market_state.get(symbol))
            {
                exposures_by_symbol.insert(symbol.clone(), exposure);
            }
        }

        let total_margin_exposure = exposures_by_symbol
            .values()
            .fold(Decimal::ZERO, |acc, exposure| {
                acc + exposure.margin_exposure
            });
        let total_position_to_funds_ratio = if account.total_wallet_balance > Decimal::ZERO {
            total_margin_exposure / account.total_wallet_balance
        } else {
            Decimal::ZERO
        };

        if total_position_to_funds_ratio >= self.config.risk.max_position_to_funds_ratio {
            if let Some(last_order) = risk_state.latest_order.as_ref() {
                if let Some(exposure) = exposures_by_symbol.get(&last_order.symbol) {
                    if let Some(decision) = evaluate_close_recent_order_decision(
                        &self.config,
                        &account,
                        exposure,
                        last_order,
                        total_position_to_funds_ratio,
                        total_margin_exposure,
                    ) {
                        let cooldown_key = "GLOBAL:CLOSE_RECENT".to_owned();
                        if !cooldown_active(
                            cooldowns,
                            &cooldown_key,
                            self.config.hedge.cooldown_secs,
                        ) {
                            self.execute_decision(&decision).await?;
                            cooldowns.insert(cooldown_key, Instant::now());
                        }
                        return Ok(());
                    }
                }
            }

            tracing::warn!(
                total_margin_exposure = %total_margin_exposure,
                total_wallet_balance = %account.total_wallet_balance,
                total_position_to_funds_ratio = %total_position_to_funds_ratio,
                max_position_to_funds_ratio = %self.config.risk.max_position_to_funds_ratio,
                "Total position-to-funds ratio exceeded threshold but no closable latest order found"
            );
        }

        for (symbol, exposure) in exposures_by_symbol {
            let Some(decision) = evaluate_hedge_decision(
                &self.config,
                &account,
                &exposure,
                risk_state.last_orders.get(&symbol),
                account_loss_ratio,
            ) else {
                continue;
            };

            let cooldown_key = format!("{}:NET", symbol);
            if cooldown_active(cooldowns, &cooldown_key, self.config.hedge.cooldown_secs) {
                continue;
            }

            self.execute_decision(&decision).await?;

            cooldowns.insert(cooldown_key, Instant::now());
        }

        Ok(())
    }

    async fn execute_decision(
        &self,
        decision: &HedgeDecision,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.config.monitor.dry_run {
            info!(
                symbol = %decision.symbol,
                side = ?decision.side,
                quantity = %decision.quantity,
                reason = %decision.reason,
                "Dry-run hedge triggered"
            );
            return Ok(());
        }

        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let symbol_prefix = decision.symbol.chars().take(3).collect::<String>();
        let client_order_id = format!(
            "h{}{}",
            symbol_prefix.to_lowercase(),
            timestamp_ms % 1_000_000_000 // last 9 digits for uniqueness
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

        Ok(())
    }
}

fn evaluate_hedge_decision(
    config: &AppConfig,
    account: &FuturesAccountInformation,
    exposure: &SymbolExposure,
    last_order: Option<&OrderTradeUpdate>,
    account_loss_ratio: Decimal,
) -> Option<HedgeDecision> {
    tracing::debug!(
        symbol = %exposure.symbol,
        net_qty = %exposure.net_qty,
        market_reference = %exposure.market_reference,
        net_notional = %exposure.net_notional,
        margin_exposure = %exposure.margin_exposure,
        net_unrealized_profit = %exposure.net_unrealized_profit,
        liquidation_price = %exposure.liquidation_price,
        "Net exposure evaluation starting"
    );

    if exposure.margin_exposure < config.risk.min_position_notional_usdt {
        tracing::debug!(
            symbol = %exposure.symbol,
            margin_exposure = %exposure.margin_exposure,
            min_position_notional_usdt = %config.risk.min_position_notional_usdt,
            "Margin exposure below minimum threshold, skipping"
        );
        return None;
    }

    let liquidation_buffer_ratio =
        liquidation_buffer_ratio(exposure.market_reference, exposure.liquidation_price);
    let symbol_loss_ratio =
        negative_pnl_ratio(exposure.net_unrealized_profit, exposure.margin_exposure);

    tracing::debug!(
        symbol = %exposure.symbol,
        account_loss_ratio = %account_loss_ratio,
        symbol_loss_ratio = %symbol_loss_ratio,
        liquidation_buffer_ratio = ?liquidation_buffer_ratio,
        "Calculated net exposure loss ratios"
    );

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
        tracing::debug!(
            symbol = %exposure.symbol,
            "No hedge triggers met"
        );
        return None;
    }

    tracing::info!(
        symbol = %exposure.symbol,
        triggers = ?triggers,
        "Hedge triggers detected"
    );

    if config.hedge.order_type != HedgeOrderType::Market {
        tracing::warn!(
            symbol = %exposure.symbol,
            order_type = ?config.hedge.order_type,
            "Hedge order type is not Market, skipping"
        );
        return None;
    }

    if exposure.net_qty.is_zero() {
        tracing::debug!(
            symbol = %exposure.symbol,
            "Net exposure is zero, skipping"
        );
        return None;
    }

    let mut quantity = exposure.net_qty.abs() * config.hedge.hedge_ratio;
    if let Some(max_hedge_notional_usdt) = config.hedge.max_hedge_notional_usdt {
        if exposure.market_reference > Decimal::ZERO {
            let max_qty = max_hedge_notional_usdt / exposure.market_reference;
            if quantity > max_qty {
                quantity = max_qty;
            }
        }
    }

    if quantity <= Decimal::ZERO {
        tracing::debug!(
            symbol = %exposure.symbol,
            quantity = %quantity,
            "Calculated hedge quantity is zero or negative, skipping"
        );
        return None;
    }

    // Reduce hedge quantity until available balance is sufficient
    // Assume 10x leverage = 10% margin requirement per 1% of notional
    let mut hedge_notional = quantity * exposure.market_reference;
    let mut estimated_margin = hedge_notional / Decimal::from(10);
    let min_reduction_factor = Decimal::from_str_exact("0.01").unwrap_or(Decimal::ZERO); // 0.01 minimum
    let mut reduction_factor = Decimal::ONE;

    while estimated_margin > account.available_balance && reduction_factor > min_reduction_factor {
        reduction_factor /= Decimal::from(2); // Halve the reduction factor each iteration
        quantity *= reduction_factor;
        hedge_notional = quantity * exposure.market_reference;
        estimated_margin = hedge_notional / Decimal::from(10);
    }

    if quantity <= Decimal::ZERO || estimated_margin > account.available_balance {
        tracing::warn!(
            symbol = %exposure.symbol,
            quantity = %quantity,
            estimated_margin = %estimated_margin,
            available_balance = %account.available_balance,
            "Insufficient margin for hedge even after reduction, skipping"
        );
        return None; // Cannot hedge even with minimal quantity
    }

    let (side, position_side) = match config.monitor.position_mode {
        PositionMode::OneWay => {
            if exposure.net_qty > Decimal::ZERO {
                (Side::Sell, None)
            } else {
                (Side::Buy, None)
            }
        }
        PositionMode::Hedge => {
            if exposure.net_qty > Decimal::ZERO {
                // Net long → open Short hedge (SELL + SHORT)
                (Side::Sell, Some(BinancePositionSide::Short))
            } else {
                // Net short → open Long hedge (BUY + LONG)
                (Side::Buy, Some(BinancePositionSide::Long))
            }
        }
    };

    let mut reason = format!(
        "entry_price={}, wallet_balance={}, margin_balance={}, available_balance={}, net_qty={}, net_notional={}, margin_exposure={}, net_unrealized_profit={}, {}",
        exposure.entry_price,
        account.total_wallet_balance,
        account.total_margin_balance,
        account.available_balance,
        exposure.net_qty,
        exposure.net_notional,
        exposure.margin_exposure,
        exposure.net_unrealized_profit,
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

    tracing::info!(
        symbol = %exposure.symbol,
        side = ?side,
        position_side = ?position_side,
        quantity = %quantity,
        "HedgeDecision created and will be returned"
    );

    Some(HedgeDecision {
        symbol: exposure.symbol.clone(),
        side,
        position_side,
        quantity,
        reason,
    })
}

fn evaluate_close_recent_order_decision(
    config: &AppConfig,
    account: &FuturesAccountInformation,
    exposure: &SymbolExposure,
    last_order: &OrderTradeUpdate,
    total_position_to_funds_ratio: Decimal,
    total_margin_exposure: Decimal,
) -> Option<HedgeDecision> {
    let recent_qty = if last_order.filled_accumulated_quantity > Decimal::ZERO {
        last_order.filled_accumulated_quantity
    } else {
        last_order.quantity
    };

    if recent_qty <= Decimal::ZERO {
        tracing::warn!(
            symbol = %exposure.symbol,
            total_position_to_funds_ratio = %total_position_to_funds_ratio,
            last_order_status = %last_order.order_status,
            last_exec_type = %last_order.execution_type,
            "Exposure above threshold but last order quantity is zero, skipping close-recent rule"
        );
        return None;
    }

    let quantity = recent_qty.min(exposure.net_qty.abs());
    if quantity <= Decimal::ZERO {
        return None;
    }

    let (side, position_side) = match config.monitor.position_mode {
        PositionMode::OneWay => {
            if exposure.net_qty > Decimal::ZERO {
                (Side::Sell, None)
            } else {
                (Side::Buy, None)
            }
        }
        PositionMode::Hedge => match last_order.position_side {
            BinancePositionSide::Long => (Side::Sell, Some(BinancePositionSide::Long)),
            BinancePositionSide::Short => (Side::Buy, Some(BinancePositionSide::Short)),
            BinancePositionSide::Both => {
                if exposure.net_qty > Decimal::ZERO {
                    (Side::Sell, Some(BinancePositionSide::Long))
                } else {
                    (Side::Buy, Some(BinancePositionSide::Short))
                }
            }
        },
    };

    tracing::warn!(
        symbol = %exposure.symbol,
        quantity = %quantity,
        side = ?side,
        position_side = ?position_side,
        total_position_to_funds_ratio = %total_position_to_funds_ratio,
        max_position_to_funds_ratio = %config.risk.max_position_to_funds_ratio,
        total_margin_exposure = %total_margin_exposure,
        "Close-recent-order rule triggered"
    );

    Some(HedgeDecision {
        symbol: exposure.symbol.clone(),
        side,
        position_side,
        quantity,
        reason: format!(
            "close_recent_order: total_position_to_funds_ratio={} >= {}, wallet_balance={}, total_margin_exposure={}, symbol_margin_exposure={}, recent_order_side={:?}, recent_order_position_side={}, recent_order_status={}, recent_order_exec_type={}, recent_order_qty={}, recent_order_filled_qty={}, recent_order_client_id={}",
            total_position_to_funds_ratio,
            config.risk.max_position_to_funds_ratio,
            account.total_wallet_balance,
            total_margin_exposure,
            exposure.margin_exposure,
            last_order.side,
            last_order.position_side.as_str(),
            last_order.order_status,
            last_order.execution_type,
            last_order.quantity,
            last_order.filled_accumulated_quantity,
            last_order.client_order_id,
        ),
    })
}

fn build_symbol_exposure(
    symbol: &str,
    positions: &[FuturesPositionRisk],
    market: Option<&MarketSnapshot>,
) -> Option<SymbolExposure> {
    if positions.is_empty() {
        return None;
    }

    let market_reference = positions
        .iter()
        .find(|position| position.mark_price > Decimal::ZERO)
        .map(|position| position.mark_price)
        .or_else(|| market.and_then(|state| state.reference_price))
        .unwrap_or(Decimal::ZERO);

    let net_qty = positions
        .iter()
        .fold(Decimal::ZERO, |acc, position| acc + signed_qty(position));

    let net_signed_notional = positions.iter().fold(Decimal::ZERO, |acc, position| {
        acc + signed_notional(position, market_reference)
    });

    let margin_exposure = positions.iter().fold(Decimal::ZERO, |acc, position| {
        let abs_notional = signed_notional(position, market_reference).abs();
        acc + abs_notional
    });

    let net_notional = if market_reference > Decimal::ZERO {
        net_qty.abs() * market_reference
    } else {
        net_signed_notional.abs()
    };

    let net_unrealized_profit = positions.iter().fold(Decimal::ZERO, |acc, position| {
        acc + position.unrealized_profit
    });

    let representative = if net_qty > Decimal::ZERO {
        positions
            .iter()
            .filter(|position| signed_qty(position) > Decimal::ZERO)
            .max_by_key(|position| position.position_amt.abs())
    } else if net_qty < Decimal::ZERO {
        positions
            .iter()
            .filter(|position| signed_qty(position) < Decimal::ZERO)
            .max_by_key(|position| position.position_amt.abs())
    } else {
        positions
            .iter()
            .max_by_key(|position| position.position_amt.abs())
    };

    let entry_price = representative
        .map(|position| position.entry_price)
        .unwrap_or(Decimal::ZERO);
    let liquidation_price = representative
        .map(|position| position.liquidation_price)
        .unwrap_or(Decimal::ZERO);

    Some(SymbolExposure {
        symbol: symbol.to_owned(),
        market_reference,
        net_qty,
        net_notional,
        margin_exposure,
        net_unrealized_profit,
        entry_price,
        liquidation_price,
    })
}

fn signed_qty(position: &FuturesPositionRisk) -> Decimal {
    match position.position_side {
        BinancePositionSide::Long => position.position_amt.abs(),
        BinancePositionSide::Short => -position.position_amt.abs(),
        BinancePositionSide::Both => position.position_amt,
    }
}

fn signed_notional(position: &FuturesPositionRisk, market_reference: Decimal) -> Decimal {
    let abs_notional = if position.notional.abs() > Decimal::ZERO {
        position.notional.abs()
    } else {
        position.position_amt.abs() * market_reference
    };

    match position.position_side {
        BinancePositionSide::Long => abs_notional,
        BinancePositionSide::Short => -abs_notional,
        BinancePositionSide::Both => {
            if position.notional != Decimal::ZERO {
                position.notional
            } else {
                position.position_amt * market_reference
            }
        }
    }
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
