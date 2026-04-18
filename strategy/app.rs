use crate::{
    binance::{BinanceClient, PositionSide},
    command::{parse_command, CommandSide, TradeCommand},
    config::load_config,
    telegram::TelegramClient,
};
use barter::logging::init_logging_with_prefix;
use rust_decimal::Decimal;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
struct ExecutionPlan {
    qty: Decimal,
    entry_side: CommandSide,
    entry_position_side: PositionSide,
    reverse_side: CommandSide,
    reverse_position_side: PositionSide,
    tp_price: Decimal,
    sl_price: Decimal,
}

fn build_execution_plan(
    side: CommandSide,
    price: Decimal,
    qty_usdt: Decimal,
    profit_ratio: Decimal,
    stop_ratio: Decimal,
) -> Result<ExecutionPlan, Box<dyn std::error::Error + Send + Sync>> {
    if price <= Decimal::ZERO {
        return Err("价格无效".into());
    }

    let qty = (qty_usdt / price).round_dp(6);
    if qty <= Decimal::ZERO {
        return Err("计算得到的下单数量无效".into());
    }

    let (entry_side, entry_position_side, reverse_side, reverse_position_side) = match side {
        CommandSide::Buy => (
            CommandSide::Buy,
            PositionSide::Long,
            CommandSide::Sell,
            PositionSide::Long,
        ),
        CommandSide::Sell => (
            CommandSide::Sell,
            PositionSide::Short,
            CommandSide::Buy,
            PositionSide::Short,
        ),
    };

    let tp_price = match side {
        CommandSide::Buy => price * (Decimal::ONE + profit_ratio),
        CommandSide::Sell => price * (Decimal::ONE - profit_ratio),
    }
    .round_dp(7);

    let sl_price = match side {
        CommandSide::Buy => price * (Decimal::ONE - stop_ratio),
        CommandSide::Sell => price * (Decimal::ONE + stop_ratio),
    }
    .round_dp(7);

    Ok(ExecutionPlan {
        qty,
        entry_side,
        entry_position_side,
        reverse_side,
        reverse_position_side,
        tp_price,
        sl_price,
    })
}

pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging_with_prefix("binance-futures-strategy.log");

    let config = load_config("strategy/config.json")?;
    let bot_token = config.telegram.bot_token.clone();

    let telegram = TelegramClient::new(&bot_token);
    let binance = BinanceClient::from_env(&config.binance)?;

    let mut offset = 0_i64;
    let mut last_execution_at = Instant::now() - Duration::from_millis(config.strategy.cooldown_ms);

    info!("telegram hedge strategy started");

    loop {
        let updates = telegram.get_updates(offset).await?;
        for update in updates {
            offset = update.update_id + 1;

            if !config.telegram.allowed_chat_ids.is_empty()
                && !config.telegram.allowed_chat_ids.contains(&update.chat_id)
            {
                continue;
            }

            let text = update.text.trim();
            if text.eq_ignore_ascii_case("/help") {
                let _ = telegram
                    .send_message(
                        update.chat_id,
                            "用法: /buy SYMBOL [profit_points] [qty_usdt]\n     /sell SYMBOL [profit_points] [qty_usdt]\n参数均可省略，使用默认配置\n示例: /buy BTCUSDT\n     /buy BTCUSDT 5 20\n状态: /status [SYMBOL]\n订单: /orders [SYMBOL]",
                    )
                    .await;
                continue;
            }

            if text.to_lowercase().starts_with("/status") {
                let status_text = match build_status_message(&binance, &config.strategy, text).await
                {
                    Ok(message) => message,
                    Err(error) => format!("获取状态失败: {error}"),
                };
                let _ = telegram.send_message(update.chat_id, &status_text).await;
                continue;
            }

            if text.to_lowercase().starts_with("/orders") {
                let orders_text = match build_orders_message(&binance, text).await {
                    Ok(message) => message,
                    Err(error) => format!("获取订单失败: {error}"),
                };
                let _ = telegram.send_message(update.chat_id, &orders_text).await;
                continue;
            }

            let command = match parse_command(text, &config.strategy) {
                Ok(cmd) => cmd,
                Err(err) => {
                    let _ = telegram
                        .send_message(update.chat_id, &format!("命令错误: {err}"))
                        .await;
                    continue;
                }
            };

            info!(
                chat_id = update.chat_id,
                raw_command = %text,
                side = ?command.side,
                symbol = %command.symbol,
                profit_ratio = %command.profit_ratio,
                stop_ratio = %command.stop_ratio,
                qty_usdt = %command.qty_usdt,
                dry_run = config.strategy.dry_run,
                "order command received"
            );

            if last_execution_at.elapsed() < Duration::from_millis(config.strategy.cooldown_ms) {
                let _ = telegram
                    .send_message(update.chat_id, "触发冷却中，请稍后再试")
                    .await;
                continue;
            }

            let result = execute_command(
                &binance,
                &telegram,
                update.chat_id,
                &config.strategy,
                &command,
            )
            .await;
            match result {
                Ok(message) => {
                    last_execution_at = Instant::now();
                    info!(
                        symbol = %command.symbol,
                        side = ?command.side,
                        result = %message,
                        "order execution succeeded"
                    );
                    let _ = telegram.send_message(update.chat_id, &message).await;
                }
                Err(error) => {
                    info!(
                        symbol = %command.symbol,
                        side = ?command.side,
                        error = %error,
                        "order execution failed"
                    );
                    error!(?error, "failed to execute command");
                    let _ = telegram
                        .send_message(update.chat_id, &format!("执行失败: {error}"))
                        .await;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(config.telegram.poll_interval_ms)).await;
    }
}

async fn execute_command(
    binance: &BinanceClient,
    telegram: &TelegramClient,
    notify_chat_id: i64,
    strategy: &crate::config::StrategyConfig,
    command: &TradeCommand,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let price = binance.fetch_price(&command.symbol).await?;
    let plan = build_execution_plan(
        command.side,
        price,
        command.qty_usdt,
        command.profit_ratio,
        command.stop_ratio,
    )?;

    let entry_offset_ratio = strategy.entry_offset_bps / Decimal::from(10_000);
    let book_ticker = binance.fetch_book_ticker(&command.symbol).await?;
    let entry_limit_price = match command.side {
        CommandSide::Buy => book_ticker.bid_price * (Decimal::ONE - entry_offset_ratio),
        CommandSide::Sell => book_ticker.ask_price * (Decimal::ONE + entry_offset_ratio),
    }
    .round_dp(7);

    info!(
        symbol = %command.symbol,
        side = ?command.side,
        mark_price = %price,
        bid_price = %book_ticker.bid_price,
        ask_price = %book_ticker.ask_price,
        entry_offset_bps = %strategy.entry_offset_bps,
        entry_limit_price = %entry_limit_price,
        entry_timeout_ms = strategy.entry_timeout_ms,
        qty_usdt = %command.qty_usdt,
        base_qty = %plan.qty,
        tp_price = %plan.tp_price,
        sl_price = %plan.sl_price,
        dry_run = strategy.dry_run,
        "order execution plan"
    );

    if strategy.dry_run {
        info!(
            symbol = %command.symbol,
            qty = %plan.qty,
            entry = ?plan.entry_side,
            entry_limit_price = %entry_limit_price,
            entry_timeout_ms = strategy.entry_timeout_ms,
            tp_price = %plan.tp_price,
            sl_price = %plan.sl_price,
            "dry-run command evaluated"
        );
    } else {
        let now = chrono::Utc::now().timestamp_millis();
        let entry_client_order_id = format!("s_e_{}", now);
        binance
            .place_limit(
                &command.symbol,
                plan.entry_side,
                plan.entry_position_side,
                plan.qty,
                entry_limit_price,
                &entry_client_order_id,
            )
            .await?;

        tokio::time::sleep(Duration::from_millis(strategy.entry_timeout_ms)).await;

        let entry_order = binance
            .fetch_order(&command.symbol, &entry_client_order_id)
            .await?;

        let mut filled_qty = entry_order.executed_qty;
        if entry_order.status == "NEW" || entry_order.status == "PARTIALLY_FILLED" {
            binance
                .cancel_order_by_client_id(&command.symbol, &entry_client_order_id)
                .await?;

            let remaining_qty = (entry_order.orig_qty - entry_order.executed_qty).round_dp(6);
            if remaining_qty > Decimal::ZERO {
                binance
                    .place_market(
                        &command.symbol,
                        plan.entry_side,
                        plan.entry_position_side,
                        remaining_qty,
                        &format!("s_em_{}", now),
                    )
                    .await?;
                filled_qty += remaining_qty;
            }
        }

        if filled_qty <= Decimal::ZERO {
            return Err("入场未成交，未创建止盈止损订单".into());
        }

        maybe_reduce_if_notional_too_high(binance, strategy, command).await?;

        let monitor_binance = binance.clone();
        let monitor_telegram = telegram.clone();
        let monitor_chat_id = notify_chat_id;
        let monitor_symbol = command.symbol.clone();
        let monitor_side = command.side;
        let monitor_reverse_side = plan.reverse_side;
        let monitor_reverse_position_side = plan.reverse_position_side;
        let monitor_tp_price = plan.tp_price;
        let monitor_sl_price = plan.sl_price;
        let monitor_poll_ms = strategy.local_exit_poll_interval_ms;
        let monitor_telegram_for_errors = monitor_telegram.clone();

        tokio::spawn(async move {
            if let Err(error) = monitor_and_exit_locally(
                monitor_binance,
                monitor_telegram,
                monitor_chat_id,
                monitor_symbol,
                monitor_side,
                monitor_reverse_side,
                monitor_reverse_position_side,
                monitor_tp_price,
                monitor_sl_price,
                monitor_poll_ms,
            )
            .await
            {
                error!(?error, "local tp/sl monitor failed");
                let _ = monitor_telegram_for_errors
                    .send_message(monitor_chat_id, &format!("本地止盈止损监控异常: {error}"))
                    .await;
            }
        });

        info!(
            symbol = %command.symbol,
            poll_interval_ms = strategy.local_exit_poll_interval_ms,
            tp_price = %plan.tp_price,
            sl_price = %plan.sl_price,
            "local tp/sl monitor started"
        );
    }

    Ok(format!(
        "已处理 {} {}: qty={}USDT(约{}), TP={}, SL={}, dry_run={} (本地盯盘市价止盈止损)",
        match command.side {
            CommandSide::Buy => "BUY",
            CommandSide::Sell => "SELL",
        },
        command.symbol,
        command.qty_usdt,
        plan.qty,
        plan.tp_price,
        plan.sl_price,
        strategy.dry_run
    ))
}

async fn monitor_and_exit_locally(
    binance: BinanceClient,
    telegram: TelegramClient,
    notify_chat_id: i64,
    symbol: String,
    entry_side: CommandSide,
    exit_side: CommandSide,
    exit_position_side: PositionSide,
    tp_price: Decimal,
    sl_price: Decimal,
    poll_interval_ms: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
        let price = binance.fetch_price(&symbol).await?;

        let trigger = match entry_side {
            CommandSide::Buy => {
                if price >= tp_price {
                    Some("take_profit")
                } else if price <= sl_price {
                    Some("stop_loss")
                } else {
                    None
                }
            }
            CommandSide::Sell => {
                if price <= tp_price {
                    Some("take_profit")
                } else if price >= sl_price {
                    Some("stop_loss")
                } else {
                    None
                }
            }
        };

        let Some(trigger) = trigger else {
            continue;
        };

        let positions = binance.fetch_positions(&symbol).await?;
        let close_qty = positions
            .iter()
            .filter_map(|position| {
                let same_side = match exit_position_side {
                    PositionSide::Long => position.position_side == "LONG",
                    PositionSide::Short => position.position_side == "SHORT",
                };

                if same_side && position.position_amt.abs() > Decimal::ZERO {
                    Some(position.position_amt.abs())
                } else {
                    None
                }
            })
            .fold(Decimal::ZERO, |acc, qty| acc + qty);

        if close_qty <= Decimal::ZERO {
            warn!(
                symbol = %symbol,
                trigger = trigger,
                price = %price,
                "local tp/sl triggered but no closable position found"
            );

            let _ = telegram
                .send_message(
                    notify_chat_id,
                    &format!(
                        "{} 触发{}但未找到可平仓位，当前价={}。请手动检查仓位。",
                        symbol,
                        if trigger == "take_profit" {
                            "止盈"
                        } else {
                            "止损"
                        },
                        price
                    ),
                )
                .await;
            return Ok(());
        }

        let ts = chrono::Utc::now().timestamp_millis();
        let client_order_id = format!(
            "s_x_{}_{}",
            if trigger == "take_profit" { "tp" } else { "sl" },
            ts
        );

        let order_position_side = if trigger == "stop_loss" {
            match exit_position_side {
                PositionSide::Long => PositionSide::Short,
                PositionSide::Short => PositionSide::Long,
            }
        } else {
            exit_position_side
        };

        if trigger == "stop_loss" {
            info!(
                symbol = %symbol,
                trigger_price = %price,
                qty = %close_qty,
                side = ?exit_side,
                from_position_side = %match exit_position_side { PositionSide::Long => "LONG", PositionSide::Short => "SHORT" },
                to_position_side = %match order_position_side { PositionSide::Long => "LONG", PositionSide::Short => "SHORT" },
                "stop-loss reverse order mapping"
            );
        }

        binance
            .place_market(
                &symbol,
                exit_side,
                order_position_side,
                close_qty,
                &client_order_id,
            )
            .await?;

        info!(
            symbol = %symbol,
            trigger = trigger,
            trigger_price = %price,
            exit_side = ?exit_side,
            order_position_side = %match order_position_side { PositionSide::Long => "LONG", PositionSide::Short => "SHORT" },
            close_qty = %close_qty,
            client_order_id = %client_order_id,
            "local tp/sl market exit order submitted"
        );

        let action = if trigger == "take_profit" {
            "市价平仓"
        } else {
            "市价下等量反向单"
        };

        let _ = telegram
            .send_message(
                notify_chat_id,
                &format!(
                    "{} 已触发{}并{}：price={}，qty={}，side={:?}，position_side={}",
                    symbol,
                    if trigger == "take_profit" {
                        "止盈"
                    } else {
                        "止损"
                    },
                    action,
                    price,
                    close_qty,
                    exit_side,
                    match order_position_side {
                        PositionSide::Long => "LONG",
                        PositionSide::Short => "SHORT",
                    }
                ),
            )
            .await;

        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_execution_plan_buy_has_correct_tp_sl_and_sides() {
        let plan = build_execution_plan(
            CommandSide::Buy,
            Decimal::from(100),
            Decimal::from(20),
            Decimal::new(5, 2),
            Decimal::new(1, 2),
        )
        .expect("plan should be built");

        assert_eq!(plan.qty, Decimal::new(2, 1));
        assert!(matches!(plan.entry_side, CommandSide::Buy));
        assert!(matches!(plan.entry_position_side, PositionSide::Long));
        assert!(matches!(plan.reverse_side, CommandSide::Sell));
        assert!(matches!(plan.reverse_position_side, PositionSide::Long));
        assert_eq!(plan.tp_price, Decimal::from(105));
        assert_eq!(plan.sl_price, Decimal::from(99));
    }

    #[test]
    fn build_execution_plan_sell_has_correct_tp_sl_and_sides() {
        let plan = build_execution_plan(
            CommandSide::Sell,
            Decimal::from(100),
            Decimal::from(20),
            Decimal::new(5, 2),
            Decimal::new(1, 2),
        )
        .expect("plan should be built");

        assert_eq!(plan.qty, Decimal::new(2, 1));
        assert!(matches!(plan.entry_side, CommandSide::Sell));
        assert!(matches!(plan.entry_position_side, PositionSide::Short));
        assert!(matches!(plan.reverse_side, CommandSide::Buy));
        assert!(matches!(plan.reverse_position_side, PositionSide::Short));
        assert_eq!(plan.tp_price, Decimal::from(95));
        assert_eq!(plan.sl_price, Decimal::from(101));
    }

    #[test]
    fn build_execution_plan_rejects_zero_or_negative_price() {
        let err = build_execution_plan(
            CommandSide::Buy,
            Decimal::ZERO,
            Decimal::from(20),
            Decimal::new(5, 2),
            Decimal::new(1, 2),
        )
        .expect_err("zero price should fail");

        assert!(err.to_string().contains("价格无效"));
    }
}

async fn maybe_reduce_if_notional_too_high(
    binance: &BinanceClient,
    strategy: &crate::config::StrategyConfig,
    command: &TradeCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let positions = binance.fetch_positions(&command.symbol).await?;

    let total_notional = positions
        .iter()
        .map(|position| position.notional.abs())
        .fold(Decimal::ZERO, |acc, value| acc + value);

    if total_notional <= strategy.max_dual_side_notional_usdt {
        return Ok(());
    }

    warn!(
        symbol = %command.symbol,
        total_notional = %total_notional,
        threshold = %strategy.max_dual_side_notional_usdt,
        "dual-side notional exceeded threshold, reducing half"
    );

    for position in positions {
        if position.position_amt.is_zero() {
            continue;
        }

        let reduce_qty =
            (position.position_amt.abs() * strategy.reduce_fraction_when_over_limit).round_dp(6);
        if reduce_qty <= Decimal::ZERO {
            continue;
        }

        let (side, position_side) = match position.position_side.as_str() {
            "LONG" => (CommandSide::Sell, PositionSide::Long),
            "SHORT" => (CommandSide::Buy, PositionSide::Short),
            _ => continue,
        };

        let now = chrono::Utc::now().timestamp_millis();
        binance
            .place_market(
                &position.symbol,
                side,
                position_side,
                reduce_qty,
                &format!("s_r_{}", now),
            )
            .await?;
    }

    Ok(())
}

async fn build_status_message(
    binance: &BinanceClient,
    strategy: &crate::config::StrategyConfig,
    input: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let symbol = input
        .split_whitespace()
        .nth(1)
        .map(|value| value.trim_start_matches('$').to_uppercase());

    let mut message = format!(
        "策略状态\n- dry_run={}\n- default_profit_points={}\n- default_stop_points={}\n- default_qty_usdt={}\n- entry_offset_bps={}\n- entry_timeout_ms={}\n- local_exit_poll_interval_ms={}\n- max_dual_side_notional_usdt={}\n- reduce_fraction_when_over_limit={}\n- cooldown_ms={}",
        strategy.dry_run,
        strategy.default_profit_points,
        strategy.default_stop_points,
        strategy.default_qty_usdt,
        strategy.entry_offset_bps,
        strategy.entry_timeout_ms,
        strategy.local_exit_poll_interval_ms,
        strategy.max_dual_side_notional_usdt,
        strategy.reduce_fraction_when_over_limit,
        strategy.cooldown_ms
    );

    if let Some(symbol) = symbol {
        let positions = binance.fetch_positions(&symbol).await?;
        let total_notional = positions
            .iter()
            .map(|position| position.notional.abs())
            .fold(Decimal::ZERO, |acc, value| acc + value);

        let price = binance.fetch_price(&symbol).await.unwrap_or(Decimal::ZERO);

        message.push_str(&format!(
            "\n\n{} 仓位\n- mark_price={}\n- total_notional={}\n- over_limit={} (>{})",
            symbol,
            price,
            total_notional,
            total_notional > strategy.max_dual_side_notional_usdt,
            strategy.max_dual_side_notional_usdt,
        ));

        for position in positions {
            if position.position_amt.is_zero() {
                continue;
            }

            message.push_str(&format!(
                "\n- {} qty={} notional={}",
                position.position_side, position.position_amt, position.notional,
            ));
        }
    }

    Ok(message)
}

async fn build_orders_message(
    binance: &BinanceClient,
    input: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let symbol = input
        .split_whitespace()
        .nth(1)
        .map(|value| value.trim_start_matches('$').to_uppercase());

    let orders = binance.fetch_open_orders(symbol.as_deref()).await?;
    let strategy_orders = orders
        .into_iter()
        .filter(|order| order.client_order_id.starts_with("s_"))
        .collect::<Vec<_>>();

    if strategy_orders.is_empty() {
        return Ok(match symbol {
            Some(symbol) => format!("策略订单\n- {}: 当前没有未成交订单", symbol),
            None => "策略订单\n- 当前没有未成交订单".to_owned(),
        });
    }

    let total = strategy_orders.len();
    let shown = total.min(20);

    let mut message = match symbol {
        Some(symbol) => format!("策略订单 ({}，共{}条，展示前{}条)", symbol, total, shown),
        None => format!("策略订单 (全symbol，共{}条，展示前{}条)", total, shown),
    };

    for order in strategy_orders.iter().take(shown) {
        message.push_str(&format!(
            "\n- [{}] id={} {} {} {} {} qty={}/{} price={} stop={} cid={}",
            order.symbol,
            order.order_id,
            order.side,
            order.position_side,
            order.order_type,
            order.status,
            order.executed_qty,
            order.orig_qty,
            order.price,
            order.stop_price,
            order.client_order_id,
        ));
    }

    if total > shown {
        message.push_str(&format!("\n- 其余 {} 条已省略", total - shown));
    }

    Ok(message)
}
