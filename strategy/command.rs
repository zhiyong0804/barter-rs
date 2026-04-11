use crate::config::StrategyConfig;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy)]
pub enum CommandSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct TradeCommand {
    pub side: CommandSide,
    pub symbol: String,
    pub profit_ratio: Decimal,
    pub stop_ratio: Decimal,
    pub qty_usdt: Decimal,
}

pub fn parse_command(input: &str, strategy: &StrategyConfig) -> Result<TradeCommand, String> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 2 {
        return Err("格式错误。示例: /buy BTCUSDT [profit_points] [qty_usdt]".to_owned());
    }

    // strip leading '/' (slash commands like /buy) and bot mentions like /buy@botname
    let first = parts[0].trim_start_matches('/');
    let first = first.split('@').next().unwrap_or(first);
    let side = match first.to_lowercase().as_str() {
        "buy" => CommandSide::Buy,
        "sell" => CommandSide::Sell,
        _ => return Err("首个参数必须是 buy 或 sell".to_owned()),
    };

    let symbol = parts[1].trim_start_matches('$').to_uppercase();
    if symbol.is_empty() {
        return Err("symbol 不能为空".to_owned());
    }

    let profit_points = if let Some(value) = parts.get(2) {
        value
            .parse::<Decimal>()
            .map_err(|_| "profit 必须是数字".to_owned())?
    } else {
        strategy.default_profit_points
    };

    let qty_usdt = if let Some(value) = parts.get(3) {
        value
            .parse::<Decimal>()
            .map_err(|_| "qty 必须是数字".to_owned())?
    } else {
        strategy.default_qty_usdt
    };

    if qty_usdt <= Decimal::ZERO {
        return Err("qty 必须 > 0".to_owned());
    }

    let profit_ratio = points_to_ratio(profit_points);
    let stop_ratio = points_to_ratio(strategy.default_stop_points);

    Ok(TradeCommand {
        side,
        symbol,
        profit_ratio,
        stop_ratio,
        qty_usdt,
    })
}

fn points_to_ratio(points: Decimal) -> Decimal {
    if points > Decimal::ONE {
        points / Decimal::from(100)
    } else {
        points
    }
}
