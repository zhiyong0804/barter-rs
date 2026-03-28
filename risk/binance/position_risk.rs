use crate::binance::{
    BinanceError, BinancePositionSide,
    model::{RawFuturesPositionRisk, parse_decimal},
};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct FuturesPositionRisk {
    pub symbol: String,
    pub position_amt: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub liquidation_price: Decimal,
    pub unrealized_profit: Decimal,
    pub notional: Decimal,
    pub margin_type: String,
    pub position_side: BinancePositionSide,
}

impl FuturesPositionRisk {
    pub fn is_open(&self) -> bool {
        !self.position_amt.is_zero()
    }

    pub fn is_cross(&self) -> bool {
        self.margin_type.eq_ignore_ascii_case("cross")
    }
}

impl TryFrom<RawFuturesPositionRisk> for FuturesPositionRisk {
    type Error = BinanceError;

    fn try_from(value: RawFuturesPositionRisk) -> Result<Self, Self::Error> {
        Ok(Self {
            symbol: value.symbol,
            position_amt: parse_decimal("positionAmt", &value.position_amt)?,
            entry_price: parse_decimal("entryPrice", &value.entry_price)?,
            mark_price: parse_decimal("markPrice", &value.mark_price)?,
            liquidation_price: parse_decimal("liquidationPrice", &value.liquidation_price)?,
            unrealized_profit: parse_decimal("unRealizedProfit", &value.un_realized_profit)?,
            notional: parse_decimal("notional", &value.notional)?,
            margin_type: value.margin_type,
            position_side: BinancePositionSide::from_exchange_str(&value.position_side)?,
        })
    }
}
