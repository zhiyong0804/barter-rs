use crate::binance::{BinanceError, model::{RawFuturesAccountInformation, parse_decimal}};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct FuturesAccountInformation {
    pub total_wallet_balance: Decimal,
    pub total_unrealized_profit: Decimal,
    pub total_margin_balance: Decimal,
    pub available_balance: Decimal,
}

impl TryFrom<RawFuturesAccountInformation> for FuturesAccountInformation {
    type Error = BinanceError;

    fn try_from(value: RawFuturesAccountInformation) -> Result<Self, Self::Error> {
        Ok(Self {
            total_wallet_balance: parse_decimal("totalWalletBalance", &value.total_wallet_balance)?,
            total_unrealized_profit: parse_decimal(
                "totalUnrealizedProfit",
                &value.total_unrealized_profit,
            )?,
            total_margin_balance: parse_decimal("totalMarginBalance", &value.total_margin_balance)?,
            available_balance: parse_decimal("availableBalance", &value.available_balance)?,
        })
    }
}