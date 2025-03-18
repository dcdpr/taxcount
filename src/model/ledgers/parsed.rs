use crate::model::ledgers::rows::{LedgerRow, LedgerRowDeposit, LedgerRowTypical, TradeRow};
use crate::model::pairs::{get_asset_pair, Trade};
use crate::util::{fifo::FIFO, year_ext::GetYear};
use crate::{basis::AssetName, model::KrakenAmount};
use chrono::{DateTime, Datelike as _, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[cfg(test)]
mod prop_tests;

#[cfg_attr(test, derive(Deserialize, Eq, PartialEq))]
#[derive(Debug, Error)]
pub enum ParseLedgerError {
    #[error("Second trade row not found")]
    MissingRow,

    #[error("Second row is not a matching trade")]
    RefIdMismatch,

    #[error("Matching trade row not found")]
    MissingTrade,
}

/// `LedgerParsed` is the "final form" for the rows that are parsed from the CSV ledger. These
/// consume possibly more than one row.
#[cfg_attr(test, derive(Deserialize, Eq, PartialEq))]
#[derive(Clone, Debug)]
// TODO: Change the name, since this no longer correlates exactly with `LedgerRow`
pub enum LedgerParsed {
    Trade {
        row_out: LedgerRowTypical,
        row_in: LedgerRowTypical,
    },
    MarginPositionOpen(LedgerRowTypical),
    MarginPositionRollover(LedgerRowTypical),
    MarginPositionClose {
        row_proceeds: LedgerRowTypical,
        row_fee: MarginFeeRow,
        exchange_rate: KrakenAmount,
    },
    MarginPositionSettle {
        row_out: LedgerRowTypical,
        row_in: LedgerRowTypical, // Profit or loss, not necessarily incoming.
    },
    Deposit(LedgerRowDeposit),
    Withdrawal(LedgerRowTypical),
}

/// Enable consistency checks on years.
impl GetYear for LedgerParsed {
    fn get_year(&self) -> i32 {
        self.get_time().year()
    }
}

/// This is a clone of `LedgerRowTypical` except the `balance` field is optional.
#[cfg_attr(test, derive(Eq, PartialEq))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MarginFeeRow {
    pub txid: String,
    pub refid: String,
    pub time: DateTime<Utc>,
    pub amount: KrakenAmount,
    pub fee: KrakenAmount,
    pub balance: Option<KrakenAmount>,
}

impl From<LedgerRowTypical> for MarginFeeRow {
    fn from(value: LedgerRowTypical) -> Self {
        Self {
            txid: value.txid,
            refid: value.refid,
            time: value.time,
            amount: value.amount,
            fee: value.fee,
            balance: Some(value.balance),
        }
    }
}

impl From<&MarginFeeRow> for LedgerRowTypical {
    fn from(value: &MarginFeeRow) -> Self {
        Self {
            txid: value.txid.clone(),
            refid: value.refid.clone(),
            time: value.time,
            amount: value.amount,
            fee: value.fee,
            balance: value.balance.unwrap_or_else(|| {
                KrakenAmount::zero(value.amount.get_asset().as_kraken()).unwrap()
            }),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LedgerTwoRowTrade {
    pub(crate) row_out: LedgerRowTypical,
    pub(crate) row_in: LedgerRowTypical,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LedgerMarginClose {
    pub(crate) row_proceeds: LedgerRowTypical,
    pub(crate) row_fee: LedgerRowTypical,
    pub(crate) exchange_rate: KrakenAmount,
}

impl LedgerParsed {
    pub(crate) fn get_event_name(&self) -> String {
        use AssetName::*;

        match self {
            Self::Trade { row_out, row_in } => {
                let asset_out = row_out.amount.get_asset();
                let asset_in = row_in.amount.get_asset();
                let (_pair, trade) = get_asset_pair(asset_out, asset_in);

                match trade {
                    Trade::Buy => format!("Buy {asset_in} with {asset_out}"),
                    Trade::Sell => format!("Sell {asset_out} for {asset_in}"),
                }
            }

            Self::MarginPositionClose {
                row_proceeds,
                row_fee,
                ..
            } => {
                let asset_proceeds = row_proceeds.amount.get_asset();
                let asset_fee = row_fee.amount.get_asset();
                match asset_proceeds {
                    Chf | Eur | Jpy | Usd => {
                        format!("Close Long, base: {asset_proceeds}, asset: {asset_fee}")
                    }
                    Btc | Eth | EthW | Usdc | Usdt => {
                        format!("Close Short, base: {asset_fee}, asset: {asset_proceeds}")
                    }
                }
            }

            Self::MarginPositionSettle { row_out, row_in } => {
                let asset_out = row_out.amount.get_asset();
                let asset_in = row_in.amount.get_asset();
                match asset_out {
                    Chf | Eur | Jpy | Usd => {
                        format!("Settle Short, base: {asset_out}, asset: {asset_in}")
                    }
                    Btc | Eth | EthW | Usdc | Usdt => {
                        format!("Settle Long, base: {asset_in}, asset: {asset_out}")
                    }
                }
            }

            Self::MarginPositionOpen(lrt) => format!("Open: {}", lrt.fee.get_asset()),
            Self::MarginPositionRollover(lrt) => format!("Rollover: {}", lrt.fee.get_asset()),
            Self::Withdrawal(lrt) => format!("Withdrawal: {}", lrt.amount.get_asset()),
            Self::Deposit(lrt) => format!("Deposit: {}", lrt.fee.get_asset()),
        }
    }

    pub(crate) fn get_time(&self) -> DateTime<Utc> {
        match self {
            Self::Trade { row_out: lrt, .. }
            | Self::MarginPositionSettle { row_out: lrt, .. }
            | Self::MarginPositionOpen(lrt)
            | Self::MarginPositionRollover(lrt)
            | Self::Withdrawal(lrt)
            | Self::MarginPositionClose {
                row_proceeds: lrt, ..
            } => lrt.time,

            Self::Deposit(lrd) => lrd.time,
        }
    }
}

impl FIFO<LedgerRow> {
    pub fn parse(
        mut self,
        trades: &FIFO<TradeRow>,
    ) -> Result<FIFO<LedgerParsed>, ParseLedgerError> {
        let mut parsed = FIFO::new();
        let trades = HashMap::from_iter(trades.iter().map(|row| (row.txid.to_string(), row)));

        while let Some(ledger) = self.pop_front() {
            let lp = match ledger {
                LedgerRow::DepositRequest(_) => continue,
                // Transfer spot from futures is treated the same as a deposit for tax reporting.
                // TODO: "Transfer" needs to be treated as income.
                LedgerRow::DepositFulfilled(lrd) | LedgerRow::TransferFutures(lrd) => {
                    LedgerParsed::Deposit(lrd)
                }
                LedgerRow::WithdrawalRequest(_) => continue,
                LedgerRow::WithdrawalFulfilled(lrt) => LedgerParsed::Withdrawal(lrt),
                LedgerRow::Rollover(lrt) => LedgerParsed::MarginPositionRollover(lrt),
                LedgerRow::Trade(row) => self.snarf_matching_trade_row(row)?,
                LedgerRow::Margin(row) => self.snarf_matching_margin_row(row, &trades)?,
                LedgerRow::SettlePosition(row) => self.snarf_matching_settle_row(row)?,
            };
            parsed.append_back(lp);
        }

        Ok(parsed)
    }

    fn snarf_matching_trade_row(
        &mut self,
        row_out: LedgerRowTypical,
    ) -> Result<LedgerParsed, ParseLedgerError> {
        let row_in = match self.pop_front().ok_or(ParseLedgerError::MissingRow)? {
            LedgerRow::Trade(lrt) => lrt,
            _ => return Err(ParseLedgerError::RefIdMismatch),
        };

        if row_out.refid != row_in.refid {
            return Err(ParseLedgerError::RefIdMismatch);
        }

        Ok(LedgerParsed::Trade { row_out, row_in })
    }

    fn snarf_matching_margin_row(
        &mut self,
        row_proceeds: LedgerRowTypical,
        trades: &HashMap<String, &TradeRow>,
    ) -> Result<LedgerParsed, ParseLedgerError> {
        let check_degenerate = if let Some(LedgerRow::Margin(lrt)) = self.peek_front() {
            row_proceeds.refid != lrt.refid
        } else {
            // If this is the last ledger line, _or_ not a "margin" row, then it needs to be checked
            // for degenerate close.
            true
        };

        if check_degenerate {
            if let Some(trade) = trades.get(&row_proceeds.refid) {
                if trade.misc.iter().any(|s| s == "closing") {
                    let fee_asset = kraken_asset_from_pair(&trade.pair);
                    let zero = KrakenAmount::zero(fee_asset).unwrap();
                    let row_fee = MarginFeeRow {
                        txid: row_proceeds.txid.clone(),
                        refid: row_proceeds.refid.clone(),
                        time: row_proceeds.time,
                        amount: zero,
                        fee: zero,
                        balance: None,
                    };

                    // Handle elided "margin fee" rows for closing trades.
                    return Ok(LedgerParsed::MarginPositionClose {
                        row_proceeds,
                        row_fee,
                        exchange_rate: trade.price,
                    });
                }
            }

            // If the next ledger line is a different ref-id, then this is an open
            return Ok(LedgerParsed::MarginPositionOpen(row_proceeds));
        }

        let row_fee = match self.pop_front().unwrap() {
            LedgerRow::Margin(lrt) => lrt,
            _ => unreachable!(),
        };

        let exchange_rate = trades
            .get(&row_proceeds.refid)
            .map(|trade| trade.price)
            .ok_or(ParseLedgerError::MissingTrade)?;

        Ok(LedgerParsed::MarginPositionClose {
            row_proceeds,
            row_fee: row_fee.into(),
            exchange_rate,
        })
    }

    fn snarf_matching_settle_row(
        &mut self,
        row_out: LedgerRowTypical,
    ) -> Result<LedgerParsed, ParseLedgerError> {
        let row_in = match self.pop_front().ok_or(ParseLedgerError::MissingRow)? {
            LedgerRow::SettlePosition(lrt) => lrt,
            _ => return Err(ParseLedgerError::RefIdMismatch),
        };

        if row_out.refid != row_in.refid {
            return Err(ParseLedgerError::RefIdMismatch);
        }

        Ok(LedgerParsed::MarginPositionSettle { row_out, row_in })
    }
}

// TODO: Deduplicate with `Pair`.
/// Returns the Kraken asset name from a pair (the pair's base). Used for constructing a zero
/// `KrakenAmount` for the asset.
fn kraken_asset_from_pair(pair: &str) -> &str {
    match pair {
        "USDCCHF" | "USDC/CHF" | "USDCEUR" | "USDC/EUR" => "USDC",
        "USDTCHF" | "USDT/CHF" | "USDTEUR" | "USDT/EUR" | "USDTZUSD" | "USDT/USD" => "USDT",
        "XETHXXBT" | "ETH/BTC" | "XETHZUSD" | "ETH/USD" => "XETH",
        "XBTCHF" | "BTC/CHF" | "XBTUSDC" | "BTC/USDC" | "XBTUSDT" | "BTC/USDT" | "XXBTZEUR"
        | "BTC/EUR" | "XXBTZJPY" | "BTC/JPY" | "XXBTZUSD" | "BTC/USD" => "XXBT",
        _ => panic!("Unknown asset pair: {pair}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::kraken::{read_ledgers, read_trades};
    use crate::model::constants;
    use crate::model::stats::Stats;
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    fn test_ledger_does_not_contain_margins_which_produce_btc() {
        let _ = tracing_log::LogTracer::init();

        use KrakenAmount::*;

        // Check the ledger for MarginClosePosition[Long|Short] that produces crypto assets. (Should not happen.)
        let mut stats = Stats::default();

        let trades = read_trades(&mut stats, constants::DEFAULT_PATH_INPUT_TRADES).unwrap();
        let ledger_rows = read_ledgers(&mut stats, constants::DEFAULT_PATH_INPUT_LEDGER).unwrap();
        let ledgers = ledger_rows.parse(&trades).unwrap();

        for ledger in ledgers {
            if let LedgerParsed::MarginPositionClose {
                row_proceeds,
                row_fee,
                ..
            } = ledger
            {
                assert!(!matches!(row_proceeds.amount, Btc(_)));
                assert!(!matches!(row_proceeds.amount, Usdt(_)));
                assert!(matches!(row_fee.amount, Btc(_) | Usdc(_) | Usdt(_)));
                assert!(row_fee.amount.is_zero());
            }
        }
    }
}
