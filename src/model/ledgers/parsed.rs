use crate::model::ledgers::rows::{LedgerRow, LedgerRowDeposit, LedgerRowTypical, TradeRow};
use crate::model::pairs::{get_asset_pair, Pair, Trade};
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
    MarginPositionOpen {
        row_open: LedgerRowTypical,
        row_fee: Option<LedgerRowTypical>, // Some when the fee is paid in a second asset.
    },
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

            Self::MarginPositionOpen { row_open, row_fee } => {
                let mut name = format!("Open: {}", row_open.fee.get_asset());
                if let Some(row_fee) = row_fee {
                    name.push_str(&format!(" and {}", row_fee.fee.get_asset()));
                }
                name
            }
            Self::MarginPositionRollover(lrt) => format!("Rollover: {}", lrt.fee.get_asset()),
            Self::Withdrawal(lrt) => format!("Withdrawal: {}", lrt.amount.get_asset()),
            Self::Deposit(lrt) => format!("Deposit: {}", lrt.fee.get_asset()),
        }
    }

    pub(crate) fn get_time(&self) -> DateTime<Utc> {
        match self {
            Self::Trade { row_out: lrt, .. }
            | Self::MarginPositionSettle { row_out: lrt, .. }
            | Self::MarginPositionOpen { row_open: lrt, .. }
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

    // A two-row "margin" whose trade lacks "closing" in the `misc` column is an open. Older parsers
    // read such a pair as a close and acquired the first row's amount into the pool. A non-zero
    // non-USD amount now panics in `handle_margin_open`, so only a non-zero USD amount (collateral)
    // is worth warning about: the old parser acquired it, this parser does not. And the USD pool is
    // short of the ledger balance.
    fn check_misclassified_margin_open(
        &self,
        row_first: &LedgerRowTypical,
        trades: &HashMap<String, &TradeRow>,
    ) -> bool {
        let is_pair = matches!(
            self.peek_front(),
            Some(LedgerRow::Margin(lrt)) if lrt.refid == row_first.refid
        );

        let is_closing = trades
            .get(&row_first.refid)
            .is_some_and(|trade| trade.misc.iter().any(|s| s == "closing"));

        let usd_collateral =
            matches!(row_first.amount, KrakenAmount::Usd(_)) && !row_first.amount.is_zero();

        let misclassified = is_pair && !is_closing && usd_collateral;

        if misclassified {
            println!(
                "  ⚠️ Two-row margin open txid=`{}` has non-zero USD amount {} (collateral). \
                 Older parsers misclassified this as a close and added the amount to the USD pool; \
                 the open handler does not, so the pool is short of the ledger balance.",
                row_first.txid, row_first.amount,
            );
        }

        misclassified
    }

    fn snarf_matching_margin_row(
        &mut self,
        row_proceeds: LedgerRowTypical,
        trades: &HashMap<String, &TradeRow>,
    ) -> Result<LedgerParsed, ParseLedgerError> {
        self.check_misclassified_margin_open(&row_proceeds, trades);

        // Lookahead one row for a matching refid to determine if this is a one- or two-row margin.
        let row_fee = match self.pop_front_if(
            |row| matches!(row, LedgerRow::Margin(lrt) if row_proceeds.refid == lrt.refid),
        ) {
            Some(LedgerRow::Margin(lrt)) => Some(lrt),
            _ => None,
        };

        let trade = trades
            .get(&row_proceeds.refid)
            .ok_or(ParseLedgerError::MissingTrade)?;

        // "closing" in the trade's `misc` column distinguishes a close from an open.
        if trade.misc.iter().any(|s| s == "closing") {
            assert!(
                trade.ledgers.len() <= 2,
                "Spot positions on margin with multiple collateral currencies are unsupported",
            );

            // The close may elide its "margin fee" row.
            let row_fee = match row_fee {
                Some(row_fee) => MarginFeeRow::from(row_fee),
                None => {
                    let zero = KrakenAmount::zero(kraken_asset_from_pair(&trade.pair)).unwrap();
                    MarginFeeRow {
                        txid: row_proceeds.txid.clone(),
                        refid: row_proceeds.refid.clone(),
                        time: row_proceeds.time,
                        amount: zero,
                        fee: zero,
                        balance: None,
                    }
                }
            };

            return Ok(LedgerParsed::MarginPositionClose {
                row_proceeds,
                row_fee,
                exchange_rate: trade.price,
            });
        }

        // On margin open, the second row may pay the remainder of the fee.
        Ok(LedgerParsed::MarginPositionOpen {
            row_open: row_proceeds,
            row_fee,
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

/// Returns the Kraken asset name from a pair (the pair's base). Used for constructing a zero
/// `KrakenAmount` for the asset.
fn kraken_asset_from_pair(pair: &str) -> &str {
    Pair::from_kraken(pair)
        .unwrap_or_else(|| panic!("Unknown asset pair: {pair}"))
        .get_base()
        .as_kraken()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::kraken::{read_ledgers, read_trades};
    use crate::model::ledgers::rows::{BookSide, TradeType};
    use crate::model::{constants, stats::Stats};
    use rust_decimal::Decimal;
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    fn test_ledger_does_not_contain_margins_which_produce_btc() {
        let _ = tracing_log::LogTracer::init();

        use KrakenAmount::*;

        // Check the ledger for MarginClosePosition[Long|Short] that produces crypto assets.
        // (Should not happen.)
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

    fn margin_row(txid: &str, refid: &str, asset: &str, amount: Decimal) -> LedgerRowTypical {
        let amount = KrakenAmount::try_from_decimal(asset, amount).unwrap();
        let zero = KrakenAmount::zero(asset).unwrap();

        LedgerRowTypical {
            txid: txid.to_string(),
            refid: refid.to_string(),
            time: Utc::now(),
            amount,
            fee: zero,
            balance: zero,
        }
    }

    fn margin_trade(misc: &str) -> TradeRow {
        let zero_usd = KrakenAmount::zero("ZUSD").unwrap();

        TradeRow {
            txid: String::new(),
            ordertxid: String::new(),
            pair: "XXBTZUSD".to_string(),
            time: Utc::now(),
            tr_type: TradeType::Sell,
            ordertype: BookSide::Market,
            price: zero_usd,
            cost: zero_usd,
            fee: zero_usd,
            vol: KrakenAmount::zero("XXBT").unwrap(),
            margin: zero_usd,
            misc: vec![misc.to_string()],
            ledgers: vec![],
        }
    }

    #[test]
    #[traced_test]
    fn test_misclassified_margin_open_warns() {
        let txid = "000000-11111-222222";
        let refid = "111111-22222-333333";

        // A two-row open with a non-zero USD amount (collateral). (Should not happen.)
        let trade = margin_trade("initiated");
        let trades = HashMap::from([(refid.to_string(), &trade)]);
        let first_row = margin_row(txid, refid, "ZUSD", Decimal::new(2575, 2));
        let second_row = LedgerRow::Margin(margin_row(txid, refid, "XXBT", Decimal::ZERO));
        let fifo = FIFO::from_iter([second_row]);
        assert!(fifo.check_misclassified_margin_open(&first_row, &trades));
    }

    #[test]
    #[traced_test]
    fn test_misclassified_margin_open_silent() {
        let txid = "000000-11111-222222";
        let refid = "111111-22222-333333";

        // A two-row open with a zero first-row amount.
        let trade = margin_trade("initiated");
        let trades = HashMap::from([(refid.to_string(), &trade)]);
        let first_row = margin_row(txid, refid, "ZUSD", Decimal::ZERO);
        let second_row = LedgerRow::Margin(margin_row(txid, refid, "XXBT", Decimal::ZERO));
        let fifo = FIFO::from_iter([second_row]);
        assert!(!fifo.check_misclassified_margin_open(&first_row, &trades));

        // A two-row open with a non-zero non-USD first-row amount. (Panics in `handle_margin_open`
        // instead of warning.)
        let trade = margin_trade("initiated");
        let trades = HashMap::from([(refid.to_string(), &trade)]);
        let first_row = margin_row(txid, refid, "ZEUR", Decimal::new(2575, 2));
        let second_row = LedgerRow::Margin(margin_row(txid, refid, "XXBT", Decimal::ZERO));
        let fifo = FIFO::from_iter([second_row]);
        assert!(!fifo.check_misclassified_margin_open(&first_row, &trades));

        // A two-row close with a non-zero first-row amount.
        let trade = margin_trade("closing");
        let trades = HashMap::from([(refid.to_string(), &trade)]);
        let first_row = margin_row(txid, refid, "ZUSD", Decimal::new(2575, 2));
        let second_row = LedgerRow::Margin(margin_row(txid, refid, "XXBT", Decimal::ZERO));
        let fifo = FIFO::from_iter([second_row]);
        assert!(!fifo.check_misclassified_margin_open(&first_row, &trades));

        // A one-row open with a non-zero first-row amount.
        let trade = margin_trade("initiated");
        let trades = HashMap::from([(refid.to_string(), &trade)]);
        let first_row = margin_row(txid, refid, "ZUSD", Decimal::new(2575, 2));
        let fifo = FIFO::new();
        assert!(!fifo.check_misclassified_margin_open(&first_row, &trades));
    }
}
