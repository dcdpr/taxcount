use crate::model::ledgers::rows::{basis_lookup_parse, ledger_parse, trade_parse};
use crate::model::ledgers::rows::{BasisRow, LedgerRow, TradeRow};
use crate::model::Stats;
use crate::util::fifo::FIFO;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum KrakenError {
    #[error("CSV Error")]
    Io(#[from] csv::Error),

    #[error("FS Error")]
    Fs(#[from] std::io::Error),

    #[error("DateTime parsing error")]
    DateTime(#[from] chrono::ParseError),
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct BasisCSVRow {
    pub(crate) synthetic_id: String,
    pub(crate) time: String,
    pub(crate) asset: String,
    pub(crate) amount: String,
    pub(crate) exchange_rate: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct LedgerCSVRow {
    pub(crate) txid: String,
    pub(crate) refid: String,
    pub(crate) time: String,
    #[serde(rename = "type")]
    pub(crate) lr_type: String,
    pub(crate) subtype: String, // "" or "spotfromfutures"
    pub(crate) aclass: String,  // Always "currency"
    pub(crate) asset: String,   // ZUSD, XXBT, ZEUR, USDT
    pub(crate) amount: String,
    pub(crate) fee: String,
    pub(crate) balance: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct TradeCSVRow {
    pub(crate) txid: String,
    pub(crate) ordertxid: String,
    pub(crate) pair: String,
    pub(crate) time: String,
    #[serde(alias = "type")]
    pub(crate) tr_type: String,
    pub(crate) ordertype: String,
    pub(crate) price: String,
    pub(crate) cost: String,
    pub(crate) fee: String,
    pub(crate) vol: String,
    pub(crate) margin: String,
    pub(crate) misc: String,
    pub(crate) ledgers: String,
}

pub fn read_basis_lookup(
    s: &mut Stats,
    path: impl AsRef<Path>,
) -> Result<FIFO<BasisRow>, KrakenError> {
    let mut rows = FIFO::new();
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    debug!("Parsing Basis Lookup rows");
    for result in reader.deserialize() {
        let record = result?;
        debug!("Deserialized: {record:?}");

        let record2 = basis_lookup_parse(record);
        debug!("Parsed: {record2:?}");

        rows.append_back(record2);
        s.inc_basis_lookup();
    }
    Ok(rows)
}

pub fn read_ledgers(s: &mut Stats, path: impl AsRef<Path>) -> Result<FIFO<LedgerRow>, KrakenError> {
    // Use a BTreeMap to sort rows by timestamp
    let mut rows = BTreeMap::new();
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    debug!("Parsing Ledger rows");
    for result in reader.deserialize() {
        let record: LedgerCSVRow = result?;
        debug!("Deserialized: {record:?}");

        let time = NaiveDateTime::parse_from_str(&record.time, "%F %T")?;

        let record2 = ledger_parse(record);
        debug!("Parsed: {record2:?}");

        let entry = rows.entry(time).or_insert_with(Vec::new);
        entry.push(record2);
        s.inc_ledgers();
    }
    Ok(rows.into_values().flatten().collect())
}

pub fn write_ledgers(path: impl AsRef<Path>, ledgers: &FIFO<LedgerRow>) -> Result<(), KrakenError> {
    let mut writer = csv::WriterBuilder::new()
        .quote_style(csv::QuoteStyle::NonNumeric)
        .from_path(path)?;
    for row in ledgers.iter() {
        writer.serialize(row.to_kraken_csv())?;
    }

    Ok(())
}

pub fn read_trades(s: &mut Stats, path: impl AsRef<Path>) -> Result<FIFO<TradeRow>, KrakenError> {
    let mut rows = FIFO::new();
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    debug!("Parsing Trade rows");
    for result in reader.deserialize() {
        let record = result?;
        debug!("Deserialized: {record:?}");

        let record2 = trade_parse(record)?;
        debug!("Parsed: {record2:?}");

        rows.append_back(record2);
        s.inc_trades();
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::constants;
    use tracing_test::traced_test;

    fn readloop_then_stats() {
        let mut stats = Stats::default();

        if let Err(err) = read_ledgers(&mut stats, constants::DEFAULT_PATH_INPUT_LEDGER) {
            panic!("error running ledgers: {err}");
        }
        if let Err(err) = read_trades(&mut stats, constants::DEFAULT_PATH_INPUT_TRADES) {
            panic!("error running trades: {err}");
        }

        stats.pretty_print();
    }

    #[test]
    #[traced_test]
    fn readall() {
        let _ = tracing_log::LogTracer::init();

        // try: `cargo test -- --nocapture --test-threads 1`
        readloop_then_stats();
    }
}
