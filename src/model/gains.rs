use crate::model::events::{Event, EventFee, EventSubType};
use crate::model::events::{EventIncomeAtom, EventPositionAtom, EventTradeAtom};
use crate::model::events::{GainPortion, GainTerm};
use crate::model::kraken_amount::UsdAmount;
use chrono::{DateTime, Utc};
use std::fmt::Display;

#[derive(Debug)]
pub struct CapGainsWorksheet {
    worksheet: Vec<CapGainsWorksheetRow>,
}

#[derive(Debug)]
struct CapGainsWorksheetRow {
    event_date: DateTime<Utc>,       // Column A
    internal_account: String,        // Column B
    ledger_row_id: String,           // Column C (Debugging only. `txid` in `LedgerRow`)
    event_subtype: EventSubType,     // Column D (Debugging only.)
    event_name: String,              // Column E (Debugging only.)
    asset_out_exchange_rate: String, // Column F (Debugging only.)
    asset_in_exchange_rate: String,  // Column G (Debugging only.)
    proceeds: UsdAmount,             // Column I
    trade_details: Vec<EventTradeAtom>,
    income_details: Vec<EventIncomeAtom>,
    position_details: Vec<EventPositionAtom>,
    tx_fees: Vec<EventFee>,
    position_fees: Vec<EventFee>,
}

#[derive(Debug)]
pub struct CapGainsTradeDetails<'a> {
    details: Vec<(&'a str, &'a EventTradeAtom)>,
}

#[derive(Debug)]
pub struct CapGainsIncomeDetails<'a> {
    details: Vec<(&'a str, &'a EventIncomeAtom)>,
}

#[derive(Debug)]
pub struct CapGainsPositionDetails<'a> {
    details: Vec<(&'a str, &'a EventPositionAtom)>,
}

#[derive(Debug)]
pub struct CapGainsFeeDetails<'a> {
    details: Vec<(&'a str, &'a EventFee)>,
}

#[derive(Debug)]
pub struct Sums {
    ledger_proceeds: UsdAmount,
    gain_matrix: GainMatrix,
    gains_us_short: UsdAmount,
    gains_us_long: UsdAmount,
    // TODO: Might be better to wrap both of these into a single Option.
    gains_bona_fide_short: Option<UsdAmount>,
    gains_bona_fide_long: Option<UsdAmount>,
}

/// The gains matrix stores intermediate gains values across the full term-length/residency matrix.
#[derive(Debug, Default)]
struct GainMatrix {
    income: UsdAmount,
    us_short: GainMatrixShort,
    us_long: GainMatrixLong,
    bona_fide_short: Option<GainMatrixShort>,
    bona_fide_long: Option<GainMatrixLong>,
}

/// Short-term columns in the gains matrix. Has `position_proceeeds`
#[derive(Clone, Debug, Default)]
struct GainMatrixShort {
    trade_basis: UsdAmount,
    trade_proceeds: UsdAmount,
    trade_gain: UsdAmount,
    trade_fees: UsdAmount,
    position_proceeds: UsdAmount,
    position_fees: UsdAmount,
    position_fees_min: UsdAmount,
    transaction_fees: UsdAmount,
}

/// Long-term columns in the gains matrix.
#[derive(Clone, Debug, Default)]
struct GainMatrixLong {
    trade_basis: UsdAmount,
    trade_proceeds: UsdAmount,
    trade_gain: UsdAmount,
    trade_fees: UsdAmount,
    position_fees: UsdAmount,
    position_fees_min: UsdAmount,
    transaction_fees: UsdAmount,
}

impl Display for CapGainsWorksheet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            concat!(
                // Columns A-D
                r#""Event Date","Internal Account","Ledger Row ID","Event Sub-Type","#,
                // Columns E-G
                r#""Event Description","Asset Out Exchange Rate","Asset In Exchange Rate","#,
                // Columns H-I
                r#""Fee Asset Name","Proceeds""#,
            )
        )?;

        // TODO: Add capital gains summary

        // Write CSV rows
        for row in &self.worksheet {
            writeln!(f, "{row}")?;
        }

        Ok(())
    }
}

impl Display for CapGainsWorksheetRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Columns A-D
        write!(
            f,
            r#""{event_date}","{internal_account}","{ledger_row_id}","{event_subtype}","#,
            event_date = self.event_date.format("%F %T %Z"),
            internal_account = self.internal_account,
            ledger_row_id = self.ledger_row_id,
            event_subtype = self.event_subtype,
        )?;
        // Columns E-G
        write!(
            f,
            r#""{event_name}","{asset_out_exchange_rate}","{asset_in_exchange_rate}","#,
            event_name = self.event_name,
            asset_out_exchange_rate = self.asset_out_exchange_rate,
            asset_in_exchange_rate = self.asset_in_exchange_rate,
        )?;
        // Columns H-I
        let fee_asset_name = self
            .tx_fees
            .iter()
            .chain(self.position_fees.iter())
            .map(|detail| detail.asset_fee.get_asset().to_string())
            .next()
            .unwrap_or_default();
        write!(
            f,
            r#""{fee_asset_name}","{proceeds}""#,
            fee_asset_name = fee_asset_name,
            proceeds = self.proceeds,
        )?;

        Ok(())
    }
}

impl Display for GainTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            GainTerm::ShortUs(us) => {
                // Columns E-I
                us.fmt_term(f, false)?;
                // Columns J-N
                f.write_str(r#","","","","","""#)
            }
            GainTerm::ShortBonaFide(bona_fide) => {
                // Columns E-I
                f.write_str(r#""","","","","","#)?;
                // Columns J-N
                bona_fide.fmt_term(f, false)
            }
            GainTerm::Short { us, bona_fide } => {
                // Columns E-I
                us.fmt_term(f, false)?;
                f.write_str(",")?;
                // Columns J-N
                bona_fide.fmt_term(f, false)
            }
            GainTerm::LongUs(us) => {
                // Columns E-I
                us.fmt_term(f, true)?;
                // Columns J-N
                f.write_str(r#","","","","","""#)
            }
            GainTerm::LongBonaFide(bona_fide) => {
                // Columns E-I
                f.write_str(r#""","","","","","#)?;
                // Columns J-N
                bona_fide.fmt_term(f, true)
            }
            GainTerm::Long { us, bona_fide } => {
                // Columns E-I
                us.fmt_term(f, true)?;
                f.write_str(",")?;
                // Columns J-N
                bona_fide.fmt_term(f, true)
            }
        }
    }
}

impl GainPortion {
    fn fmt_term(&self, f: &mut std::fmt::Formatter<'_>, is_long: bool) -> std::fmt::Result {
        write!(
            f,
            r#""{basis}","{basis_date}","{basis_synthetic_id}","#,
            basis = self.basis,
            basis_date = self.basis_date.format("%F %T %Z"),
            basis_synthetic_id = self.basis_synthetic_id,
        )?;

        if is_long {
            f.write_str(r#""","#)?;
        }

        write!(f, r#""{net_gain}""#, net_gain = self.net_gain)?;

        if !is_long {
            f.write_str(r#","""#)?;
        }

        Ok(())
    }
}

impl Display for CapGainsTradeDetails<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            concat!(
                // Columns A-D
                r#""Ledger Row ID","Asset Name","Asset Amount","Proceeds","#,
                //
                // TODO: I'm not a fan of flattening the matrix in this way. Can it be done better?
                //
                // Columns E-G
                r#""Basis (US)","Basis Date (US)","Basis Synthetic ID (US)","#,
                // Columns H-I
                r#""Net Capital Gains (US Short Term)","Net Capital Gains (US Long Term)","#,
                // Columns J-L
                r#""Basis (Non-US)","Basis Date (Non-US)","Basis Synthetic ID (Non-US)","#,
                // Columns M-N
                r#""Net Capital Gains (Non-US Short Term)","Net Capital Gains (Non-US Long Term)""#,
            )
        )?;

        // Write CSV rows
        // Column A, and columns B-N
        for (ledger_row_id, atom) in &self.details {
            writeln!(f, r#""{ledger_row_id}",{atom}"#)?;
        }

        Ok(())
    }
}

impl Display for EventTradeAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Columns B-D
        write!(
            f,
            r#""{asset_name}","{asset_amount}","{proceeds}",{net_gain}"#,
            asset_name = self.asset_amount.get_asset(),
            asset_amount = self.asset_amount,
            proceeds = self.proceeds,
            net_gain = self.net_gain,
        )
    }
}

impl Display for CapGainsIncomeDetails<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            concat!(
                // Columns A-D
                r#""Ledger Row ID","Asset Name","Asset Amount","Proceeds""#,
            )
        )?;

        // Write CSV rows
        // Column A, and columns B-D
        for (ledger_row_id, atom) in &self.details {
            writeln!(f, r#""{ledger_row_id}",{atom}"#)?;
        }

        Ok(())
    }
}

impl Display for EventIncomeAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Columns B-D
        write!(
            f,
            r#""{asset_name}","{asset_amount}","{proceeds}""#,
            asset_name = self.asset_amount.get_asset(),
            asset_amount = self.asset_amount,
            proceeds = self.proceeds,
        )?;

        Ok(())
    }
}

impl Display for CapGainsPositionDetails<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            r#""Ledger Row ID","Asset Name","Asset Amount","Proceeds (US)","Proceeds (Non-US)""#
        )?;

        // Write CSV rows
        for (ledger_row_id, atom) in &self.details {
            writeln!(f, r#""{ledger_row_id}",{atom}"#)?;
        }

        Ok(())
    }
}

impl Display for EventPositionAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Columns B-E
        write!(
            f,
            r#""{asset_name}","{asset_amount}","{proceeds_us}","{proceeds_bona_fide}""#,
            asset_name = self.asset_amount.get_asset(),
            asset_amount = self.asset_amount,
            proceeds_us = self.proceeds_us,
            proceeds_bona_fide = self.proceeds_bona_fide.to_csv_string(|x| *x),
        )?;

        Ok(())
    }
}

impl Display for CapGainsFeeDetails<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            concat!(
                // Columns A-C
                r#""Ledger Row ID","Asset Name","Asset Fee","#,
                //
                // TODO: I'm not a fan of flattening the matrix in this way. Can it be done better?
                //
                // Column D-F
                r#""Basis (US)","Basis Date (US)","Basis Synthetic ID (US)","#,
                // Columns G-H
                r#""Net Capital Loss (US Short Term)","Net Capital Loss (US Long Term)","#,
                // Column I-K
                r#""Basis (Non-US)","Basis Date (Non-US)","Basis Synthetic ID (Non-US)","#,
                // Columns L-M
                r#""Net Capital Loss (Non-US Short Term)","Net Capital Loss (Non-US Long Term)""#,
            )
        )?;

        // Write CSV rows
        // Column A, and columns B-M
        for (ledger_row_id, atom) in &self.details {
            writeln!(f, r#""{ledger_row_id}",{atom}"#)?;
        }

        Ok(())
    }
}

impl Display for EventFee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Columns B-F
        write!(
            f,
            r#""{asset_name}","{asset_fee}",{net_loss}"#,
            asset_name = self.asset_fee.get_asset(),
            asset_fee = self.asset_fee,
            net_loss = self.net_loss,
        )
    }
}

/// This trait is used to reduce code duplication when interacting with `Option<T>` where `T`
/// contains a `UsdAmount`.
trait ToCsvString {
    type Inner;

    fn to_csv_string<F: Fn(&Self::Inner) -> UsdAmount>(&self, map_fn: F) -> String;
}

impl ToCsvString for Option<UsdAmount> {
    type Inner = UsdAmount;

    fn to_csv_string<F: Fn(&Self::Inner) -> UsdAmount>(&self, _: F) -> String {
        self.as_ref().map(|col| col.to_string()).unwrap_or_default()
    }
}

impl ToCsvString for Option<GainMatrixLong> {
    type Inner = GainMatrixLong;

    fn to_csv_string<F: Fn(&Self::Inner) -> UsdAmount>(&self, map_fn: F) -> String {
        self.as_ref()
            .map(|col| map_fn(col).to_string())
            .unwrap_or_default()
    }
}

impl ToCsvString for Option<GainMatrixShort> {
    type Inner = GainMatrixShort;

    fn to_csv_string<F: Fn(&Self::Inner) -> UsdAmount>(&self, map_fn: F) -> String {
        self.as_ref()
            .map(|col| map_fn(col).to_string())
            .unwrap_or_default()
    }
}

impl Display for Sums {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            concat!(r#""","US Long Term","US Short Term","Non-US Long Term","Non-US Short Term""#,)
        )?;

        let bona_fide_long = &self.gain_matrix.bona_fide_long;
        let bona_fide_short = &self.gain_matrix.bona_fide_short;

        // Row 1
        writeln!(
            f,
            r#""Trade Basis","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = self.gain_matrix.us_long.trade_basis,
            us_short = self.gain_matrix.us_short.trade_basis,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.trade_basis),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.trade_basis),
        )?;

        // Row 2
        writeln!(
            f,
            r#""Trade Proceeds","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = self.gain_matrix.us_long.trade_proceeds,
            us_short = self.gain_matrix.us_short.trade_proceeds,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.trade_proceeds),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.trade_proceeds),
        )?;

        // Row 3
        writeln!(
            f,
            r#""Trade Fee Basis","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = self.gain_matrix.us_long.trade_fees,
            us_short = self.gain_matrix.us_short.trade_fees,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.trade_fees),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.trade_fees),
        )?;

        // Row 4
        writeln!(
            f,
            r#""Position Proceeds","","{us_short}","","{bona_fide_short}""#,
            us_short = self.gain_matrix.us_short.position_proceeds,
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.position_proceeds),
        )?;

        // Row 5
        writeln!(
            f,
            r#""Interest Expense","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = self.gain_matrix.us_long.position_fees,
            us_short = self.gain_matrix.us_short.position_fees,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.position_fees),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.position_fees),
        )?;

        // Row 6
        writeln!(
            f,
            r#""Limited Interest Expense","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = self.gain_matrix.us_long.position_fees_min,
            us_short = self.gain_matrix.us_short.position_fees_min,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.position_fees_min),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.position_fees_min),
        )?;

        // Row 7
        writeln!(
            f,
            r#""Gain","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = self.gains_us_long,
            us_short = self.gains_us_short,
            bona_fide_long = self.gains_bona_fide_long.to_csv_string(|x| *x),
            bona_fide_short = self.gains_bona_fide_short.to_csv_string(|x| *x),
        )?;

        Ok(())
    }
}

impl CapGainsWorksheet {
    pub fn new(events: Vec<Event>) -> Self {
        // Create worksheet from events
        Self {
            worksheet: events.into_iter().map(CapGainsWorksheetRow::new).collect(),
        }
    }

    pub fn trade_details(&self) -> Option<CapGainsTradeDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|event| {
                event
                    .trade_details
                    .iter()
                    .map(|detail| (event.ledger_row_id.as_str(), detail))
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsTradeDetails { details })
        }
    }

    pub fn income_details(&self) -> Option<CapGainsIncomeDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|event| {
                event
                    .income_details
                    .iter()
                    .map(|detail| (event.ledger_row_id.as_str(), detail))
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsIncomeDetails { details })
        }
    }

    pub fn position_details(&self) -> Option<CapGainsPositionDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|event| {
                event
                    .position_details
                    .iter()
                    .map(|detail| (event.ledger_row_id.as_str(), detail))
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsPositionDetails { details })
        }
    }

    pub fn tx_fees(&self) -> Option<CapGainsFeeDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|event| {
                event
                    .tx_fees
                    .iter()
                    .map(|detail| (event.ledger_row_id.as_str(), detail))
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsFeeDetails { details })
        }
    }

    pub fn position_fees(&self) -> Option<CapGainsFeeDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|event| {
                event
                    .position_fees
                    .iter()
                    .map(|detail| (event.ledger_row_id.as_str(), detail))
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsFeeDetails { details })
        }
    }

    pub fn sums(&self) -> Sums {
        let ledger_proceeds = self
            .worksheet
            .iter()
            .fold(UsdAmount::default(), |acc, row| acc + row.proceeds);

        let gain_matrix = self
            .worksheet
            .iter()
            .flat_map(|row| row.trade_details.iter())
            .fold(GainMatrix::default(), |mut acc, detail| {
                match &detail.net_gain {
                    GainTerm::ShortUs(us) => {
                        acc.us_short.trade_proceeds += detail.proceeds;
                        acc.us_short.trade_basis += us.basis;
                        acc.us_short.trade_gain += us.net_gain;
                    }
                    GainTerm::ShortBonaFide(bona_fide) => {
                        let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                        acc.trade_proceeds += detail.proceeds;
                        acc.trade_basis += bona_fide.basis;
                        acc.trade_gain += bona_fide.net_gain;
                    }
                    GainTerm::Short { us, bona_fide } => {
                        acc.us_short.trade_basis += us.basis;
                        acc.us_short.trade_gain += us.net_gain;

                        let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                        acc.trade_proceeds += detail.proceeds;
                        acc.trade_basis += bona_fide.basis;
                        acc.trade_gain += bona_fide.net_gain;
                    }
                    GainTerm::LongUs(us) => {
                        acc.us_long.trade_proceeds += detail.proceeds;
                        acc.us_long.trade_basis += us.basis;
                        acc.us_long.trade_gain += us.net_gain;
                    }
                    GainTerm::LongBonaFide(bona_fide) => {
                        let acc = acc.bona_fide_long.get_or_insert_with(Default::default);

                        acc.trade_proceeds += detail.proceeds;
                        acc.trade_basis += bona_fide.basis;
                        acc.trade_gain += bona_fide.net_gain;
                    }
                    GainTerm::Long { us, bona_fide } => {
                        acc.us_long.trade_basis += us.basis;
                        acc.us_long.trade_gain += us.net_gain;

                        let acc = acc.bona_fide_long.get_or_insert_with(Default::default);

                        acc.trade_proceeds += detail.proceeds;
                        acc.trade_basis += bona_fide.basis;
                        acc.trade_gain += bona_fide.net_gain;
                    }
                }

                acc
            });

        let gain_matrix = self
            .worksheet
            .iter()
            .flat_map(|row| row.income_details.iter())
            .fold(gain_matrix, |mut acc, detail| {
                acc.income += detail.proceeds;

                acc
            });

        let gain_matrix = self
            .worksheet
            .iter()
            .flat_map(|row| row.tx_fees.iter())
            .fold(gain_matrix, |mut acc, row| {
                match &row.net_loss {
                    GainTerm::ShortUs(us) => {
                        acc.us_short.trade_fees += us.net_gain;
                    }
                    GainTerm::ShortBonaFide(bona_fide) => {
                        let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                        acc.trade_fees += bona_fide.net_gain;
                    }
                    GainTerm::Short { us, bona_fide } => {
                        acc.us_short.trade_fees += us.net_gain;

                        let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                        acc.trade_fees += bona_fide.net_gain;
                    }
                    GainTerm::LongUs(us) => {
                        acc.us_long.trade_fees += us.net_gain;
                    }
                    GainTerm::LongBonaFide(bona_fide) => {
                        let acc = acc.bona_fide_long.get_or_insert_with(Default::default);

                        acc.trade_fees += bona_fide.net_gain;
                    }
                    GainTerm::Long { us, bona_fide } => {
                        acc.us_long.trade_fees += us.net_gain;

                        let acc = acc.bona_fide_long.get_or_insert_with(Default::default);

                        acc.trade_fees += bona_fide.net_gain;
                    }
                }

                acc
            });

        let gain_matrix = self
            .worksheet
            .iter()
            .flat_map(|row| row.position_details.iter())
            .fold(gain_matrix, |mut acc, row| {
                acc.us_short.position_proceeds += row.proceeds_us;

                if let Some(bona_fide) = &row.proceeds_bona_fide {
                    let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                    acc.position_proceeds += *bona_fide;
                }

                acc
            });

        let mut gain_matrix = self
            .worksheet
            .iter()
            .flat_map(|row| row.position_fees.iter())
            .fold(gain_matrix, |mut acc, row| {
                match &row.net_loss {
                    GainTerm::ShortUs(us) => {
                        acc.us_short.position_fees += us.net_gain;
                    }
                    GainTerm::ShortBonaFide(bona_fide) => {
                        let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                        acc.position_fees += bona_fide.net_gain;
                    }
                    GainTerm::Short { us, bona_fide } => {
                        acc.us_short.position_fees += us.net_gain;

                        let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                        acc.position_fees += bona_fide.net_gain;
                    }
                    GainTerm::LongUs(us) => {
                        acc.us_long.position_fees += us.net_gain;
                    }
                    GainTerm::LongBonaFide(bona_fide) => {
                        let acc = acc.bona_fide_long.get_or_insert_with(Default::default);

                        acc.position_fees += bona_fide.net_gain;
                    }
                    GainTerm::Long { us, bona_fide } => {
                        acc.us_long.position_fees += us.net_gain;

                        let acc = acc.bona_fide_long.get_or_insert_with(Default::default);

                        acc.position_fees += bona_fide.net_gain;
                    }
                }

                acc
            });

        // TODO: Apply interest investment expenses from the previous year.  issue #94
        // TODO: The difference carries over to the next tax year
        let (gains_us_short, _carryover) = gain_matrix
            .us_short
            .apply_interest_expenses(UsdAmount::default());

        let (gains_us_long, _carryover) = gain_matrix
            .us_long
            .apply_interest_expenses(UsdAmount::default());

        let gains_bona_fide_short = gain_matrix.bona_fide_short.as_mut().map(|bona_fide| {
            let (gains, _carryover) = bona_fide.apply_interest_expenses(UsdAmount::default());

            gains
        });

        let gains_bona_fide_long = gain_matrix.bona_fide_long.as_mut().map(|bona_fide| {
            let (gains, _carryover) = bona_fide.apply_interest_expenses(UsdAmount::default());

            gains
        });

        Sums {
            ledger_proceeds,
            gain_matrix,
            gains_us_short,
            gains_us_long,
            gains_bona_fide_short,
            gains_bona_fide_long,
        }
    }
}

impl CapGainsWorksheetRow {
    fn new(event: Event) -> Self {
        let asset_out_exchange_rate = event
            .event_info
            .asset_out_exchange_rate
            .to_csv_string(|x| *x);
        let asset_in_exchange_rate = event
            .event_info
            .asset_in_exchange_rate
            .to_csv_string(|x| *x);

        Self {
            event_date: event.event_info.event_date,
            internal_account: event.event_info.internal_account,
            ledger_row_id: event.event_info.ledger_row_id,
            event_subtype: event.event_info.event_subtype,
            event_name: event.event_info.event_name,
            asset_out_exchange_rate,
            asset_in_exchange_rate,
            proceeds: event.event_info.proceeds,
            trade_details: event.trade_details,
            income_details: event.income_details,
            position_details: event.position_details,
            tx_fees: event.tx_fees,
            position_fees: event.position_fees,
        }
    }
}

impl Sums {
    /// Assert that the calculated error checks are zero (within EPSILON tolerance).
    pub fn assert_error_check(&self) {
        let us_short = &self.gain_matrix.us_short;
        let us_long = &self.gain_matrix.us_long;
        let bona_fide_short = self.gain_matrix.bona_fide_short.clone().unwrap_or_default();
        let bona_fide_long = self.gain_matrix.bona_fide_long.clone().unwrap_or_default();

        let proceeds = self.gain_matrix.income
            + us_short.trade_proceeds
            + us_long.trade_proceeds
            + us_short.position_proceeds
            + bona_fide_short.trade_proceeds
            + bona_fide_long.trade_proceeds
            + bona_fide_short.position_proceeds;

        assert!(
            self.ledger_proceeds.is_fuzzy_eq(proceeds),
            "Expected {ledger_proceeds} ~= {proceeds}",
            ledger_proceeds = self.ledger_proceeds,
        );
    }
}

impl GainMatrixShort {
    /// Apply the interest expenses from the previous year and this year. Returns the gain and the
    /// carryover for next year.
    fn apply_interest_expenses(
        &mut self,
        _previous_carryover: UsdAmount,
    ) -> (UsdAmount, UsdAmount) {
        // TODO: apply interest investment expenses from the previous year.  issues #94
        let gain_before_interest_expenses =
            self.trade_gain + self.trade_fees + self.position_proceeds + self.transaction_fees;
        self.position_fees_min = -(gain_before_interest_expenses
            .min(self.position_fees.abs())
            .max(UsdAmount::default()));

        let gains = gain_before_interest_expenses + self.position_fees_min;
        let carryover = self.position_fees - self.position_fees_min;

        (gains, carryover)
    }
}

impl GainMatrixLong {
    /// Apply the interest expenses from the previous year and this year.  issues #94
    /// Returns the gain and the carryover for next year.
    fn apply_interest_expenses(
        &mut self,
        _previous_carryover: UsdAmount,
    ) -> (UsdAmount, UsdAmount) {
        // TODO: apply interest investment expenses from the previous year.  issue #94
        let gain_before_interest_expenses =
            self.trade_gain + self.trade_fees + self.transaction_fees;
        self.position_fees_min = -(gain_before_interest_expenses
            .min(self.position_fees.abs())
            .max(UsdAmount::default()));

        let gains = gain_before_interest_expenses + self.position_fees_min;
        let carryover = self.position_fees - self.position_fees_min;

        (gains, carryover)
    }
}
