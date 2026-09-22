use crate::model::events::{Event, EventAtom, EventSubType};
use crate::model::events::{GainPortion, GainTerm};
use crate::model::kraken_amount::{KrakenAmount, UsdAmount};
use chrono::{DateTime, Utc};
use std::fmt::Display;

#[cfg(test)]
mod tests;

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
    event_details: Vec<EventAtom>,
}

#[derive(Debug)]
pub struct CapGainsEventDetails<'a> {
    details: Vec<(&'a str, &'a EventAtom)>,
}

#[derive(Debug)]
pub struct CapGainsFeeDetails<'a> {
    details: Vec<(&'a str, &'a EventAtom)>,
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
    /// Total investment interest expense (payment-time value).
    position_fees: UsdAmount,
    /// Investment interest expense deduction taken this year.
    position_fees_min: UsdAmount,
    position_proceeds: UsdAmount,
}

/// Long-term columns in the gains matrix.
#[derive(Clone, Debug, Default)]
struct GainMatrixLong {
    trade_basis: UsdAmount,
    trade_proceeds: UsdAmount,
    trade_gain: UsdAmount,
    /// Total investment interest expense (payment-time value).
    position_fees: UsdAmount,
    /// Investment interest expense deduction taken this year.
    position_fees_min: UsdAmount,
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
            .event_details
            .iter()
            .find(|atom| {
                matches!(
                    atom,
                    EventAtom::Fee { .. } | EventAtom::InvestmentFee { .. }
                )
            })
            .map(|atom| {
                match atom {
                    EventAtom::Fee { asset_amount, .. }
                    | EventAtom::InvestmentFee { asset_amount, .. } => asset_amount,
                    _ => unreachable!(),
                }
                .get_asset()
                .to_string()
            })
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

impl Display for CapGainsEventDetails<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            concat!(
                // Columns A-B
                r#""Ledger Row ID","Atom","#,
                // Columns C-D
                r#""Asset Name","Asset Amount","#,
                // Column E
                r#""Proceeds","#,
                //
                // TODO: I'm not a fan of flattening the matrix in this way. Can it be done better?
                //
                // Columns F-H
                r#""Basis (US)","Basis Date (US)","Basis Synthetic ID (US)","#,
                // Columns I-J
                r#""Net Capital Gains (US Short Term)","Net Capital Gains (US Long Term)","#,
                // Columns K-M
                r#""Basis (Non-US)","Basis Date (Non-US)","Basis Synthetic ID (Non-US)","#,
                // Columns N-O
                r#""Net Capital Gains (Non-US Short Term)","Net Capital Gains (Non-US Long Term)""#,
            )
        )?;

        // Write CSV rows
        // Column A-B, and columns C-O
        for (ledger_row_id, atom) in &self.details {
            writeln!(
                f,
                r#""{ledger_row_id}","{atom_name}",{atom}"#,
                atom_name = atom.name()
            )?;
        }

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
                r#""Ledger Row ID","Atom","Asset Name","Asset Amount","#,
                // Column D
                r#""Proceeds","#,
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
        // Column A-D, and columns E-N
        for (ledger_row_id, atom) in &self.details {
            writeln!(
                f,
                r#""{ledger_row_id}","{atom_name}",{atom}"#,
                atom_name = atom.name()
            )?;
        }

        Ok(())
    }
}

impl Display for EventAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Columns B-D
        write!(
            f,
            r#""{asset_name}","{asset_amount}","{proceeds}","#,
            asset_name = self.asset_amount().get_asset(),
            asset_amount = self.asset_amount(),
            proceeds = self.proceeds().unwrap_or_default(),
        )?;
        // Columns E-N
        match self.net_gain() {
            Some(net_gain) => write!(f, "{net_gain}"),
            None => f.write_str(r#""","","","","","""#),
        }
    }
}

impl EventAtom {
    /// The name of this atom for the detail CSVs.
    fn name(&self) -> &'static str {
        match self {
            Self::Trade { .. } => "Trade",
            Self::Income { .. } => "Income",
            Self::Position { .. } => "Position",
            Self::Fee { .. } => "Fee",
            Self::InvestmentFee { .. } => "InvestmentFee",
        }
    }

    fn asset_amount(&self) -> &KrakenAmount {
        match self {
            Self::Trade { asset_amount, .. }
            | Self::Income { asset_amount, .. }
            | Self::Position { asset_amount, .. }
            | Self::Fee { asset_amount, .. }
            | Self::InvestmentFee { asset_amount, .. } => asset_amount,
        }
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
            r#""","US Long Term","US Short Term","Non-US Long Term","Non-US Short Term""#,
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
        // The interest expense is stored in the positive convention; render it negated.
        writeln!(
            f,
            r#""Interest Expense","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = negated(self.gain_matrix.us_long.position_fees),
            us_short = negated(self.gain_matrix.us_short.position_fees),
            bona_fide_long = negated_str(bona_fide_long.to_csv_string(|col| col.position_fees)),
            bona_fide_short = negated_str(bona_fide_short.to_csv_string(|col| col.position_fees)),
        )?;

        // Row 4
        // The limited interest expense is stored in the positive convention; render it negated.
        writeln!(
            f,
            r#""Limited Interest Expense","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = negated(self.gain_matrix.us_long.position_fees_min),
            us_short = negated(self.gain_matrix.us_short.position_fees_min),
            bona_fide_long = negated_str(bona_fide_long.to_csv_string(|col| col.position_fees_min)),
            bona_fide_short =
                negated_str(bona_fide_short.to_csv_string(|col| col.position_fees_min)),
        )?;

        // Row 5
        writeln!(
            f,
            r#""Position Proceeds","","{us_short}","","{bona_fide_short}""#,
            us_short = self.gain_matrix.us_short.position_proceeds,
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.position_proceeds),
        )?;

        // Row 6
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

/// Render an amount that is stored in the positive convention as its negation.
fn negated(amount: UsdAmount) -> String {
    negated_str(amount.to_string())
}

/// Render a CSV column string that is stored in the positive convention as its negation.
fn negated_str(s: String) -> String {
    if s.is_empty() {
        s
    } else {
        format!("-{s}")
    }
}

impl CapGainsWorksheet {
    pub fn new(events: Vec<Event>) -> Self {
        // Create worksheet from events
        Self {
            worksheet: events.into_iter().map(CapGainsWorksheetRow::new).collect(),
        }
    }

    pub fn event_details(&self) -> Option<CapGainsEventDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|row| {
                row.event_details
                    .iter()
                    .map(|detail| (row.ledger_row_id.as_str(), detail))
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsEventDetails { details })
        }
    }

    pub fn fee_details(&self) -> Option<CapGainsFeeDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|row| {
                row.event_details
                    .iter()
                    .filter(|atom| {
                        matches!(
                            atom,
                            EventAtom::Fee { .. } | EventAtom::InvestmentFee { .. }
                        )
                    })
                    .map(|detail| (row.ledger_row_id.as_str(), detail))
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsFeeDetails { details })
        }
    }

    pub fn pr_statement24_dates(&self) -> PrStatement24Dates {
        let mut dates = PrStatement24Dates::default();

        for row in &self.worksheet {
            for atom in &row.event_details {
                match atom {
                    EventAtom::Trade { net_gain, .. }
                    | EventAtom::Fee { net_gain, .. }
                    | EventAtom::InvestmentFee { net_gain, .. } => {
                        dates.update(net_gain, row.event_date);
                    }
                    EventAtom::Position {
                        proceeds_bona_fide, ..
                    } => {
                        if proceeds_bona_fide.is_some() {
                            dates.update_short(row.event_date, row.event_date);
                        }
                    }
                    EventAtom::Income { .. } => {}
                }
            }
        }

        dates
    }

    pub fn sums(&self) -> Sums {
        let ledger_proceeds = self
            .worksheet
            .iter()
            .fold(UsdAmount::default(), |acc, row| acc + row.proceeds);

        let mut gain_matrix = self
            .worksheet
            .iter()
            .fold(GainMatrix::default(), |mut acc, row| {
                // A fee atom's proceeds flow to `trade_proceeds` only when the row contains at
                // least one trade atom: in that case the fee offsets the trade atom's proceeds,
                // and the trade atoms' and fee atoms' proceeds must sum to the event's total
                // proceeds. Rows without a trade atom (withdrawal, deposit, margin, wallet
                // events) have no proceeds to offset, so the fee atom's proceeds go nowhere.
                let has_trade_atom = row
                    .event_details
                    .iter()
                    .any(|atom| matches!(atom, EventAtom::Trade { .. }));

                for atom in &row.event_details {
                    match atom {
                        EventAtom::Trade {
                            proceeds, net_gain, ..
                        } => {
                            Self::fold_gain_term(&mut acc, net_gain, Some(*proceeds));
                        }
                        EventAtom::Income { proceeds, .. } => {
                            acc.income += *proceeds;
                        }
                        EventAtom::Position {
                            proceeds_us,
                            proceeds_bona_fide,
                            ..
                        } => {
                            acc.us_short.position_proceeds += *proceeds_us;

                            if let Some(bona_fide) = *proceeds_bona_fide {
                                let acc = acc.bona_fide_short.get_or_insert_with(Default::default);

                                acc.position_proceeds += bona_fide;
                            }
                        }
                        EventAtom::Fee {
                            proceeds, net_gain, ..
                        } => {
                            Self::fold_gain_term(
                                &mut acc,
                                net_gain,
                                has_trade_atom.then_some(*proceeds),
                            );
                        }
                        EventAtom::InvestmentFee {
                            proceeds, net_gain, ..
                        } => {
                            // Investment interest expense: the basis and net gain book to the
                            // trade cells; the payment-time value accumulates in `position_fees`.
                            Self::fold_gain_term(&mut acc, net_gain, None);
                            Self::fold_position_fee(&mut acc, net_gain, *proceeds);
                        }
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

    /// Fold a gain term's basis and net gain into the trade cells, and its proceeds (if any)
    /// into the `trade_proceeds` cell.
    fn fold_gain_term(
        gain_matrix: &mut GainMatrix,
        net_gain: &GainTerm,
        proceeds: Option<UsdAmount>,
    ) {
        match net_gain {
            GainTerm::ShortUs(us) => {
                if let Some(proceeds) = proceeds {
                    gain_matrix.us_short.trade_proceeds += proceeds;
                }
                gain_matrix.us_short.trade_basis += us.basis;
                gain_matrix.us_short.trade_gain += us.net_gain;
            }
            GainTerm::ShortBonaFide(bona_fide) => {
                let gain_matrix = gain_matrix
                    .bona_fide_short
                    .get_or_insert_with(Default::default);

                if let Some(proceeds) = proceeds {
                    gain_matrix.trade_proceeds += proceeds;
                }
                gain_matrix.trade_basis += bona_fide.basis;
                gain_matrix.trade_gain += bona_fide.net_gain;
            }
            GainTerm::Short { us, bona_fide } => {
                if let Some(proceeds) = proceeds {
                    gain_matrix.us_short.trade_proceeds += proceeds;
                }
                gain_matrix.us_short.trade_basis += us.basis;
                gain_matrix.us_short.trade_gain += us.net_gain;

                let gain_matrix = gain_matrix
                    .bona_fide_short
                    .get_or_insert_with(Default::default);

                gain_matrix.trade_basis += bona_fide.basis;
                gain_matrix.trade_gain += bona_fide.net_gain;
            }
            GainTerm::LongUs(us) => {
                if let Some(proceeds) = proceeds {
                    gain_matrix.us_long.trade_proceeds += proceeds;
                }
                gain_matrix.us_long.trade_basis += us.basis;
                gain_matrix.us_long.trade_gain += us.net_gain;
            }
            GainTerm::LongBonaFide(bona_fide) => {
                let gain_matrix = gain_matrix
                    .bona_fide_long
                    .get_or_insert_with(Default::default);

                if let Some(proceeds) = proceeds {
                    gain_matrix.trade_proceeds += proceeds;
                }
                gain_matrix.trade_basis += bona_fide.basis;
                gain_matrix.trade_gain += bona_fide.net_gain;
            }
            GainTerm::Long { us, bona_fide } => {
                if let Some(proceeds) = proceeds {
                    gain_matrix.us_long.trade_proceeds += proceeds;
                }
                gain_matrix.us_long.trade_basis += us.basis;
                gain_matrix.us_long.trade_gain += us.net_gain;

                let gain_matrix = gain_matrix
                    .bona_fide_long
                    .get_or_insert_with(Default::default);

                gain_matrix.trade_basis += bona_fide.basis;
                gain_matrix.trade_gain += bona_fide.net_gain;
            }
        }
    }

    /// Fold an investment interest expense's payment-time value into the `position_fees` cell
    /// of its term/residency.
    fn fold_position_fee(gain_matrix: &mut GainMatrix, net_gain: &GainTerm, proceeds: UsdAmount) {
        match net_gain {
            GainTerm::ShortUs(_) => {
                gain_matrix.us_short.position_fees += proceeds;
            }
            GainTerm::ShortBonaFide(_) => {
                let gain_matrix = gain_matrix
                    .bona_fide_short
                    .get_or_insert_with(Default::default);

                gain_matrix.position_fees += proceeds;
            }
            GainTerm::Short { us, bona_fide } => {
                // A single atom's gain is currently always attributed to either the US or the
                // bona fide column exclusively; the combined variant splits the proceeds
                // proportionally to basis if it is ever produced.
                let us_share = split_proceeds(proceeds, us.basis, bona_fide.basis);
                gain_matrix.us_short.position_fees += us_share;

                let gain_matrix = gain_matrix
                    .bona_fide_short
                    .get_or_insert_with(Default::default);

                gain_matrix.position_fees += proceeds - us_share;
            }
            GainTerm::LongUs(_) => {
                gain_matrix.us_long.position_fees += proceeds;
            }
            GainTerm::LongBonaFide(_) => {
                let gain_matrix = gain_matrix
                    .bona_fide_long
                    .get_or_insert_with(Default::default);

                gain_matrix.position_fees += proceeds;
            }
            GainTerm::Long { us, bona_fide } => {
                // See `GainTerm::Short` above.
                let us_share = split_proceeds(proceeds, us.basis, bona_fide.basis);
                gain_matrix.us_long.position_fees += us_share;

                let gain_matrix = gain_matrix
                    .bona_fide_long
                    .get_or_insert_with(Default::default);

                gain_matrix.position_fees += proceeds - us_share;
            }
        }
    }
}

/// Split `proceeds` between the US and bona fide columns proportionally to basis.
fn split_proceeds(
    proceeds: UsdAmount,
    us_basis: UsdAmount,
    bona_fide_basis: UsdAmount,
) -> UsdAmount {
    let total = us_basis + bona_fide_basis;
    if total == UsdAmount::default() {
        UsdAmount::default()
    } else {
        proceeds * us_basis / total
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
            event_details: event.event_details,
        }
    }
}

impl Sums {
    /// The US short-term gain after the interest-expense cap.
    #[cfg(test)]
    pub(crate) fn gains_us_short(&self) -> UsdAmount {
        self.gains_us_short
    }

    /// The US long-term gain after the interest-expense cap.
    #[cfg(test)]
    pub(crate) fn gains_us_long(&self) -> UsdAmount {
        self.gains_us_long
    }

    /// The US long-term trade gain before the interest-expense cap.
    #[cfg(test)]
    pub(crate) fn us_long_trade_gain(&self) -> UsdAmount {
        self.gain_matrix.us_long.trade_gain
    }

    /// The US long-term investment interest expense (payment-time value).
    #[cfg(test)]
    pub(crate) fn us_long_position_fees(&self) -> UsdAmount {
        self.gain_matrix.us_long.position_fees
    }

    /// The total ledger proceeds (worksheet column I).
    #[cfg(test)]
    pub(crate) fn ledger_proceeds(&self) -> UsdAmount {
        self.ledger_proceeds
    }

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
    ///
    /// The investment interest expense (`position_fees`) is stored in the positive convention:
    /// the payment-time value of the rollover fees. The deduction is capped at the gain before
    /// the expense; the excess carries over to the next tax year (unused until issue #94).
    fn apply_interest_expenses(
        &mut self,
        _previous_carryover: UsdAmount,
    ) -> (UsdAmount, UsdAmount) {
        // TODO: apply interest investment expenses from the previous year.  issue #94
        let gain_before = self.trade_gain + self.position_proceeds;
        self.position_fees_min = gain_before
            .min(self.position_fees)
            .max(UsdAmount::default());

        let gains = gain_before - self.position_fees_min;
        let carryover = self.position_fees - self.position_fees_min;

        (gains, carryover)
    }
}

impl GainMatrixLong {
    /// Apply the interest expenses from the previous year and this year.  issue #94
    /// Returns the gain and the carryover for next year.
    ///
    /// The investment interest expense (`position_fees`) is stored in the positive convention:
    /// the payment-time value of the rollover fees. The deduction is capped at the gain before
    /// the expense; the excess carries over to the next tax year (unused until issue #94).
    fn apply_interest_expenses(
        &mut self,
        _previous_carryover: UsdAmount,
    ) -> (UsdAmount, UsdAmount) {
        // TODO: apply interest investment expenses from the previous year.  issue #94
        let gain_before = self.trade_gain;
        self.position_fees_min = gain_before
            .min(self.position_fees)
            .max(UsdAmount::default());

        let gains = gain_before - self.position_fees_min;
        let carryover = self.position_fees - self.position_fees_min;

        (gains, carryover)
    }
}

/// Newtype for worksheet names used in PR Statement-24.
#[derive(Clone, Debug)]
pub struct WorksheetName(String);

impl From<String> for WorksheetName {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl Display for WorksheetName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Collected trade dates per (worksheet, LT/ST) for PR Statement-24.
#[derive(Debug, Default)]
pub struct PrStatement24Dates {
    pub lt_earliest_acquired: Option<DateTime<Utc>>,
    pub lt_latest_sold: Option<DateTime<Utc>>,
    pub st_earliest_acquired: Option<DateTime<Utc>>,
    pub st_latest_sold: Option<DateTime<Utc>>,
}

impl PrStatement24Dates {
    fn update(&mut self, gain_term: &GainTerm, event_date: DateTime<Utc>) {
        let (is_long, basis_date) = match gain_term {
            GainTerm::LongUs(p) | GainTerm::LongBonaFide(p) => (true, p.basis_date),
            GainTerm::Long { us, .. } => (true, us.basis_date),
            GainTerm::ShortUs(p) | GainTerm::ShortBonaFide(p) => (false, p.basis_date),
            GainTerm::Short { us, .. } => (false, us.basis_date),
        };

        if is_long {
            self.lt_earliest_acquired = Some(match self.lt_earliest_acquired {
                Some(existing) => existing.min(basis_date),
                None => basis_date,
            });
            self.lt_latest_sold = Some(match self.lt_latest_sold {
                Some(existing) => existing.max(event_date),
                None => event_date,
            });
        } else {
            self.update_short(basis_date, event_date);
        }
    }

    fn update_short(&mut self, basis_date: DateTime<Utc>, event_date: DateTime<Utc>) {
        self.st_earliest_acquired = Some(match self.st_earliest_acquired {
            Some(existing) => existing.min(basis_date),
            None => basis_date,
        });
        self.st_latest_sold = Some(match self.st_latest_sold {
            Some(existing) => existing.max(event_date),
            None => event_date,
        });
    }
}

/// One row of PR Statement-24 (F1 Part III statement 24 :: capital gains).
#[derive(Debug)]
struct PrStatement24Row {
    description: String,
    date_acquired: DateTime<Utc>,
    date_sold: DateTime<Utc>,
    sale_price: UsdAmount,
    market_value: UsdAmount,
    adjusted_basis: UsdAmount,
    gain_or_loss: UsdAmount,
    us_gain: UsdAmount,
    pr_gain: UsdAmount,
}

/// PR Statement-24 report containing rows for each (worksheet, LT/ST) with bona fide data.
#[derive(Debug)]
pub struct PrStatement24 {
    rows: Vec<PrStatement24Row>,
}

impl PrStatement24 {
    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn extend(&mut self, other: Self) {
        self.rows.extend(other.rows);
    }

    /// Build Statement-24 rows from a single worksheet's Sums and collected dates.
    pub fn from_worksheet(
        worksheet_name: &WorksheetName,
        sums: &Sums,
        dates: &PrStatement24Dates,
    ) -> Self {
        let mut rows = Vec::new();

        if let (Some(pr_gain), Some(bona_fide_long), Some(date_acquired), Some(date_sold)) = (
            sums.gains_bona_fide_long,
            &sums.gain_matrix.bona_fide_long,
            dates.lt_earliest_acquired,
            dates.lt_latest_sold,
        ) {
            let us_gain = sums.gains_us_long;
            let sale_price = bona_fide_long.trade_proceeds;
            let gain_or_loss = us_gain + pr_gain;
            let market_value = sale_price - pr_gain;
            let adjusted_basis = sale_price - gain_or_loss;

            rows.push(PrStatement24Row {
                description: format!("investment assets {worksheet_name} LT"),
                date_acquired,
                date_sold,
                sale_price,
                market_value,
                adjusted_basis,
                gain_or_loss,
                us_gain,
                pr_gain,
            });
        }

        if let (Some(pr_gain), Some(bona_fide_short), Some(date_acquired), Some(date_sold)) = (
            sums.gains_bona_fide_short,
            &sums.gain_matrix.bona_fide_short,
            dates.st_earliest_acquired,
            dates.st_latest_sold,
        ) {
            let us_gain = sums.gains_us_short;
            let sale_price = bona_fide_short.trade_proceeds;
            let gain_or_loss = us_gain + pr_gain;
            let market_value = sale_price - pr_gain;
            let adjusted_basis = sale_price - gain_or_loss;

            rows.push(PrStatement24Row {
                description: format!("investment assets {worksheet_name} ST"),
                date_acquired,
                date_sold,
                sale_price,
                market_value,
                adjusted_basis,
                gain_or_loss,
                us_gain,
                pr_gain,
            });
        }

        Self { rows }
    }
}

impl Display for PrStatement24 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            concat!(
                r#""Property Description","Date Acquired","Date Sold","A: Sale Price","#,
                r#""B: Market Value","C: Adjusted Basis","D: Gain or Loss","E: US-Sourced Gain","#,
                r#""F: PR-Sourced Gain""#,
            ),
        )?;

        for row in &self.rows {
            writeln!(
                f,
                concat!(
                    r#""{description}","{date_acquired}","{date_sold}","{sale_price}","#,
                    r#""{market_value}","{adjusted_basis}","{gain_or_loss}","{us_gain}","#,
                    r#""{pr_gain}""#,
                ),
                description = row.description,
                date_acquired = row.date_acquired.format("%F"),
                date_sold = row.date_sold.format("%F"),
                sale_price = row.sale_price,
                market_value = row.market_value,
                adjusted_basis = row.adjusted_basis,
                gain_or_loss = row.gain_or_loss,
                us_gain = row.us_gain,
                pr_gain = row.pr_gain,
            )?;
        }

        Ok(())
    }
}
