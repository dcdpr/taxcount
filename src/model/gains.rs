// 04 / Bona Fide Move-Date Value is required in ledgers-sums for reconciliation
// issue-004-sums-fees.md

use crate::model::events::{event_proceeds, Event, EventAtom, EventSubType, GainPortion, GainTerm};
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
    event_details: Vec<EventAtom>,   // Column H is event_proceeds() of these atoms.
}

/// One line for each atom of each event: the ledger row, the atom, and the atom's share of the
/// sale expenses of its event (see [`fee_shares`]).
#[derive(Debug)]
pub struct CapGainsEventDetails<'a> {
    details: Vec<(&'a str, &'a EventAtom, Option<UsdAmount>)>,
}

/// The sums table of one worksheet.
///
/// In each cell, the gain is the trade proceeds plus the move-date value minus the trade basis
/// plus the position proceeds minus the fees. `assert_error_check` makes sure of this identity.
// Section 3, Sums Table :: Step 3, Sums Struct
#[derive(Debug)]
pub struct Sums {
    gain_matrix: GainMatrix,
}

/// The gains matrix stores intermediate gains values across the full term-length/residency matrix.
///
/// The bona fide cells are `None` when no atom has a bona fide portion. The sums table shows a
/// `None` cell as blank.
#[derive(Debug, Default)]
struct GainMatrix {
    us_short: GainCell,
    us_long: GainCell,
    bona_fide_short: Option<GainCell>,
    bona_fide_long: Option<GainCell>,
}

/// One cell of the gains matrix: one term and one residency.
// Section 3, Sums Table :: Step 1, One Cell Struct
#[derive(Clone, Debug, Default)]
struct GainCell {
    /// The proceeds of each trade atom, fee atom, and investment fee atom.
    trade_proceeds: UsdAmount,

    /// The value on the move date of each split that the bona fide move date divides. The US
    /// cells only. The bona fide portion of the same split has this value as its basis.
    move_date_value: UsdAmount,

    /// The basis of each trade atom, fee atom, and investment fee atom.
    trade_basis: UsdAmount,

    /// The sum of the net gains of the portions folded into this cell. The error check compares
    /// it with the identity in `gain`.
    trade_gain: UsdAmount,

    /// The proceeds of each position atom. The short-term cells only.
    position_proceeds: UsdAmount,

    /// The fee value of each fee atom that is an expense of a sale, as a positive quantity.
    fees: UsdAmount,
}

/// The address of one cell in the gains matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CellId {
    UsShort,
    UsLong,
    BonaFideShort,
    BonaFideLong,
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
                // Column H
                r#""Proceeds""#,
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
        // Column H
        write!(
            f,
            r#""{proceeds}""#,
            proceeds = event_proceeds(&self.event_details)
        )?;

        Ok(())
    }
}

impl Display for GainTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            GainTerm::ShortUs(us) => {
                // Columns G-K
                us.fmt_term(f, false)?;
                // Columns L-P
                f.write_str(r#","","","","","""#)
            }
            GainTerm::ShortBonaFide(bona_fide) => {
                // Columns G-K
                f.write_str(r#""","","","","","#)?;
                // Columns L-P
                bona_fide.fmt_term(f, false)
            }
            GainTerm::Short { us, bona_fide } => {
                // Columns G-K
                us.fmt_term(f, false)?;
                f.write_str(",")?;
                // Columns L-P
                bona_fide.fmt_term(f, false)
            }
            GainTerm::LongUs(us) => {
                // Columns G-K
                us.fmt_term(f, true)?;
                // Columns L-P
                f.write_str(r#","","","","","""#)
            }
            GainTerm::LongBonaFide(bona_fide) => {
                // Columns G-K
                f.write_str(r#""","","","","","#)?;
                // Columns L-P
                bona_fide.fmt_term(f, true)
            }
            GainTerm::Long { us, bona_fide } => {
                // Columns G-K
                us.fmt_term(f, true)?;
                f.write_str(",")?;
                // Columns L-P
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
                // Columns A-F
                r#""Ledger Row ID","Event","Asset Name","Asset Amount","Proceeds","Fees","#,
                //
                // TODO: I'm not a fan of flattening the matrix in this way. Can it be done better?
                //
                // Columns G-I
                r#""Basis (US)","Basis Date (US)","Basis Synthetic ID (US)","#,
                // Columns J-K
                r#""Net Capital Gains (US Short Term)","Net Capital Gains (US Long Term)","#,
                // Columns L-N
                r#""Basis (Non-US)","Basis Date (Non-US)","Basis Synthetic ID (Non-US)","#,
                // Columns O-P
                r#""Net Capital Gains (Non-US Short Term)","Net Capital Gains (Non-US Long Term)""#,
            )
        )?;

        // Write CSV rows
        // Section 4, Event Details CSV
        for (ledger_row_id, atom, fee_share) in &self.details {
            // Columns A-F
            write!(
                f,
                r#""{ledger_row_id}","{name}","{asset_name}","{asset_amount}","{proceeds}","{fees}","#,
                name = atom.name(),
                asset_name = atom.asset_amount().get_asset(),
                asset_amount = atom.asset_amount(),
                proceeds = atom.proceeds().unwrap_or_default(),
                fees = fee_share.to_csv_string(|x| *x),
            )?;
            // Columns G-P
            match atom.net_gain() {
                Some(net_gain) => writeln!(f, "{net_gain}")?,
                None => writeln!(f, r#""","","","","","","","","","""#)?,
            }
        }

        Ok(())
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

impl ToCsvString for Option<GainCell> {
    type Inner = GainCell;

    fn to_csv_string<F: Fn(&Self::Inner) -> UsdAmount>(&self, map_fn: F) -> String {
        self.as_ref()
            .map(|col| map_fn(col).to_string())
            .unwrap_or_default()
    }
}

impl Display for Sums {
    // Section 3, Sums Table :: Step 2, Rows
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Write the CSV header
        writeln!(
            f,
            r#""","US Long Term","US Short Term","Non-US Long Term","Non-US Short Term""#,
        )?;

        let us_long = &self.gain_matrix.us_long;
        let us_short = &self.gain_matrix.us_short;
        let bona_fide_long = &self.gain_matrix.bona_fide_long;
        let bona_fide_short = &self.gain_matrix.bona_fide_short;

        // Row 1
        writeln!(
            f,
            r#""Trade Proceeds","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = us_long.trade_proceeds,
            us_short = us_short.trade_proceeds,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.trade_proceeds),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.trade_proceeds),
        )?;

        // Row 2
        // The move-date value is in the US cells only. The bona fide portion of the same split
        // has it as the basis, in the Non-US cells of row 3.
        writeln!(
            f,
            r#""Move-Date Value","{us_long}","{us_short}","","""#,
            us_long = us_long.move_date_value,
            us_short = us_short.move_date_value,
        )?;

        // Row 3
        writeln!(
            f,
            r#""Trade Basis","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = us_long.trade_basis,
            us_short = us_short.trade_basis,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.trade_basis),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.trade_basis),
        )?;

        // Row 4
        writeln!(
            f,
            r#""Position Proceeds","","{us_short}","","{bona_fide_short}""#,
            us_short = us_short.position_proceeds,
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.position_proceeds),
        )?;

        // Row 5
        writeln!(
            f,
            r#""Fees","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = us_long.fees,
            us_short = us_short.fees,
            bona_fide_long = bona_fide_long.to_csv_string(|col| col.fees),
            bona_fide_short = bona_fide_short.to_csv_string(|col| col.fees),
        )?;

        // Row 6
        writeln!(
            f,
            r#""Gain","{us_long}","{us_short}","{bona_fide_long}","{bona_fide_short}""#,
            us_long = us_long.gain(),
            us_short = us_short.gain(),
            bona_fide_long = bona_fide_long.to_csv_string(GainCell::gain),
            bona_fide_short = bona_fide_short.to_csv_string(GainCell::gain),
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

    pub fn event_details(&self) -> Option<CapGainsEventDetails<'_>> {
        let details: Vec<_> = self
            .worksheet
            .iter()
            .flat_map(|row| {
                let fee_shares = fee_shares(&row.event_details);
                row.event_details
                    .iter()
                    .zip(fee_shares)
                    .map(|(detail, fee_share)| {
                        let ledger_row_id =
                            detail.ledger_row_id().unwrap_or(row.ledger_row_id.as_str());

                        (ledger_row_id, detail, fee_share)
                    })
            })
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(CapGainsEventDetails { details })
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

    // Section 3, Sums Table :: Step 2, Rows
    pub fn sums(&self) -> Sums {
        let gain_matrix = self
            .worksheet
            .iter()
            .fold(GainMatrix::default(), |mut acc, row| {
                let fee_shares = fee_shares(&row.event_details);
                for (atom, fee_share) in row.event_details.iter().zip(fee_shares) {
                    match atom {
                        EventAtom::Trade {
                            proceeds, net_gain, ..
                        } => {
                            Self::fold_gain_term(&mut acc, net_gain, *proceeds);
                            if let Some(fee_share) = fee_share {
                                acc.cell_mut(net_gain.sale_cell()).fees += fee_share;
                            }
                        }
                        EventAtom::Income { .. } => {}
                        EventAtom::Position {
                            proceeds_us,
                            proceeds_bona_fide,
                            ..
                        } => {
                            acc.us_short.position_proceeds += *proceeds_us;

                            if let Some(bona_fide) = *proceeds_bona_fide {
                                acc.cell_mut(CellId::BonaFideShort).position_proceeds += bona_fide;
                            }
                            if let Some(fee_share) = fee_share {
                                let cell = position_cell(*proceeds_bona_fide);
                                acc.cell_mut(cell).fees += fee_share;
                            }
                        }
                        EventAtom::Fee {
                            proceeds, net_gain, ..
                        }
                        | EventAtom::InvestmentFee {
                            proceeds, net_gain, ..
                        } => {
                            Self::fold_gain_term(&mut acc, net_gain, *proceeds);
                        }
                    }
                }

                acc
            });

        Sums { gain_matrix }
    }

    /// Fold a disposal into the gains matrix: its proceeds into the sale cell, and the basis and
    /// the net gain of each portion into the cell of that portion.
    ///
    /// For a split that the move date divides, the US portion has the basis and the gain up to
    /// the move date, and the move-date value goes into the US cell. The bona fide portion has the
    /// move-date value as its basis, and the proceeds go into the Non-US cell.
    fn fold_gain_term(gain_matrix: &mut GainMatrix, net_gain: &GainTerm, proceeds: UsdAmount) {
        match net_gain {
            GainTerm::ShortUs(portion)
            | GainTerm::ShortBonaFide(portion)
            | GainTerm::LongUs(portion)
            | GainTerm::LongBonaFide(portion) => {
                let cell = gain_matrix.cell_mut(net_gain.sale_cell());
                cell.trade_proceeds += proceeds;
                cell.trade_basis += portion.basis;
                cell.trade_gain += portion.net_gain;
            }
            GainTerm::Short { us, bona_fide } | GainTerm::Long { us, bona_fide } => {
                let us_cell = gain_matrix.cell_mut(net_gain.us_cell());
                us_cell.move_date_value += bona_fide.basis;
                us_cell.trade_basis += us.basis;
                us_cell.trade_gain += us.net_gain;

                let bona_fide_cell = gain_matrix.cell_mut(net_gain.sale_cell());
                bona_fide_cell.trade_proceeds += proceeds;
                bona_fide_cell.trade_basis += bona_fide.basis;
                bona_fide_cell.trade_gain += bona_fide.net_gain;
            }
        }
    }
}

/// True when the atoms contain a sale: a trade atom or a position atom.
///
/// In a sale event, each fee atom is an expense of the sale. In each other event, a fee atom is
/// the disposal of the fee coin only.
// Section 1, Atoms :: Step 2, Sale Event Rule
fn is_sale_event(atoms: &[EventAtom]) -> bool {
    atoms
        .iter()
        .any(|atom| matches!(atom, EventAtom::Trade { .. } | EventAtom::Position { .. }))
}

/// The cell of the sale expenses of a position atom: the short-term cell of its residency.
fn position_cell(proceeds_bona_fide: Option<UsdAmount>) -> CellId {
    if proceeds_bona_fide.is_some() {
        CellId::BonaFideShort
    } else {
        CellId::UsShort
    }
}

/// The share of the sale expenses of each atom, by index.
///
/// In a sale event, the fee value of each fee atom is an expense of the sale. The sum of the fee
/// values divides across the trade atoms pro-rata by amount, with the rounding remainder on the
/// last trade atom. When the event has no trade atom, the full sum goes to the last position
/// atom. Each other atom, and each atom of an event that is not a sale, has `None`.
// Section 2, Fee Shares
fn fee_shares(atoms: &[EventAtom]) -> Vec<Option<UsdAmount>> {
    let mut shares = vec![None; atoms.len()];
    if !is_sale_event(atoms) {
        return shares;
    }

    let fee_value = atoms
        .iter()
        .filter_map(|atom| match atom {
            EventAtom::Fee { proceeds, .. } => Some(*proceeds),
            _ => None,
        })
        .fold(UsdAmount::default(), |acc, proceeds| acc + proceeds);

    let trades: Vec<(usize, KrakenAmount)> = atoms
        .iter()
        .enumerate()
        .filter_map(|(index, atom)| match atom {
            EventAtom::Trade { asset_amount, .. } => Some((index, asset_amount.abs())),
            _ => None,
        })
        .collect();

    if trades.is_empty() {
        let positions: Vec<usize> = atoms
            .iter()
            .enumerate()
            .filter_map(|(index, atom)| matches!(atom, EventAtom::Position { .. }).then_some(index))
            .collect();
        for (count, index) in positions.iter().enumerate() {
            let is_last = count + 1 == positions.len();
            shares[*index] = Some(if is_last {
                fee_value
            } else {
                UsdAmount::default()
            });
        }
        return shares;
    }

    // The trade atoms of an event are one asset at one rate, so the pro-rata share by amount is
    // the pro-rata share by proceeds.
    let total_amount = trades
        .iter()
        .map(|(_, amount)| *amount)
        .reduce(|acc, amount| acc + amount)
        .expect("trades is not empty");
    let fee_value_per_unit = fee_value.sub_divide(total_amount);

    let mut applied = UsdAmount::default();
    for (count, (index, amount)) in trades.iter().enumerate() {
        let is_last = count + 1 == trades.len();
        let share = if is_last {
            fee_value - applied
        } else {
            amount.get_value_usd(fee_value_per_unit)
        };
        applied += share;
        shares[*index] = Some(share);
    }

    shares
}

impl GainMatrix {
    /// The cell at `id`. A bona fide cell is created on its first use.
    fn cell_mut(&mut self, id: CellId) -> &mut GainCell {
        match id {
            CellId::UsShort => &mut self.us_short,
            CellId::UsLong => &mut self.us_long,
            CellId::BonaFideShort => self.bona_fide_short.get_or_insert_with(Default::default),
            CellId::BonaFideLong => self.bona_fide_long.get_or_insert_with(Default::default),
        }
    }
}

impl GainCell {
    /// The capital gain of the cell: the Schedule D value.
    fn gain(&self) -> UsdAmount {
        self.trade_proceeds + self.move_date_value - self.trade_basis + self.position_proceeds
            - self.fees
    }
}

impl GainTerm {
    /// The cell of the sale: the proceeds and the sale expenses go there. For a split that the
    /// move date divides, the sale is after the move date, in the Non-US cell.
    fn sale_cell(&self) -> CellId {
        match self {
            Self::ShortUs(_) => CellId::UsShort,
            Self::LongUs(_) => CellId::UsLong,
            Self::ShortBonaFide(_) | Self::Short { .. } => CellId::BonaFideShort,
            Self::LongBonaFide(_) | Self::Long { .. } => CellId::BonaFideLong,
        }
    }

    /// The US cell of the term.
    fn us_cell(&self) -> CellId {
        match self {
            Self::ShortUs(_) | Self::ShortBonaFide(_) | Self::Short { .. } => CellId::UsShort,
            Self::LongUs(_) | Self::LongBonaFide(_) | Self::Long { .. } => CellId::UsLong,
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
            event_details: event.event_details,
        }
    }
}

impl Sums {
    /// The US short-term capital gain.
    #[cfg(test)]
    pub(crate) fn gains_us_short(&self) -> UsdAmount {
        self.gain_matrix.us_short.gain()
    }

    /// The US long-term capital gain.
    #[cfg(test)]
    pub(crate) fn gains_us_long(&self) -> UsdAmount {
        self.gain_matrix.us_long.gain()
    }

    /// The US long-term trade proceeds, with the fee coin proceeds.
    #[cfg(test)]
    pub(crate) fn us_long_trade_proceeds(&self) -> UsdAmount {
        self.gain_matrix.us_long.trade_proceeds
    }

    /// The US long-term trade basis.
    #[cfg(test)]
    pub(crate) fn us_long_trade_basis(&self) -> UsdAmount {
        self.gain_matrix.us_long.trade_basis
    }

    /// The sum of the US long-term net gains of each portion, before the sale expenses.
    #[cfg(test)]
    pub(crate) fn us_long_trade_gain(&self) -> UsdAmount {
        self.gain_matrix.us_long.trade_gain
    }

    /// The US long-term sale expenses.
    #[cfg(test)]
    pub(crate) fn us_long_fees(&self) -> UsdAmount {
        self.gain_matrix.us_long.fees
    }

    /// The US short-term sale expenses.
    #[cfg(test)]
    pub(crate) fn us_short_fees(&self) -> UsdAmount {
        self.gain_matrix.us_short.fees
    }

    /// Assert that, in each cell, the sum of the folded net gains is equal to the identity of
    /// `GainCell::gain`, in the EPSILON tolerance of `UsdAmount::is_fuzzy_eq`.
    // Section 3, Sums Table :: Step 4, Error Check
    pub fn assert_error_check(&self) {
        let cells = [
            ("US Short Term", Some(&self.gain_matrix.us_short)),
            ("US Long Term", Some(&self.gain_matrix.us_long)),
            (
                "Non-US Short Term",
                self.gain_matrix.bona_fide_short.as_ref(),
            ),
            ("Non-US Long Term", self.gain_matrix.bona_fide_long.as_ref()),
        ];
        for (name, cell) in cells
            .into_iter()
            .filter_map(|(name, cell)| cell.map(|c| (name, c)))
        {
            let folded = cell.trade_gain + cell.position_proceeds - cell.fees;
            let gain = cell.gain();
            assert!(
                folded.is_fuzzy_eq(gain),
                "Expected {name} gain {folded} ~= {gain}",
            );
        }
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

        if let (Some(bona_fide_long), Some(date_acquired), Some(date_sold)) = (
            &sums.gain_matrix.bona_fide_long,
            dates.lt_earliest_acquired,
            dates.lt_latest_sold,
        ) {
            let pr_gain = bona_fide_long.gain();
            let us_gain = sums.gain_matrix.us_long.gain();
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

        if let (Some(bona_fide_short), Some(date_acquired), Some(date_sold)) = (
            &sums.gain_matrix.bona_fide_short,
            dates.st_earliest_acquired,
            dates.st_latest_sold,
        ) {
            let pr_gain = bona_fide_short.gain();
            let us_gain = sums.gain_matrix.us_short.gain();
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
