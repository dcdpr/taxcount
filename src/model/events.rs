use crate::basis::{Asset, AssetName, PoolAsset, PoolAssetNonSplittable};
use crate::errors::ExchangeRateError;
use crate::imports::wallet::TxType;
use crate::model::kraken_amount::{KrakenAmount, UsdAmount};
use crate::model::{exchange_rate::ExchangeRates, ledgers::parsed::LedgerParsed};
use chrono::{DateTime, Months, Utc};
use serde::{Deserialize, Serialize};
use std::rc::Rc;

/// When a `PoolAsset<BTC>` must be split to fulfill a `LedgerParsed`, then the `Event` will
/// reference one or more splits for that `LedgerParsed` via `asset_idx_out`.
///
/// There is a 1:1 correspondence between`CapGainsWorksheetRow`, `Event`, and `LedgerParsed`.
#[derive(Clone, Debug)]
pub struct Event {
    /// Worksheet name are typically either wallet names or exchange names.
    pub(crate) worksheet_name: Rc<str>,

    /// Each taxable event has one worksheet row, encapsulated in `EventInfo`.
    pub(crate) event_info: EventInfo,

    // TODO: This is created as an incomplete type, and this boolean informs the code that a pending
    // withdrawal must be queued for the consumed assets. Replace this boolean with a DSL "pipeline"
    // for manipulating the PoolAsset FIFOs. See: https://gl1.dcdpr.com/rgrant/taxcount/-/issues/59
    pub(crate) is_withdrawal: bool,

    /// Zero or more detail atoms (may span multiple asset splits).
    ///
    /// Trade and fee atoms have a cost basis, position atoms do not.
    pub(crate) event_details: Vec<EventAtom>,

    /// The asset in which this event paid its fees.
    ///
    /// This may be `Some` even when no fee atoms exist: a fiat-denominated buy capitalizes its fee
    /// into the acquired split's basis. No fee atom is created but a fee was still paid.
    pub(crate) fee_asset_name: Option<AssetName>,
}

/// Extra info for taxable events.
#[derive(Clone, Debug)]
pub(crate) struct EventInfo {
    // NOTE: Columns E and H are derived
    pub(crate) event_date: DateTime<Utc>,   // Column A
    pub(crate) internal_account: String,    // Column B
    pub(crate) ledger_row_id: String,       // Column C
    pub(crate) event_subtype: EventSubType, // Column D
    pub(crate) event_name: String,          // Column E
    pub(crate) asset_out_exchange_rate: Option<UsdAmount>, // Column F
    pub(crate) asset_in_exchange_rate: Option<UsdAmount>, // Column G
    pub(crate) proceeds: UsdAmount,         // Column I
}

/// Atomized details for a taxable event.
///
/// Every taxable event detail is recorded as an atom in the [`Event::event_details`] list:
///
/// - Trade atoms for disposed assets
/// - Income atoms for received assets
/// - Position atoms for loaned assets (margin positions)
/// - Fee atoms for fees paid from the pool
/// - Investment fee atoms for margin position rollovers (investment interest expenses)
#[derive(Clone, Debug)]
pub(crate) enum EventAtom {
    /// The taxable asset (outgoing) is disposed of in a trade.
    Trade {
        asset_amount: KrakenAmount, // Column C
        proceeds: UsdAmount,        // Column D
        net_gain: GainTerm,         // Columns E-...
    },

    /// The taxable asset (incoming) is received as income.
    Income {
        asset_amount: KrakenAmount, // Column C
        proceeds: UsdAmount,        // Column D
    },

    /// The asset is loaned (margin position). There is no cost basis, because the asset is
    /// loaned.
    Position {
        asset_amount: KrakenAmount,            // Column C
        proceeds_us: UsdAmount,                // Column D
        proceeds_bona_fide: Option<UsdAmount>, // Column E
    },

    /// A fee paid from the pool. `proceeds` is the USD value of the fee at the moment of
    /// payment; `net_gain` is the difference between that value and the fee coin's cost basis.
    Fee {
        asset_amount: KrakenAmount, // Column C
        proceeds: UsdAmount,        // Column D
        net_gain: GainTerm,         // Columns E-...

        /// True when the fee offsets the trade atoms' proceeds on the same ledger row as the
        /// outgoing amount. Only then do the fee's proceeds book into the trade cells.
        reduces_proceeds: bool,
    },

    /// An investment interest expense (margin position rollover).
    InvestmentFee {
        asset_amount: KrakenAmount, // Column C
        proceeds: UsdAmount,        // Column D
        net_gain: GainTerm,         // Columns E-...
    },
}

impl EventAtom {
    /// The USD proceeds of this atom, if any.
    pub(crate) fn proceeds(&self) -> Option<UsdAmount> {
        match self {
            Self::Trade { proceeds, .. }
            | Self::Income { proceeds, .. }
            | Self::Fee { proceeds, .. }
            | Self::InvestmentFee { proceeds, .. } => Some(*proceeds),
            Self::Position { .. } => None,
        }
    }

    /// The net gain term of this atom, if any.
    pub(crate) fn net_gain(&self) -> Option<&GainTerm> {
        match self {
            Self::Trade { net_gain, .. }
            | Self::Fee { net_gain, .. }
            | Self::InvestmentFee { net_gain, .. } => Some(net_gain),
            _ => None,
        }
    }
}

/// Capital gains are classified as either short-term or long-term, based on whether the asset has
/// been held for shorter or longer than one year until date of sale.
/// The gains are further classified as either US-sourced or Territory-sourced as a bona fide
/// resident of a US territory.
#[derive(Clone, Debug)]
pub(crate) enum GainTerm {
    /// Short-term, US-sources gains only.
    ShortUs(GainPortion),

    /// Short-term, Territory-sources gains only.
    ShortBonaFide(GainPortion),

    /// Short-term, both US-sourced and Territory-sourced gains.
    Short {
        us: GainPortion,
        bona_fide: GainPortion,
    },

    /// Long-term, US-sources gains only.
    LongUs(GainPortion),

    /// Long-term, Territory-sources gains only.
    LongBonaFide(GainPortion),

    /// Long-term, both US-sourced and Territory-sourced gains.
    Long {
        us: GainPortion,
        bona_fide: GainPortion,
    },
}

/// Gains are attributable to US holding periods and territory holding periods. This is the common
/// data between them.
#[derive(Clone, Debug)]
pub(crate) struct GainPortion {
    pub(crate) basis: UsdAmount,
    pub(crate) basis_date: DateTime<Utc>,
    pub(crate) basis_synthetic_id: String,
    pub(crate) net_gain: UsdAmount,
}

/// Global configuration required to calculate capital gains.
#[derive(Debug)]
pub struct GainConfig {
    pub exchange_rates_db: ExchangeRates,
    // TODO: This wants to address the special rules that apply to federal tax reporting within US
    // territories. It does not attempt to concern itself with US state taxes, which are subject to
    // different rules and do not affect reporting on federal tax filings. Handling state taxes
    // would require something else.
    // See: https://www.irs.gov/publications/p570#en_US_2022_publink1000221230
    pub bona_fide_residency: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum WalletDirection {
    /// Move between own wallets.
    Move,

    /// Receive from outside party.
    Receive,

    /// Send to other party.
    Send,
}

#[derive(Clone, Debug)]
pub(crate) enum EventSubType {
    Trade,
    MarginClose,
    MarginSettle,
    MarginOpen,
    MarginRollover,
    Withdrawal,
    Deposit,

    /// Untagged wallet transaction
    Wallet {
        is_sender: bool,
    },

    /// Move between our own wallets
    Move,

    /// Tagged wallet transaction
    TxType {
        tx_type: TxType,
        is_sender: bool,
    },
}

impl Event {
    pub(crate) fn get_row_time(&self) -> DateTime<Utc> {
        self.event_info.event_date
    }
}

impl Event {
    pub(crate) fn from_ledger_parsed(
        worksheet_name: Rc<str>,
        lp: Rc<LedgerParsed>,
        event_date: DateTime<Utc>,
        internal_account: String,
        ledger_row_id: String,
    ) -> Self {
        use LedgerParsed::*;

        let is_withdrawal = matches!(&*lp, Withdrawal(_));
        let event_subtype = EventSubType::from(&*lp);
        let event_name = lp.get_event_name();
        Self {
            worksheet_name,
            event_info: EventInfo::new(
                event_date,
                internal_account,
                ledger_row_id,
                event_subtype,
                event_name,
            ),
            is_withdrawal,
            event_details: vec![],
            fee_asset_name: None,
        }
    }

    pub(crate) fn from_transaction(
        worksheet_name: Rc<str>,
        direction: WalletDirection,
        tx_type: &Option<TxType>,
        event_date: DateTime<Utc>,
        internal_account: String,
        event_name: String,
        ledger_row_id: String,
    ) -> Self {
        let event_subtype = match direction {
            WalletDirection::Move => EventSubType::Move,
            WalletDirection::Send => match tx_type {
                Some(tx_type) => EventSubType::TxType {
                    tx_type: tx_type.clone(),
                    is_sender: true,
                },
                None => EventSubType::Wallet { is_sender: true },
            },
            WalletDirection::Receive => match tx_type {
                Some(tx_type) => EventSubType::TxType {
                    tx_type: tx_type.clone(),
                    is_sender: false,
                },
                None => EventSubType::Wallet { is_sender: false },
            },
        };
        Self {
            worksheet_name,
            event_info: EventInfo::new(
                event_date,
                internal_account,
                ledger_row_id,
                event_subtype,
                event_name,
            ),
            is_withdrawal: false,
            event_details: vec![],
            fee_asset_name: None,
        }
    }

    /// Add a trade atom for each pool split consumed by this event.
    pub(crate) fn add_trade<A>(
        &mut self,
        split_assets: Vec<PoolAssetNonSplittable<A>>,
        gain_config: &GainConfig,
    ) -> Vec<ExchangeRateError>
    where
        A: Asset,
        KrakenAmount: From<A>,
    {
        let mut errors = vec![];

        for asset in split_assets {
            match EventAtom::trade_from_split(asset, &self.event_info, gain_config) {
                Ok(atom) => self.event_details.push(atom),
                Err(err) => errors.push(err),
            }
        }

        errors
    }

    /// Add an income atom for each pool split received by this event.
    pub(crate) fn add_income<'a, A, I>(&mut self, split_assets: I) -> Vec<ExchangeRateError>
    where
        A: Asset + Copy + 'a,
        KrakenAmount: From<A>,
        I: Iterator<Item = &'a PoolAsset<A>>,
    {
        let mut errors = vec![];

        for asset in split_assets {
            match EventAtom::income_from_split(asset, &self.event_info) {
                Ok(atom) => self.event_details.push(atom),
                Err(err) => errors.push(err),
            }
        }

        errors
    }

    /// Add a position atom for a loaned asset (margin position).
    pub(crate) fn add_position(&mut self, asset_amount: KrakenAmount, gain_config: &GainConfig) {
        self.event_details
            .push(EventAtom::position_from_kraken_amount(
                asset_amount,
                &self.event_info,
                gain_config,
            ));
    }

    /// Add a fee atom for each pool split consumed as a fee by this event.
    ///
    /// Each fee atom's `proceeds` is the fee amount times `fee_rate`. When `fee_rate` is `None`
    /// (the event has no defined rate covering the fee asset), the fee asset's market rate at the
    /// event date-time is used.
    ///
    /// Returns the sum of all fee atoms' proceeds, along with any exchange rate errors.
    pub(crate) fn add_fee<A>(
        &mut self,
        split_assets: Vec<PoolAssetNonSplittable<A>>,
        fee_rate: Option<UsdAmount>,
        gain_config: &GainConfig,
    ) -> (Vec<ExchangeRateError>, UsdAmount)
    where
        A: Asset + Copy,
        KrakenAmount: From<A>,
    {
        let mut errors = vec![];
        let mut fee_value = UsdAmount::default();

        for asset in split_assets {
            let rate = match fee_rate {
                Some(rate) => rate,
                None => {
                    let amount = KrakenAmount::from(asset.amount);
                    match amount.get_exchange_rate(
                        self.event_info.event_date,
                        &gain_config.exchange_rates_db,
                    ) {
                        Ok(rate) => rate,
                        Err(err) => {
                            errors.push(err);
                            continue;
                        }
                    }
                }
            };

            match EventAtom::from_fee_split(asset, &self.event_info, rate, gain_config) {
                Ok(atom) => {
                    fee_value += atom.proceeds().expect("Fee atom has proceeds");
                    self.set_fee_asset_name(atom.asset_amount().get_asset());
                    self.event_details.push(atom);
                }
                Err(err) => errors.push(err),
            }
        }

        (errors, fee_value)
    }

    /// Record the asset of a fee paid by this event.
    ///
    /// Called both when a fee atom is added and when a fee is paid without an atom (a
    /// fiat-denominated buy capitalizes its fee into the acquired split's basis).
    ///
    /// An event pays all of its fees in a single asset; a second, different asset is a bug.
    pub(crate) fn set_fee_asset_name(&mut self, name: AssetName) {
        if let Some(known) = self.fee_asset_name {
            assert_eq!(known, name, "event pays fees in more than one asset");
        }
        self.fee_asset_name = Some(name);
    }

    /// Reduce the proceeds of the trade atoms added after `start`, pro-rata to each atom's amount
    /// so that the sum of the reductions is exactly `fee_value`.
    ///
    /// Trade fees offset the trade atoms' proceeds: the trade atoms' and fee atoms' proceeds must
    /// sum to the event's total proceeds. The fee atoms added after `start` are marked with
    /// `reduces_proceeds`, so their proceeds book into the trade cells.
    pub(crate) fn reduce_trade_proceeds(&mut self, start: usize, fee_value: UsdAmount) {
        if fee_value == UsdAmount::default() {
            return;
        }

        // The trade atoms and the fee are denominated in the same asset (both are consumed from the
        // event's pool), and every trade atom in an event is valued at the event's single
        // definitional rate, so the pro-rata share of the fee by proceeds equals the pro-rata
        // share by amount.
        let mut total_amount: Option<KrakenAmount> = None;
        for atom in self.event_details.iter().skip(start) {
            if let EventAtom::Trade { asset_amount, .. } = atom {
                total_amount = Some(match total_amount {
                    Some(total) => total + asset_amount.abs(),
                    None => asset_amount.abs(),
                });
            }
        }
        let Some(total_amount) = total_amount else {
            return;
        };

        // The fee's USD value per unit of the traded amount (the one division of this pro-rata
        // split).
        let fee_value_per_unit = fee_value.sub_divide(total_amount);

        // Find the last trade atom so any rounding difference lands there and the reductions sum
        // to exactly `fee_value`.
        let last_trade = self
            .event_details
            .iter()
            .enumerate()
            .skip(start)
            .rev()
            .find_map(|(i, atom)| matches!(atom, EventAtom::Trade { .. }).then_some(i));

        let mut applied = UsdAmount::default();
        for (i, atom) in self.event_details.iter_mut().enumerate().skip(start) {
            match atom {
                EventAtom::Trade {
                    asset_amount,
                    proceeds,
                    net_gain,
                } => {
                    let reduction = if Some(i) == last_trade {
                        fee_value - applied
                    } else {
                        asset_amount.abs().get_value_usd(fee_value_per_unit)
                    };
                    applied += reduction;
                    *proceeds = *proceeds - reduction;

                    // Keep the gain portion(s) consistent with the reduced proceeds: each portion's
                    // net gain is its proceeds minus its basis. For split (US/territory) gains,
                    // only the territory portion's proceeds change; the US portion's gain is
                    // `bona_fide_basis - basis` and does not depend on the sale proceeds.
                    match net_gain {
                        GainTerm::ShortUs(us) | GainTerm::LongUs(us) => {
                            us.net_gain = *proceeds - us.basis;
                        }
                        GainTerm::ShortBonaFide(bf) | GainTerm::LongBonaFide(bf) => {
                            bf.net_gain = *proceeds - bf.basis;
                        }
                        GainTerm::Short { bona_fide, .. } | GainTerm::Long { bona_fide, .. } => {
                            bona_fide.net_gain = *proceeds - bona_fide.basis;
                        }
                    }
                }
                EventAtom::Fee {
                    reduces_proceeds, ..
                } => {
                    // This fee atom's proceeds book into the trade cells: they offset the trade
                    // atoms' proceeds reduced above.
                    *reduces_proceeds = true;
                }
                _ => {}
            }
        }
    }
}

impl EventInfo {
    pub(crate) fn new(
        event_date: DateTime<Utc>,
        internal_account: String,
        ledger_row_id: String,
        event_subtype: EventSubType,
        event_name: String,
    ) -> Self {
        Self {
            event_date,
            internal_account,
            ledger_row_id,
            event_subtype,
            event_name,
            asset_out_exchange_rate: None,
            asset_in_exchange_rate: None,
            proceeds: UsdAmount::default(),
        }
    }
}

impl EventAtom {
    /// Build a trade atom from a consumed pool split.
    ///
    /// `proceeds` is the disposed amount times the event's `asset_out_exchange_rate`.
    ///
    /// `net_gain` is `proceeds` minus the split's cost basis. The cost basis is the amount times
    /// the acquisition rate. The gain is classified by the split's basis date: short-term or
    /// long-term against the event date, US or territory against the bona fide residency date.
    ///
    /// When the basis date is before the move date and the event date is not, the gain is split
    /// at the coin's move-date value. The US portion is the gain accrued to the move date. The
    /// territory portion is the gain from the move date to the sale, re-based at the move date.
    ///
    /// Disposed amounts are recorded negative.
    fn trade_from_split<A>(
        split: PoolAssetNonSplittable<A>,
        event_info: &EventInfo,
        gain_config: &GainConfig,
    ) -> Result<Self, ExchangeRateError>
    where
        A: Asset,
        KrakenAmount: From<A>,
    {
        let asset_amount = KrakenAmount::from(split.amount);
        let proceeds = asset_amount.get_value_usd(
            event_info
                .asset_out_exchange_rate
                .expect("Exchange rate is required"),
        );
        let net_gain = {
            let basis = asset_amount.get_value_usd(
                split
                    .lifecycle
                    .get_exchange_rate_at_acquisition(&gain_config.exchange_rates_db)?,
            );
            let basis_date = split.lifecycle.get_datetime();
            let basis_synthetic_id = split.lifecycle.get_synthetic_id().to_string();

            let total_net_gain = GainPortion {
                basis,
                basis_date,
                basis_synthetic_id,
                net_gain: proceeds - basis,
            };

            let (us, bona_fide) = match gain_config.bona_fide_residency {
                Some(move_date) => {
                    if event_info.event_date < move_date {
                        // Gain is allocated as US-sourced gains.
                        (Some(total_net_gain), None)
                    } else if basis_date < move_date {
                        // Split the total gain between US-sourced and Territory-sourced gains.
                        let bona_fide_basis = asset_amount.get_value_usd(
                            asset_amount
                                .get_exchange_rate(move_date, &gain_config.exchange_rates_db)?,
                        );

                        let us_portion = Some(GainPortion {
                            net_gain: bona_fide_basis - basis,
                            ..total_net_gain
                        });
                        let bona_fide_portion = Some(GainPortion {
                            basis: bona_fide_basis,
                            basis_date: move_date,
                            basis_synthetic_id: "Special election for bona fide residents"
                                .to_string(),
                            net_gain: proceeds - bona_fide_basis,
                        });

                        (us_portion, bona_fide_portion)
                    } else {
                        (None, Some(total_net_gain))
                    }
                }
                None => (Some(total_net_gain), None),
            };

            // The short-term/long-term threshold for capital gains is one year.
            // This subtraction clamps February 29th (leap year) to February 28th.
            let is_long_term = basis_date < event_info.event_date - Months::new(12);

            match (is_long_term, us, bona_fide) {
                (false, Some(us), None) => GainTerm::ShortUs(us),
                (false, None, Some(bona_fide)) => GainTerm::ShortBonaFide(bona_fide),
                (false, Some(us), Some(bona_fide)) => GainTerm::Short { us, bona_fide },
                (true, Some(us), None) => GainTerm::LongUs(us),
                (true, None, Some(bona_fide)) => GainTerm::LongBonaFide(bona_fide),
                (true, Some(us), Some(bona_fide)) => GainTerm::Long { us, bona_fide },
                _ => unreachable!(),
            }
        };

        Ok(Self::Trade {
            asset_amount: -asset_amount, // Traded assets are always outgoing.
            proceeds,
            net_gain,
        })
    }

    /// Build an income atom from a received pool split.
    ///
    /// `proceeds` is the received amount times the event's `asset_in_exchange_rate`.
    ///
    /// Income atoms hold no cost basis or net gain: receiving income is not a disposal. The coin
    /// enters the pool with its receipt value as cost basis, so a later disposal is taxed as a
    /// capital gain from that basis.
    fn income_from_split<A>(
        split: &PoolAsset<A>,
        event_info: &EventInfo,
    ) -> Result<Self, ExchangeRateError>
    where
        A: Asset + Copy,
        KrakenAmount: From<A>,
    {
        let asset_amount = KrakenAmount::from(split.amount);
        let proceeds = asset_amount.get_value_usd(
            event_info
                .asset_in_exchange_rate
                .expect("Exchange rate is required"),
        );

        Ok(Self::Income {
            asset_amount,
            proceeds,
        })
    }

    /// Build a position atom for a loaned asset (margin position).
    ///
    /// `proceeds` is the loaned amount times the event's `asset_in_exchange_rate`.
    ///
    /// The proceeds are US-sourced when the event date is before the bona fide residency move date,
    /// and bona fide from the move date onward. Loaned assets have no acquisition date, so
    /// residency at the time of the loan determines the sourcing.
    ///
    /// Position atoms hold no cost basis or net gain: the asset is loaned, not purchased.
    fn position_from_kraken_amount(
        asset_amount: KrakenAmount,
        event_info: &EventInfo,
        gain_config: &GainConfig,
    ) -> Self {
        let proceeds = asset_amount.get_value_usd(
            event_info
                .asset_in_exchange_rate
                .expect("Exchange rate is required"),
        );

        let (proceeds_us, proceeds_bona_fide) = match gain_config.bona_fide_residency {
            Some(move_date) => {
                if event_info.event_date < move_date {
                    (proceeds, None)
                } else {
                    (UsdAmount::default(), Some(proceeds))
                }
            }
            None => (proceeds, None),
        };

        Self::Position {
            asset_amount,
            proceeds_us,
            proceeds_bona_fide,
        }
    }

    /// Build a fee atom from a consumed pool split.
    ///
    /// `proceeds` is the fee's USD value at the moment of payment (`fee_rate`).
    ///
    /// `net_gain` is that value minus the fee coin's cost basis, classified by the fee coin's basis
    /// date (short-term/long-term against the event date, US/territory against the bona fide
    /// residency date). Margin position rollovers become investment fee atoms (investment interest
    /// expenses). All other fees become fee atoms.
    fn from_fee_split<A>(
        split: PoolAssetNonSplittable<A>,
        event_info: &EventInfo,
        fee_rate: UsdAmount,
        gain_config: &GainConfig,
    ) -> Result<Self, ExchangeRateError>
    where
        A: Asset,
        KrakenAmount: From<A>,
    {
        let asset_fee = KrakenAmount::from(split.amount);
        let proceeds = asset_fee.get_value_usd(fee_rate);
        let net_gain = {
            let basis = asset_fee.get_value_usd(
                split
                    .lifecycle
                    .get_exchange_rate_at_acquisition(&gain_config.exchange_rates_db)?,
            );
            let basis_date = split.lifecycle.get_datetime();
            let basis_synthetic_id = split.lifecycle.get_synthetic_id().to_string();

            let net_gain_portion = GainPortion {
                basis,
                basis_date,
                basis_synthetic_id,
                net_gain: proceeds - basis,
            };

            let (us, bona_fide) = match gain_config.bona_fide_residency {
                Some(move_date) => {
                    if basis_date < move_date {
                        (Some(net_gain_portion), None)
                    } else {
                        (None, Some(net_gain_portion))
                    }
                }
                None => (Some(net_gain_portion), None),
            };

            // The short-term/long-term threshold for capital gains is one year
            // This subtraction clamps February 29th (leap year) to February 28th
            let is_long_term = basis_date < event_info.event_date - Months::new(12);

            match (is_long_term, us, bona_fide) {
                (false, Some(us), None) => GainTerm::ShortUs(us),
                (false, None, Some(bona_fide)) => GainTerm::ShortBonaFide(bona_fide),
                (true, Some(us), None) => GainTerm::LongUs(us),
                (true, None, Some(bona_fide)) => GainTerm::LongBonaFide(bona_fide),
                _ => unreachable!(),
            }
        };

        if matches!(event_info.event_subtype, EventSubType::MarginRollover) {
            Ok(Self::InvestmentFee {
                asset_amount: -asset_fee, // Fees are always outgoing.
                proceeds,
                net_gain,
            })
        } else {
            Ok(Self::Fee {
                asset_amount: -asset_fee, // Fees are always outgoing.
                proceeds,
                net_gain,
                reduces_proceeds: false,
            })
        }
    }

    /// The name of this atom.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Trade { .. } => "Trade",
            Self::Income { .. } => "Income",
            Self::Position { .. } => "Position",
            Self::Fee { .. } => "Fee",
            Self::InvestmentFee { .. } => "Investment Fee",
        }
    }

    /// The atom's asset amount.
    pub(crate) fn asset_amount(&self) -> KrakenAmount {
        match self {
            Self::Trade { asset_amount, .. }
            | Self::Income { asset_amount, .. }
            | Self::Position { asset_amount, .. }
            | Self::Fee { asset_amount, .. }
            | Self::InvestmentFee { asset_amount, .. } => *asset_amount,
        }
    }
}

impl std::fmt::Display for EventSubType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trade => f.write_str("Trade"),
            Self::MarginClose => f.write_str("Margin Position Close"),
            Self::MarginSettle => f.write_str("Margin Position Settle"),
            Self::MarginOpen => f.write_str("Margin Position Open"),
            Self::MarginRollover => f.write_str("Margin Position Rollover"),
            Self::Withdrawal => f.write_str("Withdrawal"),
            Self::Deposit => f.write_str("Deposit"),
            Self::Wallet { is_sender } => write!(
                f,
                "{dir} untagged wallet transaction",
                dir = if *is_sender { "Send" } else { "Receive" },
            ),
            Self::Move => f.write_str("Move between wallets"),
            Self::TxType { tx_type, is_sender } => match tx_type {
                TxType::Spend | TxType::CapGain => write!(f, "{tx_type}"),
                _ => write!(
                    f,
                    "{dir} {tx_type}",
                    dir = if *is_sender { "Send" } else { "Receive" },
                ),
            },
        }
    }
}

impl<'a> From<&'a LedgerParsed> for EventSubType {
    fn from(value: &'a LedgerParsed) -> Self {
        match value {
            LedgerParsed::Trade { .. } => Self::Trade,
            LedgerParsed::MarginPositionClose { .. } => Self::MarginClose,
            LedgerParsed::MarginPositionSettle { .. } => Self::MarginSettle,
            LedgerParsed::MarginPositionOpen(_) => Self::MarginOpen,
            LedgerParsed::MarginPositionRollover(_) => Self::MarginRollover,
            LedgerParsed::Withdrawal(_) => Self::Withdrawal,
            LedgerParsed::Deposit(_) => Self::Deposit,
        }
    }
}
