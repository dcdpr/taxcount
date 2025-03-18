use crate::basis::{Asset, PoolAsset, PoolAssetNonSplittable};
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

    // TODO: This is created as an incomplete type, and this boolean informs the code which of the
    // inner "variants" needs to be filled out; either trade or margin position.
    // Replace these booleans with a DSL "pipeline" for manipulating the PoolAsset FIFOs.
    // See: https://gl1.dcdpr.com/rgrant/taxcount/-/issues/59
    pub(crate) has_interest_fees: bool,
    pub(crate) is_withdrawal: bool,

    // TODO: This looks like an enum. There should be no "crossing the streams". Trade+Position
    // should not happen.
    /// The event has zero or more details rows (may span multiple asset splits).
    /// Trade event atoms have a cost basis.
    pub(crate) trade_details: Vec<EventTradeAtom>,

    /// The event has zero or more details rows (may span multiple asset splits).
    /// Trade event atoms have a cost basis.
    pub(crate) income_details: Vec<EventIncomeAtom>,

    /// The event has zero or more details rows (may span multiple asset splits).
    /// Position event atoms do not have a cost basis, because the asset is loaned.
    pub(crate) position_details: Vec<EventPositionAtom>,

    /// Zero or more transaction or trade fee rows (may span multiple asset splits).
    pub(crate) tx_fees: Vec<EventFee>,

    /// Zero or more investment interest expenses rows (may span multiple asset splits).
    pub(crate) position_fees: Vec<EventFee>,
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

/// Atomized details for taxable trade events.
#[derive(Clone, Debug)]
pub(crate) struct EventTradeAtom {
    // Column A is ledger_row_id
    // Column B is asset_name
    pub(crate) asset_amount: KrakenAmount, // Column C
    pub(crate) proceeds: UsdAmount,        // Column D
    pub(crate) net_gain: GainTerm,         // Columns E-...
}

/// Atomized details for taxable income events.
#[derive(Clone, Debug)]
pub(crate) struct EventIncomeAtom {
    // Column A is ledger_row_id
    // Column B is asset_name
    pub(crate) asset_amount: KrakenAmount, // Column C
    pub(crate) proceeds: UsdAmount,        // Column D
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

/// Atomized details for taxable position events.
#[derive(Clone, Debug)]
pub(crate) struct EventPositionAtom {
    // Column A is ledger_row_id
    // Column B is asset_name
    pub(crate) asset_amount: KrakenAmount, // Column C
    pub(crate) proceeds_us: UsdAmount,     // Column D
    pub(crate) proceeds_bona_fide: Option<UsdAmount>, // Column E
}

/// Atomized details for fees.
#[derive(Clone, Debug)]
pub(crate) struct EventFee {
    // Column A is ledger_row_id
    // Column B is asset_name
    pub(crate) asset_fee: KrakenAmount, // Column C
    pub(crate) net_loss: GainTerm,      // Columns D-...
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

        let has_interest_fees = matches!(&*lp, MarginPositionOpen(_) | MarginPositionRollover(_));
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
            has_interest_fees,
            is_withdrawal,
            trade_details: vec![],
            income_details: vec![],
            position_details: vec![],
            tx_fees: vec![],
            position_fees: vec![],
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
            has_interest_fees: false,
            is_withdrawal: false,
            trade_details: vec![],
            income_details: vec![],
            position_details: vec![],
            tx_fees: vec![],
            position_fees: vec![],
        }
    }

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
            match EventTradeAtom::from_split(asset, &self.event_info, gain_config) {
                Ok(atom) => self.trade_details.push(atom),
                Err(err) => errors.push(err),
            }
        }

        errors
    }

    pub(crate) fn add_income<'a, A, I>(&mut self, split_assets: I) -> Vec<ExchangeRateError>
    where
        A: Asset + Copy + 'a,
        KrakenAmount: From<A>,
        I: Iterator<Item = &'a PoolAsset<A>>,
    {
        let mut errors = vec![];

        for asset in split_assets {
            match EventIncomeAtom::from_split(asset, &self.event_info) {
                Ok(atom) => self.income_details.push(atom),
                Err(err) => errors.push(err),
            }
        }

        errors
    }

    pub(crate) fn add_position(&mut self, asset_amount: KrakenAmount, gain_config: &GainConfig) {
        self.position_details
            .push(EventPositionAtom::from_kraken_amount(
                asset_amount,
                &self.event_info,
                gain_config,
            ));
    }

    pub(crate) fn add_tx_fee<A>(
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
            match EventFee::from_split(asset, &self.event_info, gain_config) {
                Ok(fee) => self.tx_fees.push(fee),
                Err(err) => errors.push(err),
            }
        }

        errors
    }

    pub(crate) fn add_position_fee<A>(
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
            match EventFee::from_split(asset, &self.event_info, gain_config) {
                Ok(fee) => self.position_fees.push(fee),
                Err(err) => errors.push(err),
            }
        }

        errors
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

impl EventTradeAtom {
    fn from_split<A>(
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
                        // Gain is allocated as US-sourced gains
                        (Some(total_net_gain), None)
                    } else if basis_date < move_date {
                        // Split the total gain between US-sourced and Territory-sourced gains
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

            // The short-term/long-term threshold for capital gains is one year
            // This subtraction clamps February 29th (leap year) to February 28th
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

        Ok(Self {
            asset_amount: -asset_amount, // Traded assets are always outgoing.
            proceeds,
            net_gain,
        })
    }
}

impl EventIncomeAtom {
    fn from_split<A>(
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
        Ok(Self {
            asset_amount,
            proceeds,
        })
    }
}

impl EventPositionAtom {
    fn from_kraken_amount(
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

        Self {
            asset_amount,
            proceeds_us,
            proceeds_bona_fide,
        }
    }
}

impl EventFee {
    fn from_split<A>(
        split: PoolAssetNonSplittable<A>,
        event_info: &EventInfo,
        gain_config: &GainConfig,
    ) -> Result<Self, ExchangeRateError>
    where
        A: Asset,
        KrakenAmount: From<A>,
    {
        let asset_fee = KrakenAmount::from(split.amount);
        let net_loss = {
            // For fees, we calculate the net loss as the negative of fee basis, which simplifies
            // the formula to not need the current exchange rate.
            let basis = asset_fee.get_value_usd(
                split
                    .lifecycle
                    .get_exchange_rate_at_acquisition(&gain_config.exchange_rates_db)?,
            );
            let basis_date = split.lifecycle.get_datetime();
            let basis_synthetic_id = split.lifecycle.get_synthetic_id().to_string();

            let loss_to_fee = GainPortion {
                basis,
                basis_date,
                basis_synthetic_id,
                net_gain: -basis, // Here is the negative fee basis.
            };

            let (us, bona_fide) = match gain_config.bona_fide_residency {
                Some(move_date) => {
                    if basis_date < move_date {
                        (Some(loss_to_fee), None)
                    } else {
                        (None, Some(loss_to_fee))
                    }
                }
                None => (Some(loss_to_fee), None),
            };

            // The short-term/long-term threshold for capital gains is one year
            // This subtraction clamps February 29th (leap year) to February 28th
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

        Ok(Self {
            asset_fee: -asset_fee, // Fees are always outgoing.
            net_loss,
        })
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
