use crate::basis::{Asset, BasisLifecycle, OriginSerializable, PoolAsset};
use crate::basis::{PoolBTC, PoolETH, PoolETHW, PoolUSDC, PoolUSDT};
use crate::basis::{PoolCHF, PoolEUR, PoolJPY, PoolUSD};
use crate::gitver_hashes;
use crate::imports::wallet::{Auditor, TxType};
use crate::model::blockchain::{Account, Utxo};
use crate::model::events::WalletDirection;
use crate::model::kraken_amount::{KrakenAmount, UsdAmount};
use crate::util::fifo::FIFO;
use bdk::bitcoin::Network;
use chrono::{DateTime, Utc};
use gitver::GitverHashes;
use ron::{de::SpannedError, ser::PrettyConfig};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::{fmt::Debug, path::Path, rc::Rc};
use thiserror::Error;

pub const CHECKPOINT_VERSION: &str = "3.0.0";

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("Unable to deserialize")]
    Deserialize(#[from] SpannedError),

    #[error("Unable to serialize")]
    Serialize(#[from] ron::Error),

    #[error("Expected version `{CHECKPOINT_VERSION}`, found `{0}`")]
    Version(String),
}

/// State for saving and restoring accounting records across tax years.
#[derive(Debug)]
pub struct State {
    pub(crate) header: CheckpointHeader,
    pub(crate) bona_fide_residency: Option<DateTime<Utc>>,

    /// Balances held on the user's wallets.
    ///
    /// Internally contains cost-basis information for each asset held.
    pub on_chain_balances: UtxoBalances,

    /// Balances held on the exchange.
    ///
    /// Internally contains cost-basis information for each asset held.
    pub exchange_balances: Balances,

    pub(crate) pending_deposits: Pending,
    pub(crate) pending_withdrawals: Pending,

    /// Borrowers place collateral into a suspended state where the assets cannot be spent until the
    /// loan is paid and collateral is returned. The asset's basis is moved here while held as
    /// collateral.
    ///
    /// The basis is removed from the suspended state by one or more transactions which create new
    /// UTXOs. Upon return of collateral, the asset regains its original cost basis.
    ///
    /// This claim, on a wallet other than the borrower's, does not list the remote
    /// wallet's UTXO as its index, but instead the [`loan_id`].
    ///
    /// [`loan_id`]: crate::imports::wallet::TxType#variant.LoanCollateral.field.loan_id
    pub borrower_collateral: UtxoBalances,

    /// Lenders place capital into a suspended state where the assets cannot be spent until the loan
    /// is repaid. The asset's basis is moved here for the duration of the loan.
    ///
    /// The basis is removed from the suspended state by one or more transactions which create new
    /// UTXOs. Upon receipt of repayment, the asset regains its original cost basis.
    ///
    /// This claim, on a wallet other than the lender's, does not list the remote
    /// wallet's UTXO as its index, but instead the [`loan_id`].
    ///
    /// [`loan_id`]: crate::imports::wallet::TxType#variant.LoanCapital.field.loan_id
    pub lender_capital: UtxoBalances,

    /// The full cache for every address ever claimed.
    ///
    /// Contains stand-alone addresses, extended public keys, and all keychains derived from the
    /// xpubs.
    pub auditor: Auditor,
}

/// Asset balances for on-chain Unspent Transaction Outputs (UTXO).
///
/// Stores a list of all "basis-splits" for each transaction output (TXO).
///
/// When an asset is received, its TXO acquires one or more "pooled assets" representing the total
/// value on the TXO and all of the cost basis lifecycle information for those assets.
///
/// When a TXO is spent, it is removed and the basis-splits propagate to the destination. E.g. to a
/// change address or another wallet address owned by the user. Spends will split the "pooled
/// assets" as necessary to fulfill the transaction.
///
/// In this way, UTXOs act as a kind of wallet balance (as they do on the block chain) with the
/// inclusion of cost basis information.
///
/// It is important to note that this structure does not distinguish between addresses belonging to
/// unrelated wallets. All addresses belonging to the user are collected in one `Utxo` struct,
/// forming a complete financial state of all wallets.
///
/// "Basis-splits" are the same as used in the FIFOs for exchange balances. They are just stored in
///  a different structure that resembles the blockchain instead of a giant FIFO pool.
///
/// [`State::lender_capital`] (and [`State::borrower_collateral`])
/// also use UtxoBalances, even though they are claims on a wallet
/// other than this lender's (or this borrower's), and do not list the
/// remote wallet's UTXO as its index, but instead the
/// [`LoanCapital.loan_id`] (or [`LoanCollateral.loan_id`]).
///
/// [`LoanCapital.loan_id`]: crate::imports::wallet::TxType#variant.LoanCapital.field.loan_id
/// [`LoanCollateral.loan_id`]: crate::imports::wallet::TxType#variant.LoanCollateral.field.loan_id
#[derive(Debug, Default)]
pub struct UtxoBalances {
    pub btc: Utxo<PoolBTC>,
    pub chf: Account<PoolCHF>,
    pub eth: Account<PoolETH>,
    pub ethw: Account<PoolETHW>,
    pub eur: Account<PoolEUR>,
    pub jpy: Account<PoolJPY>,
    pub usd: Account<PoolUSD>,
    pub usdc: Account<PoolUSDC>,
    pub usdt: Account<PoolUSDT>,
}

/// Asset balances for exchange trades.
///
/// Stores a FIFO of all "basis-splits".
///
/// When an asset is bought, it is appended to the tail of the FIFO as a "pooled asset".
///
/// When sold, the pooled assets at the head of the FIFO are removed until the sale can be
/// fulfilled. The remainder is split off and returned to the head of the FIFO.
///
/// In this way, FIFOs act as a kind of exchange balance with the inclusion of cost basis
/// information.
#[derive(Debug, Default)]
pub struct Balances {
    pub btc: FIFO<PoolBTC>,
    pub chf: FIFO<PoolCHF>,
    pub eth: FIFO<PoolETH>,
    pub ethw: FIFO<PoolETHW>,
    pub eur: FIFO<PoolEUR>,
    pub jpy: FIFO<PoolJPY>,
    pub usd: FIFO<PoolUSD>,
    pub usdc: FIFO<PoolUSDC>,
    pub usdt: FIFO<PoolUSDT>,
}

/// Pending deposits and withdrawals are mapped to timestamps for quick range lookups.
#[derive(Debug, Default)]
pub struct Pending {
    pub btc: BTreeMap<DateTime<Utc>, PendingUtxo>,
    pub chf: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolCHF>>,
    pub eth: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolETH>>,
    pub ethw: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolETHW>>,
    pub eur: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolEUR>>,
    pub jpy: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolJPY>>,
    pub usd: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolUSD>>,
    pub usdc: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolUSDC>>,
    pub usdt: BTreeMap<DateTime<Utc>, PendingAccountTx<PoolUSDT>>,
}

#[derive(Debug)]
pub struct PendingUtxo {
    pub worksheet_name: Rc<str>,
    pub utxos: Utxo<PoolBTC>,
    // Tx info provides extra context to the UTXOs. Namely exchange rate and account name.
    pub tx_info: HashMap<String, PendingTxInfo<PoolBTC>>,
}

#[derive(Debug)]
pub struct PendingAccountTx<A> {
    pub worksheet_name: Rc<str>,
    pub account: Account<A>,
    // Tx info provides extra context to the Accounts. Namely exchange rate and account name.
    pub tx_info: HashMap<String, PendingTxInfo<A>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PendingTxInfo<A> {
    pub exchange_rate: Option<UsdAmount>,
    pub dir: WalletDirection,
    pub tx_type: Option<TxType>,
    pub account_name: String,
    pub note: String,
    pub fees: FIFO<A>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Checkpoint {
    header: CheckpointHeader,
    bona_fide_residency: Option<DateTime<Utc>>,
    on_chain_balances: CheckpointUtxoBalances,
    exchange_balances: CheckpointBalances,
    pending_deposits: CheckpointPending,
    pending_withdrawals: CheckpointPending,
    borrower_collateral: CheckpointUtxoBalances,
    lender_capital: CheckpointUtxoBalances,
    auditor: Auditor,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckpointUtxoBalances {
    btc: HashMap<String, FIFO<CheckpointPoolAsset>>,
    chf: HashMap<String, FIFO<CheckpointPoolAsset>>,
    eth: HashMap<String, FIFO<CheckpointPoolAsset>>,
    ethw: HashMap<String, FIFO<CheckpointPoolAsset>>,
    eur: HashMap<String, FIFO<CheckpointPoolAsset>>,
    jpy: HashMap<String, FIFO<CheckpointPoolAsset>>,
    usd: HashMap<String, FIFO<CheckpointPoolAsset>>,
    usdc: HashMap<String, FIFO<CheckpointPoolAsset>>,
    usdt: HashMap<String, FIFO<CheckpointPoolAsset>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckpointBalances {
    btc: FIFO<CheckpointPoolAsset>,
    chf: FIFO<CheckpointPoolAsset>,
    eth: FIFO<CheckpointPoolAsset>,
    ethw: FIFO<CheckpointPoolAsset>,
    eur: FIFO<CheckpointPoolAsset>,
    jpy: FIFO<CheckpointPoolAsset>,
    usd: FIFO<CheckpointPoolAsset>,
    usdc: FIFO<CheckpointPoolAsset>,
    usdt: FIFO<CheckpointPoolAsset>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckpointPending {
    btc: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    chf: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    eth: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    ethw: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    eur: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    jpy: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    usd: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    usdc: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
    usdt: BTreeMap<DateTime<Utc>, CheckpointBlockchain>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckpointBlockchain {
    worksheet_name: String,
    transactions: HashMap<String, FIFO<CheckpointPoolAsset>>,
    tx_info: HashMap<String, PendingTxInfo<CheckpointPoolAsset>>,
}

/// Header for checkpoints.
#[derive(Debug, Deserialize, Serialize)]
pub struct CheckpointHeader {
    /// When the checkpoint was saved.
    pub time: String,

    /// Checkpoint version.
    pub semver: String,

    /// List of git file hashes that generated the checkpoint.
    pub gitver: GitverHashes,

    /// Timestamps for the most recent ledger row processed.
    pub latest_row_time: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckpointPoolAsset {
    balance: KrakenAmount,
    origin: OriginSerializable,
}

/// Information about pending deposits and withdrawals.
#[derive(Debug)]
pub struct PendingInfo {
    /// Asset short name like "BTC" or "ETH".
    pub asset: &'static str,

    /// Synthetic ID for pending deposit or withdrawal.
    pub synthetic_id: String,

    /// Timestamps for pending deposit or withdrawal.
    pub time: DateTime<Utc>,

    /// A list of `(synthetic_id, amount)` for basis details.
    pub details: Vec<(String, KrakenAmount)>,
}

impl State {
    /// Create a new `State` with the given Bitcoin `Network`.
    pub fn new(network: Network) -> Self {
        Self {
            header: CheckpointHeader::default(),
            bona_fide_residency: None,
            on_chain_balances: UtxoBalances::default(),
            exchange_balances: Balances::default(),
            pending_deposits: Pending::default(),
            pending_withdrawals: Pending::default(),
            borrower_collateral: UtxoBalances::default(),
            lender_capital: UtxoBalances::default(),
            auditor: Auditor::new(network),
        }
    }

    /// Load a `State` from the given path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        let serialized = std::fs::read_to_string(path)?;
        let checkpoint: Checkpoint = ron::from_str(&serialized)?;

        // Validate header.
        if checkpoint.header.semver == CHECKPOINT_VERSION {
            let mut state: State = checkpoint.into();
            //patch compiled-in gitver over prior checkpoint gitvers
            state.header.gitver = gitver_hashes::get().clone();
            Ok(state)
        } else {
            Err(CheckpointError::Version(checkpoint.header.semver))
        }
    }

    /// Save a `State` to the given path.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), CheckpointError> {
        let header = CheckpointHeader {
            latest_row_time: self.header.latest_row_time.clone(),
            ..Default::default()
        };
        let checkpoint = Checkpoint {
            header,
            bona_fide_residency: self.bona_fide_residency,
            on_chain_balances: self.on_chain_balances.to_checkpoint(),
            exchange_balances: self.exchange_balances.to_checkpoint(),
            pending_deposits: self.pending_deposits.to_checkpoint(),
            pending_withdrawals: self.pending_withdrawals.to_checkpoint(),
            borrower_collateral: self.borrower_collateral.to_checkpoint(),
            lender_capital: self.lender_capital.to_checkpoint(),
            auditor: self.auditor.clone(),
        };
        let serialized = ron::ser::to_string_pretty(&checkpoint, PrettyConfig::default())?;

        std::fs::write(path, serialized)?;

        Ok(())
    }

    /// Set Bona Fide Residency date, returning the old value (if any).
    pub fn bona_fide_residency(
        &mut self,
        bona_fide_residency: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.bona_fide_residency.replace(bona_fide_residency)
    }

    /// Get a pair of iterators for pending deposit and withdrawal information.
    pub fn check_pending(
        &self,
    ) -> (
        impl Iterator<Item = PendingInfo> + '_,
        impl Iterator<Item = PendingInfo> + '_,
    ) {
        type PendingInfos = Vec<PendingInfo>;

        // Returns a partial function with the given asset name. The function maps a key-value pair
        // to a `PendingInfo`.
        fn mpu(asset: &'static str) -> impl FnMut((&DateTime<Utc>, &PendingUtxo)) -> PendingInfos {
            move |(time, pending)| {
                pending
                    .utxos
                    .iter()
                    .map(|(synthetic_id, pool_assets)| PendingInfo {
                        asset,
                        synthetic_id: synthetic_id.clone(),
                        time: *time,
                        details: pool_assets
                            .iter()
                            .map(|pool_asset| {
                                (
                                    pool_asset.lifecycle.get_synthetic_id().to_string(),
                                    pool_asset.amount.into(),
                                )
                            })
                            .collect(),
                    })
                    .collect()
            }
        }
        fn mpa<A: Asset + Copy>(
            asset: &'static str,
        ) -> impl FnMut((&DateTime<Utc>, &PendingAccountTx<PoolAsset<A>>)) -> PendingInfos {
            move |(time, pending)| {
                pending
                    .account
                    .iter()
                    .map(|(synthetic_id, pool_assets)| PendingInfo {
                        asset,
                        synthetic_id: synthetic_id.clone(),
                        time: *time,
                        details: pool_assets
                            .iter()
                            .map(|pool_asset| {
                                (
                                    pool_asset.lifecycle.get_synthetic_id().to_string(),
                                    pool_asset.amount.into(),
                                )
                            })
                            .collect(),
                    })
                    .collect()
            }
        }

        let deposits = self
            .pending_deposits
            .btc
            .iter()
            .flat_map(mpu("BTC"))
            .chain(self.pending_deposits.chf.iter().flat_map(mpa("CHF")))
            .chain(self.pending_deposits.eth.iter().flat_map(mpa("ETH")))
            .chain(self.pending_deposits.ethw.iter().flat_map(mpa("ETHW")))
            .chain(self.pending_deposits.eur.iter().flat_map(mpa("EUR")))
            .chain(self.pending_deposits.jpy.iter().flat_map(mpa("JPY")))
            .chain(self.pending_deposits.usdc.iter().flat_map(mpa("USDC")))
            .chain(self.pending_deposits.usdt.iter().flat_map(mpa("USDT")));

        let withdrawals = self
            .pending_withdrawals
            .btc
            .iter()
            .flat_map(mpu("BTC"))
            .chain(self.pending_withdrawals.chf.iter().flat_map(mpa("CHF")))
            .chain(self.pending_withdrawals.eth.iter().flat_map(mpa("ETH")))
            .chain(self.pending_withdrawals.ethw.iter().flat_map(mpa("ETHW")))
            .chain(self.pending_withdrawals.eur.iter().flat_map(mpa("EUR")))
            .chain(self.pending_withdrawals.jpy.iter().flat_map(mpa("JPY")))
            .chain(self.pending_withdrawals.usdc.iter().flat_map(mpa("USDC")))
            .chain(self.pending_withdrawals.usdt.iter().flat_map(mpa("USDT")));

        (deposits, withdrawals)
    }
}

impl Default for CheckpointHeader {
    fn default() -> Self {
        Self {
            time: format!("{}", Utc::now().format("%F %T")),
            semver: CHECKPOINT_VERSION.to_string(),
            gitver: gitver_hashes::get().clone(),
            latest_row_time: None,
        }
    }
}

impl From<Checkpoint> for State {
    fn from(checkpoint: Checkpoint) -> Self {
        Self {
            header: checkpoint.header,
            bona_fide_residency: checkpoint.bona_fide_residency,
            on_chain_balances: checkpoint.on_chain_balances.into(),
            exchange_balances: checkpoint.exchange_balances.into(),
            pending_deposits: checkpoint.pending_deposits.into(),
            pending_withdrawals: checkpoint.pending_withdrawals.into(),
            borrower_collateral: checkpoint.borrower_collateral.into(),
            lender_capital: checkpoint.lender_capital.into(),
            auditor: checkpoint.auditor,
        }
    }
}

impl From<CheckpointUtxoBalances> for UtxoBalances {
    fn from(balances: CheckpointUtxoBalances) -> Self {
        Self {
            btc: balances.btc.into(),
            chf: balances.chf.into(),
            eth: balances.eth.into(),
            ethw: balances.ethw.into(),
            eur: balances.eur.into(),
            jpy: balances.jpy.into(),
            usd: balances.usd.into(),
            usdc: balances.usdc.into(),
            usdt: balances.usdt.into(),
        }
    }
}

impl From<CheckpointBalances> for Balances {
    fn from(balances: CheckpointBalances) -> Balances {
        Balances {
            btc: balances.btc.into(),
            chf: balances.chf.into(),
            eth: balances.eth.into(),
            ethw: balances.ethw.into(),
            eur: balances.eur.into(),
            jpy: balances.jpy.into(),
            usd: balances.usd.into(),
            usdc: balances.usdc.into(),
            usdt: balances.usdt.into(),
        }
    }
}

impl<A: Asset> From<PendingTxInfo<CheckpointPoolAsset>> for PendingTxInfo<PoolAsset<A>>
where
    FIFO<PoolAsset<A>>: From<FIFO<CheckpointPoolAsset>>,
{
    fn from(value: PendingTxInfo<CheckpointPoolAsset>) -> Self {
        Self {
            exchange_rate: value.exchange_rate,
            dir: value.dir,
            tx_type: value.tx_type,
            account_name: value.account_name,
            note: value.note,
            fees: value.fees.into(),
        }
    }
}

impl<A: Asset> From<PendingTxInfo<PoolAsset<A>>> for PendingTxInfo<CheckpointPoolAsset>
where
    FIFO<CheckpointPoolAsset>: From<FIFO<PoolAsset<A>>>,
{
    fn from(value: PendingTxInfo<PoolAsset<A>>) -> Self {
        Self {
            exchange_rate: value.exchange_rate,
            dir: value.dir,
            tx_type: value.tx_type,
            account_name: value.account_name,
            note: value.note,
            fees: value.fees.into(),
        }
    }
}

impl From<CheckpointBlockchain> for PendingUtxo {
    fn from(value: CheckpointBlockchain) -> Self {
        Self {
            worksheet_name: Rc::from(value.worksheet_name.as_str()),
            utxos: value.transactions.into(),
            tx_info: hashmap_to_pending(value.tx_info),
        }
    }
}

impl<A> From<CheckpointBlockchain> for PendingAccountTx<A>
where
    Account<A>: From<HashMap<String, FIFO<CheckpointPoolAsset>>>,
    PendingTxInfo<A>: From<PendingTxInfo<CheckpointPoolAsset>>,
{
    fn from(value: CheckpointBlockchain) -> Self {
        Self {
            worksheet_name: Rc::from(value.worksheet_name.as_str()),
            account: value.transactions.into(),
            tx_info: value
                .tx_info
                .into_iter()
                .map(|(txid, pending)| (txid.clone(), pending.into()))
                .collect(),
        }
    }
}

impl From<CheckpointPending> for Pending {
    fn from(value: CheckpointPending) -> Self {
        Self {
            btc: value
                .btc
                .into_iter()
                .map(|(time, utxo)| (time, utxo.into()))
                .collect(),
            chf: value
                .chf
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
            eth: value
                .eth
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
            ethw: value
                .ethw
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
            eur: value
                .eur
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
            jpy: value
                .jpy
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
            usd: value
                .usd
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
            usdc: value
                .usdc
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
            usdt: value
                .usdt
                .into_iter()
                .map(|(time, address)| (time, address.into()))
                .collect(),
        }
    }
}

impl<A: Asset> From<HashMap<String, FIFO<CheckpointPoolAsset>>> for Utxo<PoolAsset<A>>
where
    <A as TryFrom<KrakenAmount>>::Error: Debug,
{
    fn from(value: HashMap<String, FIFO<CheckpointPoolAsset>>) -> Self {
        Self::from_iter(value.into_iter().map(|(txid, fifo)| (txid, fifo.into())))
    }
}

impl<A: Asset> From<HashMap<String, FIFO<CheckpointPoolAsset>>> for Account<PoolAsset<A>>
where
    <A as TryFrom<KrakenAmount>>::Error: Debug,
{
    fn from(value: HashMap<String, FIFO<CheckpointPoolAsset>>) -> Self {
        Self::from_iter(
            value
                .into_iter()
                .map(|(address, fifo)| (address, fifo.into())),
        )
    }
}

impl<A: Asset> From<FIFO<CheckpointPoolAsset>> for FIFO<PoolAsset<A>>
where
    <A as TryFrom<KrakenAmount>>::Error: Debug,
{
    fn from(value: FIFO<CheckpointPoolAsset>) -> Self {
        value
            .into_iter()
            .map(|checkpoint| checkpoint.into())
            .collect()
    }
}

impl<A> From<CheckpointPoolAsset> for PoolAsset<A>
where
    A: Asset + TryFrom<KrakenAmount>,
    <A as TryFrom<KrakenAmount>>::Error: Debug,
{
    fn from(checkpoint: CheckpointPoolAsset) -> Self {
        Self {
            amount: checkpoint.balance.try_into().unwrap(),
            lifecycle: BasisLifecycle::lifecycle_from_origin(checkpoint.origin),
        }
    }
}

impl UtxoBalances {
    fn to_checkpoint(&self) -> CheckpointUtxoBalances {
        CheckpointUtxoBalances {
            btc: self.btc.to_checkpoint(),
            chf: self.chf.to_checkpoint(),
            eth: self.eth.to_checkpoint(),
            ethw: self.ethw.to_checkpoint(),
            eur: self.eur.to_checkpoint(),
            jpy: self.jpy.to_checkpoint(),
            usd: self.usd.to_checkpoint(),
            usdc: self.usdc.to_checkpoint(),
            usdt: self.usdt.to_checkpoint(),
        }
    }
}

impl Balances {
    fn to_checkpoint(&self) -> CheckpointBalances {
        CheckpointBalances {
            btc: self.btc.to_checkpoint(),
            chf: self.chf.to_checkpoint(),
            eth: self.eth.to_checkpoint(),
            ethw: self.ethw.to_checkpoint(),
            eur: self.eur.to_checkpoint(),
            jpy: self.jpy.to_checkpoint(),
            usd: self.usd.to_checkpoint(),
            usdc: self.usdc.to_checkpoint(),
            usdt: self.usdt.to_checkpoint(),
        }
    }
}

impl PendingUtxo {
    fn to_checkpoint(&self) -> CheckpointBlockchain {
        CheckpointBlockchain {
            worksheet_name: self.worksheet_name.to_string(),
            transactions: self.utxos.to_checkpoint(),
            tx_info: hashmap_to_checkpoint(&self.tx_info),
        }
    }
}

impl<A> PendingAccountTx<PoolAsset<A>>
where
    A: Asset + Copy,
    KrakenAmount: From<A>,
{
    fn to_checkpoint(&self) -> CheckpointBlockchain {
        CheckpointBlockchain {
            worksheet_name: self.worksheet_name.to_string(),
            transactions: self.account.to_checkpoint(),
            tx_info: hashmap_to_checkpoint(&self.tx_info),
        }
    }
}

impl Pending {
    fn to_checkpoint(&self) -> CheckpointPending {
        CheckpointPending {
            btc: self
                .btc
                .iter()
                .map(|(time, utxo)| (*time, utxo.to_checkpoint()))
                .collect(),
            chf: self
                .chf
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
            eth: self
                .eth
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
            ethw: self
                .ethw
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
            eur: self
                .eur
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
            jpy: self
                .jpy
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
            usd: self
                .usd
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
            usdc: self
                .usdc
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
            usdt: self
                .usdt
                .iter()
                .map(|(time, account)| (*time, account.to_checkpoint()))
                .collect(),
        }
    }
}

impl<A> Utxo<PoolAsset<A>>
where
    A: Asset + Copy,
    KrakenAmount: From<A>,
{
    fn to_checkpoint(&self) -> HashMap<String, FIFO<CheckpointPoolAsset>> {
        self.iter()
            .map(|(address, pool_assets)| {
                (
                    address.to_string(),
                    pool_assets
                        .iter()
                        .map(|pool_asset| CheckpointPoolAsset {
                            balance: KrakenAmount::from(pool_asset.amount),
                            origin: pool_asset.lifecycle.get_serializable_origin(),
                        })
                        .collect(),
                )
            })
            .collect()
    }
}

impl<A> Account<PoolAsset<A>>
where
    A: Asset + Copy,
    KrakenAmount: From<A>,
{
    fn to_checkpoint(&self) -> HashMap<String, FIFO<CheckpointPoolAsset>> {
        self.iter()
            .map(|(address, pool_assets)| {
                (
                    address.to_string(),
                    pool_assets
                        .iter()
                        .map(|pool_asset| CheckpointPoolAsset {
                            balance: KrakenAmount::from(pool_asset.amount),
                            origin: pool_asset.lifecycle.get_serializable_origin(),
                        })
                        .collect(),
                )
            })
            .collect()
    }
}

impl<A> FIFO<PoolAsset<A>>
where
    A: Asset + Copy,
    KrakenAmount: From<A>,
{
    fn to_checkpoint(&self) -> FIFO<CheckpointPoolAsset> {
        self.iter()
            .map(|pool_asset| CheckpointPoolAsset {
                balance: KrakenAmount::from(pool_asset.amount),
                origin: pool_asset.lifecycle.get_serializable_origin(),
            })
            .collect()
    }
}

impl<A> PendingTxInfo<PoolAsset<A>>
where
    A: Asset + Copy,
    KrakenAmount: From<A>,
{
    fn to_checkpoint(&self) -> PendingTxInfo<CheckpointPoolAsset> {
        PendingTxInfo {
            exchange_rate: self.exchange_rate,
            dir: self.dir,
            tx_type: self.tx_type.clone(),
            account_name: self.account_name.clone(),
            note: self.note.clone(),
            fees: self.fees.to_checkpoint(),
        }
    }
}

fn hashmap_to_checkpoint<A>(
    tx_info: &HashMap<String, PendingTxInfo<PoolAsset<A>>>,
) -> HashMap<String, PendingTxInfo<CheckpointPoolAsset>>
where
    A: Asset + Copy,
    KrakenAmount: From<A>,
{
    tx_info
        .iter()
        .map(|(txid, pending)| (txid.clone(), pending.to_checkpoint()))
        .collect()
}

fn hashmap_to_pending<A>(
    tx_info: HashMap<String, PendingTxInfo<CheckpointPoolAsset>>,
) -> HashMap<String, PendingTxInfo<PoolAsset<A>>>
where
    A: Asset,
    <A as TryFrom<KrakenAmount>>::Error: Debug,
{
    tx_info
        .into_iter()
        .map(|(txid, pending)| (txid.clone(), pending.into()))
        .collect()
}
