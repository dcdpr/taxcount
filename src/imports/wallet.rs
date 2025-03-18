use self::{electrum::ElectrumCsvRow, ledgerlive::LedgerLiveCsvRow};
use crate::client::{transpose_arc_result, ClientApi, ClientError};
use crate::imports::address::{AddressCache, Xpub};
use crate::util::{fifo::FIFO, year_ext::GetYear};
use crate::{basis::AssetName, model::kraken_amount::KrakenAmount};
use bdk::bitcoin::{Address, BlockHash, Network, Script, ScriptHash, Txid};
use chrono::{DateTime, Datelike as _, Utc};
use esploda::esplora::Status;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::{convert::identity, path::Path};
use thiserror::Error;
use tracing::{debug, trace};

pub mod electrum;
pub mod ledgerlive;

#[derive(Debug, Error)]
pub enum WalletError {
    #[error("CSV parsing error")]
    Csv(#[from] csv::Error),
}

#[derive(Debug, Error)]
pub enum TxTagsError {
    #[error("CSV parsing error")]
    Csv(#[from] csv::Error),

    #[error("Exchange rate parsing error")]
    ExchangeRate(#[from] crate::errors::ConvertAmountError),

    #[error("Unknown tx_type: `{0}`")]
    TxType(String),
}

#[derive(Debug, Error)]
pub enum TxoError {
    #[error("Auditor error")]
    Auditor(#[from] AuditorError),

    #[error("time parsing error")]
    Time(#[from] chrono::ParseError),

    #[error("txid parsing error")]
    TxId(#[from] bdk::bitcoin::hashes::hex::Error),

    #[error("Bitcoin address parsing error")]
    Address(#[from] bdk::bitcoin::address::Error),

    #[error("tx_index parsing error")]
    TxIndex(#[from] std::num::ParseIntError),

    #[error("Client error")]
    Client(#[from] ClientError),

    #[error("Invalid tx_type")]
    InvalidTxType,

    #[error("Asset error")]
    Asset(#[from] crate::errors::AssetNameError),

    #[error("Amount error")]
    Amount(#[from] crate::errors::ConvertAmountError),

    #[error("Transactions do not match any known xpub or address:\n{0:#?}")]
    NotMine(Vec<String>),
}

#[derive(Debug, Error)]
pub enum AuditorError {
    #[error("Address error")]
    Address(#[from] crate::imports::address::AddressError),

    #[error("Unable to parse address")]
    AddressParse(#[from] bdk::bitcoin::address::Error),
}

/// A container for wallet CSV rows. Allows transactions from multiple sources to be combined in a
/// single sorted collection.
#[derive(Debug)]
pub enum WalletCsvRow {
    /// CSV row from a generic wallet.
    Generic(GenericTxCsvRow),

    /// CSV row from an Electrum wallet.
    Electrum(ElectrumCsvRow),

    /// CSV row from a LedgerLive wallet.
    LedgerLive(LedgerLiveCsvRow),
}

/// The generic wallet CSV is used as a lowest common denominator for wallets that do not offer a
/// transaction history export.
#[derive(Clone, Debug, Deserialize)]
pub struct GenericTxCsvRow {
    pub(crate) asset: String, // "XXBT", "XETH", etc.
    pub(crate) txid: String,
    pub(crate) tx_index: String,
    pub(crate) account: String,
    pub(crate) note: String, // Invoice ID, transaction purpose, etc.
}

/// A transaction created by enriching wallet CSV rows with information from the blockchain.
#[derive(Clone, Debug)]
pub struct Tx {
    pub(crate) time: DateTime<Utc>,
    pub(crate) asset: AssetName,
    pub(crate) txid: String,
    pub(crate) ins: Vec<Txi>,
    pub(crate) outs: Vec<Txo>,
    /// These are optional because they may not be provided by the wallet or tx tags.
    pub(crate) tx_type: Option<TxType>,
    pub(crate) exchange_rate: Option<KrakenAmount>,
}

/// A transaction input. This is just a "cache" of previous TXOs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Txi {
    /// External ID is "txid:index" for Bitcoin.
    pub(crate) external_id: String,
    /// `None` for UTXO-based blockchains like Bitcoin. `Some` for account-based blockchains like
    /// Ethereum.
    pub(crate) amount: Option<KrakenAmount>,
    pub(crate) mine: bool, // Whether the TXO can be spent by any of the user's wallets.
}

/// A transaction output. Loosely coupled to an individual wallet CSV row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Txo {
    pub(crate) amount: KrakenAmount,
    pub(crate) mine: bool, // Whether the TXO can be spent by any of the user's wallets.
    pub(crate) wallet_info: BTreeSet<TxoInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct TxoInfo {
    pub(crate) account: String, // Wallet or account name.
    pub(crate) note: String,    // Invoice ID, transaction purpose, etc.
}

/// Maps a TxId to a tag (`tx_type`) and an optional exchange rate.
#[derive(Clone, Debug, Default)]
pub struct TxTags {
    tags: HashMap<String, TxTag>,
}

#[derive(Clone, Debug)]
pub struct TxTag {
    pub(crate) tx_type: TxType,
    pub(crate) exchange_rate: Option<KrakenAmount>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TxType {
    /// Declare asset acquisition as resulting from exchange for another asset.
    Trade,

    /// Declare asset acquisition as economically irrelevant & unwanted spam.
    Spam,

    /// Declare asset acquisition as income (e.g. labor).
    Income,

    /// Declare asset acquisition as capital gain (e.g. sold a chair).
    CapGain,

    /// Declare asset acquisition as a loan (e.g. borrowed capital or return of capital).
    LoanCapital { role: LoanRole, loan_id: String },

    /// Declare asset acquisition as a loan (e.g. assignment of collateral or return of collateral).
    LoanCollateral { role: LoanRole, loan_id: String },

    /// Declare asset acquisition as a result of a fork (e.g. Bitcoin Cash, Ethereum Proof of Work).
    Fork,

    /// Declare asset divestment as lost or stolen.
    Lost,

    /// Declare asset acquisition or divestment as a gift (exempt up to limit).
    Gift,

    /// Declare asset divestment as a tax-deductible donation to a 501(c)(3) nonprofit organization.
    Donation,

    /// Declare asset divestment as a spend with capital gains.
    Spend,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum LoanRole {
    /// I am the lender in the loan agreement.
    Lender,

    /// I am the borrower in the loan agreement.
    Borrower,
}

/// Auditor can answer the question of whether a ScriptPubKey belongs to the user, and therefore it
/// can identify whether a transaction input or output is ours.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Auditor {
    address_cache: AddressCache,
    addresses: HashSet<ScriptHash>,
    network: Network,
}

impl Tx {
    /// Create a wallet transaction.
    ///
    /// # Panics
    ///
    /// Asserts that `ins` and `outs` are non-empty.
    pub fn new(
        time: DateTime<Utc>,
        asset: AssetName,
        txid: &str,
        ins: Vec<Txi>,
        outs: Vec<Txo>,
        tx_type: Option<TxType>,
        exchange_rate: Option<KrakenAmount>,
    ) -> Self {
        assert!(!ins.is_empty());
        assert!(!outs.is_empty());

        Self {
            time,
            asset,
            txid: txid.to_string(),
            ins,
            outs,
            tx_type,
            exchange_rate,
        }
    }
}

/// Enable consistency checks on years.
impl GetYear for Tx {
    fn get_year(&self) -> i32 {
        self.time.year()
    }
}

impl Txi {
    /// Create a wallet transaction input.
    pub fn new(external_id: &str, amount: Option<KrakenAmount>, mine: bool) -> Self {
        Self {
            external_id: external_id.to_string(),
            amount,
            mine,
        }
    }
}

impl Txo {
    /// Create a wallet transaction output.
    pub fn new(amount: KrakenAmount, mine: bool, account: &str, note: &str) -> Self {
        Self {
            amount,
            mine,
            wallet_info: BTreeSet::from_iter([TxoInfo {
                account: account.to_string(),
                note: note.to_string(),
            }]),
        }
    }

    pub(crate) fn accounts(&self) -> impl Iterator<Item = &str> {
        self.wallet_info.iter().map(|info| info.account.as_str())
    }

    pub(crate) fn notes(&self) -> impl Iterator<Item = &str> {
        self.wallet_info.iter().map(|info| info.note.as_str())
    }
}

/// Read a generic wallet CSV and append rows to a vector.
pub fn read_generic(
    path: impl AsRef<Path>,
    rows: &mut Vec<WalletCsvRow>,
) -> Result<(), WalletError> {
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    for row in reader.deserialize() {
        rows.push(WalletCsvRow::Generic(row?));
    }

    Ok(())
}

impl WalletCsvRow {
    /// Get xpub from CSV row.
    ///
    /// Not all CSV sources have this field.
    fn xpub(&self) -> Option<&str> {
        match self {
            Self::LedgerLive(row) => Some(row.xpub.as_str()),
            _ => None,
        }
    }

    /// Get parsed transaction ID from CSV row.
    fn parse_txid(&self) -> Result<Txid, TxoError> {
        Ok(self.txid().parse()?)
    }

    /// Get transaction ID as a string from CSV row.
    fn txid(&self) -> &str {
        match self {
            Self::Generic(row) => &row.txid,
            Self::Electrum(row) => &row.transaction_hash,
            Self::LedgerLive(row) => &row.txid,
        }
    }

    /// Get asset name from CSV row.
    fn asset(&self) -> Result<AssetName, TxoError> {
        let asset = match self {
            Self::Generic(row) => &row.asset,
            Self::Electrum(_) => "BTC",
            Self::LedgerLive(row) => &row.asset,
        };

        Ok(asset.parse()?)
    }

    /// Get TXO index if CSV row supports it.
    fn txo_index(&self) -> Option<usize> {
        match self {
            Self::Generic(row) => row.tx_index.parse().ok(),
            Self::Electrum(_) | Self::LedgerLive(_) => None,
        }
    }

    // Get TXO Info from CSV row.
    fn txo_info(&self) -> TxoInfo {
        match self {
            Self::Generic(row) => TxoInfo {
                account: row.account.clone(),
                note: row.note.clone(),
            },
            Self::Electrum(row) => TxoInfo {
                account: row.account.clone(),
                note: row.label.clone(),
            },
            Self::LedgerLive(row) => TxoInfo {
                account: row.account.clone(),
                note: String::new(),
            },
        }
    }
}

/// Bitcoin blockchain index of the form `(block_height, tx_index_within_block)`.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BtcIndex(u32, u32);

/// Collect a mapping of `Txid` to `BtcIndex` from a series of `Txid`s.
fn get_indices<C: ClientApi>(
    client: &C,
    txids: &[Txid],
) -> Result<HashMap<String, BtcIndex>, ClientError> {
    // Get Transactions for all TxIds.
    let tx_responses = client.get_transactions(txids);
    let transactions = transpose_arc_result(&tx_responses)?;

    // Get Block Hashes for all Transactions.
    let block_hashes: Vec<BlockHash> = transactions
        .values()
        .filter_map(|tx| match tx.status {
            Status::Confirmed { block_hash, .. } => Some(block_hash),
            Status::Unconfirmed => None,
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Get TxIds for all Block Hashes.
    trace!("Fetching Block hashes from Bitcoin backend: {block_hashes:#?}");
    let block_responses = client.get_blocks(&block_hashes);
    let blocks = transpose_arc_result(&block_responses)?;

    // Create a BtcIndex for each TxId.
    let indices = transactions
        .into_iter()
        .map(|(txid, tx)| {
            let Status::Confirmed {
                block_height,
                block_hash,
                ..
            } = &tx.status
            else {
                unreachable!();
            };
            let pos = blocks[block_hash]
                .iter()
                .position(|id| *id == txid)
                .unwrap();

            Ok((txid.to_string(), BtcIndex(*block_height, pos as u32)))
        })
        .collect::<Result<_, ClientError>>()?;

    Ok(indices)
}

/// All resolved transactions, grouped by `[AssetName]`.
pub type ResolvedTransactions = HashMap<AssetName, FIFO<Tx>>;

/// Resolve all wallet CSV rows into transactions.
///
/// Requires the CSV rows to be sorted by time in ascending order.
pub fn resolve<C: ClientApi>(
    mut rows: Vec<WalletCsvRow>,
    client: &C,
    auditor: &mut Auditor,
    tags: &TxTags,
) -> Result<ResolvedTransactions, TxoError> {
    // Derive addresses in parallel.
    let xpubs = rows.iter().filter_map(|row| row.xpub());
    auditor.add_xpubs(xpubs)?;

    // Fetch all TXIDs from blockchain
    let txids: Vec<Txid> = rows
        .iter()
        .map(|row| row.parse_txid())
        .collect::<Result<_, _>>()?;
    trace!("Fetching Transaction IDs from Bitcoin backend: {txids:#?}");
    let tx_responses = client.get_transactions(&txids);
    let resolved_tx = transpose_arc_result(&tx_responses)?;

    // Fetch a mapping of Txid -> BtcIndex.
    let indices = get_indices(client, &txids)?;

    // Sort all wallet CSVs chronologically.
    trace!("Sorting transactions by Block Height and Tx index");
    rows.sort_unstable_by_key(|row| indices[row.txid()]);

    // Cache for deduplicating TXIDs.
    let mut txo_cache = HashMap::<_, Vec<_>>::new();

    let mut txs = Vec::new();

    // Resolve each CSV row to transaction details, deduplicating TXIDs by merging `TxoInfo`s.
    for row in rows {
        let tx = &resolved_tx[&row.parse_txid()?];
        trace!("Resolving Txid {:?} to a Transaction", tx.txid);

        let asset = row.asset()?;

        let seen = txo_cache.contains_key(row.txid());
        let txos = txo_cache.entry(row.txid().to_string()).or_insert_with(|| {
            tx.outputs
                .iter()
                .map(|txout| {
                    let amount =
                        KrakenAmount::try_from_decimal(asset.as_kraken(), txout.value).unwrap();
                    let script_pubkey = txout.script_pubkey.as_script();
                    trace!("Checking if script pubkey {script_pubkey:?} is mine on TXO");
                    let mine = auditor.is_mine(script_pubkey);
                    let mut wallet_info = BTreeSet::new();
                    if row.txo_index().is_none() {
                        wallet_info.insert(row.txo_info());
                    }

                    Txo {
                        amount,
                        mine,
                        wallet_info,
                    }
                })
                .collect()
        });

        // Update the TXO
        if let Some(index) = row.txo_index() {
            // If the CSV row specifies a TXO index, use it.
            txos[index].wallet_info.insert(row.txo_info());
        } else {
            // Otherwise update all TXOs.
            for txo in txos {
                txo.wallet_info.insert(row.txo_info());
            }
        }

        // Add the Tx only if it hasn't been seen before
        if !seen {
            let ins = tx
                .inputs
                .iter()
                .map(|txin| {
                    let txid = txin.txid;
                    let index = txin.index as usize;
                    let script_pubkey = txin
                        .previous_output
                        .as_ref()
                        .expect("Mined inputs are not supported")
                        .script_pubkey
                        .as_script();
                    trace!("Checking if script pubkey {script_pubkey:?} is mine on previous TXO");
                    let mine = auditor.is_mine(script_pubkey);

                    Txi {
                        external_id: format!("{txid}:{index}"),
                        amount: None,
                        mine,
                    }
                })
                .collect();

            let tx = Tx {
                time: match tx.status {
                    Status::Confirmed { block_time, .. } => block_time,
                    Status::Unconfirmed => panic!("Unconfirmed transactions are not supported"),
                },
                asset,
                txid: row.txid().to_string(),
                ins,
                outs: Vec::new(),
                tx_type: tags.get(row.txid()).map(|tag| tag.tx_type.clone()),
                exchange_rate: tags.get(row.txid()).and_then(|tag| tag.exchange_rate),
            };
            txs.push(tx);
        }
    }

    let mut not_mine = Vec::new();

    // Merge TXO cache into txs
    for tx in &mut txs {
        tx.outs = txo_cache.remove(tx.txid.as_str()).unwrap();

        // Sanity check: At least one of `tx.ins` or `tx.outs` must have `mine == true`.
        let mut mine = tx
            .ins
            .iter()
            .map(|txi| txi.mine)
            .chain(tx.outs.iter().map(|txo| txo.mine));

        if !mine.any(identity) {
            not_mine.push(tx.txid.clone());
        }
    }

    if !not_mine.is_empty() {
        return Err(TxoError::NotMine(not_mine));
    }

    // Group txs by AssetName
    let mut output = HashMap::new();
    for tx in txs {
        let entry: &mut FIFO<Tx> = output.entry(tx.asset).or_default();
        entry.append_back(tx);
    }

    Ok(output)
}

impl Default for Auditor {
    fn default() -> Self {
        Self {
            address_cache: AddressCache::default(),
            addresses: HashSet::default(),
            network: Network::Bitcoin,
        }
    }
}

impl Auditor {
    /// Create an auditor. Requires a network reference.
    pub fn new(network: Network) -> Self {
        Self {
            address_cache: AddressCache::default(),
            addresses: HashSet::new(),
            network,
        }
    }

    /// Add a list of BIP-32 extended public keys (xpub) to the auditor, allowing it to claim
    /// ownership over transaction inputs and outputs derived from the given key.
    pub fn add_xpubs<'a, I>(&mut self, xpubs: I) -> Result<(), AuditorError>
    where
        I: Iterator<Item = &'a str> + Send,
    {
        for xpub in xpubs {
            // Deduplicate xpubs
            if self.address_cache.contains_xpub(xpub) {
                continue;
            }

            debug!("Adding xpub `{xpub}` to Auditor...");

            let pubkey = Xpub::decode(xpub, self.network)?;
            self.address_cache.add_xpub(pubkey);
        }

        // If the cache was previously initialized, re-initialize with the new pub keys.
        if self.address_cache.initialized() {
            self.address_cache.initialize(None);
        }

        Ok(())
    }

    /// Add an invoice address to the auditor, allowing it to claim ownership over transaction
    /// outputs spent to the given address.
    ///
    /// This method is intended only for situations where the user does not have access to a
    /// BIP-32 extended public key.
    pub fn add_address(&mut self, address: &str) -> Result<(), AuditorError> {
        let address = address
            .parse::<Address<_>>()?
            .require_network(self.network)?;
        self.addresses.insert(address.script_pubkey().script_hash());

        Ok(())
    }

    /// Check if a script pubkey belongs to this auditor.
    pub(crate) fn is_mine(&mut self, script_pubkey: &Script) -> bool {
        if self.addresses.contains(&script_pubkey.script_hash()) {
            return true;
        }

        self.address_cache.is_mine(script_pubkey)
    }
}

pub fn read_tx_tags(path: impl AsRef<Path>) -> Result<TxTags, TxTagsError> {
    #[derive(Debug, Deserialize)]
    struct TxTagsCsvRow {
        tx_type: String,
        txid: String,
        exchange_rate_asset: String,
        exchange_rate: String,
        loan_id: String,
    }

    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    let mut tags = HashMap::new();
    for row in reader.deserialize::<TxTagsCsvRow>() {
        let row = row?;
        let tag = TxTag {
            tx_type: (row.tx_type, row.loan_id).try_into()?,
            exchange_rate: if row.exchange_rate_asset.is_empty() {
                None
            } else {
                let amount = KrakenAmount::new(&row.exchange_rate_asset, &row.exchange_rate)?;

                Some(amount)
            },
        };

        tags.insert(row.txid, tag);
    }

    Ok(TxTags { tags })
}

impl TxTags {
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }

    pub(crate) fn get(&self, txid: &str) -> Option<&TxTag> {
        self.tags.get(txid)
    }
}

impl std::iter::IntoIterator for TxTags {
    type Item = (String, TxTag);
    type IntoIter = std::collections::hash_map::IntoIter<String, TxTag>;

    fn into_iter(self) -> Self::IntoIter {
        self.tags.into_iter()
    }
}

impl std::iter::Extend<(String, TxTag)> for TxTags {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (String, TxTag)>,
    {
        self.tags.extend(iter)
    }
}

impl TryFrom<(String, String)> for TxType {
    type Error = TxTagsError;

    fn try_from((tag, id): (String, String)) -> Result<Self, Self::Error> {
        match tag.as_str() {
            "trade" => Ok(Self::Trade),
            "spam" => Ok(Self::Spam),
            "income" => Ok(Self::Income),
            "capgain" => Ok(Self::CapGain),
            "loan_capital_borrower" => Ok(Self::LoanCapital {
                role: LoanRole::Borrower,
                loan_id: id,
            }),
            "loan_collateral_borrower" => Ok(Self::LoanCollateral {
                role: LoanRole::Borrower,
                loan_id: id,
            }),
            "loan_capital_lender" => Ok(Self::LoanCapital {
                role: LoanRole::Lender,
                loan_id: id,
            }),
            "loan_collateral_lender" => Ok(Self::LoanCollateral {
                role: LoanRole::Lender,
                loan_id: id,
            }),
            "fork" => Ok(Self::Fork),
            "lost" => Ok(Self::Lost),
            "gift" => Ok(Self::Gift),
            "donation" => Ok(Self::Donation),
            "spend" => Ok(Self::Spend),
            _ => Err(TxTagsError::TxType(tag)),
        }
    }
}

impl std::fmt::Display for TxType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trade => f.write_str("Wallet trade"),
            Self::Spam => f.write_str("Wallet spam"),
            Self::Income => f.write_str("Wallet income"),
            Self::CapGain => f.write_str("Wallet capital gain"),
            Self::LoanCapital { role, loan_id } => {
                write!(f, "Wallet loan ID `{loan_id}` capital as {role}")
            }
            Self::LoanCollateral { role, loan_id } => {
                write!(f, "Wallet loan ID `{loan_id}` collateral as {role}")
            }
            Self::Fork => f.write_str("Wallet fork"),
            Self::Lost => f.write_str("Wallet loss"),
            Self::Gift => f.write_str("Wallet gift"),
            Self::Donation => f.write_str("Wallet donation"),
            Self::Spend => f.write_str("Wallet spend"),
        }
    }
}

impl std::fmt::Display for LoanRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Lender => "Lender",
            Self::Borrower => "Borrower",
        })
    }
}

pub fn read_addresses(path: impl AsRef<Path>) -> Result<Vec<String>, csv::Error> {
    #[derive(Debug, Deserialize)]
    struct AddressesCsvRow {
        address: String,
    }

    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;

    let mut addresses = vec![];
    for row in reader.deserialize::<AddressesCsvRow>() {
        let row = row?;

        addresses.push(row.address);
    }

    Ok(addresses)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::client::{BlockResult, TxResult};
    use bdk::bitcoin::{BlockHash, ScriptBuf};
    use esploda::esplora::Transaction;
    use std::sync::Arc;
    use tracing_test::traced_test;

    #[derive(Debug, Default, Deserialize)]
    pub(crate) struct MockClient {
        tx: HashMap<Txid, Transaction>,
        blocks: HashMap<BlockHash, Vec<Txid>>,
    }

    impl MockClient {
        pub(crate) fn new<P: AsRef<Path>>(file: P) -> Self {
            let data = std::fs::read_to_string(file).expect("Could not open RON file");

            ron::from_str(&data).expect("RON decoding error")
        }
    }

    impl ClientApi for MockClient {
        fn get_transactions(&self, txids: &[Txid]) -> HashMap<Txid, TxResult> {
            txids
                .iter()
                .map(|txid| (*txid, Arc::new(Ok(self.tx[txid].clone()))))
                .collect()
        }

        fn get_blocks(&self, block_hashes: &[BlockHash]) -> HashMap<BlockHash, BlockResult> {
            block_hashes
                .iter()
                .map(|block_hash| (*block_hash, Arc::new(Ok(self.blocks[block_hash].clone()))))
                .collect()
        }
    }

    fn create_auditor() -> Auditor {
        let mut auditor = Auditor::new(Network::Testnet);
        auditor
            .add_address("2N5eWoXhzNUNHZ7zmFYtxYZBrihuGoeizLk")
            .unwrap();
        auditor
            .add_address("2MwoYr8HBJxzPDPD4tziUdxpbvaDSFHC883")
            .unwrap();
        auditor
            .add_address("2NB1U5BfsrvoXQKPebbpynKjVJbJJZ6HFLb")
            .unwrap();
        auditor
            .add_address("2MtUrNAMPv3UMZs6o6oNtqFUdWpxgZA31eZ")
            .unwrap();
        auditor
            .add_address("tb1q59d3eus0lgpjyhhq740fahsq8p9cmy89kcwr0d")
            .unwrap();

        auditor
    }

    #[test]
    #[traced_test]
    fn test_wallet_resolve_tx_empty() {
        let _ = tracing_log::LogTracer::init();

        let mut auditor = Auditor::new(Network::Testnet);
        let client = MockClient::default();
        let tags = TxTags::default();
        assert!(resolve(vec![], &client, &mut auditor, &tags).is_ok());
    }

    #[test]
    #[traced_test]
    fn test_wallet_resolve_tx() {
        let _ = tracing_log::LogTracer::init();

        let mut rows = Vec::new();
        let wallet = "./references/mock-testnet/mock-testnet-wallet.csv";
        read_generic(wallet, &mut rows).unwrap();
        let mut auditor = create_auditor();
        let client = MockClient::new("./fixtures/wallet/mock_client.ron");
        let tags = read_tx_tags("./references/mock-testnet/mock-testnet-tx-tags.csv").unwrap();
        let resolved = resolve(rows, &client, &mut auditor, &tags).unwrap();
        assert_eq!(resolved.len(), 1);

        let tx = &resolved[&AssetName::Btc];
        assert_eq!(tx.len(), 6);

        // Move
        assert_eq!(tx[0].ins.len(), 1);
        let expected = "8bd71e13e1c7c241be570f0f78deb7c5e76a388dd5336cea0223c02b0b29a2bb:1";
        assert_eq!(tx[0].ins[0].external_id, expected);
        assert!(!tx[0].ins[0].mine);
        assert_eq!(tx[0].outs.len(), 2);
        let expected = KrakenAmount::new("XXBT", "0.00010000").unwrap();
        assert_eq!(tx[0].outs[0].amount, expected);
        assert!(tx[0].outs[0].mine);
        assert_eq!(
            tx[0].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "move".to_string()
            }])
        );
        assert!(!tx[0].outs[1].mine);
        assert_eq!(tx[0].outs[1].wallet_info, BTreeSet::new());
        assert_eq!(tx[0].exchange_rate, None);

        // Income
        assert_eq!(tx[1].ins.len(), 1);
        let expected = "21bdf0ecccb41886f9be3a6d493d3cc86f1c26440fcb196347c6bf54af8908e8:0";
        assert_eq!(tx[1].ins[0].external_id, expected);
        assert!(!tx[1].ins[0].mine);
        assert_eq!(tx[1].outs.len(), 2);
        assert!(!tx[1].outs[0].mine);
        assert_eq!(tx[1].outs[0].wallet_info, BTreeSet::new());
        let expected = KrakenAmount::new("XXBT", "0.00010000").unwrap();
        assert_eq!(tx[1].outs[1].amount, expected);
        assert!(tx[1].outs[1].mine);
        assert_eq!(
            tx[1].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "plumbing work 1".to_string()
            }])
        );
        let expected = KrakenAmount::new("ZUSD", "112.00").unwrap();
        assert_eq!(tx[1].exchange_rate, Some(expected));

        // Change + Deposit
        assert_eq!(tx[2].ins.len(), 1);
        let expected = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74:0";
        assert_eq!(tx[2].ins[0].external_id, expected);
        assert!(tx[2].ins[0].mine);
        assert_eq!(tx[2].outs.len(), 2);
        let expected = KrakenAmount::new("XXBT", "0.00001794").unwrap();
        assert_eq!(tx[2].outs[0].amount, expected);
        assert!(tx[2].outs[0].mine);
        assert_eq!(
            tx[2].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "spare change that I found in the couch".to_string()
            }])
        );
        let expected = KrakenAmount::new("XXBT", "0.00008000").unwrap();
        assert_eq!(tx[2].outs[1].amount, expected);
        assert!(!tx[2].outs[1].mine);
        assert_eq!(
            tx[2].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "my first deposit".to_string()
            }])
        );
        assert_eq!(tx[2].exchange_rate, None);

        // Withdrawal
        assert_eq!(tx[3].ins.len(), 1);
        let expected = "54fd32320b7715d5a45f692af73b6c179be30392d67a04fba9110bf3436a1208:1";
        assert_eq!(tx[3].ins[0].external_id, expected);
        assert!(!tx[3].ins[0].mine);
        assert_eq!(tx[3].outs.len(), 2);
        assert!(!tx[3].outs[0].mine);
        assert_eq!(tx[3].outs[0].wallet_info, BTreeSet::new());
        let expected = KrakenAmount::new("XXBT", "0.00001000").unwrap();
        assert_eq!(tx[3].outs[1].amount, expected);
        assert!(tx[3].outs[1].mine);
        assert_eq!(
            tx[3].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "made some money!".to_string()
            }])
        );
        assert_eq!(tx[3].exchange_rate, None);

        // Change + Spend
        assert_eq!(tx[4].ins.len(), 2);
        let expected = "5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9:1";
        assert_eq!(tx[4].ins[0].external_id, expected);
        assert!(tx[4].ins[0].mine);
        let expected = "940539548baeec9f761e1016b29347aeefe2803f6e3a4a14fadd859fd7076630:1";
        assert_eq!(tx[4].ins[1].external_id, expected);
        assert!(tx[4].ins[1].mine);
        assert_eq!(tx[4].outs.len(), 2);
        let expected = KrakenAmount::new("XXBT", "0.00000663").unwrap();
        assert_eq!(tx[4].outs[0].amount, expected);
        assert!(tx[4].outs[0].mine);
        assert_eq!(
            tx[4].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "more loose change".to_string()
            }])
        );
        let expected = KrakenAmount::new("XXBT", "0.00010000").unwrap();
        assert_eq!(tx[4].outs[1].amount, expected);
        assert!(!tx[4].outs[1].mine);
        assert_eq!(
            tx[4].outs[1].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "invoice DCD-SOW3-001".to_string()
            }])
        );
        let expected = KrakenAmount::new("ZUSD", "200.00").unwrap();
        assert_eq!(tx[4].exchange_rate, Some(expected));

        // Move
        assert_eq!(tx[5].ins.len(), 2);
        let expected = "54fd32320b7715d5a45f692af73b6c179be30392d67a04fba9110bf3436a1208:0";
        assert_eq!(tx[5].ins[0].external_id, expected);
        assert!(tx[5].ins[0].mine);
        let expected = "aa8d28251c5594df72248dbe914208149fdf45a96fcebc78729cd4464fb00694:0";
        assert_eq!(tx[5].ins[1].external_id, expected);
        assert_eq!(tx[5].outs.len(), 1);
        assert!(tx[5].ins[1].mine);
        let expected = KrakenAmount::new("XXBT", "0.00002153").unwrap();
        assert_eq!(tx[5].outs[0].amount, expected);
        assert!(tx[5].outs[0].mine);
        assert_eq!(
            tx[5].outs[0].wallet_info,
            BTreeSet::from_iter([TxoInfo {
                account: "dcd".to_string(),
                note: "transfer to wallet b".to_string()
            }])
        );
        assert_eq!(tx[5].exchange_rate, None);
    }

    #[test]
    #[traced_test]
    fn test_wallet_auditor_invoice_address() {
        let _ = tracing_log::LogTracer::init();

        let mut auditor = create_auditor();

        let mut assert_is_mine = |script_pub_key: &str, is_mine: bool| {
            let script = ScriptBuf::from_hex(script_pub_key).unwrap();
            assert_eq!(auditor.is_mine(script.as_script()), is_mine);
        };

        // txid 7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74: Move
        // Inputs
        assert_is_mine("0014648e740b0f5747d34def33d083855fc6fefdb722", false);
        // Outputs
        assert_is_mine("a91488091cd5ffeaa4e32adcdb56495e79e9b9c6255287", true);
        assert_is_mine("a914e19ac98a6f9da9c2467fa1aec1730da85a4658f587", false);

        // txid 5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9: Income
        // Inputs
        assert_is_mine("a9149205e786ac6d445fcbfa3083ae557243463359c287", false);
        // Outputs
        assert_is_mine("a91409141559fda465f054f629965d113bfb1c1fe78787", false);
        assert_is_mine("a91488091cd5ffeaa4e32adcdb56495e79e9b9c6255287", true);

        // txid 54fd32320b7715d5a45f692af73b6c179be30392d67a04fba9110bf3436a1208: Deposit
        // Inputs
        assert_is_mine("a91488091cd5ffeaa4e32adcdb56495e79e9b9c6255287", true);
        // Outputs
        assert_is_mine("a91431fd8e9161a16e4033b544cd527f0a3b273f0d4687", true);
        assert_is_mine("a914ce8da91e506eef33e5e7e493cfbd90a5f582fa6487", false);

        // txid 940539548baeec9f761e1016b29347aeefe2803f6e3a4a14fadd859fd7076630: Withdrawal
        // Inputs
        assert_is_mine("a914ce8da91e506eef33e5e7e493cfbd90a5f582fa6487", false);
        // Outputs
        assert_is_mine("a91450992e7109f835bfa2b13a3e53cbbff814eb0a8387", false);
        assert_is_mine("a914c2d832a24e3532fc9b4f021b604a4b5dfcb69e6d87", true);

        // txid aa8d28251c5594df72248dbe914208149fdf45a96fcebc78729cd4464fb00694: Spend
        // Inputs
        assert_is_mine("a91488091cd5ffeaa4e32adcdb56495e79e9b9c6255287", true);
        assert_is_mine("a914c2d832a24e3532fc9b4f021b604a4b5dfcb69e6d87", true);
        // Outputs
        assert_is_mine("a9140d8be6427513bd5f9b0941e3c428a65f4a92c07587", true);
        assert_is_mine("a914452d03fb9b52b865eab86370898d99b8e73944fc87", false);

        // txid a3eef08bef357e32d4a606a341538b578239e278b09e9198962b53757ca6ca1d: Move
        // Inputs
        assert_is_mine("a91431fd8e9161a16e4033b544cd527f0a3b273f0d4687", true);
        assert_is_mine("a9140d8be6427513bd5f9b0941e3c428a65f4a92c07587", true);
        // Outputs
        assert_is_mine("0014a15b1cf20ffa03225ee0f55e9ede00384b8d90e5", true);
    }

    #[test]
    #[traced_test]
    fn test_wallet_auditor_xpub() {
        let _ = tracing_log::LogTracer::init();

        let mut auditor = Auditor::new(Network::Testnet);

        auditor
            .add_xpubs(
                [
                    // Wallet A
                    concat!(
                        "vpub5Vwo9xtdB77E1m21Wxyi2UuurxSMoKCv7xQs7zDHSPeR7RpGm1rqQgWT8jzm",
                        "q8KNj3XwWw4Y7hWhZ6Q9Bhkh6U8tH6tbcdgrVo45iYpmH8t",
                    ),
                    // Wallet B
                    concat!(
                        "vpub5VFW5nRjMxMStjxyBSZQoGZeeiVWKK8UipiskvBBxJBGZhXwEP74riZigv9N",
                        "KRthLZdKUQFLF6XT1u6CyX3Rgo8B3t5KTL4htYk7JgyUmyv",
                    ),
                ]
                .into_iter(),
            )
            .unwrap();

        let mut assert_is_mine = |script_pub_key: &str, is_mine: bool| {
            let script = ScriptBuf::from_hex(script_pub_key).unwrap();
            assert_eq!(auditor.is_mine(script.as_script()), is_mine);
        };

        // txid 60575f03d3457dd8c67eed1c37ef6b6b7950a1ce109b300a6d770665bfaa9fe7: Income
        // Inputs
        assert_is_mine("00141f8de200a822f289cd33d1025858a8359704f8ab", false);
        // Outputs
        assert_is_mine("001464665b96bda6997a72039edd447b69816d6707c2", true);
        assert_is_mine("00141f8de200a822f289cd33d1025858a8359704f8ab", false);

        // txid 19e06c33d6870057cad36ed17c45ec3fd95bfe4bf8802f26fe1c47b8b06c6805: Income
        // Inputs
        assert_is_mine("0014c7e9974593bb88ea66121966831f5e4467b1d5cf", false);
        // Outputs
        assert_is_mine("00146e553187733725a2f7cd0448a1334fed884eb38b", false);
        assert_is_mine("0014ccc7f2b23727802bb108c63aff5ad78b298d1bd5", true);

        // txid 1112f46698e586f781ada13bd5db58c54eb24495ab669d8518dd37c4af6cb622: Move A -> B
        // Inputs
        assert_is_mine("0014ccc7f2b23727802bb108c63aff5ad78b298d1bd5", true);
        // Outputs
        assert_is_mine("0014eb1c1d6fd2419fadc899fcf826397e3774820bd5", true);

        // txid feceb335210ee31662a8f251cfac24b605b51db3d53d10f436470e5f473a6fa3: Deposit
        // Inputs
        assert_is_mine("001464665b96bda6997a72039edd447b69816d6707c2", true);
        // Outputs
        assert_is_mine("00143afa813c9b668ff4210178f7204ca47bfa528317", true);
        assert_is_mine("00147435eb56f025995951d655198455e83719cdf468", false);

        // txid fc62eb2e25bb44a146a93c75471f229ef79b93eac2e9307300b9fa6b28e481ee: Spend on Ice cream
        // Inputs
        assert_is_mine("00143afa813c9b668ff4210178f7204ca47bfa528317", true);
        // Outputs
        assert_is_mine("00146d53ef9158aa23ca67157ec12b4f5f6879cede85", false);
        assert_is_mine("001480ec3053738d0347a97034709cdcf54e95a5a70b", true);

        // txid cb8e1ca8865f921eef861cd40ea9f29de1450fd1d922c4aa02e8acc83728dc1c: Withdrawal
        // Inputs
        assert_is_mine("0014b12cbf4c2433b34ff9d2c3440e82c71ed92ab249", false);
        // Outputs
        assert_is_mine("00148b77aad39810908755a15a69df77fdb00fffa871", false);
        assert_is_mine("0014292baf9fd19e7517221127414c779853e20e0f7d", true);

        // txid 2192c1aef57f976db693d1444e9d1465db55f81624b8834689ced974e5532000: Move B -> A
        // Inputs
        assert_is_mine("0014eb1c1d6fd2419fadc899fcf826397e3774820bd5", true);
        // Outputs
        assert_is_mine("0014084298ef36bde347ddc71d6843526da072955fca", true);

        // txid 33f312a2585e8df768b406c118bed170c8a87aebc9e0ae371faeb06b6e3e9507: Move A -> B
        // Inputs
        assert_is_mine("0014292baf9fd19e7517221127414c779853e20e0f7d", true);
        // Outputs
        assert_is_mine("0014eb1c1d6fd2419fadc899fcf826397e3774820bd5", true);
        assert_is_mine("0014510a8c74c2378069c1343743b94a9f967e171965", true);

        // txid a358f7d4814f6a87236323aa2bfb05c117e53c6573d5ecb4b30444b36bdfea5d: Spend on Ice cream
        // Inputs
        assert_is_mine("0014084298ef36bde347ddc71d6843526da072955fca", true);
        assert_is_mine("0014510a8c74c2378069c1343743b94a9f967e171965", true);
        assert_is_mine("001480ec3053738d0347a97034709cdcf54e95a5a70b", true);
        // Outputs
        assert_is_mine("00146d53ef9158aa23ca67157ec12b4f5f6879cede85", false);
    }
}
