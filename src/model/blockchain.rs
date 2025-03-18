pub use self::{account::*, utxo::*};
use crate::basis::{Asset, PoolAsset};
use crate::model::checkpoint::{PendingAccountTx, PendingTxInfo, PendingUtxo};
use crate::model::kraken_amount::BitcoinAmount;
use crate::util::fifo::FIFO;
use chrono::{DateTime, Utc};
use std::collections::hash_map::{Drain, Iter};
use std::collections::{BTreeMap, HashMap};
use std::ops::{Add, RangeBounds};
use std::rc::Rc;
use thiserror::Error;

pub(crate) mod account;
pub(crate) mod utxo;

type Basis<A> = FIFO<PoolAsset<A>>;

#[derive(Debug, Error)]
pub enum BlockchainError {
    #[error("UTXO-based blockchain error")]
    Utxo(#[from] utxo::UtxoError),

    #[error("Account-based blockchain error")]
    Account(#[from] account::AccountError),
}

/// Extension trait for Blockchain Models.
pub(crate) trait BlockchainExt {
    type Asset: Asset;

    // TODO: Remove the `ext_` prefix, or rename methods to be less generic.
    // The prefix is here to avoid ambiguities with other traits, like `FromIter::from_iter()`

    /// Constructor from a worksheet name and an iterator.
    fn ext_new<I>(worksheet_name: Rc<str>, iter: I) -> Self
    where
        I: IntoIterator<Item = (String, Basis<Self::Asset>)>;

    /// Get a worksheet name for this blockchain.
    fn worksheet_name(&self) -> &Rc<str>;

    /// Get pending transaction info by key.
    fn tx_info<S>(&self, key: S) -> Option<&PendingTxInfo<PoolAsset<Self::Asset>>>
    where
        S: AsRef<str>;

    /// Get an iterator over the blockchain.
    fn ext_iter(&self) -> Iter<'_, String, Basis<Self::Asset>>;

    /// Drain the blockchain, returning an iterator over its keys and values.
    fn ext_drain(&mut self) -> Drain<'_, String, Basis<Self::Asset>>;

    /// Extend the blockchain with an iterator of keys and values.
    fn ext_extend<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = (String, Basis<Self::Asset>)>;

    /// Remove an item from the blockchain by key.
    fn ext_remove<K>(&mut self, key: K) -> Option<Basis<Self::Asset>>
    where
        K: AsRef<str>;

    /// Check if the blockchain is empty.
    fn ext_is_empty(&self) -> bool;
}

/// Trait for time-ordered blockchain models.
pub(crate) trait TimeOrderedBlockchain {
    /// The asset type, e.g. [`BitcoinAmount`].
    /// [`BitcoinAmount`]: crate::model::kraken_amount::BitcoinAmount
    type Asset: Asset;
    /// The blockchain type, e.g. [`Utxo`] or [`Account`].
    type Blockchain: BlockchainExt;

    /// Extend an asset with the given blockchain model identified by time.
    fn extend(&mut self, time: DateTime<Utc>, blockchain: Self::Blockchain);

    /// Extracts a deposit within the given time window matching the deposit amount. Returns the
    /// timestamp and basis FIFO.
    ///
    /// This method cleans up after itself, removing empty maps.
    fn extract_deposit<R>(
        &mut self,
        time_range: R,
        amount: Self::Asset,
    ) -> Option<(String, DateTime<Utc>, Basis<Self::Asset>)>
    where
        R: RangeBounds<DateTime<Utc>>,
        Self::Asset: Copy + Default + Add<Output = Self::Asset>;

    /// Get fee basis by timestamp and txid.
    fn fee_basis<S>(&self, time: DateTime<Utc>, txid: S) -> Option<&Basis<Self::Asset>>
    where
        S: AsRef<str>;
}

impl BlockchainExt for PendingUtxo {
    type Asset = BitcoinAmount;

    fn ext_new<I>(worksheet_name: Rc<str>, iter: I) -> Self
    where
        I: IntoIterator<Item = (String, Basis<Self::Asset>)>,
    {
        Self {
            worksheet_name,
            utxos: Utxo::from_iter(iter),
            tx_info: HashMap::default(),
        }
    }

    fn worksheet_name(&self) -> &Rc<str> {
        &self.worksheet_name
    }

    fn tx_info<S>(&self, key: S) -> Option<&PendingTxInfo<PoolAsset<Self::Asset>>>
    where
        S: AsRef<str>,
    {
        self.tx_info.get(key.as_ref())
    }

    fn ext_iter(&self) -> Iter<'_, String, Basis<Self::Asset>> {
        self.utxos.iter()
    }

    fn ext_drain(&mut self) -> Drain<'_, String, Basis<Self::Asset>> {
        self.utxos.drain()
    }

    fn ext_extend<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = (String, Basis<Self::Asset>)>,
    {
        self.utxos.extend(items)
    }

    fn ext_remove<K>(&mut self, key: K) -> Option<Basis<Self::Asset>>
    where
        K: AsRef<str>,
    {
        self.utxos.remove(key)
    }

    fn ext_is_empty(&self) -> bool {
        self.utxos.is_empty()
    }
}

impl<A: Asset> BlockchainExt for PendingAccountTx<PoolAsset<A>> {
    type Asset = A;

    fn ext_new<I>(worksheet_name: Rc<str>, iter: I) -> Self
    where
        I: IntoIterator<Item = (String, Basis<Self::Asset>)>,
    {
        Self {
            worksheet_name,
            account: Account::from_iter(iter),
            tx_info: HashMap::default(),
        }
    }

    fn worksheet_name(&self) -> &Rc<str> {
        &self.worksheet_name
    }

    fn tx_info<S>(&self, key: S) -> Option<&PendingTxInfo<PoolAsset<Self::Asset>>>
    where
        S: AsRef<str>,
    {
        self.tx_info.get(key.as_ref())
    }

    fn ext_iter(&self) -> Iter<'_, String, Basis<Self::Asset>> {
        self.account.iter()
    }

    fn ext_drain(&mut self) -> Drain<'_, String, Basis<Self::Asset>> {
        self.account.drain()
    }

    fn ext_extend<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = (String, Basis<Self::Asset>)>,
    {
        self.account.extend(items)
    }

    fn ext_remove<K>(&mut self, key: K) -> Option<Basis<Self::Asset>>
    where
        K: AsRef<str>,
    {
        self.account.remove(key)
    }

    fn ext_is_empty(&self) -> bool {
        self.account.is_empty()
    }
}

impl<A: Asset, B> TimeOrderedBlockchain for BTreeMap<DateTime<Utc>, B>
where
    B: BlockchainExt<Asset = A>,
{
    type Asset = A;
    type Blockchain = B;

    fn extend(&mut self, time: DateTime<Utc>, mut inner: Self::Blockchain) {
        self.entry(time)
            .and_modify(|entry| entry.ext_extend(inner.ext_drain()))
            .or_insert(inner);
    }

    fn extract_deposit<R>(
        &mut self,
        time_range: R,
        amount: Self::Asset,
    ) -> Option<(String, DateTime<Utc>, Basis<Self::Asset>)>
    where
        R: RangeBounds<DateTime<Utc>>,
        Self::Asset: Copy + Default + Add<Output = Self::Asset>,
    {
        // Find a deposit within the search window that has the expected amount.
        let (time, txid) = self.range(time_range).find_map(|(time, blockchain)| {
            blockchain.ext_iter().find_map(|(txid, basis)| {
                (amount == basis.amount()).then_some((*time, txid.clone()))
            })
        })?;

        // Consume the pending deposit so that it doesn't get reused or hang around forever.
        let basis = self.get_mut(&time).unwrap().ext_remove(&txid).unwrap();

        // Garbage collect the pending deposit container.
        if self[&time].ext_is_empty() {
            self.remove(&time);
        }

        Some((txid, time, basis))
    }

    fn fee_basis<S>(&self, time: DateTime<Utc>, txid: S) -> Option<&Basis<Self::Asset>>
    where
        S: AsRef<str>,
    {
        match self.get(&time) {
            Some(map) => map.tx_info(txid.as_ref()).map(|tx_info| &tx_info.fees),
            None => None,
        }
    }
}
