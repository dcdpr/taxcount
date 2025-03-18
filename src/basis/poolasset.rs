use crate::basis::{BasisLifecycle, Bucket, SplittableTakeWhile};
use crate::imports::wallet::Tx;
use crate::model::kraken_amount::{BitcoinAmount, EthWAmount, EtherAmount, UsdcAmount, UsdtAmount};
use crate::model::kraken_amount::{KrakenAmount, UsdAmount};
use crate::model::kraken_amount::{PoolChfAmount, PoolEurAmount, PoolJpyAmount, PoolUsdAmount};
use crate::util::{fifo::FIFO, HasSplit};
use std::ops::{Add, Sub};
use std::{cmp::Ordering, fmt::Debug, rc::Rc, str::FromStr};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SplitBasisError {
    #[error("FATAL: Balance too low to fulfill the sell: {0}")]
    FatalLowBalance(String),

    #[error("Cannot take a zero or negative amount")]
    ZeroOrNegative,
}

#[derive(Debug, Error)]
pub enum AssetNameError {
    #[error("Parse error")]
    Parse,
}

/// Defines a unique asset by name.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum AssetName {
    // Fiat
    Usd,
    Chf,
    Eur,
    Jpy,

    // Cryptocurrencies
    Btc,
    Eth,
    EthW,
    Usdc,
    Usdt,
}

/// Types that implement `Asset` can be pooled into a `PoolAsset`.
/// KrakenAmounts satisfy this.
pub trait Asset: Debug + PartialOrd + Ord + TryFrom<KrakenAmount> + Into<KrakenAmount> {}

pub(crate) trait PoolAssetSplit {
    type Amount: Asset;

    fn split(self, take_amount: Self::Amount) -> HasSplit<PoolAsset<Self::Amount>>;
}

pub(crate) trait ToNonSplittable {
    type Amount: Asset;

    fn to_non_splittable(&self) -> PoolAssetNonSplittable<Self::Amount>;
}

pub type PoolBTC = PoolAsset<BitcoinAmount>;
pub type PoolCHF = PoolAsset<PoolChfAmount>;
pub type PoolETH = PoolAsset<EtherAmount>;
pub type PoolETHW = PoolAsset<EthWAmount>;
pub type PoolEUR = PoolAsset<PoolEurAmount>;
pub type PoolJPY = PoolAsset<PoolJpyAmount>;
pub type PoolUSD = PoolAsset<PoolUsdAmount>;
pub type PoolUSDC = PoolAsset<UsdcAmount>;
pub type PoolUSDT = PoolAsset<UsdtAmount>;

// on exchange, where i'm unable to bucketize further
// We are deliberately NOT implementing Clone so we can avoid "double spends" of lifecycles.
//
// See `PoolAssetNonSplittable` for a lightweight variant that allows cloning.
//
// `PoolAsset` ends up in the FIFOs for splitting trades.
#[derive(Debug)]
pub struct PoolAsset<A: Asset> {
    pub amount: A,
    pub lifecycle: BasisLifecycle,
}

// `PoolAssetNonSplittable` is only used in the `ZipperedMatch`.
#[derive(Clone, Debug)]
pub(crate) struct PoolAssetNonSplittable<A: Asset> {
    pub(crate) amount: A,
    pub(crate) lifecycle: BasisLifecycle,
}

impl std::fmt::Display for AssetName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Usd => "USD",
            Self::Btc => "BTC",
            Self::Chf => "CHF",
            Self::Eur => "EUR",
            Self::Eth => "ETH",
            Self::EthW => "ETHW",
            Self::Jpy => "JPY",
            Self::Usdc => "USDC",
            Self::Usdt => "USDT",
        })
    }
}

impl FromStr for AssetName {
    type Err = AssetNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "USD" | "ZUSD" => Ok(AssetName::Usd),
            "BTC" | "XXBT" => Ok(AssetName::Btc),
            "CHF" => Ok(AssetName::Chf),
            "EUR" | "ZEUR" => Ok(AssetName::Eur),
            "ETH" | "XETH" => Ok(AssetName::Eth),
            "ETHW" => Ok(AssetName::EthW),
            "JPY" | "ZJPY" => Ok(AssetName::Jpy),
            "USDC" => Ok(AssetName::Usdc),
            "USDT" => Ok(AssetName::Usdt),
            _ => Err(AssetNameError::Parse),
        }
    }
}

impl From<BitcoinAmount> for AssetName {
    fn from(_value: BitcoinAmount) -> Self {
        Self::Btc
    }
}

impl From<PoolChfAmount> for AssetName {
    fn from(_value: PoolChfAmount) -> Self {
        Self::Chf
    }
}

impl From<EtherAmount> for AssetName {
    fn from(_value: EtherAmount) -> Self {
        Self::Eth
    }
}

impl From<EthWAmount> for AssetName {
    fn from(_value: EthWAmount) -> Self {
        Self::EthW
    }
}

impl From<PoolEurAmount> for AssetName {
    fn from(_value: PoolEurAmount) -> Self {
        Self::Eur
    }
}

impl From<PoolJpyAmount> for AssetName {
    fn from(_value: PoolJpyAmount) -> Self {
        Self::Jpy
    }
}

impl From<PoolUsdAmount> for AssetName {
    fn from(_value: PoolUsdAmount) -> Self {
        Self::Usd
    }
}

impl From<UsdcAmount> for AssetName {
    fn from(_value: UsdcAmount) -> Self {
        Self::Usdc
    }
}

impl From<UsdtAmount> for AssetName {
    fn from(_value: UsdtAmount) -> Self {
        Self::Usdt
    }
}

impl AssetName {
    pub(crate) fn as_kraken(&self) -> &str {
        match self {
            Self::Usd => "ZUSD",
            Self::Btc => "XXBT",
            Self::Chf => "CHF",
            Self::Eur => "ZEUR",
            Self::Eth => "XETH",
            Self::EthW => "ETHW",
            Self::Jpy => "ZJPY",
            Self::Usdc => "USDC",
            Self::Usdt => "USDT",
        }
    }
}

impl<A: Asset + Copy + Default + Add<Output = A> + Sub<Output = A>> FIFO<PoolAsset<A>> {
    pub(crate) fn splittable_take_while(
        &mut self,
        amount: A,
    ) -> Result<SplittableTakeWhile<PoolAsset<A>>, SplitBasisError>
    where
        PoolAsset<A>: PoolAssetSplit<Amount = A>,
    {
        // Special case for taking amount <= 0.
        // This avoids popping any PoolAssets from the FIFO.
        if amount <= A::default() {
            return Err(SplitBasisError::ZeroOrNegative);
        }

        let mut takes = Vec::new(); // build up the return vector here
        let mut so_far = A::default();

        while let Some(x) = self.pop_front() {
            //add x to return vector
            let sum = so_far + x.amount;

            match sum.cmp(&amount) {
                Ordering::Equal => {
                    // there is no remainder
                    let remain = None;
                    takes.push(x);

                    return Ok(SplittableTakeWhile { takes, remain });
                }
                Ordering::Greater => {
                    // there is a remainder
                    let split = x.split(amount - so_far);

                    takes.push(split.take);
                    let remain = Some(split.leave);

                    return Ok(SplittableTakeWhile { takes, remain });
                }
                Ordering::Less => {
                    // We are consuming the entire PoolAsset and continuing the loop.
                    takes.push(x);

                    so_far = sum;
                }
            }
        }

        // In the case that we are asking to sell assets we don't have.
        // This leaves the FIFO empty and drops all PoolAssets!
        Err(SplitBasisError::FatalLowBalance(format!(
            "Have {so_far:?}, need {amount:?}",
        )))
    }
}

impl<A: Asset> PoolAsset<A>
where
    BasisLifecycle: From<Rc<PoolAsset<A>>>,
{
    fn with_parent(parent: &Rc<PoolAsset<A>>, amount: A) -> Self {
        Self {
            amount,
            lifecycle: BasisLifecycle::from(parent.clone()),
        }
    }
}

impl<A: Asset + Copy + Sub<Output = A>> PoolAssetSplit for PoolAsset<A>
where
    BasisLifecycle: From<Rc<PoolAsset<A>>>,
{
    type Amount = A;

    fn split(self, take_amount: Self::Amount) -> HasSplit<PoolAsset<Self::Amount>> {
        let parent = Rc::new(self);
        let take = Self::with_parent(&parent, take_amount);
        let leave = Self::with_parent(&parent, parent.amount - take_amount);

        HasSplit { take, leave }
    }
}

impl<A: Asset + Copy> ToNonSplittable for PoolAsset<A> {
    type Amount = A;

    fn to_non_splittable(&self) -> PoolAssetNonSplittable<Self::Amount> {
        PoolAssetNonSplittable {
            amount: self.amount,
            lifecycle: self.lifecycle.clone(),
        }
    }
}

impl<A: Asset + Copy + Default + Add<Output = A>> FIFO<PoolAsset<A>> {
    /// Return the sum of all assets in this FIFO.
    pub(crate) fn amount(&self) -> A {
        let mut sum = A::default();
        for pool_asset in self.iter() {
            sum = sum + pool_asset.amount;
        }
        sum
    }

    pub(crate) fn try_from_tx(tx: &Tx, exchange_rate: Option<UsdAmount>) -> Option<Self>
    where
        A: TryFrom<KrakenAmount>,
        <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
    {
        let fifo = tx
            .outs
            .iter()
            .enumerate()
            .filter_map(|(index, txo)| {
                if let (true, Some(exchange_rate)) = (txo.mine, exchange_rate) {
                    let synthetic_id = format!("{txid}:{index}", txid = tx.txid);

                    let bucket = Bucket {
                        synthetic_id,
                        time: tx.time,
                        amount: txo.amount,
                        exchange_rate,
                    };

                    Some(PoolAsset {
                        amount: txo.amount.try_into().unwrap(),
                        lifecycle: BasisLifecycle::lifecycle_from_income(bucket),
                    })
                } else {
                    None
                }
            })
            .collect::<FIFO<_>>();

        (!fifo.is_empty()).then_some(fifo)
    }
}
