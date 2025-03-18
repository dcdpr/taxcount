use super::{Asset, PoolAsset};
use super::{PoolBTC, PoolETH, PoolETHW, PoolUSDC, PoolUSDT};
use super::{PoolCHF, PoolEUR, PoolJPY};
use crate::model::blockchain::{Account, Utxo};
use crate::model::ledgers::rows::BasisRow;
use crate::util::fifo::FIFO;

#[derive(Debug, Default)]
pub(crate) struct BasisLookup {
    pub(crate) btc: Utxo<PoolBTC>,
    pub(crate) chf: Account<PoolCHF>,
    pub(crate) eth: Account<PoolETH>,
    pub(crate) ethw: Account<PoolETHW>,
    pub(crate) eur: Account<PoolEUR>,
    pub(crate) jpy: Account<PoolJPY>,
    pub(crate) usdc: Account<PoolUSDC>,
    pub(crate) usdt: Account<PoolUSDT>,
}

pub(crate) trait BasisLookupExt {
    type Asset: Asset;

    /// Append a pool asset to the back of the FIFO indexed by `key`. Where `key` is a TXID or
    /// account address.
    fn push<S>(&mut self, key: S, value: PoolAsset<Self::Asset>)
    where
        S: ToString;

    /// Take the basis FIFO indexed by `key`. Where `key` is a TXID or account address.
    fn take_basis<S>(&mut self, key: S) -> Option<FIFO<PoolAsset<Self::Asset>>>
    where
        S: AsRef<str>;
}

impl<A: Asset> BasisLookupExt for Utxo<PoolAsset<A>> {
    type Asset = A;

    fn push<S>(&mut self, key: S, value: PoolAsset<Self::Asset>)
    where
        S: ToString,
    {
        self.entry(key).or_default().append_back(value);
    }

    fn take_basis<S>(&mut self, key: S) -> Option<FIFO<PoolAsset<Self::Asset>>>
    where
        S: AsRef<str>,
    {
        self.remove(key)
    }
}

impl<A: Asset> BasisLookupExt for Account<PoolAsset<A>> {
    type Asset = A;

    fn push<S>(&mut self, key: S, value: PoolAsset<Self::Asset>)
    where
        S: ToString,
    {
        self.entry(key).or_default().append_back(value);
    }

    fn take_basis<S>(&mut self, key: S) -> Option<FIFO<PoolAsset<Self::Asset>>>
    where
        S: AsRef<str>,
    {
        self.remove(key)
    }
}

/// Create `BasisLookup` from the basis lookup CSV.
///
/// CSV rows with synthetic_id will be merged into a single blockchain model.
impl FIFO<BasisRow> {
    pub(crate) fn parse(self) -> BasisLookup {
        let mut basis_lookup = BasisLookup::default();

        for row in self {
            let synthetic_id = &row.synthetic_id;

            match row.asset.as_str() {
                "CHF" => basis_lookup
                    .chf
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                "ETHW" => basis_lookup
                    .ethw
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                "USDC" => basis_lookup
                    .usdc
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                "USDT" => basis_lookup
                    .usdt
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                "BTC" | "XXBT" => basis_lookup
                    .btc
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                "ETH" | "XETH" => basis_lookup
                    .eth
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                "EUR" | "ZEUR" => basis_lookup
                    .eur
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                "JPY" | "ZJPY" => basis_lookup
                    .jpy
                    .push(synthetic_id, PoolAsset::from_basis_row(&row)),
                asset => panic!("Unknown asset `{asset}`"),
            }
        }

        basis_lookup
    }
}
