use super::{Asset, Bucket, PoolAsset};
use super::{PoolBTC, PoolCHF, PoolETH, PoolETHW, PoolEUR, PoolJPY, PoolUSD, PoolUSDC, PoolUSDT};
use crate::errors::ExchangeRateError;
use crate::model::exchange_rate::ExchangeRates;
use crate::model::kraken_amount::{FiatAmount, UsdAmount};
use crate::model::ledgers::parsed::{LedgerMarginClose, LedgerTwoRowTrade};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Clone, Debug)]
pub enum Origin {
    Bucket(Bucket),
    Income(Bucket),
    PoolBTC(Rc<PoolBTC>),
    PoolCHF(Rc<PoolCHF>),
    PoolETH(Rc<PoolETH>),
    PoolETHW(Rc<PoolETHW>),
    PoolEUR(Rc<PoolEUR>),
    PoolJPY(Rc<PoolJPY>),
    PoolUSDC(Rc<PoolUSDC>),
    PoolUSDT(Rc<PoolUSDT>),
    TradeBuy(LedgerTwoRowTrade),
    MarginClose(LedgerMarginClose),

    /// The base currency, e.g. USD.
    Base,
}

/// Resolves the recursive reference-counted PoolBTCs.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum OriginSerializable {
    Bucket(Bucket),
    TradeBuy(LedgerTwoRowTrade),
    MarginClose(LedgerMarginClose),
    Base,
}

#[derive(Clone)]
pub struct BasisLifecycle {
    pub origin: Origin,
}

impl std::fmt::Debug for BasisLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasisLifecycle")
            .field("synthetic_id", &self.get_synthetic_id())
            .field("resolved_origin", &self.get_serializable_origin())
            .finish()
    }
}

impl From<Rc<PoolBTC>> for Origin {
    fn from(value: Rc<PoolBTC>) -> Self {
        Self::PoolBTC(value)
    }
}

impl From<Rc<PoolCHF>> for Origin {
    fn from(value: Rc<PoolCHF>) -> Self {
        Self::PoolCHF(value)
    }
}

impl From<Rc<PoolETH>> for Origin {
    fn from(value: Rc<PoolETH>) -> Self {
        Self::PoolETH(value)
    }
}

impl From<Rc<PoolETHW>> for Origin {
    fn from(value: Rc<PoolETHW>) -> Self {
        Self::PoolETHW(value)
    }
}

impl From<Rc<PoolEUR>> for Origin {
    fn from(value: Rc<PoolEUR>) -> Self {
        Self::PoolEUR(value)
    }
}

impl From<Rc<PoolJPY>> for Origin {
    fn from(value: Rc<PoolJPY>) -> Self {
        Self::PoolJPY(value)
    }
}

impl From<Rc<PoolUSD>> for Origin {
    fn from(_value: Rc<PoolUSD>) -> Self {
        Self::Base
    }
}

impl From<Rc<PoolUSDC>> for Origin {
    fn from(value: Rc<PoolUSDC>) -> Self {
        Self::PoolUSDC(value)
    }
}

impl From<Rc<PoolUSDT>> for Origin {
    fn from(value: Rc<PoolUSDT>) -> Self {
        Self::PoolUSDT(value)
    }
}

impl<A: Asset> From<Rc<PoolAsset<A>>> for BasisLifecycle
where
    Origin: From<Rc<PoolAsset<A>>>,
{
    fn from(value: Rc<PoolAsset<A>>) -> Self {
        Self {
            origin: Origin::from(value),
        }
    }
}

impl BasisLifecycle {
    pub(crate) fn lifecycle_from_trade_buy(ltrt: LedgerTwoRowTrade) -> Self {
        Self {
            origin: Origin::TradeBuy(ltrt),
        }
    }

    pub(crate) fn lifecycle_from_margin_close(lmc: LedgerMarginClose) -> Self {
        Self {
            origin: Origin::MarginClose(lmc),
        }
    }

    pub(crate) fn lifecycle_from_bucket(bucket: Bucket) -> Self {
        Self {
            origin: Origin::Bucket(bucket),
        }
    }

    pub(crate) fn lifecycle_from_income(bucket: Bucket) -> Self {
        Self {
            origin: Origin::Income(bucket),
        }
    }

    pub(crate) fn lifecycle_from_base() -> Self {
        Self {
            origin: Origin::Base,
        }
    }

    pub(crate) fn lifecycle_from_origin(o: OriginSerializable) -> Self {
        match o {
            OriginSerializable::Base => Self::lifecycle_from_base(),
            OriginSerializable::Bucket(bucket) => Self::lifecycle_from_bucket(bucket),
            OriginSerializable::TradeBuy(ltrt) => Self::lifecycle_from_trade_buy(ltrt),
            OriginSerializable::MarginClose(lmc) => Self::lifecycle_from_margin_close(lmc),
        }
    }

    pub(crate) fn get_serializable_origin(&self) -> OriginSerializable {
        match &self.origin {
            Origin::Base => OriginSerializable::Base,
            Origin::Bucket(bucket) | Origin::Income(bucket) => {
                OriginSerializable::Bucket(bucket.clone())
            }
            Origin::TradeBuy(ltrt) => OriginSerializable::TradeBuy(ltrt.clone()),
            Origin::MarginClose(lmc) => OriginSerializable::MarginClose(lmc.clone()),

            // Recursively resolve the origin.
            Origin::PoolBTC(pool_btc) => pool_btc.lifecycle.get_serializable_origin(),
            Origin::PoolCHF(pool_chf) => pool_chf.lifecycle.get_serializable_origin(),
            Origin::PoolETH(pool_eth) => pool_eth.lifecycle.get_serializable_origin(),
            Origin::PoolETHW(pool_ethw) => pool_ethw.lifecycle.get_serializable_origin(),
            Origin::PoolEUR(pool_eur) => pool_eur.lifecycle.get_serializable_origin(),
            Origin::PoolJPY(pool_jpy) => pool_jpy.lifecycle.get_serializable_origin(),
            Origin::PoolUSDC(pool_usdc) => pool_usdc.lifecycle.get_serializable_origin(),
            Origin::PoolUSDT(pool_usdt) => pool_usdt.lifecycle.get_serializable_origin(),
        }
    }

    /// Get the date-time of asset acquisition for this lifecycle.
    pub(crate) fn get_datetime(&self) -> DateTime<Utc> {
        match &self.origin {
            Origin::Base => Utc::now(),
            Origin::Bucket(bucket) | Origin::Income(bucket) => bucket.time,
            Origin::TradeBuy(LedgerTwoRowTrade { row_out, .. }) => row_out.time,
            Origin::MarginClose(LedgerMarginClose { row_proceeds, .. }) => row_proceeds.time,

            // Recursively resolve the origin date-time.
            Origin::PoolBTC(pool_btc) => pool_btc.lifecycle.get_datetime(),
            Origin::PoolCHF(pool_chf) => pool_chf.lifecycle.get_datetime(),
            Origin::PoolETH(pool_eth) => pool_eth.lifecycle.get_datetime(),
            Origin::PoolETHW(pool_ethw) => pool_ethw.lifecycle.get_datetime(),
            Origin::PoolEUR(pool_eur) => pool_eur.lifecycle.get_datetime(),
            Origin::PoolJPY(pool_jpy) => pool_jpy.lifecycle.get_datetime(),
            Origin::PoolUSDC(pool_usdc) => pool_usdc.lifecycle.get_datetime(),
            Origin::PoolUSDT(pool_usdt) => pool_usdt.lifecycle.get_datetime(),
        }
    }

    /// Get the exchange rate at the time of acquisition for this lifecycle.
    pub(crate) fn get_exchange_rate_at_acquisition(
        &self,
        exchange_rates_db: &ExchangeRates,
    ) -> Result<UsdAmount, ExchangeRateError> {
        match &self.origin {
            Origin::Base => Ok(UsdAmount::from("1.0".parse::<FiatAmount>().unwrap())),
            Origin::Bucket(bucket) | Origin::Income(bucket) => Ok(bucket.exchange_rate),
            Origin::TradeBuy(LedgerTwoRowTrade { row_out, row_in }) => {
                let a = row_out.amount.abs();
                let b = row_in.amount;
                let exchange_rate_for_a = a.get_exchange_rate(row_out.time, exchange_rates_db)?;

                Ok(a.get_exchange_rate_for_b(b, exchange_rate_for_a))
            }
            Origin::MarginClose(LedgerMarginClose { row_proceeds, .. }) => row_proceeds
                .amount
                .get_exchange_rate(row_proceeds.time, exchange_rates_db),

            // Recursively resolve the acquired asset amount.
            Origin::PoolBTC(pool_btc) => pool_btc
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),

            Origin::PoolCHF(pool_chf) => pool_chf
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),

            Origin::PoolETH(pool_eth) => pool_eth
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),

            Origin::PoolETHW(pool_ethw) => pool_ethw
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),

            Origin::PoolEUR(pool_eur) => pool_eur
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),

            Origin::PoolJPY(pool_jpy) => pool_jpy
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),

            Origin::PoolUSDC(pool_usdc) => pool_usdc
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),

            Origin::PoolUSDT(pool_usdt) => pool_usdt
                .lifecycle
                .get_exchange_rate_at_acquisition(exchange_rates_db),
        }
    }

    /// Get the so-called "synthetic ID" for the asset.
    pub(crate) fn get_synthetic_id(&self) -> &str {
        match &self.origin {
            Origin::Base => "",
            Origin::Bucket(bucket) | Origin::Income(bucket) => &bucket.synthetic_id,

            // okay to get refid from either row of LedgerTwoRowTrade or LedgerMarginClose
            Origin::TradeBuy(tworow) => &tworow.row_out.refid,
            Origin::MarginClose(lmc) => &lmc.row_proceeds.refid,

            // Recursively resolve the synthetic id.
            Origin::PoolBTC(pool_btc) => pool_btc.lifecycle.get_synthetic_id(),
            Origin::PoolCHF(pool_chf) => pool_chf.lifecycle.get_synthetic_id(),
            Origin::PoolETH(pool_eth) => pool_eth.lifecycle.get_synthetic_id(),
            Origin::PoolETHW(pool_ethw) => pool_ethw.lifecycle.get_synthetic_id(),
            Origin::PoolEUR(pool_eur) => pool_eur.lifecycle.get_synthetic_id(),
            Origin::PoolJPY(pool_jpy) => pool_jpy.lifecycle.get_synthetic_id(),
            Origin::PoolUSDC(pool_usdc) => pool_usdc.lifecycle.get_synthetic_id(),
            Origin::PoolUSDT(pool_usdt) => pool_usdt.lifecycle.get_synthetic_id(),
        }
    }
}
