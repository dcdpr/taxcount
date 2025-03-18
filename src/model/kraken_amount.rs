use crate::basis::{Asset, AssetName};
use crate::errors::ExchangeRateError;
use crate::model::exchange_rate::ExchangeRates;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Neg, Sub};
use std::{fmt, str::FromStr};
use thiserror::Error;

pub(crate) const KRAKEN_BITCOIN_DIGITS: u8 = 8;
pub(crate) const KRAKEN_CRYPTO_INPUT_DIGITS: u8 = 10;
pub(crate) const KRAKEN_ETHER_DIGITS: u8 = 18;
pub(crate) const KRAKEN_STABLECOIN_DIGITS: u8 = 8;
pub(crate) const KRAKEN_FIAT_DIGITS: u8 = 4;

/// Since we can't ensure that all arithmetic has infinite precision, we need an epsilon for
/// comparisons. This seems like a good enough number that is smaller than any USD precision used.
const USD_EPSILON: &str = "0.000_000_1";

#[derive(Debug, Error)]
pub enum ConvertAmountError {
    /// Type is incongruent.
    #[error("Type is incongruent")]
    WrongCurrency,

    /// Unable to parse decimal string.
    #[error("Unable to parse decimal string")]
    Decimal(#[from] rust_decimal::Error),

    /// Unknown asset.
    #[error("Unknown asset: {0}")]
    UnknownAsset(String),
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BitcoinAmount(Decimal);

/// Poolable Swiss Francs amounts. These must be distinct from `FiatAmount` because it must implement
/// `TryFrom<KrakenAmount> + Into<KrakenAmount>`.
#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PoolChfAmount(Decimal);

/// Poolable Euro amounts. These must be distinct from `FiatAmount` because it must implement
/// `TryFrom<KrakenAmount> + Into<KrakenAmount>`.
#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PoolEurAmount(Decimal);

/// Poolable Japanese Yen amounts. These must be distinct from `FiatAmount` because it must implement
/// `TryFrom<KrakenAmount> + Into<KrakenAmount>`.
#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PoolJpyAmount(Decimal);

/// Poolable USD amounts. These must be distinct from `FiatAmount` because it must implement
/// `TryFrom<KrakenAmount> + Into<KrakenAmount>`.
///
/// This is also a base currency with special handling for deposits.
#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PoolUsdAmount(Decimal);

#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EtherAmount(Decimal);

#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EthWAmount(Decimal);

#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct UsdcAmount(Decimal);

#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct UsdtAmount(Decimal);

#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FiatAmount(Decimal);

/// Spreadsheet representation of USD value.
#[derive(Copy, Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct UsdAmount(FiatAmount);

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum KrakenAmount {
    Btc(BitcoinAmount),
    Chf(FiatAmount),
    Eth(EtherAmount),
    EthW(EthWAmount),
    Eur(FiatAmount),
    Usd(FiatAmount),
    Usdc(UsdcAmount),
    Usdt(UsdtAmount),
    Jpy(FiatAmount),
}

macro_rules! impl_math_ops {
    ($name:ident) => {
        impl ::std::ops::Add for $name {
            type Output = Self;

            fn add(self, rhs: Self) -> Self::Output {
                Self(self.0 + rhs.0)
            }
        }

        impl ::std::ops::AddAssign for $name {
            fn add_assign(&mut self, rhs: Self) {
                self.0 += rhs.0;
            }
        }

        impl ::std::ops::Neg for $name {
            type Output = Self;

            fn neg(self) -> Self::Output {
                Self(-self.0)
            }
        }

        impl ::std::ops::Sub for $name {
            type Output = Self;

            fn sub(self, rhs: Self) -> Self::Output {
                Self(self.0 - rhs.0)
            }
        }
    };
}

impl_math_ops!(BitcoinAmount);
impl_math_ops!(PoolChfAmount);
impl_math_ops!(PoolEurAmount);
impl_math_ops!(PoolJpyAmount);
impl_math_ops!(PoolUsdAmount);
impl_math_ops!(EtherAmount);
impl_math_ops!(EthWAmount);
impl_math_ops!(UsdcAmount);
impl_math_ops!(UsdtAmount);
impl_math_ops!(FiatAmount);
impl_math_ops!(UsdAmount);

impl Add for KrakenAmount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        use KrakenAmount::*;

        match (self, rhs) {
            (Btc(amount1), Btc(amount2)) => Btc(amount1 + amount2),
            (Chf(amount1), Chf(amount2)) => Chf(amount1 + amount2),
            (Eth(amount1), Eth(amount2)) => Eth(amount1 + amount2),
            (EthW(amount1), EthW(amount2)) => EthW(amount1 + amount2),
            (Eur(amount1), Eur(amount2)) => Eur(amount1 + amount2),
            (Jpy(amount1), Jpy(amount2)) => Jpy(amount1 + amount2),
            (Usd(amount1), Usd(amount2)) => Usd(amount1 + amount2),
            (Usdc(amount1), Usdc(amount2)) => Usdc(amount1 + amount2),
            (Usdt(amount1), Usdt(amount2)) => Usdt(amount1 + amount2),
            (Btc(_), _)
            | (Chf(_), _)
            | (Eth(_), _)
            | (EthW(_), _)
            | (Eur(_), _)
            | (Jpy(_), _)
            | (Usd(_), _)
            | (Usdc(_), _)
            | (Usdt(_), _) => panic!("Invalid differing types for add: {self:?} + {rhs:?}"),
        }
    }
}

impl Neg for KrakenAmount {
    type Output = Self;

    fn neg(self) -> Self::Output {
        use KrakenAmount::*;

        match self {
            Btc(amount) => Btc(-amount),
            Chf(amount) => Chf(-amount),
            Eth(amount) => Eth(-amount),
            EthW(amount) => EthW(-amount),
            Eur(amount) => Eur(-amount),
            Jpy(amount) => Jpy(-amount),
            Usd(amount) => Usd(-amount),
            Usdc(amount) => Usdc(-amount),
            Usdt(amount) => Usdt(-amount),
        }
    }
}

impl Sub for KrakenAmount {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        use KrakenAmount::*;

        match (self, rhs) {
            (Btc(amount1), Btc(amount2)) => Btc(amount1 - amount2),
            (Chf(amount1), Chf(amount2)) => Chf(amount1 - amount2),
            (Eth(amount1), Eth(amount2)) => Eth(amount1 - amount2),
            (EthW(amount1), EthW(amount2)) => EthW(amount1 - amount2),
            (Eur(amount1), Eur(amount2)) => Eur(amount1 - amount2),
            (Jpy(amount1), Jpy(amount2)) => Jpy(amount1 - amount2),
            (Usd(amount1), Usd(amount2)) => Usd(amount1 - amount2),
            (Usdc(amount1), Usdc(amount2)) => Usdc(amount1 - amount2),
            (Usdt(amount1), Usdt(amount2)) => Usdt(amount1 - amount2),
            (Btc(_), _)
            | (Chf(_), _)
            | (Eth(_), _)
            | (EthW(_), _)
            | (Eur(_), _)
            | (Jpy(_), _)
            | (Usd(_), _)
            | (Usdc(_), _)
            | (Usdt(_), _) => panic!("Invalid differing types for subtract: {self:?} + {rhs:?}"),
        }
    }
}

impl Ord for KrakenAmount {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use KrakenAmount::*;

        match (self, other) {
            (Btc(amount1), Btc(amount2)) => amount1.cmp(amount2),
            (Chf(amount1), Chf(amount2)) => amount1.cmp(amount2),
            (Eth(amount1), Eth(amount2)) => amount1.cmp(amount2),
            (EthW(amount1), EthW(amount2)) => amount1.cmp(amount2),
            (Eur(amount1), Eur(amount2)) => amount1.cmp(amount2),
            (Jpy(amount1), Jpy(amount2)) => amount1.cmp(amount2),
            (Usd(amount1), Usd(amount2)) => amount1.cmp(amount2),
            (Usdc(amount1), Usdc(amount2)) => amount1.cmp(amount2),
            (Usdt(amount1), Usdt(amount2)) => amount1.cmp(amount2),
            (Btc(_), _)
            | (Chf(_), _)
            | (Eth(_), _)
            | (EthW(_), _)
            | (Eur(_), _)
            | (Jpy(_), _)
            | (Usd(_), _)
            | (Usdc(_), _)
            | (Usdt(_), _) => panic!("Invalid differing types for cmp: {self:?} + {other:?}"),
        }
    }
}

impl PartialOrd for KrakenAmount {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// TODO: Correct the `Decimal` precision after parsing from a string.
impl FromStr for BitcoinAmount {
    type Err = ConvertAmountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl FromStr for EtherAmount {
    type Err = ConvertAmountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl FromStr for EthWAmount {
    type Err = ConvertAmountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl FromStr for UsdcAmount {
    type Err = ConvertAmountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl FromStr for UsdtAmount {
    type Err = ConvertAmountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl FromStr for FiatAmount {
    type Err = ConvertAmountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl From<PoolChfAmount> for FiatAmount {
    fn from(value: PoolChfAmount) -> Self {
        Self(value.0)
    }
}

impl From<PoolEurAmount> for FiatAmount {
    fn from(value: PoolEurAmount) -> Self {
        Self(value.0)
    }
}

impl From<PoolJpyAmount> for FiatAmount {
    fn from(value: PoolJpyAmount) -> Self {
        Self(value.0)
    }
}

impl From<PoolUsdAmount> for FiatAmount {
    fn from(value: PoolUsdAmount) -> Self {
        Self(value.0)
    }
}

impl From<BitcoinAmount> for KrakenAmount {
    fn from(value: BitcoinAmount) -> Self {
        Self::Btc(value)
    }
}

impl From<PoolChfAmount> for KrakenAmount {
    fn from(value: PoolChfAmount) -> Self {
        Self::Chf(FiatAmount(value.0))
    }
}

impl From<PoolEurAmount> for KrakenAmount {
    fn from(value: PoolEurAmount) -> Self {
        Self::Eur(FiatAmount(value.0))
    }
}

impl From<PoolJpyAmount> for KrakenAmount {
    fn from(value: PoolJpyAmount) -> Self {
        Self::Jpy(FiatAmount(value.0))
    }
}

impl From<PoolUsdAmount> for KrakenAmount {
    fn from(value: PoolUsdAmount) -> Self {
        Self::Usd(FiatAmount(value.0))
    }
}

impl From<EtherAmount> for KrakenAmount {
    fn from(value: EtherAmount) -> Self {
        Self::Eth(value)
    }
}

impl From<EthWAmount> for KrakenAmount {
    fn from(value: EthWAmount) -> Self {
        Self::EthW(value)
    }
}

impl From<UsdcAmount> for KrakenAmount {
    fn from(value: UsdcAmount) -> Self {
        Self::Usdc(value)
    }
}

impl From<UsdtAmount> for KrakenAmount {
    fn from(value: UsdtAmount) -> Self {
        Self::Usdt(value)
    }
}

impl From<FiatAmount> for UsdAmount {
    fn from(value: FiatAmount) -> Self {
        Self(value)
    }
}

impl TryFrom<(AssetName, FiatAmount)> for KrakenAmount {
    type Error = ConvertAmountError;

    fn try_from((asset, value): (AssetName, FiatAmount)) -> Result<Self, Self::Error> {
        match asset {
            AssetName::Chf => Ok(KrakenAmount::Chf(value)),
            AssetName::Eur => Ok(KrakenAmount::Eur(value)),
            AssetName::Jpy => Ok(KrakenAmount::Jpy(value)),
            AssetName::Usd => Ok(KrakenAmount::Usd(value)),
            AssetName::Btc
            | AssetName::Eth
            | AssetName::EthW
            | AssetName::Usdc
            | AssetName::Usdt => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for BitcoinAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Btc(amount) => Ok(amount),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for PoolChfAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Chf(FiatAmount(amount)) => Ok(Self(amount)),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for PoolEurAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Eur(FiatAmount(amount)) => Ok(Self(amount)),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for PoolJpyAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Jpy(FiatAmount(amount)) => Ok(Self(amount)),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for PoolUsdAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Usd(FiatAmount(amount)) => Ok(Self(amount)),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for EtherAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Eth(amount) => Ok(amount),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for EthWAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::EthW(amount) => Ok(amount),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for UsdcAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Usdc(amount) => Ok(amount),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

impl TryFrom<KrakenAmount> for UsdtAmount {
    type Error = ConvertAmountError;

    fn try_from(value: KrakenAmount) -> Result<Self, Self::Error> {
        match value {
            KrakenAmount::Usdt(amount) => Ok(amount),
            _ => Err(ConvertAmountError::WrongCurrency),
        }
    }
}

// These types are allowed to be used as pooled assets.
impl Asset for BitcoinAmount {}
impl Asset for PoolChfAmount {}
impl Asset for PoolEurAmount {}
impl Asset for PoolJpyAmount {}
impl Asset for PoolUsdAmount {}
impl Asset for EtherAmount {}
impl Asset for EthWAmount {}
impl Asset for UsdcAmount {}
impl Asset for UsdtAmount {}

impl BitcoinAmount {
    fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl EtherAmount {
    fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl EthWAmount {
    fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl UsdtAmount {
    fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl UsdcAmount {
    fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl FiatAmount {
    fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl KrakenAmount {
    /// Try to create a new KrakenAmount.
    pub fn new(asset: &str, amount: &str) -> Result<Self, ConvertAmountError> {
        match asset {
            "CHF" => Ok(KrakenAmount::Chf(amount.parse()?)),
            "ETHW" => Ok(KrakenAmount::EthW(amount.parse()?)),
            "USDC" => Ok(KrakenAmount::Usdc(amount.parse()?)),
            "USDT" => Ok(KrakenAmount::Usdt(amount.parse()?)),
            "BTC" | "XXBT" => Ok(KrakenAmount::Btc(amount.parse()?)),
            "ETH" | "XETH" => Ok(KrakenAmount::Eth(amount.parse()?)),
            "EUR" | "ZEUR" => Ok(KrakenAmount::Eur(amount.parse()?)),
            "JPY" | "ZJPY" => Ok(KrakenAmount::Jpy(amount.parse()?)),
            "USD" | "ZUSD" => Ok(KrakenAmount::Usd(amount.parse()?)),
            _ => Err(ConvertAmountError::UnknownAsset(asset.to_string())),
        }
    }

    /// Create a new KrakenAmount from a `Decimal` using its own precision.
    pub(crate) fn try_from_decimal(
        asset: &str,
        mut amount: Decimal,
    ) -> Result<Self, ConvertAmountError> {
        Self::rescale_decimal(asset, &mut amount)?;

        match asset {
            "CHF" => Ok(KrakenAmount::Chf(FiatAmount(amount))),
            "ETHW" => Ok(KrakenAmount::EthW(EthWAmount(amount))),
            "USDC" => Ok(KrakenAmount::Usdc(UsdcAmount(amount))),
            "USDT" => Ok(KrakenAmount::Usdt(UsdtAmount(amount))),
            "BTC" | "XXBT" => Ok(KrakenAmount::Btc(BitcoinAmount(amount))),
            "ETH" | "XETH" => Ok(KrakenAmount::Eth(EtherAmount(amount))),
            "EUR" | "ZEUR" => Ok(KrakenAmount::Eur(FiatAmount(amount))),
            "JPY" | "ZJPY" => Ok(KrakenAmount::Jpy(FiatAmount(amount))),
            "USD" | "ZUSD" => Ok(KrakenAmount::Usd(FiatAmount(amount))),
            _ => Err(ConvertAmountError::UnknownAsset(asset.to_string())),
        }
    }

    /// Create a new zero-valued KrakenAmount with the given asset.
    ///
    /// This constructor uses the same internal precision as defined by the Kraken ledger CSV.
    pub(crate) fn zero(asset: &str) -> Result<Self, ConvertAmountError> {
        match asset {
            "CHF" => Self::new("CHF", "0.0000"),
            "ETHW" => Self::new("ETHW", "0.0000000000"),
            "USDC" => Self::new("USDC", "0.00000000"),
            "USDT" => Self::new("USDT", "0.00000000"),
            "BTC" | "XXBT" => Self::new("BTC", "0.0000000000"),
            "ETH" | "XETH" => Self::new("ETH", "0.0000000000"),
            "EUR" | "ZEUR" => Self::new("EUR", "0.0000"),
            "JPY" | "ZJPY" => Self::new("JPY", "0.0000"),
            "USD" | "ZUSD" => Self::new("USD", "0.0000"),
            _ => Err(ConvertAmountError::UnknownAsset(asset.to_string())),
        }
    }

    /// Rescale the Decimal amount to a valid Kraken precision.
    fn rescale_decimal(asset: &str, amount: &mut Decimal) -> Result<(), ConvertAmountError> {
        match asset {
            "CHF" | "EUR" | "ZEUR" | "JPY" | "ZJPY" | "USD" | "ZUSD" => {
                amount.rescale(KRAKEN_FIAT_DIGITS as u32);
            }
            "BTC" | "XXBT" | "ETH" | "XETH" | "ETHW" => {
                amount.rescale(KRAKEN_CRYPTO_INPUT_DIGITS as u32);
            }
            "USDC" | "USDT" => {
                amount.rescale(KRAKEN_STABLECOIN_DIGITS as u32);
            }
            _ => return Err(ConvertAmountError::UnknownAsset(asset.to_string())),
        }

        Ok(())
    }

    /// Get the exchange rate for a KrakenAmount.
    pub(crate) fn get_exchange_rate(
        self,
        datetime: DateTime<Utc>,
        exchange_rates_db: &ExchangeRates,
    ) -> Result<UsdAmount, ExchangeRateError> {
        match self {
            Self::Btc(_) => Ok(exchange_rates_db.get(AssetName::Btc, datetime)?),
            Self::Chf(_) => Ok(exchange_rates_db.get(AssetName::Chf, datetime)?),
            Self::Eth(_) => Ok(exchange_rates_db.get(AssetName::Eth, datetime)?),
            Self::EthW(_) => Ok(exchange_rates_db.get(AssetName::EthW, datetime)?),
            Self::Eur(_) => Ok(exchange_rates_db.get(AssetName::Eur, datetime)?),
            Self::Jpy(_) => Ok(exchange_rates_db.get(AssetName::Jpy, datetime)?),
            Self::Usd(_) => Ok(UsdAmount(FiatAmount(Decimal::from(1)))),
            Self::Usdc(_) => Ok(exchange_rates_db.get(AssetName::Usdc, datetime)?),
            Self::Usdt(_) => Ok(exchange_rates_db.get(AssetName::Usdt, datetime)?),
        }
    }

    /// Given A, get the exchange rate for B.
    ///
    /// Exchange rate for B = `A * R_A / B` where `R_A` is exchange rate for A.
    pub(crate) fn get_exchange_rate_for_b(
        self,
        b: Self,
        exchange_rate_for_a: UsdAmount,
    ) -> UsdAmount {
        let usd_for_a = self.get_value_usd(exchange_rate_for_a);

        // There is no way to avoid this divide because change splits must split the realized
        // trade value.
        usd_for_a.sub_divide(b)
    }

    /// Get the value of this KrakenAmount in USD from a known exchange rate.
    pub(crate) fn get_value_usd(self, exchange_rate: UsdAmount) -> UsdAmount {
        // TODO: Rescale the `Decimal` value after multiplication.
        match self {
            Self::Btc(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
            Self::Chf(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
            Self::Eth(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
            Self::EthW(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
            Self::Eur(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
            Self::Jpy(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
            Self::Usd(amount) => UsdAmount(amount),
            Self::Usdc(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
            Self::Usdt(amount) => UsdAmount(FiatAmount(amount.0 * exchange_rate.0 .0)),
        }
    }

    pub(crate) fn get_asset(self) -> AssetName {
        match self {
            Self::Btc(_) => AssetName::Btc,
            Self::Chf(_) => AssetName::Chf,
            Self::Eth(_) => AssetName::Eth,
            Self::EthW(_) => AssetName::EthW,
            Self::Eur(_) => AssetName::Eur,
            Self::Jpy(_) => AssetName::Jpy,
            Self::Usd(_) => AssetName::Usd,
            Self::Usdc(_) => AssetName::Usdc,
            Self::Usdt(_) => AssetName::Usdt,
        }
    }

    /// Get the number of decimal places for this asset.
    ///
    /// - `BTC`: "satoshi" = 8 decimal places.
    /// - `ETH`, `ETHW`: "wei" = 18 decimal places.
    /// - `CHF`, `EUR`, `JPY`, `USD`: "centimils" (century of millionths) = 4 decimal places.
    /// - `USDC`, `USDT`: "stablecoin" = 8 decimal places
    pub(crate) fn get_precision(self) -> u8 {
        match self {
            Self::Btc(_) => KRAKEN_BITCOIN_DIGITS,
            Self::Eth(_) | Self::EthW(_) => KRAKEN_ETHER_DIGITS,
            Self::Chf(_) | Self::Eur(_) | Self::Jpy(_) | Self::Usd(_) => KRAKEN_FIAT_DIGITS,
            Self::Usdc(_) | Self::Usdt(_) => KRAKEN_STABLECOIN_DIGITS,
        }
    }

    pub(crate) fn is_zero(self) -> bool {
        self.to_decimal().is_zero()
    }

    pub(crate) fn is_positive(self) -> bool {
        self.to_decimal() > Decimal::ZERO
    }

    pub(crate) fn is_negative(self) -> bool {
        self.to_decimal() < Decimal::ZERO
    }

    pub(crate) fn abs(self) -> Self {
        match self {
            Self::Btc(amount) => Self::Btc(amount.abs()),
            Self::Chf(amount) => Self::Chf(amount.abs()),
            Self::Eth(amount) => Self::Eth(amount.abs()),
            Self::EthW(amount) => Self::EthW(amount.abs()),
            Self::Eur(amount) => Self::Eur(amount.abs()),
            Self::Jpy(amount) => Self::Jpy(amount.abs()),
            Self::Usd(amount) => Self::Usd(amount.abs()),
            Self::Usdc(amount) => Self::Usdc(amount.abs()),
            Self::Usdt(amount) => Self::Usdt(amount.abs()),
        }
    }

    pub(crate) fn to_decimal(self) -> Decimal {
        match &self {
            Self::Btc(BitcoinAmount(amount)) => *amount,
            Self::Eth(EtherAmount(amount)) => *amount,
            Self::EthW(EthWAmount(amount)) => *amount,
            Self::Chf(FiatAmount(amount))
            | Self::Eur(FiatAmount(amount))
            | Self::Usd(FiatAmount(amount))
            | Self::Jpy(FiatAmount(amount)) => *amount,
            Self::Usdc(UsdcAmount(amount)) => *amount,
            Self::Usdt(UsdtAmount(amount)) => *amount,
        }
    }

    pub(crate) fn inverse(self) -> Self {
        // TODO: Rescale the `Decimal` value after division.
        let one = Decimal::from(1);
        match self {
            Self::Btc(BitcoinAmount(amount)) => Self::Btc(BitcoinAmount(one / amount)),
            Self::Chf(FiatAmount(amount)) => Self::Chf(FiatAmount(one / amount)),
            Self::Eth(EtherAmount(amount)) => Self::Eth(EtherAmount(one / amount)),
            Self::EthW(EthWAmount(amount)) => Self::EthW(EthWAmount(one / amount)),
            Self::Eur(FiatAmount(amount)) => Self::Eur(FiatAmount(one / amount)),
            Self::Jpy(FiatAmount(amount)) => Self::Jpy(FiatAmount(one / amount)),
            Self::Usd(FiatAmount(amount)) => Self::Usd(FiatAmount(one / amount)),
            Self::Usdc(UsdcAmount(amount)) => Self::Usdc(UsdcAmount(one / amount)),
            Self::Usdt(UsdtAmount(amount)) => Self::Usdt(UsdtAmount(one / amount)),
        }
    }

    /// Conversions between KrakenAmounts are only done in tests.
    #[cfg(test)]
    pub(crate) fn convert(
        self,
        to_asset: AssetName,
        exchange_rates: &ExchangeRates,
        datetime: DateTime<Utc>,
    ) -> Result<Self, ExchangeRateError> {
        // We only have exchange rates denominated in USD. Converting between non-USD pairs is
        // therefore a bit involved.

        // First, get the (USD) exchange rate for the outgoing asset and its value in USD.
        let exchange_rate_out = self.get_exchange_rate(datetime, exchange_rates)?;
        let amount_out = self.get_value_usd(exchange_rate_out).0 .0;

        // Then get the (USD) exchange rate for the incoming asset.
        let zero = Self::zero(to_asset.as_kraken()).unwrap();
        let exchange_rate_in = zero.get_exchange_rate(datetime, exchange_rates)?.0 .0;

        // Finally, divide the outgoing asset amount by the incoming exchange rate.
        let amount = amount_out / exchange_rate_in;

        Ok(Self::try_from_decimal(to_asset.as_kraken(), amount).unwrap())
    }

    pub(crate) fn get_kraken_precision(self) -> u8 {
        match self {
            Self::Btc(_) | Self::Eth(_) | Self::EthW(_) => KRAKEN_CRYPTO_INPUT_DIGITS,
            Self::Chf(_) | Self::Eur(_) | Self::Jpy(_) | Self::Usd(_) => KRAKEN_FIAT_DIGITS,
            Self::Usdc(_) | Self::Usdt(_) => KRAKEN_STABLECOIN_DIGITS,
        }
    }

    /// Like [`Display`], but uses Kraken's preferred precision.
    pub(crate) fn to_kraken_csv(self) -> String {
        let precision = self.get_kraken_precision();
        let amount = self.to_decimal().round_dp(precision.into());

        format!("{amount:.precision$}", precision = precision.into())
    }
}

impl fmt::Display for KrakenAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let precision = self.get_precision();
        // We use "banker's rounding" when writing the CSV. This rounding strategy most closely
        // resembles the original code (fixed point math) prior to the `Decimal` conversion. The
        // `Decimal` type supports alternative rounding strategies, if we want to change it.
        //
        // SEE: https://docs.rs/rust_decimal/latest/rust_decimal/prelude/enum.RoundingStrategy.html
        let amount = self.to_decimal().round_dp(precision.into());

        write!(f, "{amount:.precision$}", precision = precision.into())
    }
}

impl UsdAmount {
    /// Get the absolute value.
    pub(crate) fn abs(self) -> Self {
        Self(self.0.abs())
    }

    /// Get the minimum between two [`UsdAmount`]s.
    pub(crate) fn min(self, other: Self) -> Self {
        Self(FiatAmount(self.0 .0.min(other.0 .0)))
    }

    /// Get the maximum between two [`UsdAmount`]s.
    pub(crate) fn max(self, other: Self) -> Self {
        Self(FiatAmount(self.0 .0.max(other.0 .0)))
    }

    /// A typed division between differing units.
    pub(crate) fn sub_divide(self, other: KrakenAmount) -> Self {
        // TODO: Rescale the `Decimal` value after division.
        match other {
            // BtcChf,
            // BtcEur,
            // BtcJpy,
            // BtcUsd,
            // BtcUsdc,
            // BtcUsdt,
            KrakenAmount::Btc(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // ChfEur
            // ChfJpy
            // ChfUsd
            KrakenAmount::Chf(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // EthBtc,
            // EthChf,
            // EthEur,
            // EthJpy,
            // EthUsd,
            // EthUsdc,
            // EthUsdt,
            KrakenAmount::Eth(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // EthWEth,
            // EthWEur,
            // EthWUsd,
            KrakenAmount::EthW(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // EurChf,
            // EurJpy,
            // EurUsd,
            KrakenAmount::Eur(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // JpyChf,
            // JpyEur,
            // JpyUsd,
            KrakenAmount::Jpy(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // UsdChf,
            // UsdEur,
            // UsdJpy,
            KrakenAmount::Usd(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // UsdcChf,
            // UsdcEur,
            // UsdcJpy,
            // UsdcUsd,
            // UsdcUsdt,
            KrakenAmount::Usdc(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),

            // UsdtChf,
            // UsdtEur,
            // UsdtJpy,
            // UsdtUsd,
            KrakenAmount::Usdt(amount2) => Self(FiatAmount(self.0 .0 / amount2.0)),
        }
    }

    pub(crate) fn is_fuzzy_eq(self, other: Self) -> bool {
        (self.0 .0 - other.0 .0).abs() < USD_EPSILON.parse().unwrap()
    }
}

impl fmt::Display for UsdAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // We are asserting a precision cutoff for USD values that is supported by Kraken (4 decimal
        // places). It is enough that there is no ambiguity for monetary value (e.g. $0.0090 is
        // effectively $0.01).
        //
        // But there are still cases with _inconsequentially small amounts_ where the numbers in the
        // spreadsheets will not match up. For instance, `0.0004 - 0.0005 = 0.0000` due to precision
        // loss. The actual numbers are: `0.0004_29 - 0.0004_68 = -0.0000_39`.
        let precision = KRAKEN_FIAT_DIGITS;
        // We use "banker's rounding" when writing the CSV. This rounding strategy most closely
        // resembles the original code (fixed point math) prior to the `Decimal` conversion. The
        // `Decimal` type supports alternative rounding strategies, if we want to change it.
        //
        // SEE: https://docs.rs/rust_decimal/latest/rust_decimal/prelude/enum.RoundingStrategy.html
        let amount = self.0 .0.round_dp(precision.into());

        write!(f, "{amount:.precision$}", precision = precision.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbtest::arbitrary::{Result as ArbResult, Unstructured};
    use arbtest::arbtest;
    use tracing_test::traced_test;

    fn trim_leading_zeros(amount: String) -> String {
        let (is_neg, trimmed) = if let Some(trimmed) = amount.strip_prefix('-') {
            (true, trimmed)
        } else {
            (false, amount.as_str())
        };

        let trimmed = trimmed.trim_start_matches('0');
        let zero = if trimmed.starts_with('.') { "0" } else { "" };
        let neg = if is_neg { "-" } else { "" };

        // Negative zero is a special case
        if is_neg && trimmed.trim_end_matches('0') == "." {
            return format!("0{trimmed}");
        }

        format!("{neg}{zero}{trimmed}")
    }

    fn generate_kraken_amount_string(u: &mut Unstructured<'_>, precision: u8) -> ArbResult<String> {
        const DIGITS: [char; 10] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
        let mut amount = String::new();

        // `Decimal` supports up to 28 digits total, and `precision` tells us where to put the
        // decimal point.
        let leading_digits = 28 - precision;

        // Randomly positive or negative.
        amount.push_str(u.choose(&["-", ""])?);

        // Add random leading digits.
        for _ in 1..=u.int_in_range(1..=leading_digits)? {
            amount.push(*u.choose(&DIGITS)?);
        }

        // Decimal point.
        amount.push('.');

        // Add random trailing digits.
        for _ in 1..=u.int_in_range(1..=precision)? {
            amount.push(*u.choose(&DIGITS)?);
        }

        Ok(amount)
    }

    #[test]
    #[traced_test]
    fn prop_test_parse_kraken_amount_eighteen_digits() {
        arbtest(|u| {
            let arb_asset = u.choose(&["ETHW", "XETH"])?;
            let arb_amount = generate_kraken_amount_string(u, 18)?;
            let amount = KrakenAmount::new(arb_asset, &arb_amount).unwrap();
            let expected = trim_leading_zeros(arb_amount);
            assert_eq!(amount.to_decimal().to_string(), expected);
            Ok(())
        });
    }

    #[test]
    #[traced_test]
    fn prop_test_parse_kraken_amount_ten_digits() {
        arbtest(|u| {
            let arb_amount = generate_kraken_amount_string(u, 10)?;
            let amount = KrakenAmount::new("XXBT", &arb_amount).unwrap();
            let expected = trim_leading_zeros(arb_amount);
            assert_eq!(amount.to_decimal().to_string(), expected);
            Ok(())
        });
    }

    #[test]
    #[traced_test]
    fn prop_test_parse_kraken_amount_eight_digits() {
        arbtest(|u| {
            let arb_asset = u.choose(&["USDC", "USDT", "XXBT", "XETH"])?;
            let arb_amount = generate_kraken_amount_string(u, 8)?;
            let amount = KrakenAmount::new(arb_asset, &arb_amount).unwrap();
            let expected = trim_leading_zeros(arb_amount);
            assert_eq!(amount.to_decimal().to_string(), expected);
            Ok(())
        });
    }

    #[test]
    #[traced_test]
    fn prop_test_parse_kraken_amount_five_digits() {
        arbtest(|u| {
            let arb_asset = u.choose(&["CHF", "ZEUR", "ZJPY", "ZUSD"])?;
            let arb_amount = generate_kraken_amount_string(u, 5)?;
            let amount = KrakenAmount::new(arb_asset, &arb_amount).unwrap();
            let expected = trim_leading_zeros(arb_amount);
            assert_eq!(amount.to_decimal().to_string(), expected);
            Ok(())
        });
    }

    #[test]
    #[traced_test]
    fn prop_test_parse_kraken_amount_four_digits() {
        arbtest(|u| {
            let arb_asset = u.choose(&["CHF", "ZEUR", "ZJPY", "ZUSD"])?;
            let arb_amount = generate_kraken_amount_string(u, 4)?;
            let amount = KrakenAmount::new(arb_asset, &arb_amount).unwrap();
            let expected = trim_leading_zeros(arb_amount);
            assert_eq!(amount.to_decimal().to_string(), expected);
            Ok(())
        });
    }

    #[test]
    #[traced_test]
    fn prop_test_parse_fiat_amount() {
        arbtest(|u| {
            let arb_amount = generate_kraken_amount_string(u, 4)?;
            let amount = arb_amount.parse::<FiatAmount>().unwrap();
            let amount_str = amount.0.to_string();
            let digits = amount_str.split_once('.').unwrap().1;
            assert!(!digits.is_empty() && digits.len() <= 4);
            Ok(())
        });
    }
}
