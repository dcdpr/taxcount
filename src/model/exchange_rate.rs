use crate::{basis::AssetName, model::kraken_amount::UsdAmount};
use chrono::{DateTime, Utc};
use std::{collections::BTreeMap, ffi::OsStr, fs, path::Path};
use thiserror::Error;
use tracing::warn;

/// Exchange rate lookup failures.
#[cfg_attr(test, derive(Eq, PartialEq))]
#[derive(Debug, Error)]
pub enum ExchangeRateError {
    #[error("DateTime {1} does not exist for {0}")]
    NotFound(AssetName, DateTime<Utc>),
}

#[derive(Debug, Error)]
pub enum ExchangeRatesDbError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("RON parsing error")]
    Parse(#[from] ron::de::SpannedError),

    #[error("DB has an invalid aggregation granularity")]
    InvalidGranularity,
}

pub type ExchangeRateMap = BTreeMap<u64, UsdAmount>;

#[derive(Debug)]
pub struct ExchangeRates {
    /// Timestamp granularity.
    ///
    /// Must be equal to the interval between each timestamp in the keys, minus 1 second.
    granularity: u64,

    btc: ExchangeRateMap,
    chf: ExchangeRateMap,
    eth: ExchangeRateMap,
    ethw: ExchangeRateMap,
    eur: ExchangeRateMap,
    jpy: ExchangeRateMap,
    usdc: ExchangeRateMap,
    usdt: ExchangeRateMap,
}

impl ExchangeRates {
    /// Create an ExchangeRates DB from the given directory path.
    pub fn new<P>(path: P) -> Result<Self, ExchangeRatesDbError>
    where
        P: AsRef<Path>,
    {
        let mut db = Self {
            granularity: 0,
            btc: ExchangeRateMap::default(),
            chf: ExchangeRateMap::default(),
            eth: ExchangeRateMap::default(),
            ethw: ExchangeRateMap::default(),
            eur: ExchangeRateMap::default(),
            jpy: ExchangeRateMap::default(),
            usdc: ExchangeRateMap::default(),
            usdt: ExchangeRateMap::default(),
        };

        for entry in fs::read_dir(path.as_ref())? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension() == Some(OsStr::new("ron")) {
                let name = match path.file_stem().and_then(OsStr::to_str) {
                    Some(name) => name,
                    None => continue,
                };

                if name.ends_with("-btcusd") {
                    db.btc.extend(read_ron(&path)?);
                } else if name.ends_with("-chfusd") {
                    db.chf.extend(read_ron(&path)?);
                } else if name.ends_with("-ethusd") {
                    db.eth.extend(read_ron(&path)?);
                } else if name.ends_with("-ethwusd") {
                    db.ethw.extend(read_ron(&path)?);
                } else if name.ends_with("-eurusd") {
                    db.eur.extend(read_ron(&path)?);
                } else if name.ends_with("-jpyusd") {
                    db.jpy.extend(read_ron(&path)?);
                } else if name.ends_with("-usdcusd") {
                    db.usdc.extend(read_ron(&path)?);
                } else if name.ends_with("-usdtusd") {
                    db.usdt.extend(read_ron(&path)?);
                }
            }
        }

        if db.btc.is_empty() {
            warn!("Missing BTCUSD exchange rates");
        }
        if db.chf.is_empty() {
            warn!("Missing CHFUSD exchange rates");
        }
        if db.eth.is_empty() {
            warn!("Missing ETHUSD exchange rates");
        }
        if db.ethw.is_empty() {
            warn!("Missing ETHWUSD exchange rates");
        }
        if db.eur.is_empty() {
            warn!("Missing EURUSD exchange rates");
        }
        if db.jpy.is_empty() {
            warn!("Missing JPYUSD exchange rates");
        }
        if db.usdc.is_empty() {
            warn!("Missing USDCUSD exchange rates");
        }
        if db.usdt.is_empty() {
            warn!("Missing USDTUSD exchange rates");
        }

        db.granularity = check_granularity(&db.btc, db.granularity)?;
        db.granularity = check_granularity(&db.chf, db.granularity)?;
        db.granularity = check_granularity(&db.eth, db.granularity)?;
        db.granularity = check_granularity(&db.ethw, db.granularity)?;
        db.granularity = check_granularity(&db.eur, db.granularity)?;
        db.granularity = check_granularity(&db.jpy, db.granularity)?;
        db.granularity = check_granularity(&db.usdc, db.granularity)?;
        db.granularity = check_granularity(&db.usdt, db.granularity)?;

        if db.granularity == 0 {
            Err(ExchangeRatesDbError::InvalidGranularity)
        } else {
            // Patch the detected granularity to make lower-bound searches exclusive
            db.granularity -= 1;

            Ok(db)
        }
    }

    pub fn get(
        &self,
        asset: AssetName,
        datetime: DateTime<Utc>,
    ) -> Result<UsdAmount, ExchangeRateError> {
        let end = datetime.timestamp() as u64;
        let start = end - self.granularity;
        let map = match asset {
            AssetName::Usd => unreachable!(),
            AssetName::Btc => &self.btc,
            AssetName::Chf => &self.chf,
            AssetName::Eth => &self.eth,
            AssetName::EthW => &self.ethw,
            AssetName::Eur => &self.eur,
            AssetName::Jpy => &self.jpy,
            AssetName::Usdc => &self.usdc,
            AssetName::Usdt => &self.usdt,
        };

        map.range(start..=end)
            .next_back()
            .map(|(_k, v)| *v)
            .ok_or(ExchangeRateError::NotFound(asset, datetime))
    }
}

fn read_ron(path: &Path) -> Result<ExchangeRateMap, ExchangeRatesDbError> {
    let data = fs::read_to_string(path)?;
    let rates = ron::from_str::<ExchangeRateMap>(&data)?;

    Ok(rates)
}

fn check_granularity(map: &ExchangeRateMap, granularity: u64) -> Result<u64, ExchangeRatesDbError> {
    map.keys()
        .try_fold((0, granularity), |(acc, granularity), timestamp| {
            if acc == 0 || timestamp - acc == granularity {
                Some((*timestamp, granularity))
            } else if granularity == 0 {
                Some((*timestamp, timestamp - acc))
            } else {
                None
            }
        })
        .map(|(_, granularity)| granularity)
        .ok_or(ExchangeRatesDbError::InvalidGranularity)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
impl ExchangeRates {
    pub(crate) fn from_raw(
        granularity: u64,
        btc: ExchangeRateMap,
        chf: ExchangeRateMap,
        eth: ExchangeRateMap,
        ethw: ExchangeRateMap,
        eur: ExchangeRateMap,
        jpy: ExchangeRateMap,
        usdc: ExchangeRateMap,
        usdt: ExchangeRateMap,
    ) -> Self {
        Self {
            granularity,
            btc,
            chf,
            eth,
            ethw,
            eur,
            jpy,
            usdc,
            usdt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{constants::DEFAULT_PATH_EXCHANGE_RATES_DB, kraken_amount::FiatAmount};

    const ONE_DAY: u64 = 60 * 60 * 24 - 1;

    #[test]
    fn test_exchange_rates() {
        let mut exchange_rates = ExchangeRates {
            granularity: ONE_DAY,
            btc: BTreeMap::new(),
            chf: BTreeMap::new(),
            eth: BTreeMap::new(),
            ethw: BTreeMap::new(),
            eur: BTreeMap::new(),
            jpy: BTreeMap::new(),
            usdc: BTreeMap::new(),
            usdt: BTreeMap::new(),
        };

        // Small sample of 2022 BTC daily VWAP
        let jan_01_vwap = UsdAmount::from("47034.96810552307".parse::<FiatAmount>().unwrap());
        let jan_02_vwap = UsdAmount::from("47196.52015139371".parse::<FiatAmount>().unwrap());
        let jan_03_vwap = UsdAmount::from("46645.508568351295".parse::<FiatAmount>().unwrap());
        exchange_rates.btc.insert(1640995200, jan_01_vwap); // 2022-01-01
        exchange_rates.btc.insert(1641081600, jan_02_vwap); // 2022-01-02
        exchange_rates.btc.insert(1641168000, jan_03_vwap); // 2022-02-03

        // Sample some pseudorandom times
        let datetime = "2022-01-01 13:42:00+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_01_vwap));

        let datetime = "2022-01-02 02:27:57+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_02_vwap));

        let datetime = "2022-01-03 21:51:03+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_03_vwap));

        // Bounds checks; in bounds
        let datetime = "2022-01-01 00:00:00+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_01_vwap));

        let datetime = "2022-01-01 23:59:59+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_01_vwap));

        let datetime = "2022-01-02 00:00:00+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_02_vwap));

        let datetime = "2022-01-02 23:59:59+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_02_vwap));

        let datetime = "2022-01-03 00:00:00+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_03_vwap));

        let datetime = "2022-01-03 23:59:59+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        assert_eq!(actual, Ok(jan_03_vwap));

        // Bounds checks; out of bounds
        let datetime = "2021-12-31 23:59:59+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        let expected = Err(ExchangeRateError::NotFound(AssetName::Btc, datetime));
        assert_eq!(actual, expected);

        let datetime = "2022-01-04 00:00:00+0000".parse().unwrap();
        let actual = exchange_rates.get(AssetName::Btc, datetime);
        let expected = Err(ExchangeRateError::NotFound(AssetName::Btc, datetime));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_granularity() {
        let exchange_rates = ExchangeRates::new(DEFAULT_PATH_EXCHANGE_RATES_DB).unwrap();
        assert_eq!(exchange_rates.granularity, ONE_DAY);
    }
}
