use crate::{consts::*, fetch, Buckets};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, ffi::OsStr, io::Write as _, path::PathBuf, time::Duration};
use taxcount::model::{ExchangeRateMap, FiatAmount, UsdAmount};
use thiserror::Error;
use tokio::fs;

#[derive(Debug, Error)]
pub enum ModeError {
    #[error("Floating point parsing error")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("JSON error")]
    Json(#[from] serde_json::Error),

    #[error("RON error")]
    Ron(#[from] ron::Error),

    #[error("Fetch error")]
    Fetch(#[from] fetch::FetchError),
}

trait OhlcvMapAccExt {
    /// Calculate the VWAP.
    fn calc(self) -> OhlcvMap;
}

type OhlcvMap = BTreeMap<u64, Ohlcv>;
type OhlcvMapAcc = BTreeMap<u64, OhlcvAcc>;

#[derive(Debug, Default, Deserialize, Serialize)]
struct Ohlcv {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    vwap: f64,
    volume: f64,
}

#[derive(Debug, Default)]
struct OhlcvAcc {
    ohlcv: Ohlcv,
    open_time: u64,
    close_time: u64,
    weighted_price: f64,
}

impl OhlcvMapAccExt for OhlcvMapAcc {
    fn calc(self) -> OhlcvMap {
        self.into_iter()
            .map(|(k, v)| {
                let mut ohlcv = v.ohlcv;
                ohlcv.vwap = v.weighted_price / ohlcv.volume;

                (k, ohlcv)
            })
            .collect()
    }
}

pub async fn get_ohlc_kraken(
    dir: PathBuf,
    buckets: Buckets,
    pair: String,
    since: u64,
) -> Result<(), ModeError> {
    let data = fetch::ohlc_kraken(&pair, since, buckets).await?;
    let path = dir.join(format!("ohlc_{since}.json"));

    fs::write(&path, serde_json::to_string(&data)?).await?;

    println!("OHLC saved as `{}`", path.display());

    Ok(())
}

pub async fn get_ohlc_bitstamp(
    dir: PathBuf,
    buckets: Buckets,
    pair: String,
    mut start: u64,
    end: u64,
) -> Result<(), ModeError> {
    let step = match buckets {
        Buckets::Daily => ONE_DAY * 1000,
        Buckets::Hourly => ONE_HOUR * 1000,
    };

    while start < end {
        let data = fetch::ohlc_bitstamp(&pair, start, buckets).await?;
        let path = dir.join(format!("ohlc_{start}.json"));

        fs::write(&path, serde_json::to_string(&data)?).await?;

        println!("OHLC saved as `{}`", path.display());

        start += step;

        // The Bitstamp REST API will rate limit our requests. Try to avoid hitting the limit.
        // See: https://www.bitstamp.net/api/#section/Request-limits
        // In practice, waiting 1 second after each request seems to work fine...
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

pub async fn get_trades(
    dir: PathBuf,
    pair: String,
    mut since: u64,
    end: u64,
) -> Result<(), ModeError> {
    let mut count = 0;

    loop {
        let (data, last) = fetch::trades(&pair, since).await?;
        if last == since {
            break;
        }

        print!(".");
        std::io::stdout().lock().flush()?;

        let path = dir.join(format!("trades_{since}.json"));
        fs::write(path, serde_json::to_string(&data)?).await?;

        if last >= end || data.len() < 1000 {
            break;
        }
        since = last + 1;
        count += 1;

        // The Kraken REST API will rate limit our requests. Try to avoid hitting the limit.
        // See: https://docs.kraken.com/rest/#section/Rate-Limits/REST-API-Rate-Limits
        // In practice, waiting 1 second after each request seems to work fine...
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    println!(
        "\n{count} trade file{} saved",
        if count == 1 { "" } else { "s" },
    );

    Ok(())
}

pub async fn get_fiat(dir: PathBuf, pair: String, since: u64, end: u64) -> Result<(), ModeError> {
    let data = fetch::fiat(&pair, since, end).await?;

    // Write the `data` as a RON file.
    let path = dir.join(format!("{pair}.ron"));
    let config = ron::ser::PrettyConfig::default().struct_names(true);
    let vwap: ExchangeRateMap = data
        .into_iter()
        .map(|row| {
            // TODO: What precision do we want to store in the DB?
            let amount: FiatAmount = format!("{:.6}", row.rate).parse().unwrap();

            (row.date.timestamp() as u64, UsdAmount::from(amount))
        })
        .collect();
    fs::write(&path, ron::ser::to_string_pretty(&vwap, config)?).await?;

    println!("DB saved as `{}`", path.display());

    Ok(())
}

pub async fn taxcount(dir: PathBuf, since: u64, end: u64) -> Result<(), ModeError> {
    println!("Converting files in `{}` to taxcount DB...", dir.display());
    let since = crate::normalize_timestamp(since);
    let end = crate::normalize_timestamp(end);
    println!("since={since}, end={end}...");

    convert(dir, Buckets::Daily, since, end, false).await
}

pub async fn ohlcv(dir: PathBuf, buckets: Buckets, since: u64, end: u64) -> Result<(), ModeError> {
    println!("Converting files in `{}` to OHLCV...", dir.display());
    let since = crate::normalize_timestamp(since);
    let end = crate::normalize_timestamp(end);
    println!("since={since}, end={end}...");

    convert(dir, buckets, since, end, true).await
}

async fn convert(
    dir: PathBuf,
    buckets: Buckets,
    since: u64,
    end: u64,
    full: bool,
) -> Result<(), ModeError> {
    let mut db = OhlcvMap::new();
    let mut acc = OhlcvMapAcc::new();
    let mut entries = fs::read_dir(&dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if entry.file_type().await?.is_dir() || path.extension() != Some(OsStr::new("json")) {
            continue;
        }

        let data = fs::read(&path).await?;
        if let Ok(ohlcs) = serde_json::from_slice::<Vec<fetch::Ohlc>>(&data) {
            for ohlc in ohlcs {
                let time = match buckets {
                    Buckets::Daily => ohlc.time / ONE_DAY * ONE_DAY,
                    Buckets::Hourly => ohlc.time / ONE_HOUR * ONE_HOUR,
                };

                // Filter samples that are out of range.
                if time < since || time >= end {
                    continue;
                }

                let ohlcv = Ohlcv {
                    open: ohlc.open.parse()?,
                    high: ohlc.high.parse()?,
                    low: ohlc.low.parse()?,
                    close: ohlc.close.parse()?,
                    vwap: ohlc.vwap.parse()?,
                    volume: ohlc.volume.parse()?,
                };

                // Prevent NaN
                let volume = if ohlcv.volume == 0.0 {
                    1.0
                } else {
                    ohlcv.volume
                };

                let weighted_price = if ohlcv.vwap == 0.0 {
                    // Estimate price from the high-low-close average.
                    (ohlcv.high + ohlcv.low + ohlcv.close) / 3.0
                } else {
                    ohlcv.vwap
                } * volume;

                // Accumulate weighted price and volume
                acc.entry(time)
                    .and_modify(|v| {
                        if ohlc.time < v.open_time {
                            v.open_time = ohlc.time;
                            v.ohlcv.open = ohlcv.open;
                        }
                        if ohlc.time > v.close_time {
                            v.close_time = ohlc.time;
                            v.ohlcv.close = ohlcv.close;
                        }
                        v.ohlcv.high = v.ohlcv.high.max(ohlcv.high);
                        v.ohlcv.low = v.ohlcv.low.min(ohlcv.low);
                        v.ohlcv.volume += volume;
                        v.weighted_price += weighted_price;
                    })
                    .or_insert(OhlcvAcc {
                        ohlcv,
                        open_time: time,
                        close_time: time,
                        weighted_price,
                    });
            }
        } else if let Ok(trades) = serde_json::from_slice::<Vec<fetch::Trade>>(&data) {
            for trade in trades {
                let time = trade.time as u64;
                let time = match buckets {
                    Buckets::Daily => time / ONE_DAY * ONE_DAY,
                    Buckets::Hourly => time / ONE_HOUR * ONE_HOUR,
                };

                // Filter samples that are out of range.
                if time < since || time >= end {
                    continue;
                }

                let volume = trade.volume.parse()?;
                let price = trade.price.parse()?;
                let weighted_price = price * volume;

                // Accumulate weighted price and volume
                acc.entry(time)
                    .and_modify(|v| {
                        let time = trade.time as u64;
                        if time < v.open_time {
                            v.open_time = time;
                            v.ohlcv.open = price;
                        }
                        if time > v.close_time {
                            v.close_time = time;
                            v.ohlcv.close = price;
                        }
                        v.ohlcv.high = v.ohlcv.high.max(price);
                        v.ohlcv.low = v.ohlcv.low.min(price);
                        v.ohlcv.volume += volume;
                        v.weighted_price += weighted_price;
                    })
                    .or_insert(OhlcvAcc {
                        ohlcv: Ohlcv {
                            open: price,
                            high: price,
                            low: price,
                            close: price,
                            vwap: 0.0,
                            volume,
                        },
                        open_time: time,
                        close_time: time,
                        weighted_price,
                    });
            }
        } else {
            println!("Unknown file type: `{}`", path.display());
        }
    }

    // Extend DB with the VWAP accumulator, ensuring any keys in DB take priority.
    let mut acc = acc.calc();
    acc.extend(db);
    db = acc;

    // Write the `db` as a RON file.
    let path = dir.join("db.ron");
    let config = ron::ser::PrettyConfig::default().struct_names(true);
    if full {
        fs::write(&path, ron::ser::to_string_pretty(&db, config)?).await?;
    } else {
        let vwap: ExchangeRateMap = db
            .into_iter()
            .map(|(k, v)| {
                // TODO: What precision do we want to store in the DB?
                let amount: FiatAmount = format!("{:.12}", v.vwap).parse().unwrap();

                (k, UsdAmount::from(amount))
            })
            .collect();
        fs::write(&path, ron::ser::to_string_pretty(&vwap, config)?).await?;
    }

    println!("DB saved as `{}`", path.display());

    Ok(())
}
