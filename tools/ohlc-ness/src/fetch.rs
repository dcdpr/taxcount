use crate::{consts::*, normalize_timestamp, Buckets};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("Floating point parsing error")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("Date parsing error")]
    ParseDate(#[from] chrono::ParseError),

    #[error("Timestamp conversion error")]
    Timestamp(#[from] std::num::TryFromIntError),

    #[error("API error: {0}")]
    Api(String),

    #[error("CSV parsing error")]
    Csv(#[from] csv::Error),

    #[error("HTTP error")]
    Http(#[from] reqwest::Error),

    #[error("Initial row is missing a rate")]
    MissingRate,

    #[error("Initial row is more recent than requested start date")]
    MissingDate,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Ohlc {
    #[serde(alias = "timestamp")]
    #[serde(deserialize_with = "string_or_u64")]
    pub(crate) time: u64,
    pub(crate) open: String,
    pub(crate) high: String,
    pub(crate) low: String,
    pub(crate) close: String,
    #[serde(default = "default_vwap")]
    pub(crate) vwap: String,
    pub(crate) volume: String,
    #[serde(default = "default_count")]
    pub(crate) count: u64,
}

fn default_vwap() -> String {
    "0.0".to_string()
}

fn default_count() -> u64 {
    1
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Trade {
    pub(crate) price: String,
    pub(crate) volume: String,
    pub(crate) time: f64,
    pub(crate) buy_sell: String,
    pub(crate) market_limit: String,
    pub(crate) misc: String,
    pub(crate) trade_id: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
struct FiatExchangeRateCsv {
    #[serde(alias = "observation_date")]
    date: String,
    #[serde(alias = "DEXJPUS")]
    #[serde(alias = "DEXSZUS")]
    #[serde(alias = "DEXUSEU")]
    rate: String,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct FiatExchangeRate {
    pub(crate) date: DateTime<Utc>,
    pub(crate) rate: f64,
}

pub(crate) async fn ohlc_kraken(
    pair: &str,
    since: u64,
    buckets: Buckets,
) -> Result<Vec<Ohlc>, FetchError> {
    let interval = match buckets {
        Buckets::Daily => 1440,
        Buckets::Hourly => 60,
    };
    let url = format!("{KRAKEN_ENDPOINT}/OHLC?pair={pair}&since={since}&interval={interval}");
    let mut json: serde_json::Value = reqwest::get(url).await?.json().await?;

    handle_errors(&json["error"])?;

    Ok(serde_json::from_value(json["result"][pair].take()).unwrap())
}

pub(crate) async fn ohlc_bitstamp(
    pair: &str,
    start: u64,
    buckets: Buckets,
) -> Result<Vec<Ohlc>, FetchError> {
    let step = match buckets {
        Buckets::Daily => ONE_DAY,
        Buckets::Hourly => ONE_HOUR,
    };
    let url = format!(
        "{BITSTAMP_ENDPOINT}/ohlc/{pair}?start={start}&step={step}&limit=1000&\
exclude_current_candle=true"
    );
    let mut json: serde_json::Value = reqwest::get(url).await?.json().await?;

    // TODO: What errors need to be handled for Bitstamp?
    // handle_errors(&json["error"])?;

    Ok(serde_json::from_value(json["data"]["ohlc"].take()).unwrap())
}

pub(crate) async fn trades(pair: &str, since: u64) -> Result<(Vec<Trade>, u64), FetchError> {
    let url = format!("{KRAKEN_ENDPOINT}/Trades?pair={pair}&since={since}");
    let mut json: serde_json::Value = reqwest::get(url).await?.json().await?;

    handle_errors(&json["error"])?;

    let data = serde_json::from_value(json["result"][pair].take()).unwrap();
    let last = json["result"]["last"].as_str().unwrap().parse().unwrap();

    Ok((data, last))
}

pub(crate) async fn fiat(
    pair: &str,
    since: u64,
    end: u64,
) -> Result<Vec<FiatExchangeRate>, FetchError> {
    // The given rate needs to be inverted when the quote currency is USD.
    // Taxcount expects the base currency to be USD.
    let (pair, inverse) = match pair.to_uppercase().as_str() {
        "CHFUSD" | "DEXSZUS" => ("DEXSZUS", true),
        "JPYUSD" | "DEXJPUS" => ("DEXJPUS", true),
        "EURUSD" | "DEXUSEU" => ("DEXUSEU", false),
        _ => return Err(FetchError::Api(format!("Unknown trade pair: `{pair}`"))),
    };
    let mut since = normalize_timestamp(since);
    let end = normalize_timestamp(end);

    fn timestamp_to_datetime(ts: u64) -> Result<DateTime<Utc>, FetchError> {
        let datetime = DateTime::from_timestamp(i64::try_from(ts)?, 0)
            .ok_or_else(|| FetchError::Api("Invalid timestamp".to_string()))?;
        Ok(datetime)
    }
    fn timestamp_to_string(ts: u64) -> Result<String, FetchError> {
        Ok(timestamp_to_datetime(ts)?.format("%F").to_string())
    }

    let start_date = timestamp_to_string(since)?;
    let end_date = timestamp_to_string(end)?;
    let url = format!("{FRED_ENDPOINT}?id={pair}&cosd={start_date}&coed={end_date}");
    let csv = reqwest::get(url).await?.text().await?;
    let mut reader = csv::Reader::from_reader(csv.as_bytes());

    let mut data = BTreeMap::new();
    for record in reader.deserialize() {
        let record: FiatExchangeRateCsv = record?;

        let date = NaiveDateTime::new(
            NaiveDate::parse_from_str(&record.date, "%F")?,
            NaiveTime::default(),
        )
        .and_utc();

        if let Ok(rate) = record.rate.parse() {
            let rate = if inverse { 1.0 / rate } else { rate };
            data.insert(date, rate);
        }
    }

    // Fill in missing dates.
    while since < end {
        let date = timestamp_to_datetime(since)?;
        if !data.contains_key(&date) {
            let prev = data.range(..date).next_back();
            let next = data.range(date..).next();
            match (prev, next) {
                (Some((&prev_date, &prev_rate)), Some((&next_date, &next_rate))) => {
                    // Use the rate from the closest day.
                    if date - prev_date <= next_date - date {
                        data.insert(date, prev_rate);
                    } else {
                        data.insert(date, next_rate);
                    }
                }
                (Some((_, &rate)), None) | (None, Some((_, &rate))) => {
                    data.insert(date, rate);
                }
                (None, None) => unreachable!(),
            }
        }
        since += ONE_DAY;
    }

    Ok(data
        .into_iter()
        .map(|(date, rate)| FiatExchangeRate { date, rate })
        .collect())
}

fn handle_errors(errors: &serde_json::Value) -> Result<(), FetchError> {
    if let Some(err) = errors.as_array() {
        if !err.is_empty() {
            let msg = err
                .iter()
                .map(|v| v.as_str().unwrap_or("Unknown error"))
                .collect::<Vec<_>>()
                .join(", ");

            return Err(FetchError::Api(msg));
        }
    }

    Ok(())
}

fn string_or_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StringOrU64;

    impl serde::de::Visitor<'_> for StringOrU64 {
        type Value = u64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("string or u64")
        }

        fn visit_str<E>(self, value: &str) -> Result<u64, E>
        where
            E: serde::de::Error,
        {
            Ok(value.parse().unwrap())
        }

        fn visit_u64<E>(self, value: u64) -> Result<u64, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }
    }

    deserializer.deserialize_any(StringOrU64)
}
