#![forbid(unsafe_code)]

use error_iter::ErrorIter as _;
use ohlc_ness::{modes, Buckets};
use onlyargs::{CliError, OnlyArgs as _};
use onlyargs_derive::OnlyArgs;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{path::PathBuf, process::ExitCode};
use thiserror::Error;

#[derive(Debug, Error)]
enum Error {
    #[error("Invalid tool mode. See `--help`")]
    Mode,

    #[error("Invalid OHLC API. See `--help`")]
    Api,

    #[error("Bucket error")]
    Bucket(#[from] ohlc_ness::BucketError),

    #[error("CLI error")]
    Cli(#[from] CliError),

    #[error("Application error")]
    App(#[from] ohlc_ness::modes::ModeError),
}

/// Collect BTC prices from Kraken and produce a DB of daily VWAPs.
#[derive(Debug, OnlyArgs)]
struct Args {
    /// Work directory for all intermediate and result files. [default: `.`]
    dir: Option<PathBuf>,

    /// Run the tool in the given mode.
    ///
    /// Available modes:
    /// - "get_ohlc" [default]
    /// - "get_trades"
    /// - "get_fiat"
    /// - "taxcount"
    /// - "ohlcv"
    mode: Option<String>,

    /// Run the tool in the given mode.
    ///
    /// Available APIs:
    /// - "kraken" [default]
    /// - "bitstamp"
    api: Option<String>,

    /// Bucket granularity for `get_ohlc` and `ohlcv` modes.
    ///
    /// Bucket sizes:
    /// - "daily" [default]
    /// - "hourly"
    buckets: Option<String>,

    /// Trading pair.
    ///
    /// Defaults:
    /// - Kraken: "XXBTZUSD"
    /// - Bitstamp: "btcusd"
    pair: Option<String>,

    /// Timestamp of starting point for siphoning OHLC or Trades.
    /// Seconds or nanoseconds since Unix epoch.
    since: u64,

    /// Timestamp of stopping point for siphoning OHLC or Trades.
    /// Seconds or nanoseconds since Unix epoch. [default: now]
    end: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    GetOhlc,
    GetTrades,
    GetFiat,
    Taxcount,
    Ohlcv,
}

#[derive(Clone, Copy, Debug)]
enum Api {
    Kraken,
    Bitstamp,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            if matches!(err, Error::Cli(_)) {
                eprintln!("{}", Args::HELP);
            }

            eprintln!("Error: {err}");
            for source in err.sources().skip(1) {
                eprintln!("  Caused by: {source}");
            }

            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Error> {
    let args: Args = onlyargs::parse()?;

    let mode = Mode::try_from(args.mode.unwrap_or_else(|| "get_ohlc".to_string()))?;
    let api = Api::try_from(args.api.unwrap_or_else(|| "kraken".to_string()))?;
    let buckets = Buckets::try_from(args.buckets.unwrap_or_else(|| "daily".to_string()))?;
    let dir = args.dir.unwrap_or_else(|| PathBuf::from("."));
    let pair = args.pair.unwrap_or_else(|| {
        match api {
            Api::Kraken => "XXBTZUSD",
            Api::Bitstamp => "btcusd",
        }
        // TODO: Use a real URI encoder and directory traversal filter.
        .replace('#', "%23")
        .replace('%', "%25")
        .replace('/', "%2F")
        .replace('?', "%3F")
        .replace("..", "")
    });
    let since = ohlc_ness::normalize_timestamp_ns(args.since);
    let end = ohlc_ness::normalize_timestamp_ns(args.end.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }));

    if matches!(mode, Mode::GetOhlc | Mode::GetTrades) {
        println!("Calling {api:?} API {mode:?} with pair={pair}, since={since}, end={end}...");
        println!("Work directory: `{}`", dir.display());
    }

    match (mode, api) {
        (Mode::GetOhlc, Api::Kraken) => modes::get_ohlc_kraken(dir, buckets, pair, since).await?,
        (Mode::GetOhlc, Api::Bitstamp) => {
            let start = ohlc_ness::normalize_timestamp(since);
            let end = ohlc_ness::normalize_timestamp(end);
            modes::get_ohlc_bitstamp(dir, buckets, pair, start, end).await?
        }
        (Mode::GetTrades, _) => modes::get_trades(dir, pair, since, end).await?,
        (Mode::GetFiat, _) => modes::get_fiat(dir, pair, since, end).await?,
        (Mode::Taxcount, _) => modes::taxcount(dir, since, end).await?,
        (Mode::Ohlcv, _) => modes::ohlcv(dir, buckets, since, end).await?,
    }

    println!();

    Ok(())
}

impl TryFrom<String> for Mode {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "get_ohlc" => Ok(Self::GetOhlc),
            "get_trades" => Ok(Self::GetTrades),
            "get_fiat" => Ok(Self::GetFiat),
            "taxcount" => Ok(Self::Taxcount),
            "ohlcv" => Ok(Self::Ohlcv),
            _ => Err(Error::Mode),
        }
    }
}

impl TryFrom<String> for Api {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "kraken" => Ok(Self::Kraken),
            "bitstamp" => Ok(Self::Bitstamp),
            _ => Err(Error::Api),
        }
    }
}
