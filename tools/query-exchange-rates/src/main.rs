#![forbid(unsafe_code)]

use chrono::{DateTime, Utc};
use error_iter::ErrorIter as _;
use onlyargs::{CliError, OnlyArgs as _};
use onlyargs_derive::OnlyArgs;
use std::{path::PathBuf, process::ExitCode};
use taxcount::{basis::AssetName, model::ExchangeRates};
use thiserror::Error;

#[derive(Debug, Error)]
enum Error {
    #[error("CLI error")]
    Cli(#[from] CliError),

    #[error("DateTime parsing error")]
    DateTime(#[from] chrono::ParseError),

    #[error("Asset error")]
    Asset(#[from] taxcount::errors::AssetNameError),

    #[error("Exchange Rates DB error")]
    ExchangeRatesDB(#[from] taxcount::errors::ExchangeRatesDbError),

    #[error("Exchange Rate error")]
    ExchangeRate(#[from] taxcount::errors::ExchangeRateError),
}

/// Query the Taxcount exchange rates DB.
#[derive(Debug, OnlyArgs)]
struct Args {
    /// Asset name.
    #[default("BTC")]
    asset: String,

    /// Lookup date.
    date: String,

    /// Path to exchange rates DB.
    #[default("./references/exchange-rates-db/daily-vwap/")]
    exchange_rates_db: PathBuf,
}

fn main() -> ExitCode {
    match run() {
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

fn run() -> Result<(), Error> {
    let args: Args = onlyargs::parse()?;

    let asset: AssetName = args.asset.parse()?;
    let date: DateTime<Utc> = args.date.parse()?;
    let exchange_rates_db = ExchangeRates::new(args.exchange_rates_db)?;
    let amount = exchange_rates_db.get(asset, date)?;

    println!("asset:\t{asset}");
    println!("date:\t{date}");
    println!("amount:\t${amount}");

    Ok(())
}
