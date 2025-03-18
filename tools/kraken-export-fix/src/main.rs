//! # kraken-export-fix
//!
//! Fix running balances in edited Kraken ledger exports.

#![forbid(unsafe_code)]

use error_iter::ErrorIter as _;
use onlyargs::{CliError, OnlyArgs as _};
use onlyargs_derive::OnlyArgs;
use std::{path::PathBuf, process::ExitCode};
use taxcount::imports::kraken::{read_ledgers, write_ledgers};
use taxcount::model::{exchange::Balances, ledgers::rows::LedgerRow, Stats};
use taxcount::util::fifo::FIFO;
use thiserror::Error;

#[derive(Debug, Error)]
enum Error {
    #[error("CLI error")]
    Cli(#[from] CliError),

    #[error("Filename error")]
    FileName,

    #[error("CSV import error")]
    Kraken(#[from] taxcount::errors::KrakenError),
}

/// Fix running balances in edited Kraken ledger exports.
#[derive(Debug, OnlyArgs)]
struct Args {
    /// Path to Kraken ledgers.
    ///   May be specified multiple times for many CSVs
    input_ledgers: Vec<PathBuf>,

    /// Directory path to output rewritten CSV files.
    output_directory: PathBuf,
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

    let mut stats = Stats::default();

    for path in args.input_ledgers {
        let out_path = args
            .output_directory
            .join(path.file_name().ok_or(Error::FileName)?);

        let mut balances = Balances::default();
        let mut ledgers = read_ledgers(&mut stats, &path)?;
        rebalance(&mut balances, &mut ledgers);

        println!("Writing {}...", out_path.display());
        write_ledgers(&out_path, &ledgers).unwrap();
    }

    println!("Done!");

    Ok(())
}

fn rebalance(balances: &mut Balances, rows: &mut FIFO<LedgerRow>) {
    for row in rows.iter_mut() {
        match row {
            LedgerRow::DepositFulfilled(lrd) | LedgerRow::TransferFutures(lrd) => {
                balances.rebalance(lrd.amount, lrd.fee, &mut lrd.balance)
            }
            LedgerRow::WithdrawalFulfilled(lrt)
            | LedgerRow::Trade(lrt)
            | LedgerRow::Margin(lrt)
            | LedgerRow::Rollover(lrt)
            | LedgerRow::SettlePosition(lrt) => {
                balances.rebalance(lrt.amount, lrt.fee, &mut lrt.balance)
            }
            _ => (),
        }
    }
}
