#![forbid(unsafe_code)]

use chrono::{DateTime, Utc};
use error_iter::ErrorIter as _;
use onlyargs::{CliError, OnlyArgs as _};
use onlyargs_derive::OnlyArgs;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::{env, process::ExitCode, str::FromStr};
use taxcount::basis::{Asset, AssetName, PoolAsset};
use taxcount::model::{ledgers::rows::BasisRow, KrakenAmount, State};
use taxcount::util::fifo::FIFO;
use thiserror::Error;

#[derive(Debug, Error)]
enum Error {
    #[error("CLI error")]
    Cli(#[from] CliError),

    #[error("CSV parsing error")]
    Csv(#[from] csv::Error),

    #[error("DateTime parsing error")]
    DateTime(#[from] chrono::ParseError),

    #[error("Invalid balance_type: {0}")]
    BalanceType(String),

    #[error("Asset error")]
    Asset(#[from] taxcount::errors::AssetNameError),

    #[error("Convert amount error")]
    ConvertAmount(#[from] taxcount::errors::ConvertAmountError),

    #[error("Checkpoint save error")]
    Checkpoint(#[from] taxcount::errors::CheckpointError),

    #[error("Bitcoin network parsing error")]
    BitcoinNetwork(#[from] taxcount::bdk::bitcoin::network::constants::ParseNetworkError),
}

/// Bootstrap taxcount by creating a new checkpoint file.
#[derive(Debug, OnlyArgs)]
#[footer = "Additional environment variables:"]
#[footer = "  - BITCOIN_NETWORK accepts {bitcoin (default), testnet, signet, regtest}"]
#[footer = "      https://docs.rs/bitcoin/latest/bitcoin/network/enum.Network.html"]
struct Args {
    /// Path to bootstrap CSV.
    input_bootstrap: PathBuf,

    /// Path for checkpoint file to create.
    output_checkpoint: PathBuf,
}

#[derive(Debug, Deserialize)]
struct BootstrapCsvRow {
    balance_type: String,
    asset: String,
    aquisition_date: String,
    id: String,
    amount_at_aquisition: String,
    split_amount: String,
    exchange_rate_at_aquisition: String,
}

#[derive(Copy, Clone, Debug)]
enum BalanceType {
    OnChainBalances,
    ExchangeBalances,
    PendingDeposits,
    PendingWithdrawals,
    BorrowerCollateral,
    LenderCapital,
}

impl FromStr for BalanceType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "on_chain_balances" => Ok(Self::OnChainBalances),
            "exchange_balances" => Ok(Self::ExchangeBalances),
            "pending_deposits" => Ok(Self::PendingDeposits),
            "pending_withdrawals" => Ok(Self::PendingWithdrawals),
            "borrower_collateral" => Ok(Self::BorrowerCollateral),
            "lender_capital" => Ok(Self::LenderCapital),
            _ => Err(Error::BalanceType(s.to_string())),
        }
    }
}

macro_rules! insert_asset {
    ($balance:expr, $row:expr) => {{
        let mut fifo = create_fifo($row)?;

        $balance
            .entry(&$row.id)
            .and_modify(|entry| {
                entry.extend(fifo.drain(..));
            })
            .or_insert_with(|| fifo.drain(..).collect());
    }};
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

    let network = env::var("BITCOIN_NETWORK")
        .unwrap_or_else(|_| "bitcoin".to_string())
        .parse()?;

    let mut state = State::new(network);
    for row in read_csv(args.input_bootstrap)? {
        let balance_type = row.balance_type.parse()?;
        let asset = row.asset.parse()?;

        match (balance_type, asset) {
            // On-chain-balances represent the state of our blockchain assets at bootstrap-time.
            // For BTC, this is all of our UTXOs. For account-based blockchains, this is the value
            // on each of our accounts.
            //
            // Regardless of UTXO or account, each one has a FIFO of split assets, each of which may
            // have a unique cost basis.
            (BalanceType::OnChainBalances, AssetName::Btc) => {
                insert_asset!(state.on_chain_balances.btc, &row)
            }
            (BalanceType::OnChainBalances, AssetName::Eth) => {
                insert_asset!(state.on_chain_balances.eth, &row)
            }
            (BalanceType::OnChainBalances, AssetName::EthW) => {
                insert_asset!(state.on_chain_balances.ethw, &row)
            }
            (BalanceType::OnChainBalances, AssetName::Usdc) => {
                insert_asset!(state.on_chain_balances.usdc, &row)
            }
            (BalanceType::OnChainBalances, AssetName::Usdt) => {
                insert_asset!(state.on_chain_balances.usdt, &row)
            }

            // Lender capital is an asset which is held in the borrower's custody. Instead of a TXID
            // or account ID, it is identified by our own notion of a "loan ID", which can contain
            // arbitrary metadata, like "loan to Bob for pizza".
            //
            // Lenders expect the loan to be repaid in the future, and this state holds the asset's
            // original cost basis (possibly multiple splits) so that the cost basis returns when
            // the loan is repaid.
            (BalanceType::LenderCapital, AssetName::Btc) => {
                insert_asset!(state.lender_capital.btc, &row)
            }
            (BalanceType::LenderCapital, AssetName::Eth) => {
                insert_asset!(state.lender_capital.eth, &row)
            }
            (BalanceType::LenderCapital, AssetName::EthW) => {
                insert_asset!(state.lender_capital.ethw, &row)
            }
            (BalanceType::LenderCapital, AssetName::Usdc) => {
                insert_asset!(state.lender_capital.usdc, &row)
            }
            (BalanceType::LenderCapital, AssetName::Usdt) => {
                insert_asset!(state.lender_capital.usdt, &row)
            }

            // Borrower collateral is conceptually the same as lender capital, but from the
            // perspective of the borrower providing collateral to secure the loan. These assets
            // are held in custody by the lender. The "loan ID" may be the same as used for lender
            // capital if the loans are related.
            //
            // This state allows a borrower's collateral to retain its original cost basis when the
            // collateral is returned.
            (BalanceType::BorrowerCollateral, AssetName::Btc) => {
                insert_asset!(state.borrower_collateral.btc, &row)
            }
            (BalanceType::BorrowerCollateral, AssetName::Eth) => {
                insert_asset!(state.borrower_collateral.eth, &row)
            }
            (BalanceType::BorrowerCollateral, AssetName::EthW) => {
                insert_asset!(state.borrower_collateral.ethw, &row)
            }
            (BalanceType::BorrowerCollateral, AssetName::Usdc) => {
                insert_asset!(state.borrower_collateral.usdc, &row)
            }
            (BalanceType::BorrowerCollateral, AssetName::Usdt) => {
                insert_asset!(state.borrower_collateral.usdt, &row)
            }

            _ => todo!("Need to handle {balance_type:?}, {asset:?}"),
        }
    }

    state.save(args.output_checkpoint)?;

    Ok(())
}

fn read_csv<P: AsRef<Path>>(path: P) -> Result<Vec<BootstrapCsvRow>, Error> {
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_path(path)?;
    let rows: Result<Vec<BootstrapCsvRow>, _> = reader.deserialize().collect();

    Ok(rows?)
}

fn create_fifo<A: Asset>(row: &BootstrapCsvRow) -> Result<FIFO<PoolAsset<A>>, Error>
where
    <A as TryFrom<KrakenAmount>>::Error: std::fmt::Debug,
{
    let mut pool_asset = PoolAsset::from_basis_row(&BasisRow {
        synthetic_id: row.id.to_string(),
        time: row.aquisition_date.parse::<DateTime<Utc>>()?,
        asset: row.asset.to_string(),
        amount: Some(KrakenAmount::new(&row.asset, &row.amount_at_aquisition)?),
        exchange_rate: row.exchange_rate_at_aquisition.parse().unwrap(),
    });
    pool_asset.amount = KrakenAmount::new(&row.asset, &row.split_amount)?
        .try_into()
        .unwrap();

    Ok(FIFO::from_iter([pool_asset]))
}
