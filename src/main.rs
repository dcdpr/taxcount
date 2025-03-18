//  [rgrant 20220220 11:47 UTC] taxcount

#![forbid(unsafe_code)]

use directories::ProjectDirs;
use error_iter::ErrorIter as _;
use is_terminal::IsTerminal as _;
use onlyargs::CliError;
use onlyargs_derive::OnlyArgs;
use ron::ser::PrettyConfig;
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::{env, process::ExitCode, rc::Rc};
use taxcount::basis::{AssetName, CheckList};
use taxcount::client::{bitcoind::BitcoindClient, esplora::EsploraClient, Client};
use taxcount::errors::{BitcoindClientError, EsploraClientError};
use taxcount::imports::kraken::{read_basis_lookup, read_ledgers, read_trades};
use taxcount::imports::wallet::{self, electrum, ledgerlive};
use taxcount::model::{constants, CapGainsWorksheet, ExchangeRates, GainConfig, State, Stats};
use taxcount::model::{exchange::Balances, ledgers::parsed::LedgerParsed};
use taxcount::util::{fifo::FIFO, year_ext::CheckYearsExt as _};
use taxcount::{bdk::bitcoin::Network, gitver_hashes};
use thiserror::Error;
use tracing::{debug, warn};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::prelude::*;

const BITCOIND_MEMO: &str = "bitcoind_memo";
const ESPLORA_MEMO: &str = "esplora_memo";
const TEMP_MEMO: &str = ".temp_memo.ron";

#[derive(Debug, OnlyArgs)]
#[footer = "Additional environment variables:"]
#[footer = "  - ADDRESS_GAP is the number of consecutive unused receiving addressees to search"]
#[footer = "      through for on-chain transactions. Default is 25."]
#[footer = "  - BITCOIN_NETWORK accepts {bitcoin (default), testnet, signet, regtest}"]
#[footer = "      https://docs.rs/bitcoin/latest/bitcoin/network/enum.Network.html"]
#[footer = "  - ESPLORA_URL accepts a http: or https: URL"]
#[footer = "      default is \"http://localhost:3000\""]
#[footer = "  - BITCOIND_URL accepts a http: or https: URL"]
#[footer = "      Selects the Bitcoind client if set, overriding any ESPLORA_URL setting"]
#[footer = "  - BITCOIND_CREDENTIALS accepts a `username:password` for HTTP Authorization"]
#[footer = "  - RAYON_NUM_THREADS sets the connection concurrency for the Esplora/Bitcoind client"]
#[footer = "      default is 32"]
#[footer = "  - TERM_COLOR accepts \"always\" to override automatic terminal sensing"]
struct Args {
    /// Read Kraken Ledger CSV from a file.
    #[long]
    input_ledger: Vec<PathBuf>,

    /// Read Kraken Trades CSV from a file.
    #[long]
    input_trades: Vec<PathBuf>,

    /// Read Basis Event Lookup CSV from a file.
    #[long]
    input_basis: Vec<PathBuf>,

    /// Read Tx Tags CSV from a file.
    #[long]
    input_tx_tags: Vec<PathBuf>,

    /// Read Generic Wallet history CSV from a file.
    #[long]
    input_wallet: Vec<PathBuf>,

    /// Read Electrum Wallet history CSV from a file.
    #[long]
    input_electrum: Vec<PathBuf>,

    /// Read LedgerLive Wallet history CSV from a file.
    ///
    #[long]
    input_ledgerlive: Vec<PathBuf>,

    /// An extended public key.
    ///   Primarily used for automatically claiming change addresses
    ///   in transactions. Also informs the blockchain analysis to
    ///   differentiate transactions between income and spends.
    ///
    #[long]
    input_xpub: Vec<String>,

    /// Read raw addresses from a file.
    ///   This is intended as an alternative to providing extended
    ///   public keys. Prefer using xPubs if they are available.
    ///
    #[long]
    input_addresses: Vec<PathBuf>,

    /// Read checkpoint from a file.
    #[long]
    input_checkpoint: Option<PathBuf>,

    /// Write checkpoint to a file.
    ///
    #[long]
    output_checkpoint: Option<PathBuf>,

    /// Date of US territory Bona Fide Residency Special Election.
    ///   This allows you to take a Special Election for allocating
    ///   gains by figuring the appreciation separately for your
    ///   territory and US holding periods.
    ///
    #[short('r')]
    bona_fide_residency: Option<String>,

    /// Override default OHLC-ness Exchange Rates database directory.
    ///   Default is "./references/exchange-rates-db/daily-vwap/".
    ///
    // TODO: DRY wrt exchange_rates_db constant.
    exchange_rates_db: Option<PathBuf>,

    /// Write worksheet CSVs to this output directory.
    #[short('o')]
    worksheet_path: Option<PathBuf>,

    /// Worksheet CSVs written to an output directory will be given
    ///   this prefix.
    #[short('p')]
    #[default("")]
    worksheet_prefix: String,

    /// Enable verbose output.
    /// Prints details CSV tables to stdout when not written to a file.
    verbose: bool,
}

#[derive(Debug, Error)]
enum Error {
    #[error("Failed to import {0:?}")]
    Import(PathBuf, #[source] taxcount::errors::KrakenError),

    #[error("Unable to parse ledger: {0:?}")]
    ParseLedger(PathBuf, #[source] taxcount::errors::ParseLedgerError),

    #[error("TxId {0}: Internal running balance has diverged from given balance for {1}")]
    LedgerBalance(String, AssetName),

    #[error("Ledger validation error")]
    LedgerValidation,

    #[error("Unable to load ExchangeRates DataBase")]
    ExchangeRatesDb(#[from] taxcount::errors::ExchangeRatesDbError),

    #[error("Argument parsing error")]
    Args(#[from] CliError),

    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("Checkpoint error, unable to read {0:?}")]
    InputCheckpoint(PathBuf, #[source] Box<taxcount::errors::CheckpointError>),

    #[error("Checkpoint error, unable to write {0:?}")]
    OutputCheckpoint(PathBuf, #[source] Box<taxcount::errors::CheckpointError>),

    #[error("CheckList determined some required information is missing")]
    CheckList(#[from] taxcount::errors::CheckListError),

    #[error("Date parsing error")]
    Date(#[from] chrono::ParseError),

    #[error("Unable to parse wallet: {0:?}")]
    Wallet(PathBuf, #[source] taxcount::errors::WalletError),

    #[error("Wallet transaction resolution error")]
    WalletTx(#[from] taxcount::errors::TxoError),

    #[error("Wallet Auditor error")]
    Auditor(#[from] taxcount::errors::AuditorError),

    #[error("Unable to parse Tx Tags CSV: {0:?}")]
    TxTags(PathBuf, #[source] taxcount::errors::TxTagsError),

    #[error("Unable to parse Addresses CSV: {0:?}")]
    Address(PathBuf, #[source] csv::Error),

    #[error("Electrum network parsing error")]
    ElectrumNetwork(#[from] bdk::bitcoin::network::constants::ParseNetworkError),

    #[error("Esplora client error")]
    EsploraClient(#[from] EsploraClientError),

    #[error("Bitcoind client error")]
    BitcoindClient(#[from] BitcoindClientError),

    #[error("Unable to locate user cache directory")]
    CacheDir,

    #[error("Error while resolving wallet transactions")]
    Client(#[from] taxcount::errors::ClientError),

    #[error("Unable to write client memo: `{0:?}`")]
    ClientMemoRon(PathBuf, #[source] ron::Error),

    #[error("Unable to rename client memo: `{0:?}`")]
    ClientMemoRename(PathBuf, #[source] std::io::Error),

    #[error("Input data has multiple years: Expected {0}, found {1:?}")]
    InvalidYear(i32, BTreeSet<i32>),
}

fn main() -> ExitCode {
    // Initialize the tracing subscriber for instrumentation.
    // Uses the `RUST_LOG` environment var for configuration. E.g. `RUST_LOG=debug cargo run`
    // This is very useful to see the input CSV row that caused a panic.
    //
    // See: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html#directives
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let term_color = env::var("TERM_COLOR")
        .map(|color| color == "always")
        .unwrap_or_else(|_| std::io::stdout().is_terminal());
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_ansi(term_color))
        .with(env_filter)
        .init();

    match run(onlyargs::parse()) {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err}");
            for source in err.sources().skip(1) {
                eprintln!("  Caused by: {source}");
            }

            ExitCode::FAILURE
        }
    }
}

fn run(args: Result<Args, CliError>) -> Result<(), Error> {
    let args = args?;

    // Find user's cache directory for taxcount and make sure it exists.
    let project_dir =
        ProjectDirs::from("design.contract", "DCD", "taxcount").ok_or(Error::CacheDir)?;
    let cache_dir = project_dir.cache_dir();
    std::fs::create_dir_all(cache_dir)?;

    let bitcoin_network: Network = env::var("BITCOIN_NETWORK")
        .unwrap_or_else(|_| "bitcoin".to_string())
        .parse()?;

    let mut stats = Stats::default();
    let mut trades = FIFO::new();
    let mut ledgers = HashMap::new();
    let mut ledger_errors = HashMap::new();

    // Load state from checkpoint if a path is provided.
    let mut state = args
        .input_checkpoint
        .as_ref()
        .map(|path| {
            debug!("Loading checkpoint from {path:?}");

            State::load(path)
        })
        .transpose()
        .map_err(|e| Error::InputCheckpoint(args.input_checkpoint.unwrap(), Box::new(e)))?
        .unwrap_or_else(|| State::new(bitcoin_network));

    for input_trades in args.input_trades {
        trades.extend(
            read_trades(&mut stats, &input_trades).map_err(|e| Error::Import(input_trades, e))?,
        );
    }
    for input_ledger in args.input_ledger {
        let ledger_name = Rc::from(input_ledger.file_stem().unwrap().to_str().unwrap());
        let ledger = read_ledgers(&mut stats, &input_ledger)
            .map_err(|e| Error::Import(input_ledger.clone(), e))?
            .parse(&trades)
            .map_err(|err| Error::ParseLedger(input_ledger.clone(), err))?;

        // Initialize balances from checkpoint.
        let mut balances = Balances::from(&state.exchange_balances);
        let mut errors = HashMap::default();

        // Accumulate balances and assert on any differences.
        for lp in ledger.iter() {
            assert_balance(&mut balances, &mut errors, lp);
        }
        ledger_errors.insert(input_ledger, errors);

        ledgers.insert(ledger_name, ledger);
    }

    // Validate ledger balances.
    let mut has_errors = false;
    for (ledger_name, errors) in &ledger_errors {
        for error in errors.values() {
            has_errors = true;
            println!("❌ {}, {error}", ledger_name.display());
        }
    }
    if has_errors {
        return Err(Error::LedgerValidation);
    }

    let exchange_rates_db = match args.exchange_rates_db {
        Some(path) => ExchangeRates::new(path)?,
        None => ExchangeRates::new(constants::DEFAULT_PATH_EXCHANGE_RATES_DB)?,
    };

    let bitcoind_url = env::var("BITCOIND_URL").ok();
    let esplora_url =
        env::var("ESPLORA_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());

    let mut tx_tags = wallet::TxTags::new();
    let mut rows = Vec::new();
    let mut client = None;
    let mut auditor = None;

    if !args.input_wallet.is_empty()
        || !args.input_electrum.is_empty()
        || !args.input_ledgerlive.is_empty()
    {
        client = Some(if let Some(bitcoind_url) = bitcoind_url.as_ref() {
            let filename = bitcoind_client_cache_path(cache_dir, bitcoin_network);
            get_bitcoind_client(bitcoind_url, filename)?
        } else {
            let filename = esplora_client_cache_path(cache_dir, bitcoin_network);
            get_esplora_client(&esplora_url, filename)?
        });
        let mut auditor_init = state.auditor.clone();

        // Derive addresses in parallel.
        auditor_init.add_xpubs(args.input_xpub.iter().map(|xpub| xpub.as_str()))?;

        for path in args.input_addresses {
            let addresses =
                wallet::read_addresses(&path).map_err(|err| Error::Address(path, err))?;
            for address in addresses {
                auditor_init.add_address(&address)?;
            }
        }
        auditor = Some(auditor_init);
    }

    for path in args.input_tx_tags {
        tx_tags.extend(wallet::read_tx_tags(&path).map_err(|err| Error::TxTags(path, err))?);
    }

    // Read generic wallet files.
    for path in args.input_wallet {
        wallet::read_generic(&path, &mut rows).map_err(|err| Error::Wallet(path.clone(), err))?;
    }

    // Read Electrum wallet files.
    for path in args.input_electrum {
        electrum::read_electrum(&path, &mut rows)
            .map_err(|err| Error::Wallet(path.clone(), err))?;
    }

    // Read LedgerLive wallet files.
    for path in args.input_ledgerlive {
        ledgerlive::read_ledgerlive(&path, &mut rows)
            .map_err(|err| Error::Wallet(path.clone(), err))?;
    }

    // Resolve all wallet CSV rows into transactions.
    let wallets = if let (Some(client), Some(auditor)) = (client.as_ref(), auditor.as_mut()) {
        wallet::resolve(rows, client, auditor, &tx_tags)?
    } else {
        HashMap::new()
    };

    // Load basis lookup CSV if path is provided.
    let mut basis_lookup = FIFO::new();
    for input_basis in args.input_basis {
        basis_lookup.extend(
            read_basis_lookup(&mut stats, &input_basis)
                .map_err(|e| Error::Import(input_basis, e))?,
        );
    }

    // Update the Bona Fide Residency date.
    let bona_fide_residency = args
        .bona_fide_residency
        .map(|date| date.parse())
        .transpose()?;
    if let Some(new_date) = bona_fide_residency {
        if let Some(old_date) = state.bona_fide_residency(new_date) {
            if old_date != new_date {
                warn!("Replacing old Bona Fide Residency date `{old_date}` with `{new_date}`");
            }
        }
    }

    // Get the first available year in any of the input data.
    let year = wallets
        .iter()
        .flat_map(|(_, txs)| txs.get_first_year())
        .chain(
            ledgers
                .iter()
                .flat_map(|(_, ledger)| ledger.get_first_year()),
        )
        .next()
        .or_else(|| trades.get_first_year())
        .or_else(|| basis_lookup.get_first_year());
    // Check that all dates in the input data have the same year.
    if let Some(year) = year {
        let mut errors = BTreeSet::new();
        for (asset_name, txs) in &wallets {
            if let Err(error) = txs.check_years(year) {
                // TODO: I don't carry the original path around with the parsed/resolved data.
                // So I'm kind of fudging the error message right now, providing "some" context.
                // It will print the "worksheet" name. Something is better than nothing.
                eprintln!("❌ `{asset_name}-wallet`, Multiple years found in wallet:");
                eprintln!("  Expected {year}, found {error:?}");
                eprintln!();
                errors.extend(error);
            }
        }
        for (worksheet_name, ledger) in &ledgers {
            if let Err(error) = ledger.check_years(year) {
                eprintln!("❌ `{worksheet_name}`, Multiple years found in Kraken Ledger CSV:");
                eprintln!("  Expected {year}, found {error:?}");
                eprintln!();
                errors.extend(error);
            }
        }
        if let Err(error) = trades.check_years(year) {
            eprintln!("❌ Multiple years found in Kraken Trades CSV");
            eprintln!("  Expected {year}, found {error:?}");
            eprintln!();
            errors.extend(error);
        }
        if let Err(error) = basis_lookup.check_years(year) {
            eprintln!("❌ Multiple years found in Kraken Basis Event Lookup CSV");
            eprintln!("  Expected {year}, found {error:?}");
            eprintln!();
            errors.extend(error);
        }

        if !errors.is_empty() {
            return Err(Error::InvalidYear(year, errors));
        }
    }

    // Resolve all taxable events with the current state.
    let gain_config = GainConfig {
        exchange_rates_db,
        bona_fide_residency,
    };
    let resolved = state.resolve(wallets, ledgers, gain_config, trades, basis_lookup);
    let worksheets = CheckList::execute(resolved)?;

    // Write memos
    if let Some(client) = client {
        let temp = cache_dir.join(TEMP_MEMO);
        debug!("Writing temporary client memo to {temp:?}");
        let mut file = BufWriter::new(File::create(&temp)?);
        let config = PrettyConfig::default();
        match client {
            Client::Bitcoind(bitcoind) => {
                ron::ser::to_writer_pretty(&mut file, &bitcoind.into_memo()?, config)
                    .map_err(|err| Error::ClientMemoRon(temp.clone(), err))?;

                let path = bitcoind_client_cache_path(cache_dir, bitcoin_network);
                debug!("Renaming temporary Bitcoind client memo to {path:?}");
                fs::rename(temp, &path).map_err(|err| Error::ClientMemoRename(path, err))?;
            }
            Client::Esplora(esplora) => {
                ron::ser::to_writer_pretty(&mut file, &esplora.into_memo()?, config)
                    .map_err(|err| Error::ClientMemoRon(temp.clone(), err))?;

                let path = esplora_client_cache_path(cache_dir, bitcoin_network);
                debug!("Renaming temporary Esplora client memo to {path:?}");
                fs::rename(temp, &path).map_err(|err| Error::ClientMemoRename(path, err))?;
            }
        }
        file.flush()?;
    }

    // Save state to a new checkpoint.
    if let Some(state_path) = args.output_checkpoint.as_ref() {
        // Update state with auditor.
        if let Some(auditor) = auditor {
            state.auditor = auditor;
        }

        debug!("Saving checkpoint to {state_path:?}");
        state
            .save(state_path)
            .map_err(|e| Error::OutputCheckpoint(state_path.clone(), Box::new(e)))?;
        debug!("Saving checkpoint completed");
    } else {
        gitver_hashes::print_all();
    }

    for (worksheet_name, events) in worksheets.into_iter() {
        let underline = "=".repeat(worksheet_name.len());
        println!("Worksheet {worksheet_name}");
        println!("========= {underline}");
        println!();

        let worksheet = CapGainsWorksheet::new(events);

        if let Some(path) = args.worksheet_path.as_ref().map(|root| {
            let filename = format!("{}{worksheet_name}.csv", args.worksheet_prefix);
            root.join(filename)
        }) {
            std::fs::write(&path, worksheet.to_string())?;

            let path = path.display();
            let underline = "=".repeat(path.to_string().len());
            println!("Ledger Row Worksheet written to {path}");
            println!("====== === ========= ======= == {underline}");
            println!();
        } else {
            println!("Ledger Row Worksheet");
            println!("====== === =========");
            println!();
            println!("{worksheet}");
            println!();
        }

        if let Some(details) = worksheet.trade_details() {
            if let Some(path) = args.worksheet_path.as_ref().map(|root| {
                let filename = format!(
                    "{}{worksheet_name}-trade-details.csv",
                    args.worksheet_prefix
                );
                root.join(filename)
            }) {
                std::fs::write(&path, details.to_string())?;

                let path = path.display();
                let underline = "=".repeat(path.to_string().len());
                println!("Cap Gains Trade Details written to {path}");
                println!("=== ===== ===== ======= ======= == {underline}");
                println!();
            } else if args.verbose {
                println!("Cap Gains Trade Details");
                println!("=== ===== ===== =======");
                println!();
                println!("{details}");
                println!();
            }
        }

        if let Some(details) = worksheet.income_details() {
            if let Some(path) = args.worksheet_path.as_ref().map(|root| {
                let filename = format!(
                    "{}{worksheet_name}-income-details.csv",
                    args.worksheet_prefix
                );
                root.join(filename)
            }) {
                std::fs::write(&path, details.to_string())?;

                let path = path.display();
                let underline = "=".repeat(path.to_string().len());
                println!("Cap Gains Income Details written to {path}");
                println!("=== ===== ====== ======= ======= == {underline}");
                println!();
            } else if args.verbose {
                println!("Cap Gains Income Details");
                println!("=== ===== ====== =======");
                println!();
                println!("{details}");
                println!();
            }
        }

        if let Some(fees) = worksheet.tx_fees() {
            if let Some(path) = args.worksheet_path.as_ref().map(|root| {
                let filename = format!(
                    "{}{worksheet_name}-tx-fee-details.csv",
                    args.worksheet_prefix
                );
                root.join(filename)
            }) {
                std::fs::write(&path, fees.to_string())?;

                let path = path.display();
                let underline = "=".repeat(path.to_string().len());
                println!("Cap Gains Transaction Fee Details written to {path}");
                println!("=== ===== =========== === ======= ======= == {underline}");
                println!();
            } else if args.verbose {
                println!("Cap Gains Transaction Fee Details");
                println!("=== ===== =========== === =======");
                println!();
                println!("{fees}");
                println!();
            }
        }

        if let Some(details) = worksheet.position_details() {
            if let Some(path) = args.worksheet_path.as_ref().map(|root| {
                let filename = format!(
                    "{}{worksheet_name}-margin-position-details.csv",
                    args.worksheet_prefix
                );
                root.join(filename)
            }) {
                std::fs::write(&path, details.to_string())?;

                let path = path.display();
                let underline = "=".repeat(path.to_string().len());
                println!("Cap Gains Position Details written to {path}");
                println!("=== ===== ======== ======= ======= == {underline}");
                println!();
            } else if args.verbose {
                println!("Cap Gains Position Details");
                println!("=== ===== ======== =======");
                println!();
                println!("{details}");
                println!();
            }
        }

        if let Some(fees) = worksheet.position_fees() {
            if let Some(path) = args.worksheet_path.as_ref().map(|root| {
                let filename = format!(
                    "{}{worksheet_name}-investment-interest-fee-details.csv",
                    args.worksheet_prefix
                );
                root.join(filename)
            }) {
                std::fs::write(&path, fees.to_string())?;

                let path = path.display();
                let underline = "=".repeat(path.to_string().len());
                println!("Cap Gains Investment Interest Fee Details written to {path}");
                println!("=== ===== ========== ======== === ======= ======= == {underline}");
                println!();
            } else if args.verbose {
                println!("Cap Gains Investment Interest Fee Details");
                println!("=== ===== ========== ======== === =======");
                println!();
                println!("{fees}");
                println!();
            }
        }

        let sums = worksheet.sums();
        if let Some(path) = args.worksheet_path.as_ref().map(|root| {
            let filename = format!("{}{worksheet_name}-sums.csv", args.worksheet_prefix);
            root.join(filename)
        }) {
            std::fs::write(&path, sums.to_string())?;

            let path = path.display();
            let underline = "=".repeat(path.to_string().len());
            println!("Kraken Summary (Cap Gains Sums) written to {path}");
            println!("====== ======= ================ ======= == {underline}");
            println!();
        } else {
            println!("Kraken Summary (Cap Gains Sums)");
            println!("====== ======= ================");
            println!();
            println!("{sums}");
            println!();
        }

        sums.assert_error_check();
    }
    check_pending(&state);
    stats.pretty_print();

    Ok(())
}

/// Sanity check: Report any pending deposits or withdrawals to the user. This might be indicative
/// of a bug or an error in one of the CSVs.
fn check_pending(state: &State) {
    println!("Pending Deposits and Withdrawals");
    println!("======= ======== === ===========");
    println!();

    let mut has_pending = false;
    let (deposits, withdrawals) = state.check_pending();

    for deposit in deposits {
        println!(
            "  ⚠️ {asset} deposit with synthetic ID `{synthetic_id}` at {time}:",
            asset = deposit.asset,
            synthetic_id = deposit.synthetic_id,
            time = deposit.time,
        );
        for (synthetic_id, amount) in deposit.details.into_iter() {
            println!("       Basis synthetic ID: {synthetic_id}, Amount: {amount:?}")
        }
        has_pending = true;
    }

    for withdrawal in withdrawals {
        println!(
            "  ⚠️ {asset} withdrawal with synthetic ID `{synthetic_id}` at {time}:",
            asset = withdrawal.asset,
            synthetic_id = withdrawal.synthetic_id,
            time = withdrawal.time,
        );
        for (synthetic_id, amount) in withdrawal.details.into_iter() {
            println!("       Basis synthetic ID: {synthetic_id}, Amount: {amount:?}")
        }
        has_pending = true;
    }

    if !has_pending {
        println!("Nothing is pending! 🎉");
        println!();
    }

    println!();
}

fn bitcoind_client_cache_path<P: AsRef<Path>>(cache_dir: P, bitcoin_network: Network) -> PathBuf {
    cache_dir
        .as_ref()
        .join(format!("{BITCOIND_MEMO}_{bitcoin_network}.ron"))
}

fn esplora_client_cache_path<P: AsRef<Path>>(cache_dir: P, bitcoin_network: Network) -> PathBuf {
    cache_dir
        .as_ref()
        .join(format!("{ESPLORA_MEMO}_{bitcoin_network}.ron"))
}

fn get_bitcoind_client(bitcoind_url: &str, path: PathBuf) -> Result<Client, BitcoindClientError> {
    debug!("Reading Bitcoind client memo from {path:?}");
    if let Ok(file) = File::open(path) {
        match ron::de::from_reader(file) {
            Ok(memo) => return Ok(BitcoindClient::from_memo(bitcoind_url, memo)?.into()),
            Err(_) => {
                warn!("Corrupt bitcoind client cache detected! A new one will be created.");
            }
        }
    }
    Ok(BitcoindClient::new(bitcoind_url)?.into())
}

fn get_esplora_client(esplora_url: &str, path: PathBuf) -> Result<Client, EsploraClientError> {
    debug!("Reading Esplora client memo from {path:?}");
    if let Ok(file) = File::open(path) {
        match ron::de::from_reader(file) {
            Ok(memo) => return Ok(EsploraClient::from_memo(esplora_url, memo)?.into()),
            Err(_) => {
                warn!("Corrupt esplora client cache detected! A new one will be created.");
            }
        }
    }
    Ok(EsploraClient::new(esplora_url)?.into())
}

fn assert_balance(
    balances: &mut Balances,
    errors: &mut HashMap<AssetName, Error>,
    lp: &LedgerParsed,
) {
    match lp {
        LedgerParsed::Trade { row_out, row_in }
        | LedgerParsed::MarginPositionSettle { row_out, row_in } => {
            balances.accumulate(row_out.amount, row_out.fee);
            balances.eq(row_out.balance, |asset: AssetName| {
                errors
                    .entry(asset)
                    .or_insert_with(|| Error::LedgerBalance(row_out.txid.clone(), asset));
            });
            balances.accumulate(row_in.amount, row_in.fee);
            balances.eq(row_in.balance, |asset: AssetName| {
                errors
                    .entry(asset)
                    .or_insert_with(|| Error::LedgerBalance(row_in.txid.clone(), asset));
            });
        }

        LedgerParsed::MarginPositionOpen(lrt)
        | LedgerParsed::MarginPositionRollover(lrt)
        | LedgerParsed::Withdrawal(lrt) => {
            balances.accumulate(lrt.amount, lrt.fee);
            balances.eq(lrt.balance, |asset: AssetName| {
                errors
                    .entry(asset)
                    .or_insert_with(|| Error::LedgerBalance(lrt.txid.clone(), asset));
            });
        }

        LedgerParsed::MarginPositionClose {
            row_proceeds,
            row_fee,
            ..
        } => {
            balances.accumulate(row_proceeds.amount, row_proceeds.fee);
            balances.eq(row_proceeds.balance, |asset: AssetName| {
                errors
                    .entry(asset)
                    .or_insert_with(|| Error::LedgerBalance(row_proceeds.txid.clone(), asset));
            });
            balances.accumulate(row_fee.amount, row_fee.fee);
            if let Some(balance) = row_fee.balance {
                balances.eq(balance, |asset: AssetName| {
                    errors
                        .entry(asset)
                        .or_insert_with(|| Error::LedgerBalance(row_fee.txid.clone(), asset));
                });
            }
        }

        LedgerParsed::Deposit(lrd) => {
            balances.accumulate(lrd.amount, lrd.fee);
            balances.eq(lrd.balance, |asset: AssetName| {
                errors
                    .entry(asset)
                    .or_insert_with(|| Error::LedgerBalance(lrd.txid.clone(), asset));
            });
        }
    }
}
