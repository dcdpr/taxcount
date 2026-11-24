use crate::basis::{AssetName, CheckList};
use crate::client::{bitcoind::BitcoindClient, esplora::EsploraClient, Client};
use crate::errors::{BitcoindClientError, EsploraClientError};
use crate::imports::kraken::{read_basis_lookup, read_ledgers, read_trades};
use crate::imports::wallet::{self, electrum, ledgerlive};
use crate::model::{constants, exchange::Balances, ledgers::parsed::LedgerParsed, WorksheetName};
use crate::model::{CapGainsWorksheet, ExchangeRates, GainConfig, PrStatement24, State, Stats};
use crate::util::{fifo::FIFO, year_ext::CheckYearsExt as _};
use crate::{bdk::bitcoin::Network, cli::Args, env::ENV, gitver_hashes};
use directories::ProjectDirs;
use onlyargs::CliError;
use ron::ser::PrettyConfig;
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use thiserror::Error;
use tracing::{debug, warn};

const BITCOIND_MEMO: &str = "bitcoind_memo";
const ESPLORA_MEMO: &str = "esplora_memo";
const TEMP_MEMO: &str = ".temp_memo.ron";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to import {0:?}")]
    Import(PathBuf, #[source] crate::errors::KrakenError),

    #[error("Unable to parse ledger: {0:?}")]
    ParseLedger(PathBuf, #[source] crate::errors::ParseLedgerError),

    #[error("TxId {0}: Internal running balance has diverged from given balance for {1}")]
    LedgerBalance(String, AssetName),

    #[error("Ledger validation error")]
    LedgerValidation,

    #[error("Unable to load ExchangeRates DataBase")]
    ExchangeRatesDb(#[from] crate::errors::ExchangeRatesDbError),

    #[error("Argument parsing error")]
    Args(#[from] CliError),

    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("Checkpoint error, unable to read {0:?}")]
    InputCheckpoint(PathBuf, #[source] Box<crate::errors::CheckpointError>),

    #[error("Checkpoint error, unable to write {0:?}")]
    OutputCheckpoint(PathBuf, #[source] Box<crate::errors::CheckpointError>),

    #[error("CheckList determined some required information is missing")]
    CheckList(#[from] crate::errors::CheckListError),

    #[error("Date parsing error")]
    Date(#[from] chrono::ParseError),

    #[error("Unable to parse wallet: {0:?}")]
    Wallet(PathBuf, #[source] crate::errors::WalletError),

    #[error("Wallet transaction resolution error")]
    WalletTx(#[from] crate::errors::TxoError),

    #[error("Wallet Auditor error")]
    Auditor(#[from] crate::errors::AuditorError),

    #[error("Unable to parse Tx Tags CSV: {0:?}")]
    TxTags(PathBuf, #[source] crate::errors::TxTagsError),

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
    Client(#[from] crate::errors::ClientError),

    #[error("Unable to write client memo: `{0:?}`")]
    ClientMemoRon(PathBuf, #[source] ron::Error),

    #[error("Unable to rename client memo: `{0:?}`")]
    ClientMemoRename(PathBuf, #[source] std::io::Error),

    #[error("Input data has multiple years: Expected {0}, found {1:?}")]
    InvalidYear(i32, BTreeSet<i32>),
}

pub fn run(args: Result<Args, CliError>) -> Result<(), Error> {
    let args = args?;

    // Find user's cache directory for taxcount and make sure it exists.
    let project_dir =
        ProjectDirs::from("design.contract", "DCD", "taxcount").ok_or(Error::CacheDir)?;
    let cache_dir = project_dir.cache_dir();
    std::fs::create_dir_all(cache_dir)?;

    let bitcoin_network: Network = ENV
        .bitcoin_network
        .as_deref()
        .unwrap_or_else(|| "bitcoin")
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

    let bitcoind_url = &ENV.bitcoind_url;
    let esplora_url = ENV
        .esplora_url
        .as_deref()
        .unwrap_or_else(|| "http://localhost:3000");

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
        .values()
        .flat_map(|txs| txs.get_first_year())
        .chain(ledgers.values().flat_map(|ledger| ledger.get_first_year()))
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

    let mut pr_statement24_rows: Vec<PrStatement24> = Vec::new();

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

        if let Some(details) = worksheet.event_details() {
            if let Some(path) = args.worksheet_path.as_ref().map(|root| {
                let filename = format!(
                    "{}{worksheet_name}-event-details.csv",
                    args.worksheet_prefix
                );
                root.join(filename)
            }) {
                std::fs::write(&path, details.to_string())?;

                let path = path.display();
                let underline = "=".repeat(path.to_string().len());
                println!("Cap Gains Event Details written to {path}");
                println!("=== ===== ===== ======= ======= == {underline}");
                println!();
            } else if args.verbose {
                println!("Cap Gains Event Details");
                println!("=== ===== ===== =======");
                println!();
                println!("{details}");
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

        if bona_fide_residency.is_some() {
            let name = WorksheetName::from(worksheet_name.to_string());
            let statement = PrStatement24::from_worksheet(&name, &worksheet);
            pr_statement24_rows.push(statement);
        }
    }

    if bona_fide_residency.is_some() && !pr_statement24_rows.is_empty() {
        let mut all_rows = PrStatement24::empty();
        for statement in pr_statement24_rows {
            all_rows.extend(statement);
        }

        if let Some(path) = args.worksheet_path.as_ref().map(|root| {
            let filename = format!("{}pr-statement24.csv", args.worksheet_prefix);
            root.join(filename)
        }) {
            std::fs::write(&path, all_rows.to_string())?;

            let path = path.display();
            let underline = "=".repeat(path.to_string().len());
            println!("PR Statement-24 written to {path}");
            println!("== ============ ======= == {underline}");
            println!();
        } else {
            println!("PR Statement-24");
            println!("== ============");
            println!();
            println!("{all_rows}");
            println!();
        }
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

        LedgerParsed::MarginPositionOpen { row_open, row_fee } => {
            balances.accumulate(row_open.amount, row_open.fee);
            balances.eq(row_open.balance, |asset: AssetName| {
                errors
                    .entry(asset)
                    .or_insert_with(|| Error::LedgerBalance(row_open.txid.clone(), asset));
            });
            if let Some(row_fee) = row_fee {
                balances.accumulate(row_fee.amount, row_fee.fee);
                balances.eq(row_fee.balance, |asset: AssetName| {
                    errors
                        .entry(asset)
                        .or_insert_with(|| Error::LedgerBalance(row_fee.txid.clone(), asset));
                });
            }
        }

        LedgerParsed::MarginPositionRollover(lrt) | LedgerParsed::Withdrawal(lrt) => {
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
