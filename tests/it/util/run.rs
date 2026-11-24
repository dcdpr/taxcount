//! Functions that call run() in the thread of the test.

use crate::test_error::{io_error, TestError};
use std::path::{Path, PathBuf};
use std::{fs, io};
use taxcount::{cli::Args, run::run};

/// The exchange rates database in references/.
pub const EXCHANGE_RATES_DB: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/references/exchange-rates-db/daily-vwap/"
);

/// Returns the [`Args`] with which run() writes each output file into `run_dir`, with file names
/// prefixed by [`prefix`].  These [`Args`] contain the exchange rates database
/// [`EXCHANGE_RATES_DB`].  Each list of input files in these [`Args`] is empty.  These [`Args`]
/// also contain `verbose: true`.
pub fn base_args(run_dir: &Path, prefix: &str) -> Args {
    Args {
        input_ledger: Vec::new(),
        input_trades: Vec::new(),
        input_basis: Vec::new(),
        input_tx_tags: Vec::new(),
        input_wallet: Vec::new(),
        input_electrum: Vec::new(),
        input_ledgerlive: Vec::new(),
        input_xpub: Vec::new(),
        input_addresses: Vec::new(),
        input_checkpoint: None,
        output_checkpoint: Some(run_dir.join(format!("{prefix}checkpoint.ron"))),
        bona_fide_residency: None,
        exchange_rates_db: Some(PathBuf::from(EXCHANGE_RATES_DB)),
        worksheet_path: Some(run_dir.to_path_buf()),
        worksheet_prefix: prefix.to_string(),
        verbose: true,
    }
}

/// Calls run() with `args` in the thread of the test.  If `args` contains a `worksheet_path`,
/// run_taxcount() first deletes that directory and makes an empty directory at that path.
pub fn run_taxcount(args: Args) -> Result<(), TestError> {
    if let Some(run_dir) = &args.worksheet_path {
        // A file from a previous test run can stay in the directory.  Then the test cannot find a
        // file that this run did not write.
        if let Err(source) = fs::remove_dir_all(run_dir) {
            if source.kind() != io::ErrorKind::NotFound {
                return Err(io_error(run_dir, source));
            }
        }
        fs::create_dir_all(run_dir).map_err(|source| io_error(run_dir, source))?;
    }

    Ok(run(Ok(args))?)
}
