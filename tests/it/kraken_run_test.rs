// issue #15 / The event details file contains different rows at each run

use crate::test_error::TestError;
use crate::util::compare_output::assert_eq_runs;
use crate::util::run::{base_args, run_taxcount};
use std::path::Path;
use taxcount::cli::Args;

/// The directory of the saved runs of bin/taxcount-run-kraken-tests.sh.
const FIXTURE_RUNS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/runs/kraken-tests");

/// The inputs of bin/taxcount-run-kraken-tests.sh.
const KRAKEN_INPUTS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/references/kraken-tests");

/// The start of the name of each file that the test run writes.
const TEST_RUN_PREFIX: &str = "run-";

/// The move date of bin/taxcount-run-kraken-tests.sh.
const MOVE_DATE: &str = "2024-02-24 00:00:00 UTC";

/// A run with the inputs of bin/taxcount-run-kraken-tests.sh writes the same files as the last run
/// in the directory fixtures/runs/kraken-tests/.
///
/// In the checkpoint, assert_eq_runs() ignores the time in the header and the sequence of the items
/// in each HashMap.
#[test]
fn kraken_run_matches_fixture() -> Result<(), TestError> {
    let run_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("kraken-tests");
    let inputs = Path::new(KRAKEN_INPUTS);
    run_taxcount(Args {
        input_ledger: vec![inputs.join("ledgers.csv")],
        input_trades: vec![inputs.join("trades.csv")],
        input_basis: vec![inputs.join("basis-lookup-test.csv")],
        bona_fide_residency: Some(MOVE_DATE.to_string()),
        ..base_args(&run_dir, TEST_RUN_PREFIX)
    })?;

    assert_eq_runs(Path::new(FIXTURE_RUNS), &run_dir, TEST_RUN_PREFIX)
}
