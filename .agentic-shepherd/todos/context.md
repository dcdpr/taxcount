# Current Context (creating notes suitable for a draft commit)

### [rgrant 20261115 19:38 UTC] 15 / The event details file contains different rows at each run
  - As a maintainer, when I run the Kraken tests two times with the same inputs, I expect the same
    rows in fixtures/runs/kraken-tests/YYYYMMDD-ledgers-event-details.csv.
  - Taxcount uses now() to calculate some values in the event details file.
  - As a result, git shows changed rows in that fixture after each run, also when the code and the
    inputs are the same.
  - Then a maintainer cannot find the rows that are different because of a change to the code.

  - [rgrant 20261124 20:30 UTC] Definitions
    - Cargo Feature: a name in a Cargo.toml that a command can select. The compiler includes the
      code and the dependencies of that name only when a command selects it.
    - Integration-Tests: the Cargo Feature that adds PartialEq and Eq to the checkpoint types, for
      the tests in tests/it/.

  - Progress Notes
    - [rgrant 20261124 20:30 UTC] For a split of the base currency, BasisLifecycle::get_datetime()
      returns the event date. Before this change, it returned Utc::now().
    - [rgrant 20261124 20:30 UTC] from_split() of GainTerm gives the event date to get_datetime().
    - [rgrant 20261124 20:30 UTC] usd_fee_before_move_date() and usd_fee_after_move_date() compare
      the basis date with the event date.
    - [rgrant 20261124 20:30 UTC] The test kraken_run_matches_fixture() in tests/it/ calls run()
      with the inputs of bin/taxcount-run-kraken-tests.sh. Then assert_eq_runs() compares each
      output file with the last run in fixtures/runs/kraken-tests/.
    - [rgrant 20261124 20:30 UTC] Checkpoint::assert_eq_checkpoint() compares two checkpoints and
      ignores the time in the checkpoint header. For each CSV file, assert_eq_runs() compares the
      lines.
    - [rgrant 20261124 20:30 UTC] The commit deletes bin/compare-runs.py.
      kraken_run_matches_fixture() replaces that script.
    - [rgrant 20261124 20:30 UTC] Integration-Tests is new in Cargo.toml and in
      crates/gitver/Cargo.toml.
    - [rgrant 20261124 20:30 UTC] Cargo.toml lists taxcount as a dependency of the tests, with
      Integration-Tests. As a result, `cargo test` uses Integration-Tests, and `cargo build` does
      not.
    - [rgrant 20261124 20:30 UTC] The tests in tests/it/ can use the struct Checkpoint.
      Checkpoint::load() reads a checkpoint file into a Checkpoint.
    - [rgrant 20261124 20:30 UTC] The commit moves the files in fixtures/runs/kraken-tests/ from
      the prefix 20261115- to the prefix 20261124-.
    - [rgrant 20261124 20:30 UTC] In the event details file, the "Basis Date (Non-US)" column of 24
      rows contains the event date. Before this change, it contained the time of the run.
    - [rgrant 20261124 20:30 UTC] Each of these 24 rows is a USD fee atom or a USD investment fee
      atom.
