# Completed Issues

### [rgrant 20261127 18:59 UTC] 16 / Wallet internal account names are in random order on each run
  - As a user, when I run taxcount two times with the same inputs, I expect the same internal
    account in each report.
  - A wallet transaction can contain TXOs of two or more accounts. For this wallet transaction, the
    internal account contains each account name, with ", " between the names. The order of the
    names is different from run to run.
  - The "Note" column contains values with the same problem.
  - With a random order, a person cannot compare the reports of two runs with a diff. If a test
    compares a wallet report with a fixture, the test also fails.

  - Progress Notes
    - [rgrant 20261127 21:15 UTC] Added concatenate_distinct() in src/basis/split.rs. It puts the
      names into a BTreeSet for the internal account and for the values from notes().
    - [rgrant 20261127 21:15 UTC] [offtopic] Moved the test module of src/basis/split.rs to
      src/basis/split/tests.rs.
    - [rgrant 20261127 21:15 UTC] Added concatenate_distinct_gives_byte_order() in
      src/basis/split/tests.rs.

  - Catharsis
    - [rgrant 20261127 21:29 UTC] done in commit a85d913

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

  - Catharsis
    - [rgrant 20261124 21:15 UTC] done in commit 782520d3

### [rgrant 20261031 20:20 UTC] 05 / Statement 24 basis and market value are kludged out of gain totals
  - As a bona fide resident filing Form 482 statement 24, when the report is built, I expect each
    row to sum three figures over the lots sold after the move date: the sale price net of selling
    fees, the value on the move date (or the acquisition cost, for lots acquired after the move),
    and the acquisition cost including purchase fees. Total gain, US-sourced gain, and PR-sourced
    gain follow from those three sums.
  - Lots sold before the move date are not dispositions of PR property and do not belong in any
    statement 24 figure.
  - Investment interest expense is a Form 4952 deduction and is not part of any statement 24
    figure.
  - Today the basis and market-value columns are reconstructed from gain totals, and the US and PR
    gain columns carry limited interest expense, which is only right when every sold lot straddles
    the move date and there is no interest expense.
  - The per-lot figures already exist in the trade-details output.
  - Depends on issue #4, which surfaces the move-date value in the sums table.

  - [rgrant 20261115 19:11 UTC] Definitions
    - Multi-Jurisdiction Split: a split with a basis date before the move date, that the user
      disposes of after the move date. GainTerm::Short { us, bona_fide } and GainTerm::Long { us,
      bona_fide } contain its US portion and its bona fide portion.
    - Post-Move Split: a split with a basis date after the move date. GainTerm::ShortBonaFide and
      GainTerm::LongBonaFide contain it.
    - Pre-Move Split: a split that the user disposes of before the move date. GainTerm::ShortUs and
      GainTerm::LongUs contain it.

  - [rgrant 20261115 19:12 UTC] Analysis
    - from_worksheet() in src/model/gains.rs reads the sums table. It sets A to the trade proceeds
      of the Non-US gain cell. It sets E to the capital gain of the US gain cell, and F to the
      capital gain of the Non-US gain cell. Then it calculates D = E + F, B = A - F, and C = A - D.
    - The capital gain of the US gain cell includes each Pre-Move Split. A Pre-Move Split is not PR
      property. As a result, E, D, and C are incorrect when the CapGainsWorksheet contains a
      Pre-Move Split.
    - F includes the fees and the position proceeds of the Non-US gain cell. A does not include
      them. As a result, B is incorrect when the Non-US gain cell contains fees or position
      proceeds.
    - The sums table does not contain the US basis of each Multi-Jurisdiction Split. The US gain
      cell adds that basis and the basis of each Pre-Move Split into one trade basis. For C, the US
      basis of each Multi-Jurisdiction Split is necessary, without the basis of each Pre-Move
      Split.
    - update() in src/model/gains.rs also reads the basis date and the event date of each Pre-Move
      Split. As a result, "Date Acquired" and "Date Sold" can be from a disposal before the move
      date.
    - Description item 4 contains this text: "the US and PR gain columns carry limited interest
      expense". Issue 04 deleted apply_interest_expenses(). An investment fee atom is a disposal of
      the fee coin. As a result, its proceeds and its basis are in A, B, and C. No column of PR
      Statement 24 subtracts the investment interest.
    - These are the correct columns for one gain term, for each Multi-Jurisdiction Split and each
      Post-Move Split:
      - A is the trade proceeds plus the position proceeds minus the fees of the Non-US gain cell;
      - B is the move-date value of each Multi-Jurisdiction Split plus the basis of each Post-Move
        Split;
      - C is the US basis of each Multi-Jurisdiction Split plus the basis of each Post-Move Split;
        and
      - D = A - C, E = B - C, and F = A - B.
    - B is equal to the trade basis of the Non-US gain cell.
    - In fixtures/runs/kraken-tests/20261115-ledgers-sums.csv, the "Non-US Short Term" column gives
      A = 6189.3685 + 395.2286 - 38.8939 = 6545.7032, and B = 13879.1580.
    - In fixtures/runs/kraken-tests/20261115-ledgers-event-details.csv, the 41 "Special election
      for bona fide residents" lines have a total "Basis (US)" of 6245.4605. The basis of the
      Post-Move Splits is 13879.1580 - 12630.5999 = 1248.5581. As a result, C = 7494.0186.
    - The correct row is A 6545.7032, B 13879.1580, C 7494.0186, D -948.3154, E 6385.1394, and F
      -7333.4548. fixtures/runs/kraken-tests/20261115-pr-statement24.csv contains A 6189.3685, B
      13522.8233, C 5124.7284, D 1064.6402, E 8398.0950, and F -7333.4548. Only F is correct.

  - [rgrant 20261115 19:24 UTC] Implementation Plan
    - Step 1, Read the Event Atoms: the arguments of from_worksheet() in src/model/gains.rs are the
      WorksheetName and the CapGainsWorksheet. For each event, from_worksheet() calls fee_shares().
      Then it adds each event atom to the "LT" row or to the "ST" row.
    - For a trade atom, a fee atom, or an investment fee atom with a Multi-Jurisdiction Split,
      from_worksheet() adds these values:
      - the proceeds minus the fee share, to A;
      - bona_fide.basis, to B; and
      - us.basis, to C.
    - For the same event atoms with a Post-Move Split, from_worksheet() adds the proceeds minus the
      fee share to A. It adds the basis to B and to C.
    - For a position atom with proceeds_bona_fide, from_worksheet() adds proceeds_bona_fide minus
      the fee share to A of the "ST" row.
    - from_worksheet() ignores each Pre-Move Split, each income atom, and each position atom
      without proceeds_bona_fide.
    - Step 2, Dates: "Date Acquired" is the first basis date of the event atoms in the row. "Date
      Sold" is the last event date of the same event atoms. For a Multi-Jurisdiction Split, the
      basis date is us.basis_date. For a position atom, the two dates are the event date.
    - Step 3, Rows: from_worksheet() calculates D = A - C, E = B - C, and F = A - B. It writes a
      row for each gain term that contains an event atom.
    - Step 4, Deletions: delete PrStatement24Dates, pr_statement24_dates(), and the Sums argument
      of from_worksheet(). In src/main.rs, run() calls from_worksheet() with the CapGainsWorksheet.
    - Step 5, Tests in src/model/gains/tests.rs:
      - pr_statement24_from_atoms replaces pr_statement24_from_sums, and compares the CSV text with
        the expected text;
      - in pr_statement24_from_atoms, the CapGainsWorksheet contains each type of event atom in
        Step 1, and a Pre-Move Split;
      - in pr_statement24_from_atoms, F is equal to the capital gain of the Non-US gain cell from
        sums();
      - pr_statement24_dates_from_fees_only reads the dates from the row of from_worksheet(); and
      - in a new test, the CapGainsWorksheet contains only a Pre-Move Split, and PR Statement 24
        contains only the header line.
    - Step 6, Fixture Check: run taxcount with the data in references/kraken-tests/. Compare the
      "investment assets ledgers ST" row with these values:
      - A 6545.7032;
      - B 13879.1580;
      - C 7494.0186;
      - D -948.3154;
      - E 6385.1394; and
      - F -7333.4548.
    - The maximum difference for each value is 0.0010. If each value is correct, replace
      fixtures/runs/kraken-tests/20261115-pr-statement24.csv with the new output.

  - Progress Notes
    - [rgrant 20261115 19:42 UTC] Steps 1 to 5: from_worksheet() reads the event atoms of the
      CapGainsWorksheet. I deleted PrStatement24Dates and pr_statement24_dates().
    - [rgrant 20261115 19:42 UTC] The new struct PrStatement24Sums contains the sums and the dates
      of one row. The plan does not include it.
    - [rgrant 20261115 19:42 UTC] The test helper row() in src/model/gains/tests.rs makes a
      CapGainsWorksheetRow. The plan does not include it.
    - [rgrant 20261115 19:42 UTC] Step 6: the run with references/kraken-tests/ gives A 6545.7032,
      B 13879.1580, C 7494.0189, D -948.3156, E 6385.1392, and F -7333.4548. For the plan value of
      C, I used rounded values, and that value is 0.0003 less.

  - Catharsis
    - [rgrant 20261115 21:00 UTC] done in commit 0816ec18

### [rgrant 20261113 21:59 UTC] 09 / Proceeds are 0.0000 for each margin close in event details file
  - As a user, when I read a "Position" line in the event details file, I expect a USD value in its
    "Proceeds" column. That value is the profit or the loss of the margin close.
  - Each line with "Position" in the "Event" column is a margin close, and its "Proceeds" column
    contains "0.0000".
  - For a margin close in USD, the user can read the USD value from the "Asset Amount" column. For
    a margin close in EUR, CHF, or JPY, the USD value is missing from the event details file.
  - In references/kraken-tests, line "fe7af8-f0c7b-45b4a5" contains 11.8707 EUR in the "Asset
    Amount" column and 0.0000 in the "Proceeds" column. The ledgers CSV contains 12.7859 USD for
    the same margin close.
  - The "Fees" column of that line contains the fee share 1.4370. "Proceeds" minus "Fees" is
    -1.4370 on that line, and the correct value is 11.3489.
  - The sums CSV and the ledgers CSV contain the correct values for each margin close.

  - [rgrant 20261115 18:14 UTC] Analysis
    - The fmt() function of CapGainsEventDetails writes proceeds().unwrap_or_default() of each
      EventAtom in the "Proceeds" column. EventAtom::proceeds() returns None for
      EventAtom::Position. As a result, the "Proceeds" column contains 0.0000 for each position
      atom.
    - A position atom contains its proceeds in two values, proceeds_us and proceeds_bona_fide.
      event_proceeds() and the sums() function of CapGainsWorksheet read these two values directly.
      The ledgers CSV and the sums CSV contain the correct values.
    - Event::add_fee() is the only other caller of EventAtom::proceeds(). Event::add_fee() calls
      EventAtom::proceeds() on fee atoms only.
    - The commit "[WIP]: fee-rework implementation attempt 1" of 20260921 added
      EventAtom::proceeds() with None for EventAtom::Position.
    - fixtures/runs/kraken-tests/20261031-ledgers-event-details.csv contains 0.0000 for line
      "fe7af8-f0c7b-45b4a5". The commit "20261106 an implementation of issue #04" added the "Fees"
      column after that file.

  - Progress Notes
    - [rgrant 20261115 18:22 UTC] EventAtom::proceeds() returns a UsdAmount. For a position atom,
      the value is the sum of proceeds_us and proceeds_bona_fide.
    - [rgrant 20261115 18:22 UTC] event_proceeds() calls EventAtom::proceeds() for trade atoms,
      income atoms, and position atoms.
    - [rgrant 20261115 18:22 UTC] src/model/gains/tests.rs contains the new tests
      margin_close_proceeds_in_event_details and position_proceeds_us_and_bona_fide.
    - [rgrant 20261115 18:22 UTC] In a run of references/kraken-tests, line "fe7af8-f0c7b-45b4a5"
      of the event details file contains 12.7859 in the "Proceeds" column and 1.4370 in the "Fees"
      column.

  - Catharsis
    - [rgrant 20261115 18:59 UTC] done in commit bea14e9f

### [rgrant 20261101 01:40 UTC] 08 / from_fee_split() fails to divide a fee coin gain into US and Non-US the same as a trade
  - As a bona fide resident, when I pay a fee after the move date with a fee coin that I got before
    the move date, I expect the gain of the fee coin in the US column up to the move-date value,
    and the rest in the Non-US column, the same as an EventAtom::Trade.
  - As a bona fide resident, when I pay a fee in USD, I expect the "Trade Basis" and "Trade
    Proceeds" of that fee in the column of the fee date.
  - At this time, from_fee_split() selects the column by only the basis_date of the fee coin
    (instead of also the event_date), and does not divide gain from single atoms into both US and
    bona_fide.
  - For a USD PoolAsset with Origin::Base, BasisLifecycle::get_datetime() gives Utc::now(). As a
    result, from_fee_split() puts each USD EventAtom::Fee in the Non-US column, for each event
    date.
  - The total gain is equal to the total that the code gives at this time. The division between the
    US and Non-US columns changes.

  - Catharsis
    - [rgrant 20261114 23:04 UTC] done in commit 7fe2efc0

### [rgrant 20261114 17:19 UTC] 10 / incorrect blank fee basis for all fee atoms of some trades
  - As a bona fide resident, when I have a simple sale using BTC UTXOs with basis before the bona
    fide date, I expect the same bona fide logic for trade atoms and fee atoms.
  - In the failure case, the entire gain from the fee atom is in column K ("Net Capital Gains (US
    Long Term)") and none in column P ("Net Capital Gains (Non-US Long Term)")
  - In the failure case, column L ("basis (Non-US)") is blank, despite these UTXOs having a basis
    from before the bona fide date.

  - [rgrant 20261114 20:43 UTC] Analysis
    - Issue 10 is a part of issue 08. Each description item of issue 10 is also in issue 08. This
      issue includes the requirements of the two issues:
      - for a fee atom, from_fee_split() divides the net gain between US and Non-US as
        trade_from_split() does for a trade atom (issue 10 item 1, issue 08 item 1);
      - a fee in USD is in the column of the fee date (issue 08 item 2); and
      - the sum of the net gains is equal before and after the change (issue 08 item 5).
    - Issue 10 items 2 and 3 are symptoms in the event details file. Issue 08 items 3 and 4 contain
      the causes.
    - trade_from_split() and from_fee_split() in src/model/events.rs each calculate a GainTerm from
      one split. The two functions compare different dates with the move date.
    - trade_from_split() compares the event date with the move date first. If the event date is
      before the move date, the full net gain is in the US portion. If the basis date is before the
      move date and the event date is not, trade_from_split() divides the net gain at the move-date
      value. The result is GainTerm::Short { us, bona_fide } or GainTerm::Long { us, bona_fide }.
    - from_fee_split() compares only the basis date with the move date. For the same split, the
      result is GainTerm::ShortUs or GainTerm::LongUs, with the full net gain in the US portion.
      from_fee_split() does not make a gain term with two gain portions.
    - As a result, the event details file contains the full net gain of that fee atom in a US
      column. The "Basis (Non-US)" column of that line is blank.
    - The bug is in the output of bin/taxcount-run-kraken-tests.sh. The move date of that script is
      2024-02-24. The trade of ledger row "d33a1c-88f3b-a320e5" is on 2024-08-05. Its trade atom
      and its fee atom have the basis date 2024-01-16 and the synthetic ID "a8dd21-846d1-997648".
      - The trade atom contains 0.5501 in "Basis (US)" and 256.0752 in "Basis (Non-US)".
      - The fee atom contains 0.0014 in "Basis (US)" and its full net gain 0.4424 in "Net Capital
        Gains (US Short Term)". Its five Non-US columns are blank.
    - For a USD split, BasisLifecycle::get_datetime() returns Utc::now(). As a result,
      from_fee_split() puts each USD fee atom in a Non-US column, also for an event date before the
      move date. With the code of trade_from_split(), a USD fee atom is in the column of the fee
      date.
    - The output of bin/taxcount-run-kraken-tests.sh is without a USD fee atom before the move
      date. As a result, only a unit test can check the USD requirement.
    - After the change, the "Basis Date" value of a USD fee atom is the time of the run, as before
      the change. The requirements do not include this value.
    - After the change, from_fee_split() gets the exchange rate of the fee asset at the move date.
      If that exchange rate is missing from the exchange rates database, add_fee() returns an
      ExchangeRateError, as add_trade() does.
    - These functions accept a gain term with two gain portions for each type of event atom:
      - the function CapGainsWorksheet::fold_gain_term();
      - the function fmt() in `impl Display for GainTerm` ; and
      - the function PrStatement24Dates::update().
    - As a result, a change to these functions is not necessary.
    - After the change, some values of a fee atom are in the Non-US columns. For each fee atom,
      us.net_gain plus bona_fide.net_gain is equal to proceeds minus basis. As a result, the sum of
      the four values in the "Gain" row of the sums table is equal before and after the change. In
      the output of bin/taxcount-run-kraken-tests.sh, the sum is 1064.6402.

  - [rgrant 20261114 20:44 UTC] Implementation Plan
    - For Step 1, make src/model/events/tests.rs. Add `#[cfg(test)] mod tests;` at the top of
      src/model/events.rs.
    - For Step 2, write seven tests in src/model/events/tests.rs. Put a move date in the GainConfig
      of each test. Test 1 to test 6 call EventAtom::from_fee_split().
      - In test 1, the split is BTC, and its basis date is before the move date. The event date is
        after the move date, and the holding period is less than one year. The correct result is
        GainTerm::Short { us, bona_fide }.
      - In test 2, the split is BTC, and its basis date is before the move date. The event date is
        after the move date, and the holding period is more than one year. The correct result is
        GainTerm::Long { us, bona_fide }.
      - In test 3, the split is BTC, and the event date is before the move date. The correct result
        is GainTerm::ShortUs.
      - In test 4, the split is BTC, and its basis date is after the move date. The correct result
        is GainTerm::ShortBonaFide.
      - In test 5, the split is USD, and the event date is before the move date. The correct result
        is GainTerm::ShortUs.
      - In test 6, the split is USD, and the event date is after the move date. The correct result
        is GainTerm::ShortBonaFide.
      - In test 7, call add_trade() and add_fee() on one Event. The two splits have the dates of
        test 2. The correct result is GainTerm::Long { us, bona_fide } in the two event atoms.
    - In test 1 and test 2, make sure that us.net_gain is the move-date value minus the basis. Make
      sure that bona_fide.basis is the move-date value. Make sure that bona_fide.net_gain is the
      proceeds minus the move-date value.
    - In test 7, make sure that bona_fide.basis_date is the move date in the two event atoms.
    - For Step 3, add a test to src/model/gains/tests.rs for sums(). In the test, a fee atom
      contains GainTerm::Long { us, bona_fide }. Make sure that the move-date value of the fee atom
      is in the "US Long Term" column of the sums table. Make sure that assert_error_check()
      passes.
    - For Step 4, run the tests before the code change. Make sure that test 1, test 2, test 5, and
      test 7 fail. Make sure that each other new test passes.
    - For Step 5, add GainTerm::from_split() to src/model/events.rs. Move the code that calculates
      the gain term from trade_from_split() into it. Its return type is Result<GainTerm,
      ExchangeRateError>. Its arguments are:
      - the amount;
      - the proceeds;
      - the basis lifecycle;
      - the event date; and
      - the GainConfig.
    - For Step 6, call GainTerm::from_split() from trade_from_split() and from from_fee_split().
      Delete the code that calculates the gain term in from_fee_split(). Correct the doc comment of
      from_fee_split().
    - For Step 7, run all tests. Make sure that each test passes.
    - For Step 8, run bin/taxcount-run-kraken-tests.sh. Make sure that the fee atom of
      "d33a1c-88f3b-a320e5" has a value in "Basis (Non-US)". Make sure that each USD fee atom is in
      a Non-US column, as before the change. Make sure that the sum of the four values in the
      "Gain" row is 1064.6402. A difference of 0.0002 is permitted, because each CSV value has only
      four digits after the point.

  - Progress Notes
    - [rgrant 20261114 21:12 UTC] Step 1 to Step 3: tests for from_fee_split(), for add_trade()
      with add_fee(), and for sums() with a divided fee atom
    - [rgrant 20261114 21:12 UTC] Step 5 and Step 6: GainTerm::from_split() calculates the gain
      term for trade_from_split() and from_fee_split()
    - [rgrant 20261114 21:12 UTC] Step 8: the fee atom of "d33a1c-88f3b-a320e5" contains 0.6657 in
      "Basis (Non-US)", and the sum of the "Gain" row is 1064.6402
    - [rgrant 20261114 21:12 UTC] The argument of GainTerm::from_split() is one
      PoolAssetNonSplittable, in the place of the amount and the basis lifecycle of the plan
    - [rgrant 20261114 21:12 UTC] trade_from_split(), from_fee_split(), and add_trade() require A:
      Asset + Copy

  - Catharsis
    - [rgrant 20261114 23:04 UTC] done in commit 7fe2efc0

### [rgrant 20261031 19:58 UTC] 04 / Bona Fide Move-Date Value is required in ledgers-sums for reconciliation
  - When I run taxcount with --bona-fide-residency and read the ledgers-sums CSV, this identity
    must be correct in each column: "Gain" is equal to "Trade Proceeds" plus "Move-Date Value"
    minus "Trade Basis" plus "Position Proceeds". A blank value is 0.
  - "Move-Date Value" is a new row. In a US column it contains the sum of bona_fide.basis across
    each GainTerm::Short { us, bona_fide } and GainTerm::Long { us, bona_fide }. In a Non-US column
    it is blank.
  - For GainTerm::Short { us, bona_fide } and GainTerm::Long { us, bona_fide }, sums() adds
    us.basis and us.net_gain to the US column. It does not add the proceeds.
  - For the same GainTerm values, sums() adds the proceeds, bona_fide.basis, and bona_fide.net_gain
    to the Non-US column. The identity is correct in the Non-US column.
  - "Trade Proceeds" must contain the proceeds of each EventAtom::Fee and each
    EventAtom::InvestmentFee, because each is a disposal of the fee coin on Form 8949. When
    reduce_trade_proceeds() sets reduces_proceeds, the "Trade Proceeds" row contains the
    payment-time value. For each other fee, sums() adds basis to trade_basis and net_gain to
    trade_gain. It does not add the payment-time value to a row. The other fees are the fees of
    "Withdrawal", "Deposit", "Margin Position Open", "Margin Position Close", "Margin Position
    Settle", "Margin Position Rollover", and "Move between wallets", and the fee on row_in of a
    LedgerParsed::Trade.
  - "Gain" must be trade_gain plus position_proceeds. Delete position_fees_min and
    apply_interest_expenses().
  - Delete the "Interest Expense" row and the "Limited Interest Expense" row from the ledgers-sums
    CSV, and position_fees from GainMatrixShort and GainMatrixLong.
  - assert_error_check() must continue to compare ledger_proceeds with the sum of trade_proceeds
    across all columns. Then it counts the proceeds of each EventAtom::Trade, each EventAtom::Fee,
    and each EventAtom::InvestmentFee one time. For this comparison, event_info.proceeds must
    contain the proceeds of each EventAtom::Fee with reduces_proceeds not set, and of each
    EventAtom::InvestmentFee. "Move-Date Value" must not be part of this comparison, because
    bona_fide.basis is not proceeds.
  - assert_error_check() must also make sure that the identity is correct in each column.

  - [rgrant 20261106 21:37 UTC] Analysis
    - Pub 550 (2025) gives these rules. The amount realized from a sale is the value received minus
      the expenses of the sale, so a fee of a trade decreases the gain of that trade. The fee coin
      is a capital asset, so its payment is a disposal on Form 8949, with its own proceeds, basis,
      and holding period. The fee of a margin position rollover is investment interest, a Form 4952
      deduction, and is not part of the capital gain. Pub 550 writes the first rule as a mutation:
      the filer subtracts the fee from the proceeds of the sale, and then calculates the gain. From
      a functional programming view, that is wrong. The proceeds are the amount that the parties
      agreed to transfer, and a subtraction inside that value hides the fee. taxcount keeps the
      full proceeds in the Trade atom and the fee value in the Fee atom, and the summation
      subtracts the "Fees" row from the gain. The gain is the same number, and each atom keeps its
      own fact.
    - The first problem is in CapGainsWorksheet::fold_gain_term() in src/model/gains.rs. For
      GainTerm::Short { us, bona_fide } and GainTerm::Long { us, bona_fide }, it adds us.basis and
      us.net_gain to the US column. It does not add the proceeds. For GainTerm::ShortUs and
      GainTerm::LongUs, it adds the proceeds. As a result, the identity is correct for
      GainTerm::ShortUs and GainTerm::LongUs, and is incorrect for GainTerm::Short { us, bona_fide
      } and GainTerm::Long { us, bona_fide }.
    - The move-date value is in the code at this time. EventAtom::trade_from_split() calculates
      bona_fide_basis, the value of the split on the move date, and puts it in
      bona_fide_portion.basis. us_portion.net_gain is bona_fide_basis minus basis. As a result, the
      missing value for the US column is bona_fide.basis of the same EventAtom::Trade. The
      event-details CSV has it in the "Basis (Non-US)" value of each "Special election for bona
      fide residents" line.
    - The second problem is Event::reduce_trade_proceeds(). After add_fee() builds the Fee atoms of
      a trade, it subtracts the fee value from the proceeds and the net_gain of each Trade atom on
      the ledger row, and sets reduces_proceeds on the Fee atoms. That is a mutation of the amount
      that the parties agreed to transfer, and the gain of a sale is a summation over the atoms. It
      also reaches only the fee on row_out of a trade. For the fee on row_in of a trade, and for
      the fee of a margin close, the fee value is in no row, and the gain is too high by the fee
      value. The Fee atom is correct in each case: it has the fee value as proceeds, and the basis
      of the splits that paid it.
    - The third problem is the "Gain" row. sums() gets it from apply_interest_expenses(), which
      gives trade_gain plus position_proceeds minus position_fees_min. For the capital gain of the
      column, the "Gain" row must contain trade_gain plus position_proceeds.
    - The fixture fixtures/runs/kraken-tests/20261031-ledgers-sums.csv includes the three problems.
      In "US Short Term", "Gain" 8414.2635 minus ("Trade Proceeds" 4361.3335 minus "Trade Basis"
      8592.0345) is 12644.9645. In the event-details CSV, the sixteen "Basis (Non-US)" values on
      "Special election for bona fide residents" lines have a total of 12574.8869. The US "Fee" and
      "Investment Fee" lines with proceeds that sums() did not add to a row have a total of
      70.8359. The total of the two is 12645.7228. In
      fixtures/runs/kraken-tests/20261031-ledgers-sums.csv, "Gain" is not the capital gain, because
      sums() subtracts position_fees_min from it. In "Non-US Short Term", "Gain" -7309.5686 minus
      ("Trade Proceeds" 6016.2352 minus "Trade Basis" 13823.4451 plus "Position Proceeds" 395.2286)
      is 102.4127. The Non-US "Fee" and "Investment Fee" lines with proceeds that sums() did not
      add to a row have a total of 102.4128.
    - The purpose of each row in the ledgers-sums CSV:
      - "Trade Proceeds": the proceeds of each EventAtom::Trade, each EventAtom::Fee, and each
        EventAtom::InvestmentFee, in the column of the sale. For GainTerm::Short { us, bona_fide }
        and GainTerm::Long { us, bona_fide }, the column of the sale is the Non-US column.
      - "Move-Date Value": the sum of bona_fide.basis across each GainTerm::Short { us, bona_fide }
        and GainTerm::Long { us, bona_fide }. It is in the US columns only, and blank in the Non-US
        columns. The user did not get proceeds on the move date.
      - "Trade Basis": the basis of each disposal, fee coins included. In a US column it is the
        basis. In a Non-US column it is bona_fide.basis for GainTerm::Short { us, bona_fide } and
        GainTerm::Long { us, bona_fide }, and the basis for a split with basis_date after the move
        date.
      - "Position Proceeds": the proceeds_us and proceeds_bona_fide of each EventAtom::Position,
        from the event "Margin Position Close". A "Margin Position Settle" event has Trade atoms.
        EventAtom::position_from_kraken_amount() puts it in a US or Non-US column by the event
        date. It is in the "Short Term" columns only. It is part of the capital gain.
      - "Fees": the fee value of each EventAtom::Fee in an event that has a Trade atom or a
        Position atom. The fee value divides across the Trade atoms of the event pro-rata by
        amount, into the column of each Trade atom's sale. With no Trade atom, it goes into the
        column of the Position atom. A fee of a withdrawal, a deposit, a move, a margin open, or a
        rollover is a fee coin disposal only.
      - "Gain": the capital gain of the column, the Schedule D value: "Trade Proceeds" plus
        "Move-Date Value" minus "Trade Basis" plus "Position Proceeds" minus "Fees".
    - assert_error_check() has two checks. The first compares ledger_proceeds, the sum of
      event_info.proceeds, with the sum of the proceeds of each Trade atom, Position atom, and
      Income atom: the atoms of the agreed transfer. The fee atom proceeds are in the "Trade
      Proceeds" row, and the "Fees" row subtracts each Fee Cost. The second check sums down each
      column, "Trade Proceeds" plus "Move-Date Value" minus "Trade Basis" plus "Position Proceeds"
      minus "Fees", and compares the sum with the gain from the atoms: the sum of the net_gain of
      each portion folded into the column, plus "Position Proceeds", minus "Fees".
    - taxcount builds PR Statement-24 from the sums table. Issue #5 rewrites PR Statement-24.
    - Issue #8 is the division of a fee coin gain between the US and Non-US columns.

  - Progress Notes
    - [rgrant 20261106 21:12 UTC] Design questions complete. Plan written to
      issue-004-sums-fees.md.
    - [rgrant 20261106 21:12 UTC] Decision: delete reduce_trade_proceeds() and reduces_proceeds.
      Each Trade atom keeps its full proceeds, and sums() subtracts each Fee Cost through the Fee
      atom in the same Event.
    - [rgrant 20261106 21:12 UTC] Decision: a fee is a Fee Cost when its Event has a Trade atom or
      a Position atom. A margin open fee, a rollover fee, a withdrawal fee, a deposit fee, and a
      move fee are Fee Coin Disposals only.
    - [rgrant 20261106 21:12 UTC] Decision: the "Interest Expense" rows are deleted. Form 4952 is a
      different CSV, in issue #7.
    - [rgrant 20261106 21:12 UTC] Section 1, Atoms: handle_margin_close() adds a Position atom on a
      break-even close. No other event value changes.
    - [rgrant 20261106 21:12 UTC] Section 2, Fee Shares: fee_shares() in gains.rs, a pure function
      of the atoms of one Event, pro-rata by amount across the Trade atoms.
    - [rgrant 20261106 21:12 UTC] Section 3, Sums Table: GainCell replaces GainMatrixShort and
      GainMatrixLong. The rows are "Trade Proceeds", "Move-Date Value", "Trade Basis", "Position
      Proceeds", "Fees", and "Gain". assert_error_check() compares the ledger proceeds with the
      proceeds of the Trade, Position, and Income atoms, and checks the identity in each cell.
    - [rgrant 20261106 21:12 UTC] Section 4, Event Details CSV: a "Fees" column after "Proceeds".
    - [rgrant 20261106 21:12 UTC] [offtopic] Deleted UsdAmount::min() and max(), which only
      apply_interest_expenses() used.
    - [rgrant 20261106 21:37 UTC] Plan: issue-004-sums-fees.md. Status: implementation complete,
      with the changes in the working tree for one commit. The plan has six sections, and Section 6
      is notes.
    - [rgrant 20261106 21:37 UTC] Corrected the Analysis. The second problem is
      reduce_trade_proceeds(), a mutation of each Trade atom by the fee value, and the fee cost of
      a row_in fee and a margin close fee in no row. The old text put the problem in which fee
      proceeds sums() added to "Trade Proceeds". The "Trade Proceeds" row text no longer describes
      reduce_trade_proceeds() and the ledger comparison. A "Fees" row and the Pub 550 rules are
      new. The "Gain" row is the identity. "Position Proceeds" comes from "Margin Position Close"
      only, because a "Margin Position Settle" event has Trade atoms. assert_error_check() has its
      own item.
    - [rgrant 20261106 21:37 UTC] Description items 5 and 8 of this issue state the
      reduces_proceeds rule and the event_info.proceeds rule. The plan supersedes both: no Fee atom
      has reduces_proceeds, and event_info.proceeds does not change for a fee.
    - [rgrant 20261106 21:37 UTC] Withdrawn: a follow-up issue on the fee charged in the bought
      coin on a buy. A buy may be peer-to-peer with a network fee, the lot has the full transaction
      value, and the Fee atom disposes the fee coins from the pool. Pub 550 is one treatment, and
      the treatment is a design choice.
    - [rgrant 20261107 01:59 UTC] Deleted EventInfo::proceeds. event_proceeds() in
      src/model/events.rs calculates the sum of the proceeds of the Trade, Income, and Position
      atoms of one Event. The "Proceeds" column of the worksheet CSV contains this sum.
    - [rgrant 20261107 01:59 UTC] Deleted Sums::ledger_proceeds, GainMatrix::event_proceeds, and
      the ledger proceeds check of assert_error_check(). Each handler calculated
      EventInfo::proceeds from the same quantity and the same rate as the atoms.
    - [rgrant 20261107 01:59 UTC] This change replaces the "Error Check" step of
      issue-004-sums-fees.md, the ledger_proceeds text in the Analysis, and description item 8.
    - [rgrant 20261113 22:08 UTC] Margin fees: the fee of a margin close is a sale expense of the
      closing trade. The sources are CFR 1.1001-7, Pub 550, and the Form 8949 instructions. The
      margin rollover fee is in no "Fees" row.
    - [rgrant 20261114 00:37 UTC] The margin open fee stays a disposal of the fee coin only, per
      decision of 20261106 21:12 UTC. The margin open fee melds both a trade fee and a margin fee,
      so if the trade fee could be reported there could be a legitimate reduction in the taxpayer's
      gain. However, to separate the two requires implicit knowledge of the fee table in effect at
      that time. The fee tables depends on both exchange policy and trader volume. Lacking this
      information, the potential tax liability reduction of reporting margin open fees with
      separate Atoms is explicitly left out of scope, and taxcount overreports gain from margin
      positions as a result.
    - [rgrant 20261113 22:08 UTC] [offtopic] Margin settle: the net gain is correct for USD. These
      problems are without an issue:
      - get_event_name() gives "Settle Long" to a short position and "Settle Short" to a long
        position.
      - For a long position, the holding period starts at the margin settle, and the purchase was
        at the margin open.
      - For a long position in EUR, CHF, or JPY, handle_trade() calculates the asset-out rate from
        two times. The price is from the margin open, and the market rate is from the margin
        settle.
    - [rgrant 20261113 22:08 UTC] Section 4, Event Details CSV: issue 09 contains a problem. On
      each "Position" line, the "Proceeds" column contains 0.0000. The sums CSV is correct.
    - [rgrant 20261113 22:08 UTC] Issue 04 is done. Issue 09 contains the remaining part of Section
      4.
    - [rgrant 20261114 19:18 UTC] preparing for finish-issue

  - Catharsis
    - [rgrant 20261114 19:22 UTC] done in commit 405bae9e
    - [rgrant 20261114 19:25 UTC] additional changes in commit 923f6827

### [rgrant 20260416 00:26 UTC] 03 / PR Statement-24 omits rows when bona fide gains come only from fees
  - pr_statement24_dates() only collects dates from trade_details, not from tx_fees or
    position_fees/position_details
  - When a worksheet has bona fide short-term fees but no bona fide short-term trades, the gain
    matrix is populated but dates remain None
  - The 4-way if-let in from_worksheet() fails on the None dates, silently dropping the row
  - Fix: collect dates from tx_fees (and position_fees/position_details) in pr_statement24_dates()

  - Catharsis
    - [rgrant 20260416 01:33 UTC] done in commit dea4887

### [rgrant 20260411 18:09 UTC] 02 / Emit PR Statement-24 CSV report from Sums data
  - As a PR bona fide resident, when taxcount runs with --bona-fide-residency, I expect a
    pr-statement24.csv with columns 0 (property description), 1 (date acquired), 2 (date sold), A
    (sale price), B (market value), C (adjusted basis), D (gain/loss), E (US-sourced gain), F
    (PR-sourced gain)
  - Each row is independently computed from aggregated Sums cells for one category/term combination
    (brokered LT, brokered ST, spent LT, spent ST)
  - Worksheet names ending in -wallet are spent; all others are brokered
  - Date acquired is earliest basis_date from GainPortion across all lots in that category/term
  - Date sold is latest event_date from EventInfo for trades in that category/term
  - A-F derived from Sums fields: A=trade_proceeds, E=gains_us, F=gains_bona_fide, D=E+F, B=A-F,
    C=A-D
  - Replaces manual elisp function in fixtures/pr-statement24.elisp
  - Property descriptions auto-generated to match worksheet names
  - No new CLI args required; output gated on --bona-fide-residency being set

  - [rgrant 20260411 18:31 UTC] Implementation Plan
    - Plan documented in .agentic-shepherd/plans/pr-statement24.md

  - Progress Notes
    - [rgrant 20260411 19:15 UTC] Implementation complete; ready for staging review

  - Catharsis
    - [rgrant 20260415 00:23 UTC] done in commit 28b0f97
