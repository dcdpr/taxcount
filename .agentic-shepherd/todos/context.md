# Current Context (creating notes suitable for a draft commit)

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
