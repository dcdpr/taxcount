# Current Context (creating notes suitable for a draft commit)

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
