# Current Context (creating notes suitable for a draft commit)

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

  - [rgrant 20261031 21:58 UTC] Analysis
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
    - The second problem is in CapGainsWorksheet::sums(). For an EventAtom::Fee, it adds the
      proceeds to trade_proceeds only when reduces_proceeds is set. Event::reduce_trade_proceeds()
      sets reduces_proceeds on the EventAtom::Fee values of a ledger row when it decreases the
      proceeds of the EventAtom::Trade values on that ledger row. For an EventAtom::InvestmentFee,
      sums() does not add the proceeds to trade_proceeds. For each of the two, sums() adds basis to
      trade_basis and net_gain to trade_gain. As a result, trade_gain is more than trade_proceeds
      minus trade_basis, by the sum of the payment-time values that sums() did not add to a row.
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
      - "Trade Basis": trade_basis, the basis of each disposal, fee coins included. In a US column
        it is the basis. In a Non-US column it is bona_fide.basis for GainTerm::Short { us,
        bona_fide } and GainTerm::Long { us, bona_fide }, and the basis for a split with basis_date
        after the move date.
      - "Trade Proceeds": trade_proceeds, the proceeds of each EventAtom::Trade, each
        EventAtom::Fee, and each EventAtom::InvestmentFee. For an EventAtom::Fee with
        reduces_proceeds set, reduce_trade_proceeds() decreased the proceeds of the
        EventAtom::Trade values on the same ledger row by the same quantity. Then the total of the
        ledger row is equal to event_info.proceeds. assert_error_check() compares the sum across
        all columns with ledger_proceeds.
      - "Move-Date Value" (new row): the sum of bona_fide.basis across each GainTerm::Short { us,
        bona_fide } and GainTerm::Long { us, bona_fide }. It is in the US columns only, and blank
        in the Non-US columns. It is not part of the comparison with ledger_proceeds, because the
        user did not get proceeds on the move date.
      - "Position Proceeds": position_proceeds, the proceeds_us and proceeds_bona_fide of each
        EventAtom::Position, from the events "Margin Position Close" and "Margin Position Settle".
        EventAtom::position_from_kraken_amount() puts it in a US or Non-US column by the event
        date. It is in the "Short Term" columns only. It is part of the capital gain.
      - "Gain": the capital gain of the column, the Schedule D value. At this time, sums()
        subtracts position_fees_min from it. The user does not write that result on a form.
    - taxcount builds PR Statement-24 from the sums table. Issue #5 rewrites PR Statement-24.
    - Issue #8 is the division of a fee coin gain between the US and Non-US columns.
