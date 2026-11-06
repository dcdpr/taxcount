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

  - [rgrant 20261106 21:37 UTC] Analysis
    - Pub 550 (2025) gives these rules. The amount realized from a sale is the value received minus
      the expenses of the sale, so a fee of a trade decreases the gain of that trade. The fee coin
      is a capital asset, so its payment is a disposal on Form 8949, with its own proceeds, basis,
      and holding period. The fee of a margin position rollover is investment interest, a Form 4952
      deduction, and is not part of the capital gain. Pub 550 writes the first rule as a mutation:
      the filer subtracts the fee from the proceeds of the sale, and then calculates the gain. From
      a functional programming view, that is wrong. The proceeds are the amount that the parties
      agreed to transfer, and a subtraction inside that value hides the fee. taxcount keeps the full
      proceeds in the Trade atom and the fee value in the Fee atom, and the summation subtracts the
      "Fees" row from the gain. The gain is the same number, and each atom keeps its own fact.
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
