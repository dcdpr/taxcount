# Current Context (creating notes suitable for a draft commit)

### [rgrant 20261031 19:58 UTC] 04 / Bona Fide Move-Date Value is required in ledgers-sums for reconciliation
  - As a bona fide resident running with --bona-fide-residency, when I read the ledgers-sums CSV, I
    expect each column's Gain to equal Trade Proceeds plus Move-Date Value minus Trade Basis plus
    Trade Fee Basis plus Position Proceeds plus Limited Interest Expense, where Move-Date Value is
    a US-column row holding the deemed proceeds of lots acquired before the move date and sold
    after it.
  - For a lot acquired before the move date and sold after it, the US column receives the lot's
    original basis and its US-sourced gain (move-date value minus original basis) but no proceeds.
  - The Non-US columns satisfy the identity because they receive the sale proceeds, the move-date
    value as basis, and the gain.
  - The reconciliation of Trade Proceeds across all columns against ledger proceeds in
    assert_error_check() must continue to count each real sale exactly once.
  - assert_error_check() should then be expanded to run the "i expect" formula itself.

  - [rgrant 20261031 20:23 UTC] Analysis
    - The gap is in CapGainsWorksheet::sums() in src/model/gains.rs. The GainTerm::Short { us,
      bona_fide } and GainTerm::Long { us, bona_fide } arms add us.basis and us.net_gain to the US
      column and nothing for proceeds. The matching arms for ShortUs and LongUs add
      detail.proceeds, which is why pre-move sales reconcile and split lots do not.
    - The deemed proceeds already exist per lot. In EventTradeAtom::from_split() the split computes
      bona_fide_basis, the lot's value on the move date, and stores it as the bona_fide half's
      GainPortion.basis. US net_gain is bona_fide_basis minus basis, so the US column's missing
      figure is exactly bona_fide.basis of the same split. The trade-details CSV prints it in the
      "Basis (Non-US)" column on every "Special election for bona fide residents" row.
    - Confirmed on runs/kraken-tests/20241202: US LT basis 1537.0918 plus pre-fee US LT gain
      403.5812 gives 1940.6730, and the "Basis (Non-US)" cells of the twelve long-term
      special-election rows sum to 1940.6731.
    - Tax role of each sums row, which fixes what the identity may and may not include:
      - Trade Basis: acquisition cost. US columns hold original basis. Non-US columns hold
        move-date value for split lots and original basis for lots acquired after the move.
      - Trade Proceeds: amount realized from real sales. Counted once across all columns, which is
        what assert_error_check() reconciles against ledger proceeds.
      - Move-Date Value (new row): deemed proceeds of split lots, US columns only, blank in Non-US
        columns. Excluded from the ledger-proceeds reconciliation because no cash changed hands.
      - Trade Fee Basis: fee lots consumed by TradeSell, Withdrawal, MarginPositionClose, and
        MarginPositionSettle. EventFee::from_split() models each as a capital loss equal to the fee
        lot's basis, so the row is a selling expense that reduces gain. It is capital, so it
        belongs in the identity and on Schedule D.
      - Position Proceeds: margin close and settle P&L. Short-term only, attributed by event date
        in EventPositionAtom. Capital.
      - Interest Expense: MarginPositionOpen and MarginPositionRollover fees. Investment interest
        expense under Form 4952, not capital. Informational row, not part of the identity.
      - Limited Interest Expense: the Form 4952 deduction after the cap, computed by
        apply_interest_expenses() as the lesser of interest paid and positive pre-interest gain.
        The carryover is computed and discarded (issue #94). This row, not Interest Expense, is the
        one the identity uses.
      - Gain: pre-interest capital gain plus Limited Interest Expense. It is a net-of-4952 summary,
        not the Schedule D figure. Schedule D gain is Gain minus Limited Interest Expense.
    - Identity that assert_error_check() should enforce per column, with blank cells read as zero:
      Gain = Trade Proceeds + Move-Date Value - Trade Basis + Trade Fee Basis + Position Proceeds +
      Limited Interest Expense. It holds because trade_gain accumulates proceeds minus basis for
      whole lots, and move-date value minus basis plus proceeds minus move-date value for the two
      halves of a split lot.
    - GainMatrixShort and GainMatrixLong carry a transaction_fees field that nothing ever
      populates. It enters apply_interest_expenses() as zero and has no CSV row. It does not
      disturb the identity, but the field is dead.
    - Sums is also consumed by PrStatement24::from_worksheet() and constructed directly in
      src/model/gains/tests.rs, so a new field on GainMatrixShort and GainMatrixLong touches both.
    - Fee attribution finding, out of scope for this issue: a fee lot acquired before the move and
      consumed after it is placed entirely in the US column as a loss of its full basis, because
      EventFee::from_split() chooses the column by the fee lot's basis date and never splits. By
      the special-election rule the fee lot's disposition splits like any other lot (US: move-date
      value minus basis, PR: payment-date value minus move-date value) and the selling expense of
      payment-date value attaches to the post-move sale in the PR column, netting US: move-date
      value minus basis and PR: minus move-date value. Totals agree with today's code; the US/PR
      allocation does not. Candidate for a separate issue.
