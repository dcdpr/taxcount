# Issue 04, Bona Fide Move-Date Value is required in ledgers-sums for reconciliation

## Definitions

  - Sale Event: an Event that contains an EventAtom::Trade or an EventAtom::Position.
  - Fee Cost: the proceeds of an EventAtom::Fee in a Sale Event.
  - Fee Coin Disposal: the proceeds, the basis, and the net gain of an EventAtom::Fee or an
    EventAtom::InvestmentFee.
  - Cell: one column of the sums table, for one term and one residency.
  - Sale Cell: the Cell of a Trade atom's sale. For GainTerm::Short { us, bona_fide } and
    GainTerm::Long { us, bona_fide }, the Sale Cell is the Non-US Cell.

## Tax Rules

Pub 550 (2025) gives these rules:
  - the amount realized from a sale is the value received minus the expenses of the sale, so a
    fee of a trade decreases the gain of that trade;
  - the fee coin is a capital asset, so its payment is a disposal on Form 8949, with its own
    proceeds, basis, and holding period; and
  - the fee of a margin position rollover is investment interest, a Form 4952 deduction, and is
    not part of the capital gain.

## Section 1, Atoms

### Step 1, Delete the Mutation

Delete Event::reduce_trade_proceeds() in src/model/events.rs, and the reduces_proceeds field of
EventAtom::Fee. Delete the three calls in src/basis/split.rs and the trade_atom_start variable.
After this step, each Trade atom holds the full proceeds of its split, and its net_gain is its
proceeds minus its basis.

### Step 2, Sale Event Rule

Add is_sale_event(atoms: &[EventAtom]) -> bool to src/model/gains.rs. It is true when the
list contains an EventAtom::Trade or an EventAtom::Position. It is a pure function of the atoms,
and only the summation calls it. No value in an Event changes.

### Step 3, Position Atom on a Break-Even Close

In handle_margin_close(), call add_position() also when row_proceeds.amount is zero. Then each
margin close is a Sale Event, and its fee is a Fee Cost.

## Section 2, Fee Shares

Add fee_shares(atoms: &[EventAtom]) -> Vec<UsdAmount> to src/model/gains.rs. For a Sale Event
with Trade atoms, it divides the sum of the Fee Costs across the Trade atoms, pro-rata by
asset_amount, with the remainder on the last Trade atom. This is the pro-rata code of
reduce_trade_proceeds(), as a pure function. For a Sale Event with a Position atom and no Trade
atom, the full sum goes to the Position atom.

## Section 3, Sums Table

### Step 1, One Cell Struct

Replace GainMatrixShort and GainMatrixLong with one GainCell struct with the fields
trade_proceeds, move_date_value, trade_basis, position_proceeds, and fees. Delete position_fees
and position_fees_min. GainMatrix keeps income and the four cells.

### Step 2, Rows

sums() folds each atom into the cells:
  - "Trade Proceeds": the proceeds of each Trade, Fee, and InvestmentFee atom, in its Sale Cell;
  - "Move-Date Value": bona_fide.basis of each GainTerm::Short { us, bona_fide } and
    GainTerm::Long { us, bona_fide }, in the US Cell, and blank in the Non-US Cells;
  - "Trade Basis": us.basis in the US Cell and bona_fide.basis in the Non-US Cell;
  - "Position Proceeds": proceeds_us and proceeds_bona_fide of each Position atom, in the short-term
    Cells, and blank in the long-term Cells;
  - "Fees": each fee share, in the Sale Cell of its Trade atom, or in the Cell of its Position
    atom; and
  - "Gain": "Trade Proceeds" plus "Move-Date Value" minus "Trade Basis" plus "Position Proceeds"
    minus "Fees".

Delete the "Interest Expense" row, the "Limited Interest Expense" row, apply_interest_expenses(),
and fold_position_fee(). An InvestmentFee atom is a Fee Coin Disposal only.

### Step 3, Sums Struct

Sums keeps ledger_proceeds and gain_matrix. Add GainCell::gain(), which calculates the identity.
PrStatement24::from_worksheet() reads the gain of each Cell through gain(). Issue #5 rewrites
PrStatement24.

### Step 4, Error Check

GainMatrix has event_proceeds, the sum of the proceeds of each Trade atom, Position atom, and
Income atom: the amount that the parties of each event agreed to transfer. assert_error_check()
makes sure that ledger_proceeds is equal to event_proceeds. It also makes sure that the sum of
the net gains folded into each Cell is equal to GainCell::gain() of that Cell.

## Section 4, Event Details CSV

Add a "Fees" column after "Proceeds" in CapGainsEventDetails. For a Trade atom and a Position
atom, it holds the fee share of that atom. For each other atom, it is blank. Then the per-lot
amount realized is "Proceeds" minus "Fees" on each line.

## Section 5, Tests

In src/model/gains/tests.rs:
  - the identity in each Cell for a GainTerm::Short { us, bona_fide } Trade atom with a
    "Move-Date Value";
  - a Trade Event with a long-term Trade atom and a short-term fee coin, with the Fee Cost in the
    long-term Cell and the Fee Coin Disposal in the short-term Cell;
  - a margin close Event with a Position atom and a Fee atom, with the Fee Cost in the Cell of the
    Position atom; and
  - a withdrawal Event with a Fee atom, with the Fee Coin Disposal and no Fee Cost.

In src/basis/split.rs, the tests change from reduced proceeds to full proceeds, and the
reduces_proceeds assertions are deleted. In margin_rollover(), the US long-term gain is 126.72 with
no interest cap. Each ledger_proceeds value stays as it is.

## Section 6, Notes

  - A fee charged in the bought coin on a buy is a Fee atom: the lot has the full amount at the
    full transaction value, and the Fee atom disposes the fee coins from the pool. This is a
    design choice of the user, and not a defect.
  - fixtures/runs/kraken-tests/20261031-ledgers-sums.csv is saved run output with the deleted
    rows. It is not read by a test.

## Plan Step References in Code

When you implement the steps of this plan, include Back References under these rules:
  .agentic-shepherd/doc/patterns/general-deployments/plan-step-references.md
