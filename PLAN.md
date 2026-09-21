# PLAN: Treat fees as trade events

## Problem

In the PR Statement-24 report, columns **A** (Sale Price), **B** (Market Value), and **C** (Adjusted Basis) went negative.

## Root cause

Fees are modeled in `EventFee::from_split` (`src/model/events.rs:528-588`) as a sale of the fee asset for **$0**:

- `net_gain = -basis` — a full capital loss equal to the fee's **acquisition** basis.
- The trade's proceeds are the full sale amount, with **no** fee reduction.

Booking the entire fee as a capital loss poisons the worksheet and the PR Statement-24 math (`src/model/gains.rs:1021-1079`):

```text
sale_price      = bona_fide trade_proceeds          (full sale amount, no fee reduction)
market_value    = sale_price - pr_gain              (pr_gain includes the fee loss)
adjusted_basis  = sale_price - (us_gain + pr_gain)
```

## The fix

Model fees as real trade events, fold the separate fee vectors and the three event-detail vectors into a single `event_details: Vec<EventAtom>`, and add tests from the three `fixtures/fee-cases/` scenarios. Branch: `fee-rework`.

Major refactors are approved ("churn is inconsequential").

## Design

### 1. Fees become `EventAtom`

`EventTradeAtom` becomes `EventAtom` (see Recommended patch shape); `tx_fees` / `position_fees` are removed, and the three detail fields (`trade_details` / `income_details` / `position_details`) collapse into a single `event_details: Vec<EventAtom>` that also receives the fee atoms. Rationale: `EventAtom` holds a `GainTerm`, which provides short/long-term classification and the US / bona-fide territorial split based on the **fee asset's own** `basis_date`.

Fee atom fields:

| field          | value                                                                  |
| -------------- | ---------------------------------------------------------------------- |
| `asset_amount` | `-fee` (fees are always outgoing)                                      |
| `proceeds`     | `fee_value` — USD value of the fee asset at **payment time**           |
| `net_gain`     | `fee_value - fee_basis` — appreciation between acquisition and payment |

### 2. Valuation at payment time

`fee_value = fee amount × rate`, where rate is the event's definitional rate when it covers the fee asset, else the fee asset's **market** rate at the event date from the exchange-rate DB:

- **Trade events** (`Trade`, `MarginSettle`): Kraken charges the fee in the outgoing row's asset and `handle_trade` defines the rate for the outgoing asset, so the definitional rate covers the fee asset.
- **Declared-rate wallet spends** (`cleanup_pending_spend`): the user-declared rate covers the spent (fee) asset.
- **All other events** (margin open / rollover / close, withdrawal, deposit, wallet move): no definitional rate for the fee asset → market rate at the event date. A fee denominated in the incoming row's asset also falls here.
- Fiat (ZUSD) fees are 1.0 by definition (`KrakenAmount::get_exchange_rate` returns 1.0); no DB lookup.

This is a deliberate change from today's behavior (fee booked at $0, gain `−fee_basis`).

### 3. Folding the fee vectors

Proceeds and gain routing for the folded fee atoms; `sums()` routes by variant + event subtype:

| fee kind                                | `proceeds` →                                                            | `basis` / `net_gain` →       |
|-----------------------------------------|-------------------------------------------------------------------------|------------------------------|
| trade fee (sale / margin settlement)    | `trade_proceeds`                                                        | `trade_basis` / `trade_gain` |
| withdrawal fee                          | **nowhere** (the withdrawal row has no proceeds; no trade atom exists)  | `trade_basis` / `trade_gain` |
| deposit fee (pending-spend fee basis)   | **nowhere** (the deposit row has no proceeds; no income atom to offset) | `trade_basis` / `trade_gain` |
| wallet fee (move / loan spend / cleanup)| **nowhere** (wallet events have no proceeds)                            | `trade_basis` / `trade_gain` |
| rollover fee                            | `position_fees` matrix field                                            | `trade_basis` / `trade_gain` |
| margin-open fee                         | **nowhere** (the margin-open row has no proceeds)                       | `trade_basis` / `trade_gain` |
| margin-close fee                        | **nowhere** (the position atom already books the full row proceeds)     | `trade_basis` / `trade_gain` |

General rule: a fee atom's `proceeds` flow to `trade_proceeds` **only** when the event is a Kraken trade (subtypes `Trade` / `MarginSettle`); `InvestmentFee` proceeds flow to the `position_fees` matrix field; all other fee proceeds go nowhere. A fee atom's `basis` / `net_gain` always flow to the trade matrix cells (ST/LT × US/bona-fide of the fee coin).

- **Trade fee**: reduces the amount realized — the trade atom's `proceeds` are reduced by `fee_value` and the fee atom carries `proceeds = fee_value`, `net_gain = fee_value - fee_basis`.
- **Deposit fee**: the only recipient-paid inflow fee — when the user sends to Kraken from their own wallet, the on-chain fee (pool coin) is recorded on the pending spend and booked by `handle_deposit_inner` (Findings #5). Wallet-receive income events never carry a fee: the on-chain fee is paid by the sender, and `handle_deposit` asserts the ledger deposit fee is zero (`src/basis/split.rs:1093`); the assertion is kept.

### 4. `position_fees` fold and the interest-expense cap

- **Variant by event subtype**: `MarginPositionRollover` fee atoms → `InvestmentFee`; `MarginPositionOpen` and `MarginPositionClose` fee atoms → `Fee`. Today open *and* rollover set `has_interest_fees` (`src/model/events.rs:208`) so both feed the cap — wrong for the open fee, which is a **nondeductible carrying cost** per the fixture README (the rollover fee **is** investment interest expense, capped).
- **Cap input**: `position_fees` = `sum(fee_value)` over the row's `InvestmentFee` atoms (positive; the interest actually paid — IRS §163(d) investment-interest treatment).
- **Cap formula** (restructured to a **positive** convention so the fee is not negated twice):
  ```rust
  let gain_before = self.trade_gain + self.position_proceeds; // Long: trade_gain only (no position_proceeds)
  self.position_fees_min = gain_before.min(self.position_fees).max(UsdAmount::default());
  let gains = gain_before - self.position_fees_min;
  let carryover = self.position_fees - self.position_fees_min;
  ```
  Today: `position_fees_min = -(gain_before.min(position_fees.abs()).max(0))`, where `.abs()` merely undoes the `−fee_basis` accumulation. Keep the `position_fees` / `position_fees_min` names (doc comments: "total investment interest expense (payment-time value)" / "deduction taken this year"); `carryover` becomes a positive remainder, unused until issue #94. The positive convention is **internal**: the `Sums` `Display` impl keeps rendering the "Interest Expense" and "Limited Interest Expense" rows negated, as today.
- The atom's `net_gain` is used for worksheet display and ST/LT + US/bona-fide routing. Both open-fee and rollover-fee atoms are capital gains/losses on the fee coin, so both book to the gain column; their proceeds route per Design §3.

N.B. The cap base (`gain_before`, which includes capital gains) is the existing convention for net investment income. A strict IRS §163(d) reading would exclude capital gains from the base. See https://www.irs.gov/publications/p550#id430 and https://www.irs.gov/publications/p550#id445. This note is for auditing purposes only.

### 5. Fee reduction across splits

Compute `fee_value` **once per event** (total fee amount × rate, Design §2). Reduce the event's trade atoms' proceeds **pro-rata** to each atom's pre-reduction proceeds, so `sum(reductions) = fee_value`; each fee atom keeps its own `proceeds` (split amount × rate), so `sum(fee-atom proceeds) = fee_value`. The fixtures have exactly one trade split and one fee split per event; the pro-rata rule defines the multi-split case.

## Invariants (must hold)

1. **Total capital gain is unchanged for the trade fee** (the offset case):
   - Today: `trade_gain + (-fee_basis)` = `(proceeds - basis) - fee_basis`
   - New: `(proceeds - fee_value - basis) + (fee_value - fee_basis)` = `proceeds - basis - fee_basis`
   - Non-trade fees (deposit / withdrawal / margin / wallet) have no offsetting atom, so their total gain shifts from today's `-fee_basis` to `fee_value - fee_basis` (a shift of `+fee_value`). That is the fix, not a violation of this invariant.
2. **The interest-expense cap keeps the same formula shape but a deliberately different input.** Deduction = `min(gain_before, position_fees)` floored at 0, excess carried forward — unchanged. The input changes from the fee's **acquisition basis** (`-fee_basis`, today) to its **payment-time value** (`fee_value`, positive), and only rollover fees participate (today margin-open fees do too). Numbers change accordingly (margin-rollover LT row: today gain `-$0.08` / carryover `-$1.20` → new `+$26.72` / `$0`). That is the fix, not a regression.
3. `assert_error_check` keeps passing (ledger proceeds == matrix proceeds).
4. **Buy-side capitalization is lifetime-neutral.** Today: the lot sells for `(proceeds - basis)` and the fiat fee is booked as a separate `-fee_basis` loss → lifetime total `proceeds - basis - fee_basis`. New: the lot's basis is `(basis + fee_value)` and no fee event exists → lifetime total `proceeds - (basis + fee_value)`. A fiat fee's `fee_value` equals its `fee_basis` (face value, rate 1.0), so the lifetime total is unchanged. The fee's ST/LT classification shifts from the fee coin's own holding period (basis date = fee deposit date) to the lot's; in the fixtures the fee is paid on the purchase day, so the classification is unchanged.

## Step-by-step plan

- [ ] 1. Redesign the fee atom in `src/model/events.rs`: replace `EventTradeAtom` / `EventIncomeAtom` / `EventPositionAtom` / `EventFee` with a single `EventAtom` enum (Recommended patch shape); the three detail fields collapse into a single `event_details: Vec<EventAtom>`; remove `tx_fees` / `position_fees` / `has_interest_fees`; the `has_interest_fees: true` case splits into `InvestmentFee` for rollover and `Fee` for margin open / close. Trim the TODO comment at `events.rs:22-25` along with the flag, keeping the `issues/59` DSL-pipeline reference, now scoped to `is_withdrawal`.
- [ ] 2. Add the fee-atom constructor `EventAtom::from_fee_split` and the `add_fee`-style methods (see the `src/model/events.rs` section); fold fee atoms into `event_details` per Design §3 / §4.
- [ ] 3. Update `sums()` and `apply_interest_expenses` in `src/model/gains.rs` (one loop over `event_details`, dispatching on the variant per the routing table in Design §3; cap only rollover fees; positive convention per Design §4; remove the `trade_fees` / `transaction_fees` fields and the "Trade Fee Basis" row).
- [ ] 4. Update `pr_statement24_dates` and worksheet display in gains.rs: one loop over `event_details` replaces the three detail loops and the `tx_fees` / `position_fees` loops; per-arm date updates per the `src/model/gains.rs` section below.
- [ ] 5. Update all six fee call sites in `src/basis/split.rs` per the implementation-plan table (build fee atoms; reduce trade proceeds by the total `fee_value` per Design §5; value at payment time; **no fee atom for a fiat-quoted buy fee** — capitalized in step 6 instead).
- [ ] 6. Capitalize the fiat-denominated buy-side fee into the lot basis in `src/basis/lifecycle.rs` (crypto-quoted buy fees are **not** capitalized — the disposal-side fee atom covers them).
- [ ] 7. Update existing tests in `src/model/gains/tests.rs` and `src/basis/split.rs` (including the stale "Fees change the asset's cost basis when it is acquired" claim in the split.rs test doc comment).
- [ ] 8. Add new tests from the three fee-case fixtures (`lt-sale-st-fee`, `st-sale-lt-fee`, `margin-rollover`), asserting the exact numbers in the fixtures table.
- [ ] 9. `cargo check` + `cargo test` until green.

## Implementation plan (by file)

### `src/model/events.rs`

- Per steps 1–2 above.
- `EventAtom::from_fee_split` produces `proceeds = fee_value`, `net_gain = fee_value - fee_basis`; `fee_value` per Design §2. `net_gain` is classified exactly as `EventFee::from_split` does today: ST/LT by the fee coin's `basis_date` vs `event_date − 12 months`; US / bona-fide by `basis_date` vs `bona_fide_residency`.
- Income path: no fee atom on wallet receives (the fee is paid by the sender); the pending-spend fee basis on a deposit is a `Fee` atom on the deposit event (Findings #5).

### `src/model/gains.rs`

- `sums()`: one loop over `event_details`, dispatching on the variant (routing table, Design §3):
  - `Trade` atoms as today (trade cells).
  - `Income` atoms → `income` (proceeds only).
  - `Position` atoms → `position_proceeds`.
  - `Fee` atoms: `proceeds` → `trade_proceeds` **only** when the row's event subtype is `Trade` / `MarginSettle` (offsets the reduced trade atom); nowhere otherwise. `basis` / `net_gain` → the cells, always.
  - `InvestmentFee` atoms → the cells, plus `position_fees += proceeds` (positive `fee_value`).
  - The three detail fields collapse into `event_details`, so the three detail loops become one; drop the `tx_fees` / `position_fees` loops; drop the `trade_fees` / `transaction_fees` matrix fields and the "Trade Fee Basis" summary row (deliberate one-row change to the summary CSV shape; both fields are referenced only inside gains.rs).
- `apply_interest_expenses`: restructure to the positive convention (Design §4).
- `pr_statement24_dates`: the three detail iterations collapse into one over `event_details`, and the dedicated `tx_fees` / `position_fees` iterations are dropped. The loop calls `dates.update(&net_gain, event_date)` for every arm carrying a `GainTerm`: `Trade`, `Fee`, `InvestmentFee`. In `margin-rollover` all fee atoms are LT, so the LT dates come from them. The `Position` arm keeps today's `update_short` when `proceeds_bona_fide` is present. `PrStatement24::from_worksheet` (`src/model/gains.rs:1021-1079`) is unchanged; with real fee atoms, `sale_price` / `market_value` / `adjusted_basis` no longer go negative.
- Worksheet display: columns H–I (fee asset name from the row's `EventAtom::Fee` / `InvestmentFee` atoms) and the detail CSVs. `src/main.rs` writes five detail CSVs today — trade / income / position details via `trade_details()` / `income_details()` / `position_details()` (`src/main.rs:528/552/600`) and the two fee-detail CSVs via `tx_fees()` / `position_fees()` (`src/main.rs:576`, `src/main.rs:624`) — replace them with two accessors: a single `event_details()` writing one `{worksheet_name}-event-details.csv`, and a single `fee_details()` that filters `event_details` for the `Fee` / `InvestmentFee` variants; row shapes per the Recommended patch shape.

### `src/basis/split.rs`

Six fee call sites (today all `add_tx_fee` / `add_position_fee`):

| # | call site | event | new handling |
|---|-----------|-------|--------------|
| 1 | `release_poolasset_inner` fee branch, `add_tx_fee` (split.rs:1757) | trade sale, margin settlement, withdrawal | `Fee` atoms; for a sale / margin settlement, reduce the trade atoms' proceeds by the total `fee_value` (Design §5); a withdrawal event has no trade atom (the asset goes to `pending_withdrawals`), so there is nothing to reduce |
| 2 | `release_poolasset_inner` fee branch, `add_position_fee` (split.rs:1754) | margin open / rollover / close | `Fee` atoms (open / close) or `InvestmentFee` (rollover); no proceeds reduction (only margin-close rows have proceeds, and the position atom already books them in full) |
| 3 | `match_one_tx_inner` move branch (split.rs:293) | wallet move | `Fee` atom; proceeds nowhere |
| 4 | `match_one_tx_inner` loan-spend branch (split.rs:515) | loan-return spend (loan capital / collateral) | `Fee` atom; proceeds nowhere |
| 5 | `cleanup_pending_spend` (split.rs:1588) | wallet spend to a third party (declared rate) | `Fee` atom; proceeds nowhere; the fee is valued at the declared rate (Design §2) |
| 6 | `handle_deposit_inner` pending-spend fee (split.rs:1670) | deposit | `Fee` atom; proceeds nowhere |

- The trade amount and the fee are consumed in **two independent pool splits** (`release_poolasset_inner`), so the proceeds reduction must be applied once both atoms exist, or the fee-atom builder must adjust the trade atom directly.
- Update the `EventAtom::new()` reference in the tests `setup()` comment.
- `event_info.proceeds` (worksheet column I, the `ledger_proceeds` side of `assert_error_check`) stays the full outgoing **amount** with no fee reduction.

N.B. Worksheet column I *may* need to be shown with fee reduction. Depends on how this column is used when filing the tax return. Follow the plan as above; this note is for auditing purposes only.

### `src/basis/lifecycle.rs`

- Capitalize the **fiat-denominated** buy-side fee: in `get_exchange_rate_at_acquisition` for `TradeBuy`, when the buy is fiat-quoted (`row_out` is ZUSD, fee in ZUSD), change `a = row_out.amount.abs()` to `a = row_out.amount.abs() + row_out.fee` (both are in the quote currency, so they can be added directly). Example: the 2020 buy in `lt-sale-st-fee`: `a = 10.08 ZUSD` → 1 BTC basis $10.08.
- Do **not** add the fee when the buy is crypto-quoted (`row_out` is the crypto asset, e.g. XXBT in `XETHXXBT`): the fee coin is a capital asset and its disposal is booked as a fee atom on the disposal side (reducing the disposed asset's proceeds). Capitalizing it *and* booking the fee atom would double-count, and the fixtures expect the ETH basis to be **$1,000**, not $1,008.

## Recommended patch shape

Aim for a clean final codebase, not a minimal diff:

- **`EventAtom` as an enum, not a struct of options:**
  ```rust
  pub(crate) enum EventAtom {
      Trade         { asset_amount: KrakenAmount, proceeds: UsdAmount, net_gain: GainTerm },
      Income        { asset_amount: KrakenAmount, proceeds: UsdAmount },
      Position      { asset_amount: KrakenAmount, proceeds_us: UsdAmount, proceeds_bona_fide: Option<UsdAmount> },
      Fee           { asset_amount: KrakenAmount, proceeds: UsdAmount, net_gain: GainTerm },
      InvestmentFee { asset_amount: KrakenAmount, proceeds: UsdAmount, net_gain: GainTerm },
  }
  ```
  Inexpressible states become unrepresentable (an income atom with a `GainTerm`, a fee atom without one); `sums()` and the display code match on the variant; the variant identifies fee atoms for the detail CSVs and worksheet columns H–I.
- **One detail vector, one detail CSV:** `Event.trade_details` / `income_details` / `position_details` (and the `CapGainsWorksheetRow` mirrors) collapse into `event_details: Vec<EventAtom>`; the `CapGainsWorksheet::trade_details()` / `income_details()` / `position_details()` accessors and their `CapGainsTradeDetails` / `CapGainsIncomeDetails` / `CapGainsPositionDetails` display types are replaced by a single `event_details()` accessor and one event-detail CSV (an "Atom" column names the variant per row; per-variant columns are blank where a variant lacks them). The vector carries no information: routing is by variant + event subtype (Design §3), and a fee's former "atom home" was a function of the event subtype the event already stores.
- **Delete rather than zero out:** `EventFee`, `Event.tx_fees` / `position_fees`, `Event.has_interest_fees`, the matrix `trade_fees` / `transaction_fees` fields, the "Trade Fee Basis" summary row, and the `CapGainsWorksheet::tx_fees()` / `position_fees()` accessors (replace with a single `fee_details()` that filters `event_details` for the `EventAtom::Fee` / `InvestmentFee` variants).
- **Cap in the positive convention** (Design §4): internal only; `Display` renders the expense rows negated, as today.

## Findings (fee-rework branch, 2026-08-02 fixtures)

### Fixtures

Three Kraken-format scenarios under `fixtures/fee-cases/` (added in commit `5e08b7b`). Each has `kraken-ledgers.csv` + `kraken-trades.csv`. Prices: BTC = $1,000, ETH = $50 (0.05 BTC).

| case | what it exercises | expected |
| ---- | ----------------- | -------- |
| `lt-sale-st-fee` | FIFO sells the LT lot; the fee coin comes from the ST lot | LT gain $981.92 (proceeds $1,000 − $8 fee = $992, − $10.08 basis); ST loss $0.06 on the fee coin (proceeds $8, basis $8.06; exactly: proceeds $8.00, basis $8.064, gain −$0.064); ETH basis $1,000 |
| `st-sale-lt-fee` | FIFO sells the ST lot; the fee coin comes from the LT lot | ST loss $16 (proceeds $992, basis $1,008); LT gain $7.92 on the fee coin (proceeds $8, basis $0.08); ETH basis $1,000 |
| `margin-rollover` | Superset of `st-sale-lt-fee` + a margin position with fees paid in BTC from the remaining LT lot (0.992 BTC, basis $9.92 after the sale; 0.872 BTC, basis $8.72 after all margin fees) | Rollover payments dispose 0.1 BTC: proceeds $100, basis $1.00, LT gain $99.00; the $100 is investment interest expense (capped). Open-fee payment disposes 0.02 BTC: proceeds $20, basis $0.20, LT gain $19.80; the $20 is a nondeductible carrying cost |

Derived `margin-rollover` matrix totals (for the test assertions): LT `trade_gain` = 7.92 + 99.00 + 19.80 = **+$126.72**; the $100 rollover fee value caps it to **LT gain +$26.72, carryover $0**; ST = **−$16.00**. (All rollover/open fee atoms come from the LT lot, so the cap lands on the LT row only; the ST row's negative `gain_before` consumes no deduction because of the `.max(0)` clamp.)

The README's "Alternative defensible treatment" (capitalize the fee into the ETH basis) is to be **ignored**.

### Key mechanics verified against the code

1. **Buy-side fees are NOT capitalized into the lot basis today.**
   - `lifecycle_from_trade_buy` → `get_exchange_rate_at_acquisition` computes the basis from `row_out.amount.abs()` (the trade amount, **excluding** the fee) and the exchange rate.
   - For the 2020 buy in `lt-sale-st-fee`: `a = 10.00 ZUSD`, `b = 1.00 XXBT`, so the basis for 1 BTC is $10.00, **not** $10.08.
   - The README expects $10.08 (fiat fee capitalized). So the new code capitalizes the **fiat** buy-side fee (lifecycle.rs section above); a crypto-quoted buy fee is instead booked as a fee atom.
   - **And do not book a fee atom for the capitalized fee:** today the fiat fee coin (e.g. 0.08 ZUSD in 2020, 8.00 ZUSD in 2026) *is* booked via `add_tx_fee` as an `EventFee` loss (−$0.08 / −$8.00). Capitalizing it into the basis **and** keeping that fee atom would double-count, and the fee atom's $0.08 / $8.00 proceeds would unbalance `assert_error_check` (the buy event's `ledger_proceeds` is $0 because `row_out` is USD, so no trade atom exists for it).

2. **The ledger parser keeps `amount` and `fee` separate.**
   - `parse_lrt` (`src/model/ledgers/rows.rs:236-247`) does not add the fee to the amount.
   - In the fixtures, the `amount` column does **not** include the fee (e.g. ZUSD row: amount `-10.00`, fee `0.08`). The balance is `prev + amount - fee`.

3. **Sell-side fees are trade events.**
   - The fee on the sell (e.g. 0.008 BTC) is consumed from the pool via `release_poolasset` and becomes a fee atom. In the new design, it becomes an `EventAtom` with `proceeds = fee_value` and `net_gain = fee_value - fee_basis`.

4. **Margin open fee vs. rollover fee.**
   - Ledger type `"margin"` → `LedgerRow::Margin` → `snarf_matching_margin_row` → `LedgerParsed::MarginPositionOpen` (when the trade's misc does not contain `"closing"`) → `handle_margin_open` → `release_poolasset` with `has_interest_fees = true` → `add_position_fee`.
   - Ledger type `"rollover"` → `LedgerRow::Rollover` → `LedgerParsed::MarginPositionRollover` (direct mapping, no snarfing) → `handle_margin_rollover` → `release_poolasset` with `has_interest_fees = true` → `add_position_fee`.
   - Both margin open and rollover set `has_interest_fees = true` (see `Event::from_ledger_parsed`, `src/model/events.rs:208`), so today both become capped `position_fees` — wrong for the open fee, which the README says is a **nondeductible carrying cost** (the rollover fee **is** investment interest expense, capped).
   - **How:** the `Fee` / `InvestmentFee` variants (Design §4). In `apply_interest_expenses`, only the `InvestmentFee` (rollover) fees feed the cap. Both fee kinds are capital gains/losses on the fee coin (both have proceeds and basis), so both are margin-event atoms that book to the gain column.
   - In the `margin-rollover` fixture the margin row's `refid` equals the margin trade's `txid` (`ca5e03-dea15-000003`), but the trade's `misc` is empty (not `"closing"`), so it parses as `MarginPositionOpen`. The margin trade row itself (sell 100 BTC) produces no ledger trade rows and no trade event.

5. **Inflow fees: only the deposit's pending-spend fee basis.**
   - Wallet-receive income events never carry a fee: the on-chain fee is paid by the sender (discarding is correct), and `handle_deposit` asserts that a ledger deposit row's fee is zero (`src/basis/split.rs:1093` — authoritative; the assertion is kept).
   - The only recipient-paid fee on an inflow event is the pending-spend fee basis on a deposit (pool coin): when the user sends to Kraken from their own wallet, the on-chain fee is recorded on the pending spend, and `handle_deposit_inner` (`src/basis/split.rs:1669-1676`) books it via `add_tx_fee` (today: an `EventFee` loss of `-fee_basis`). New design: a `Fee` atom on the deposit event; the deposit event has no income atom (the coins enter the exchange balance) and its `event_info.proceeds` is $0, so the fee atom's proceeds go **nowhere** (routing them into the `income` column would break `assert_error_check`), and its basis / `net_gain` routes to the gain columns.
   - The income event itself is created by `event.add_income(basis.iter())` in the `match_one_tx_inner` macro (`src/basis/split.rs:477`) — the only `add_income` call site.

6. **The trade atom's definitional rate.**
   - `handle_trade` sets `event_info.asset_out_exchange_rate` from the trade CSV price (denominated in the quote asset, `TradeRow.price`) cross-referenced with the incoming asset's market rate: inverted for sells (`get_asset_pair` picks the direction). For the fixture `XETHXXBT` sell: price 0.05 XXBT inverted → 20 XETH × $50/XETH market rate = $1,000 per XXBT.
   - `event_info.proceeds` = full outgoing **amount** (no fee) × definitional rate — this is the `ledger_proceeds` side of `assert_error_check`.
   - When `row_out` is USD (fiat-quoted buy), neither `asset_out_exchange_rate` nor `proceeds` is set (proceeds stays $0).

7. **Margin / withdrawal events have no definitional rate.**
   - `asset_out_exchange_rate` is only set by `handle_trade` (and `handle_margin_close` sets the *in* rate). Margin open / rollover / withdrawal events leave it `None`, so their fee atoms must value `fee_value` at the fee asset's **market** rate at the event date from the exchange-rate DB (Design §2).

8. **`sums()` routing and the error check (today).**
   - `sums()` routes: `trade_details` → per-ST/LT×US/bona-fide `trade_proceeds` / `trade_basis` / `trade_gain`; `income_details` → `income` (proceeds only); `tx_fees` → `trade_fees` (net_gain only); `position_details` → `position_proceeds`; `position_fees` → `position_fees` (net_gain only, i.e. `sum(-fee_basis)` over all position fee atoms — negative).
   - `assert_error_check` (`src/model/gains.rs:867-885`): `ledger_proceeds` must equal `income` + `sum(trade_proceeds)` over the US and bona-fide, short-term and long-term, matrix cells + `sum(position_proceeds)` over the US-short and bona-fide-short cells. **`trade_fees` / `position_fees` are not in the check; `position_proceeds` is** — so fee-atom proceeds must not flow into `position_proceeds`.
   - The new routing for folded fee atoms is the Design §3 table.
   - `handle_margin_close` is the only caller of `event.add_position`, so a margin position left open (as in the fixture) contributes no `position_proceeds`.
   - The `transaction_fees` matrix field is never set (always zero).

9. **PR Statement-24 plumbing.**
   - `pr_statement24_dates` iterates `trade_details` (`detail.net_gain`), `tx_fees` (`fee.net_loss`), `position_details` (short only, when bona-fide proceeds exist), and `position_fees` (`fee.net_loss`). Once fee atoms live in `event_details` with a `GainTerm`, the single detail iteration must call `dates.update(&net_gain, event_date)` for the fee arms too (trade-fee, margin-fee, and deposit-fee dates); the `Position` arm keeps today's `update_short` for bona-fide borrowed proceeds. The dedicated `tx_fees` / `position_fees` iterations are deleted.
   - `PrStatement24::from_worksheet` (`src/model/gains.rs:1021-1079`) is unchanged; with real fee atoms, `sale_price` / `market_value` / `adjusted_basis` no longer go negative.
   - The `Sums` Display writes "Trade Fee Basis" (`trade_fees`) and "Interest Expense" (`position_fees`) summary rows; `src/main.rs` writes `CapGainsFeeDetails` CSVs from the `CapGainsWorksheet::tx_fees()` / `position_fees()` accessors.

10. **Test wiring: off-exchange basis + exchange rates.**
    - Off-exchange deposit basis is resolved by `handle_deposit_inner` via `basis_lookup.take_basis(&lrd.txid)` — the key is the deposit row's **txid** (first CSV column) — falling back to pending wallet spends. The fixtures carry no basis-lookup file, so the tests must supply it: `PoolAsset::from_basis_row(&BasisRow { synthetic_id, time, asset, amount, exchange_rate })` (note `from_basis_row` unwraps `amount`, so it must be `Some(…)`), pushed into the `basis_lookup` FIFO under the deposit's txid. For `st-sale-lt-fee` / `margin-rollover`: 1.00 BTC (`Some(1.00 XXBT)`), `time = 2020-05-15` (LT), `exchange_rate = "10"` ($10/BTC → basis $10.00), keyed `"ca5e02-1ed9e-000004"` / `"ca5e03-1ed9e-000004"` respectively.
    - Exchange-rate DB for tests: `ExchangeRates::from_raw(granularity, btc, chf, eth, ethw, eur, jpy, usdc, usdt)` (test-only constructor); each map is `BTreeMap<u64 unix-ts, UsdAmount>`; `get()` returns the **latest rate ≤ query time within `granularity` seconds** (existing tests use daily granularity `60*60*24 − 1`). USD/ZUSD never hits the DB (`KrakenAmount::get_exchange_rate` returns 1.0), so only BTC ($1,000) and ETH ($50) rates need seeding, on each event date: 2020-05-15, 2026-01-10, 2026-02-01, 2026-07-20, 2026-07-25, 2026-07-26 (e.g. one entry per day at 00:00 UTC).
    - Fixture tests should drive `State::resolve(wallets, ledgers, gain_config, trades, basis_lookup)` directly (`src/basis/split.rs:673`; `src/imports/kraken.rs` has the CSV readers; ledgers need `read_ledgers(...).parse(&trades)` to become `LedgerParsed`, as `src/main.rs:278` does) and then build the `CapGainsWorksheet` / `Sums` the same way `src/main.rs` does (`CapGainsWorksheet::new(events)`, `worksheet.sums()`, `sums.assert_error_check()`). This skips the CLI / checkpoint plumbing.

## Open questions

1. **Fee-detail CSV shape:** should the single `fee_details()` CSV use the trade-atom columns (Asset Name, Asset Amount, Proceeds, Net Capital Gains) or the old `CapGainsFeeDetails` columns (Asset Fee, Net Capital Loss)?
2. **Fee paid in a crypto `row_in` asset:** for a buy fee denominated in the acquired asset, the plan books a fee atom whose proceeds go nowhere and values the fee coin at its market rate at the event date. The fixtures do not cover this case. Is this treatment correct?

## Testing plan

- Update existing tests in `src/model/gains/tests.rs` and `src/basis/split.rs`.
- Add one test per fixture, asserting the exact numbers in the fixtures table above:
  - `lt-sale-st-fee`: LT gain $981.92, ST fee-coin gain −$0.064 (README rounds to "$0.06"), ETH basis $1,000.
  - `st-sale-lt-fee`: ST loss $16.00, LT fee-coin gain $7.92, ETH basis $1,000.
  - `margin-rollover`: LT `trade_gain` +$126.72 → LT gain **+$26.72** after the $100 cap, carryover $0; ST **−$16.00**.
- Wiring (Findings #10): supply the off-exchange deposit basis via the basis lookup for `st-sale-lt-fee` / `margin-rollover` (1.00 BTC @ $10, 2020-05-15, keyed by the deposit txid); seed BTC ($1,000) / ETH ($50) rates per event date; use `GainConfig { exchange_rates_db, bona_fide_residency: None }` (all gains US — the expected totals above assume no bona-fide split).
- The **ETH basis** assertion is made against the post-`resolve` state (the exchange-balance ETH lot: 20 ETH × $50 rate = $1,000), since it does not appear in the `Sums` matrix.
