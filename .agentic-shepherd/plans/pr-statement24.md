# Plan: PR Statement-24 CSV Report (Issue #2)

## Overview

Emit a single `{worksheet_prefix}pr-statement24.csv` automatically when
`--bona-fide-residency` is set.  Rows are the cross product of
worksheet_name x (LT, ST).  Each row reads directly from that
worksheet's own Sums — no aggregation across worksheets.

The actual name of the form is:
  "F1 Part III statement 24 :: capital gains"

## Data Flow

```
Existing per-worksheet loop already computes Sums for each worksheet.

New work in the loop (dates only):
  -> scan trade_details for earliest basis_date and latest event_date
     per (worksheet, LT/ST)

After loop:
  -> for each (worksheet, LT/ST) with bona_fide data:
       read A-F directly from that worksheet's Sums
       combine with collected dates
  -> emit single {worksheet_prefix}pr-statement24.csv
```

## Steps

### Step 1: Add PrStatement24Row and PrStatement24 structs to src/model/gains.rs

```rust
pub struct PrStatement24Row {
    description: String,                // "investment assets {worksheet_name} LT" etc.
    date_acquired: DateTime<Utc>,      // earliest basis_date
    date_sold: DateTime<Utc>,          // latest event_date
    sale_price: UsdAmount,             // A = bona_fide trade_proceeds
    market_value: UsdAmount,           // B = A - F
    adjusted_basis: UsdAmount,         // C = A - D
    gain_or_loss: UsdAmount,           // D = E + F
    us_gain: UsdAmount,                // E = gains_us
    pr_gain: UsdAmount,                // F = gains_bona_fide
}

pub struct PrStatement24 {
    rows: Vec<PrStatement24Row>,
}
```

### Step 2: Implement Display for PrStatement24

CSV header:
```
"Property Description","Date Acquired","Date Sold","A: Sale Price","B: Market Value","C: Adjusted Basis","D: Gain or Loss","E: US-Sourced Gain","F: PR-Sourced Gain"
```

Each row emits one CSV line.

### Step 3: Collect trade dates in the existing worksheet loop

During the existing per-worksheet loop in main.rs, after computing
Sums (which already happens):

1. Scan trade_details only for dates:
   - GainPortion::basis_date → track earliest per (worksheet, LT/ST)
   - EventInfo::event_date → track latest per (worksheet, LT/ST)
   - The GainTerm variant tells us LT vs ST.

The Sums values (trade_proceeds, gains_us, gains_bona_fide) are
read directly from the already-computed per-worksheet Sums.

### Step 4: Compute A-F and build rows

For each (worksheet, LT/ST) that has bona_fide data in its Sums:

- E = sums.gains_us_{long,short}
- F = sums.gains_bona_fide_{long,short}
- D = E + F
- A = sums.gain_matrix.bona_fide_{long,short}.trade_proceeds
- B = A - F
- C = A - D

Combine with collected dates (earliest basis_date, latest event_date).
Skip rows where there is no bona_fide data.

### Step 5: Emit in main.rs after the worksheet loop

After the existing `sums.assert_error_check()` loop, if bona fide
residency is set, build and emit the PrStatement24:

- To file: `{worksheet_prefix}pr-statement24.csv` in worksheet_path
- To stdout: if no worksheet_path

### Step 6: Write unit test

Add a test in src/model/gains/tests.rs that:
- Constructs a Sums with simple round numbers directly
- Provides dates for one worksheet ("test-exchange") with LT and ST

Test data:

LT row:
  trade_proceeds=1000, gains_us=200, gains_bona_fide=300
  -> A=1000, B=700, C=500, D=500, E=200, F=300
  basis_date=2020-01-15, event_date=2024-12-15

ST row:
  trade_proceeds=500, gains_us=50, gains_bona_fide=100
  -> A=500, B=400, C=350, D=150, E=50, F=100
  basis_date=2024-03-01, event_date=2024-11-20

Remaining Sums fields zeroed out (not consumed by Statement-24).
Uses insta snapshot on the CSV output.

## Key Design Decisions

- No new CLI args; output gated on --bona-fide-residency
- Rows with no bona_fide gains are omitted (nothing to report)
- A-F values read directly from per-worksheet Sums (no re-accumulation)
- Only dates require scanning Events in the loop
- Dates are derived from actual trade data, not user input
- Property description uses worksheet_name (e.g., "investment assets kraken LT")
- The elisp validation (A,B,C >= 0 and computed E,F match) becomes
  a debug_assert in the Rust code

## Files Modified

- `src/model/gains.rs` — new structs, Display impl, builder
- `src/main.rs` — collect data during worksheet loop, emit after
- `src/model/gains/tests.rs` — unit test with insta snapshot
