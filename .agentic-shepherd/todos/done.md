# Completed Issues

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
