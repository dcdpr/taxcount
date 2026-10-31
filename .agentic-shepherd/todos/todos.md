# Issues Tracker (sorted by priority)

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
