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

### [rgrant 20261031 22:26 UTC] 06 / Sums tables should show only US columns without a residency date
  - As a user, when I run taxcount without --bona-fide-residency, I expect the ledgers-sums and
    wallet-sums tables to show only the "US Long Term" and "US Short Term" columns.
  - Without --bona-fide-residency, taxcount writes the "Non-US Long Term" and "Non-US Short Term"
    columns, and each cell in these columns is empty.
  - Without --bona-fide-residency, the Non-US columns cannot contain data.
  - Without --bona-fide-residency, taxcount must not write the "Move-Date Value" row in these
    tables.
  - The "Move-Date Value" row is part of issue #4.

### [rgrant 20261031 22:43 UTC] 07 / Taxcount should emit a Form 4952 worksheet for investment interest
  - As a user, when a tax year contains investment interest expense from margin positions, I expect
    taxcount to write a Form 4952 worksheet with the values that taxcount knows.
  - Investment interest expense is the fee of each margin position rollover. It is a Form 4952
    deduction and is not part of the capital gain.
  - Form 4952 limits the deduction to net investment income. The limit applies to the whole tax
    return, so the user completes the lines with values from outside the exchange.
  - When the tax year contains investment interest expense of 0, taxcount does not write the
    worksheet.
  - At this time, the ledgers-sums table shows the interest expense and the limited interest
    expense as information only. The user copies values to Form 4952 by hand.
  - This issue is part of GitHub issue #3, "interest expense carryover into subsequent years". Form
    4952 calculates the disallowed expense that carries over into the next year.
  - This issue depends on issue #4, which removes the interest expense deduction from the "Gain"
    row of the ledgers-sums table.

### [rgrant 20261101 01:40 UTC] 08 / from_fee_split() fails to divide a fee coin gain into US and Non-US the same as a trade
  - As a bona fide resident, when I pay a fee after the move date with a fee coin that I got before
    the move date, I expect the gain of the fee coin in the US column up to the move-date value,
    and the rest in the Non-US column, the same as an EventAtom::Trade.
  - As a bona fide resident, when I pay a fee in USD, I expect the "Trade Basis" and "Trade
    Proceeds" of that fee in the column of the fee date.
  - At this time, from_fee_split() selects the column by the basis_date of the fee coin, and does
    not divide the gain into us and bona_fide.
  - For a USD PoolAsset with Origin::Base, BasisLifecycle::get_datetime() gives Utc::now(). As a
    result, from_fee_split() puts each USD EventAtom::Fee in the Non-US column, for each event
    date.
  - The total gain is equal to the total that the code gives at this time. The division between the
    US and Non-US columns changes.
