# Issues Tracker (sorted by priority)

### [rgrant 20261114 23:58 UTC] 36 / Local issue numbers in commits link to unrelated GitHub issues
  - As a reader of the project on GitHub, when I see an issue number such as #4 in a commit
    message, I expect it to lead to the issue that the commit addresses, or to lead nowhere.
  - The project tracks issues locally with agentic-shepherd, which assigns its own issue numbers.
    GitHub assigns a separate sequence of numbers to its own issues.
  - GitHub turns every #N in a commit message into a link to GitHub issue N. When a commit cites a
    local issue number, GitHub shows a link to an unrelated GitHub issue, or to an issue that does
    not exist.
  - Example: commit f6ff27d cites local issues #4, #8, and #10.
  - Each new commit that cites a local issue number adds another wrong link. Changing an old commit
    message means rewriting history.
  - Reported as GitHub issue dcdpr/taxcount#36.

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

### [rgrant 20261031 22:26 UTC] 06 / Sums tables should show only US columns when no residency date
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

### [rgrant 20261114 20:57 UTC] 11 / SE Defects in src/model/events.rs
  - The file src/model/events.rs contains SE Defects in doc comments that issue 10 did not change.
  - SE Rule 1.1: "Worksheet name are typically either wallet names or exchange names." in the doc
    comment of Event::worksheet_name. Correct text: "The worksheet name is the name of a wallet or
    of an exchange."
  - SE Rule 1.1: "There is no cost basis, because the asset is loaned." in the doc comment of
    EventAtom::Position. Correct text: "A position atom contains proceeds only, because the user
    borrows the asset."
  - SE Rule 3.6: "the fee asset's market rate at the event date-time is used" in the doc comment of
    Event::add_fee(). Correct text: "add_fee() uses the market rate of the fee asset at the event
    date."
  - SE Rule 4.2: "Short-term, US-sources gains only." in the doc comment of GainTerm::ShortUs.
    Correct text: "A short-term gain term with a US portion only."

### [rgrant 20261115 18:23 UTC] 37 / SE Defects in src/model/gains.rs
  - The file src/model/gains.rs contains SE Defects in doc comments that issue 09 did not change.
  - SE Rule 4.2: "True when the atoms contain a sale: a trade atom or a position atom." in the doc
    comment of is_sale_event(). Correct text: "is_sale_event() returns true if the atoms include a
    trade atom or a position atom."
  - SE Rule 3.9: "The gains matrix stores intermediate gains values across the full
    term-length/residency matrix." in the doc comment of GainMatrix. Correct text: "A GainMatrix
    contains the four gain cells of the gain matrix."
  - SE Rule 4.2: "The address of one cell in the gains matrix." in the doc comment of CellId.
    Correct text: "A CellId is the identifier of one gain cell in the gain matrix."
  - SE Rule 3.9: "The sum of the fee values divides across the trade atoms pro-rata by amount" in
    the doc comment of fee_shares(). Correct text: "fee_shares() divides the sum of the fee values
    across the trade atoms by amount."

### [rgrant 20261115 18:23 UTC] 38 / SE Defects in src/model/gains/tests.rs
  - The file src/model/gains/tests.rs contains SE Defects in doc comments that issue 09 did not
    change.
  - SE Rule 4.2: "pr_statement24_dates() must collect dates from fee atoms in `event_details` , not
    only trade atoms." in the doc comment of pr_statement24_dates_from_fees_only(). Correct text:
    "pr_statement24_dates() collects the dates of the fee atoms and of the trade atoms in
    `event_details` ."
  - SE Rule 3.8: "A worksheet with fee-only bona fide activity must still produce dates for the PR
    Statement-24 row." in the doc comment of pr_statement24_dates_from_fees_only(). Correct text:
    "For a worksheet with bona fide fee atoms only, pr_statement24_dates() returns the dates for
    the PR Statement 24 row."
  - SE Rule 4.2: "A trade with two lots. The fee value divides across the lots pro-rata by amount."
    in the doc comment of trade_fee_cost_pro_rata_across_lots(). Correct text: "fee_shares()
    divides the fee value across the two trade atoms of a trade by amount."
