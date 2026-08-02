[rgrant 20260802 20:38 UTC] fee cases on an exchange

Three Kraken-format scenarios (kraken-ledgers.csv + kraken-trades.csv per
directory, fake IDs) covering the tax treatment of exchange fees paid in
crypto.  Prices throughout: BTC = $1,000, ETH = $50 (0.05 BTC).  Fee rates
are Kraken's published rates as of the July 2026 schedule: spot Tier-1
taker 0.80% (maker 0.40%), BTC-pair margin open 0.02%, margin rollover
0.02% per 4 hours.

Sources:
- https://www.kraken.com/features/fee-schedule
- https://support.kraken.com/articles/cross-platform-fee-tier-changes

## lt-sale-st-fee

- Lot 1 bought on-exchange 2020-05-15: 1 BTC for $10 plus $0.08 taker
  fee, basis $10.08 (long-term at sale).
- Lot 2 bought on-exchange 2026-01-10: 1 BTC for $1,000 plus $8 taker
  fee, basis $1,008 (short-term).
- 2026-07-20: buy 20 ETH for 1 BTC (XETHXXBT), taker fee 0.008 BTC ($8)
  charged in the quote currency, so 1.008 BTC leaves the account.
- FIFO: the 1 BTC sold is lot 1; the 0.008 BTC fee comes from lot 2.

Expected:
- Long-term gain $981.92: proceeds $1,000 minus $8 fee = $992, minus
  $10.08 basis.
- Short-term loss $0.06 on the fee coin: proceeds $8, basis $8.06.  The
  fee is not deductible separately; it already reduced the sale proceeds.
- ETH basis $1,000.
- Alternative defensible treatment: capitalize the fee into the ETH basis
  instead (long-term gain $989.92, ETH basis $1,008).

## st-sale-lt-fee

Same trade, lots reversed so FIFO sells the short-term coin:

- Lot 1 bought on-exchange 2026-01-10: 1 BTC for $1,000 plus $8 taker
  fee, basis $1,008 (short-term).
- Lot 2 deposited 2026-02-01 as BTC; acquired off-exchange 2020-05-15 for
  $10 (long-term).  The acquisition is not encoded in these CSVs; supply
  the basis via tx-tags / checkpoint when wiring tests.
- 2026-07-20: same 20-ETH trade, 0.008 BTC fee.

Expected:
- Short-term loss $16: proceeds $992, basis $1,008.
- Long-term gain $7.92 on the fee coin: proceeds $8, basis $0.08.
- ETH basis $1,000.

## margin-rollover

Superset of st-sale-lt-fee (same first eight ledger rows), then a margin
position with fees paid in BTC from the remaining long-term lot (0.992
BTC, basis $9.92):

- 2026-07-25 08:00: short 100 BTC on XXBTZUSD.  Margin open fee 0.02% =
  0.02 BTC ($20), ledger type "margin".  The opening trade's own volume
  fee is zeroed for simplicity.
- Five rollovers at 4-hour intervals, 0.02% of the $100,000 position =
  0.02 BTC ($20) each, ledger type "rollover", $100 total.
- The position is left open at the end of the fixture.

Expected:
- Rollover payments dispose 0.1 BTC: proceeds $100, basis $1.00,
  long-term gain $99.00.  The $100 charge itself is investment interest
  expense (Schedule A / Form 4952, capped at net investment income), or an
  ordinary business expense under trader tax status; it does not adjust
  any basis.
- Open-fee payment disposes 0.02 BTC: proceeds $20, basis $0.20,
  long-term gain $19.80.  The $20 fee is a nondeductible carrying cost
  for an investor (a business expense for a trader).

## Simplifications

- The motivating discussion traded BTC for DOGE, but KrakenAmount and
  trade_parse do not recognize XDG or any DOGE pair, so ETH via XETHXXBT
  keeps the fixtures parseable.  The tax logic is identical.
- Acquisitions are market orders charged the taker rate (0.80%), and the
  fee is capitalized into the lot basis: $10.08 and $1,008.
- Margin and rollover fees are charged in XXBT (the scenario holds no
  USD).  Rollover rows carry their own refids, distinct from the opening
  trade, so the margin open row is not mistaken for half of a close pair.
- IDs mimic Kraken's 6-5-6 hex format but are fake: "ca5e0N" encodes the
  case number, "1ed9e" ledger rows, "dea15" trades, "de905" deposits,
  "9011e" rollovers, "0bd3b" orders.
