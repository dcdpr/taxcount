# Ledgerlive Kraken fixture: margin trade on 2023-11-28

In this plan we will edit `references/ledgerlive-testnet-wallets/kraken-ledgers.csv`
and `kraken-trades.csv`. During this plan we will not change any code.

Today the testnet chain contains the trader's 0.00008 BTC send to Kraken and Kraken's
0.000065 BTC send back to wallet-a. In the edited fixture we must keep the Kraken
deposit at 0.00008 BTC at 2023-11-28 20:11:02, and the Kraken withdrawal at 0.000065
BTC at 20:18:00. The trader does everything else between those two times.

## Fee rates

| trade type             | rate               |
|------------------------|--------------------|
| BTC spot trade         | 0.25% of cost      |
| USDT/USD trade         | 0.20% of cost      |
| margin open            | 0.10% of cost      |
| margin rollover per minute | 0.10% of vol   |
| margin close           | 0.25% of cost      |
| BTC withdrawal         | 0                  |
| USD withdrawal         | 5.0000 USD flat    |
| deposit                | 0                  |

## Trade prices

We divide BTC/USD by EUR/USD to get BTC/EUR. We take EUR/USD from the 2023-11-28 rate in
`references/exchange-rates-db/daily-vwap/2023-yahoo-eurusd.ron`.

| date       | BTC/USD   | EUR/USD  | BTC/EUR   |
|------------|-----------|----------|-----------|
| 2023-11-28 | 5100000.0 | 1.096688 | 4650365.5 |

The trader closes the margin long at 4603860.0 BTC/EUR, about 1% below the open.

## Trader's actions

2023-11-28
- 20:11:02 The trader deposits 0.00008 BTC from wallet-a.
- 20:12:03 The trader sells 0.000002 BTC for EUR at market.
- 20:13:04 The trader buys back 0.00000197 BTC with EUR on a limit order and keeps 0.0933 EUR.
- 20:13:10 The trader opens a margin long of 0.0001 BTC on BTC/EUR at 5x.
- 20:13:10 Kraken takes the open fee from all 0.0933 EUR, then takes the rest of the fee in BTC.
- 20:14:05 The trader sells 0.000005 BTC for USD on a limit order.
- 20:14:10 Kraken takes the first rollover fee in BTC.
- 20:15:06 The trader requests a 10 USD withdrawal.
- 20:15:06 Kraken charges 5 USD for the withdrawal.
- 20:15:10 Kraken takes the second rollover fee in BTC.
- 20:16:00 The trader sells 0.0001 BTC to close the margin long.
- 20:16:00 Kraken takes the loss and the close fee in BTC.
- 20:16:07 The trader requests a 0.000065 BTC withdrawal back to wallet-a.
- 20:18:00 Kraken completes the BTC withdrawal.

2023-12-04
- 16:00:09 Kraken completes the USD withdrawal.

2023-12-08
- 10:10:21 The trader buys 10 USDT with USD at market.

## Fixture rows

| time     | action                 | vol BTC    | price     | cost          | fee                          | balance after               |
|----------|------------------------|------------|-----------|---------------|------------------------------|-----------------------------|
| 20:11:02 | deposit BTC            | 0.00008000 |           |               | 0                            | 0.00008000 BTC              |
| 20:12:03 | sell BTC/EUR, market   | 0.00000200 | 4650365.5 | 9.30073 EUR   | 0.02325 EUR                  | 0.00007800 BTC, 9.2774 EUR  |
| 20:13:04 | buy BTC/EUR, limit     | 0.00000197 | 4650365.5 | 9.16122 EUR   | 0.02290 EUR                  | 0.00007997 BTC, 0.0933 EUR  |
| 20:13:10 | margin open long       | 0.00010000 | 4650365.5 | 465.03655 EUR | 0.0933 EUR + 0.00000008 BTC  | 0.00007989 BTC, 0 EUR       |
| 20:14:05 | sell BTC/USD, limit    | 0.00000500 | 5100000.0 | 25.50000 USD  | 0.06375 USD                  | 0.00007489 BTC, 25.4362 USD |
| 20:14:10 | rollover               |            |           |               | 0.00000010 BTC               | 0.00007479 BTC              |
| 20:15:06 | request USD withdrawal |            |           | 10.0000 USD   | 5.0000 USD                   |                             |
| 20:15:10 | rollover               |            |           |               | 0.00000010 BTC               | 0.00007469 BTC              |
| 20:16:00 | margin close, sell     | 0.00010000 | 4603860.0 | 460.38600 EUR | 0.00000025 BTC, loss 0.00000101 BTC | 0.00007343 BTC       |
| 20:16:07 | request BTC withdrawal | 0.00006500 |           |               | 0                            |                             |
| 20:18:00 | withdraw BTC           | 0.00006500 |           |               | 0                            | 0.00000843 BTC              |
| Dec 4 16:00:09 | withdraw USD     |            |           | 10.0000 USD   | 5.0000 USD                   | 10.4362 USD                 |
| Dec 8 10:10:21 | buy USDT/USD, market | 10 USDT  | 1.0022    | 10.02200 USD  | 0.02004 USD                  | 0.3942 USD, 10 USDT         |

We round ledger EUR and USD values to 4 places. We round ledger BTC values to 8 places
and write them with 10. We round trade EUR and USD values to 5 places and vol to 8.

The margin open:
- Kraken charges 0.10% of 465.03655 EUR, which is 0.46504 EUR.
- The ZEUR ledger row contains amount 0, fee 0.0933, and balance 0.
- The XXBT ledger row contains amount 0 and fee 0.00000008 BTC. That BTC is worth the remaining 0.37174 EUR at 4650365.5.
- The trade row is a limit buy. It contains fee 0.46504, margin 93.00731, and an empty misc field. The margin is the cost divided by 5.

The margin close:
- The trader loses (4650365.5 - 4603860.0) * 0.0001 = 4.65055 EUR. At 4603860.0 that is 0.00000101 BTC.
- Kraken charges 0.25% of 460.38600 EUR, which is 1.15097 EUR. At 4603860.0 that is 0.00000025 BTC.
- The ZEUR ledger row contains amount 0, fee 0, and balance 0.
- The XXBT ledger row contains amount -0.00000101 and fee 0.00000025.
- The trade row is a limit sell. It contains fee 1.15097, margin 92.07720, and `closing` in misc.

Kraken charges each rollover at 0.10% of 0.0001 BTC, which is 0.0000001 BTC.

The USD withdrawal request row and the fulfilled row both contain amount -10.0000 and fee 5.0000.
