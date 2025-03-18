## [rgrant 20231128 21:48 GMT] description of wallets (created using Electrum) and their intent

- our user owns wallets "a" and "b".
- they receive income from a faucet.
- they send testnet bitcoins to the infamous PJFargo exchange, then withdraw them
    (minus trading fees and losses).
- they buy ice cream (and then the whole shop).

- taxcount is likely to get data such as the following:

  - lists of transactions
    `electrum-history-testnet-wallet-a-2023.csv`
    `electrum-history-testnet-wallet-b-2023.csv`

  - xpubs / `% grep xpub *`
    ```
    testnet-wallet-a-20231128:        "xpub": "vpub5Vwo9xtdB77E1m21Wxyi2UuurxSMoKCv7xQs7zDHSPeR7RpGm1rqQgWT8jzmq8KNj3XwWw4Y7hWhZ6Q9Bhkh6U8tH6tbcdgrVo45iYpmH8t"
    testnet-wallet-b-20231128:        "xpub": "vpub5VFW5nRjMxMStjxyBSZQoGZeeiVWKK8UipiskvBBxJBGZhXwEP74riZigv9NKRthLZdKUQFLF6XT1u6CyX3Rgo8B3t5KTL4htYk7JgyUmyv"
    ```

## [rgrant 20241201 18:12 UTC] new scenario: "walletonly"

- see
    bin/taxcount-run-electrum-tests-walletonly.sh

- in this test, as desired in #86, taxcount is not given any exchange data.
    https://gl1.dcdpr.com/rgrant/taxcount/-/issues/86

- the references directory is shared, but the output directory is not.

- wallets "a" and "b" don't change (since they're using real testnet
  txs).

- we define the send that was previously to PJFargo as now a spend to
  some entity.  and the withdraw instead is considered as income from
  some other entity.  in the output CSVs, the available wallet notes
  are copied, which are misleading in these cases.

- this requires using a new tx-tags file.

- lastly, skip BONA_FIDE_DATE, for variety.
