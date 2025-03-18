#!/bin/sh

# export RUST_LOG=trace
# export TERM_COLOR=always
TIMESTAMP_DATE=`date +"%Y%m%d"`
BONA_FIDE_DATE='2020-01-01 00:00:00 UTC'
export BITCOIN_NETWORK=testnet
#export BITCOIND_URL=http://localhost:18332
#export BITCOIND_CREDENTIALS='your_bitcoind_user:your_bitcoind_testnet_password'

cd $(git rev-parse --path-format=relative --show-toplevel)
mkdir -p runs/electrum-testnet/
if [ -n "${TIMESTAMP_DATE+x}" ] && [ -n "${BONA_FIDE_DATE+x}" ]
then
    REFERENCES="references/electrum-testnet"
    env RUST_BACKTRACE=1 cargo run --                                            \
        --verbose                                                                \
        --exchange-rates-db references/exchange-rates-db/daily-vwap/             \
        --input-ledger      ${REFERENCES}/kraken-ledgers.csv                     \
        --input-trades      ${REFERENCES}/kraken-trades.csv                      \
        --input-tx-tags     ${REFERENCES}/testnet-tx-tags.csv                    \
        --input-electrum    ${REFERENCES}/history-testnet-wallet-a-2023.csv      \
        --input-electrum    ${REFERENCES}/history-testnet-wallet-b-2023.csv      \
        --input-xpub        "vpub5Vwo9xtdB77E1m21Wxyi2UuurxSMoKCv7xQs7zDHSPeR7RpGm1rqQgWT8jzmq8KNj3XwWw4Y7hWhZ6Q9Bhkh6U8tH6tbcdgrVo45iYpmH8t" \
        --input-xpub        "vpub5VFW5nRjMxMStjxyBSZQoGZeeiVWKK8UipiskvBBxJBGZhXwEP74riZigv9NKRthLZdKUQFLF6XT1u6CyX3Rgo8B3t5KTL4htYk7JgyUmyv" \
        --worksheet-path    runs/electrum-testnet/                               \
        --worksheet-prefix  "$TIMESTAMP_DATE-"                                   \
        --output-checkpoint runs/electrum-testnet/$TIMESTAMP_DATE-checkpoint.ron \
        --bona-fide-residency "${BONA_FIDE_DATE}"                                \
        ;
else
    echo "error: TIMESTAMP_DATE or BONA_FIDE_DATE environment variable not available."
    exit 1
fi
