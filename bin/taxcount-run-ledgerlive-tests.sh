#!/bin/sh

# export RUST_LOG=trace
# export TERM_COLOR=always
TIMESTAMP_DATE=`date +"%Y%m%d"`
BONA_FIDE_DATE='2020-01-01 00:00:00 UTC'
export BITCOIN_NETWORK=testnet
#export BITCOIND_URL=http://localhost:18332
#export BITCOIND_CREDENTIALS='your_bitcoind_user:your_bitcoind_testnet_password'

cd $(git rev-parse --path-format=relative --show-toplevel)
mkdir -p runs/ledgerlive-testnet/
if [ -n "${TIMESTAMP_DATE+x}" ] && [ -n "${BONA_FIDE_DATE+x}" ]
then
    REFERENCES="references/ledgerlive-testnet-wallets"
    env RUST_BACKTRACE=1 cargo run --                                                     \
        --verbose                                                                         \
        --exchange-rates-db references/exchange-rates-db/daily-vwap/                      \
        --input-ledger      ${REFERENCES}/kraken-ledgers.csv                              \
        --input-trades      ${REFERENCES}/kraken-trades.csv                               \
        --input-tx-tags     ${REFERENCES}/testnet-tx-tags.csv                             \
        --input-ledgerlive  ${REFERENCES}/ledgerlive-history-testnet-wallet-20231128.csv  \
        --worksheet-path    runs/ledgerlive-testnet/                                      \
        --worksheet-prefix  "$TIMESTAMP_DATE-"                                            \
        --output-checkpoint runs/ledgerlive-testnet/$TIMESTAMP_DATE-checkpoint.ron        \
        --bona-fide-residency "${BONA_FIDE_DATE}"                                         \
        ;
else
    echo "error: TIMESTAMP_DATE or BONA_FIDE_DATE environment variable not available."
    exit 1
fi
