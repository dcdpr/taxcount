#!/usr/bin/env bash
# export RUST_LOG=trace
# export TERM_COLOR=always
TIMESTAMP_DATE=`date +"%Y%m%d"`
BONA_FIDE_DATE='2024-02-24 00:00:00 UTC'

cd $(git rev-parse --path-format=relative --show-toplevel)
mkdir -p runs/kraken-tests/
if [[ -v TIMESTAMP_DATE && -v BONA_FIDE_DATE ]]
then
    REFERENCES="references/kraken-tests"
    env RUST_BACKTRACE=1 cargo run --                                          \
        --verbose                                                              \
        --exchange-rates-db   references/exchange-rates-db/daily-vwap/         \
        --input-ledger        ${REFERENCES}/ledgers.csv                        \
        --input-trades        ${REFERENCES}/trades.csv                         \
        --input-basis         ${REFERENCES}/basis-lookup-test.csv              \
        --worksheet-path      runs/kraken-tests/                               \
        --worksheet-prefix    "$TIMESTAMP_DATE-"                               \
        --output-checkpoint   runs/kraken-tests/$TIMESTAMP_DATE-checkpoint.ron \
        --bona-fide-residency "${BONA_FIDE_DATE}"                              \
        ;
else
    echo "error: TIMESTAMP_DATE or BONA_FIDE_DATE environment variable not available."
    exit 1
fi
