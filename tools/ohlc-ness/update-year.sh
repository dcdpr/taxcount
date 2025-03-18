#!/usr/bin/env bash

set -eu -o pipefail

# Usage: ./update-year [YEAR]
# YEAR defaults to the current year.
YEAR=${1:-$(date -u '+%Y')}

declare -A pairs=(
    ["btcusd"]="XXBTZUSD"
    ["ethusd"]="XETHZUSD"
    ["ethwusd"]="ETHWUSD"
    ["usdcusd"]="USDCUSD"
    ["usdtusd"]="USDTZUSD"
)

function has_dos2unix() {
    which dos2unix >/dev/null 2>&1 ; echo $?
}

function get_fiat_year() {
    PAIR="$1"
    YEAR="$2"

    SINCE=$(date -u -d "$YEAR-01-01 00:00:00" '+%s')
    END=$(date -u -d "$(($YEAR + 1))-01-01 00:00:00" '+%s')

    mkdir -p "./raw-data/fred-${PAIR}-${YEAR}"
    cargo -q run -p ohlc-ness -- --mode get_fiat --pair "$PAIR" \
        --since "$SINCE" --end "$END" --dir "./raw-data/fred-${PAIR}-${YEAR}/"
    if [ $(has_dos2unix) -eq 0 ] ; then
        dos2unix "./raw-data/fred-${PAIR}-${YEAR}/${PAIR}.ron"
    fi
    mv "./raw-data/fred-${PAIR}-${YEAR}/${PAIR}.ron" \
        "../../references/exchange-rates-db/daily-vwap/${YEAR}-fred-${PAIR}.ron"
}

function get_ohlc_year() {
    PAIR="$1"
    YEAR="$2"

    API_PAIR="${pairs[$PAIR]}"
    SINCE=$(date -u -d "$YEAR-01-01 00:00:00" '+%s')
    END=$(date -u -d "$(($YEAR + 1))-01-01 00:00:00" '+%s')

    mkdir -p "./raw-data/kraken-${PAIR}-${YEAR}"
    cargo -q run -p ohlc-ness -- --mode get_ohlc --pair "$API_PAIR" \
        --since "$SINCE" --end "$END" --dir "./raw-data/kraken-${PAIR}-${YEAR}/"
    cargo -q run -p ohlc-ness -- --mode taxcount \
        --since "$SINCE" --end "$END" --dir "./raw-data/kraken-${PAIR}-${YEAR}/"
    if [ $(has_dos2unix) -eq 0 ] ; then
        dos2unix "./raw-data/kraken-${PAIR}-${YEAR}/db.ron"
    fi
    mv "./raw-data/kraken-${PAIR}-${YEAR}/db.ron" \
        "../../references/exchange-rates-db/daily-vwap/${YEAR}-kraken-${PAIR}.ron"
}

get_fiat_year "chfusd" "$YEAR"
get_fiat_year "eurusd" "$YEAR"
get_fiat_year "jpyusd" "$YEAR"
get_ohlc_year "btcusd" "$YEAR"
get_ohlc_year "ethusd" "$YEAR"
get_ohlc_year "ethwusd" "$YEAR"
get_ohlc_year "usdcusd" "$YEAR"
get_ohlc_year "usdtusd" "$YEAR"
