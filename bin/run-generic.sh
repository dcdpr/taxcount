# environment variables that may need to be set
#   (see also --help)
#
#   RUST_LOG=info
#     or error, warn, info (default), debug, trace
#       https://docs.rs/log/latest/log/enum.Level.html
#   TIMESTAMP_DATE=`date +"%Y%m%d"`
#   INPUT_CHECKPOINT_TIMESTAMP_DATE=`date +"%Y%m%d"`
#   RUN_YEAR=2022
#   INPUT_YEAR=2021
#   BONA_FIDE_DATE='2020-01-01 00:00:00 UTC'
#   BITCOIN_NETWORK=testnet                 <-- or try mainnet
#   ESPLORA_URL=http://localhost:3000       <-- optional
#   BITCOIND_URL=http://localhost:8332      <-- optional (overrides ESPLORA_URL)
#   BITCOIND_CREDENTIALS='bitcoin:swordfish'
#   RAYON_NUM_THREADS=1                     <-- simplify debugging
#   RELEASE='--release'
#   RELEASE_RUNS='release-runs/'

taxcount-run() {
    cd $(git rev-parse --path-format=relative --show-toplevel)
    mkdir -p runs/${RELEASE_RUNS} || exit 1
    if [ -n "${TIMESTAMP_DATE+x}" ]
    then
        if [ -n "${RUN_YEAR+x}" ] && [ -n "${INPUT_YEAR+x}" ]
        then
            if [ -z "${INPUT_CHECKPOINT_TIMESTAMP_DATE+x}" ]
            then
                INPUT_CHECKPOINT_TIMESTAMP_DATE=${TIMESTAMP_DATE}
            fi
            env RUST_BACKTRACE=1 cargo run ${RELEASE} --no-default-features --                                               \
                --exchange-rates-db   references/exchange-rates-db/daily-vwap/                                               \
                --input-ledger        references/ledgers-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                                 \
                --input-trades        references/trades-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                                  \
                --input-basis         references/basis-lookup-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                            \
                --input-ledgerlive    references/ledgerlive-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                              \
                --input-tx-tags       references/tx-tags-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                                 \
                --worksheet-path      runs/${RELEASE_RUNS}                                                                   \
                --worksheet-prefix    "tax${RUN_YEAR}-${TIMESTAMP_DATE}-"                                                    \
                --input-checkpoint    runs/${RELEASE_RUNS}tax${INPUT_YEAR}-${INPUT_CHECKPOINT_TIMESTAMP_DATE}-checkpoint.ron \
                --output-checkpoint   runs/${RELEASE_RUNS}tax${RUN_YEAR}-${TIMESTAMP_DATE}-checkpoint.ron                    \
                ;
        else
            echo "error: RUN_YEAR or INPUT_YEAR environment variable not available."
            exit 1
        fi
    else
        echo "error: TIMESTAMP_DATE environment variable not available."
        exit 1
    fi
}
