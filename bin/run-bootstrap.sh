#!/bin/sh

#export RUST_LOG=trace
# error, warn, info (default), debug, or trace
#   https://docs.rs/log/latest/log/enum.Level.html

#export RELEASE='--release'

TIMESTAMP_DATE=`date +"%Y%m%d"`
INPUT_YEAR="2023"


echo "this is not a working example." && exit 1


cd $(git rev-parse --path-format=relative --show-toplevel)
mkdir -p runs/${RELEASE_RUNS} || exit 1

env RUST_BACKTRACE=1 cargo run ${RELEASE} --package bootstrap-checkpoint --                   \
    --input-bootstrap     references/bootstrap.csv                                            \
    --output-checkpoint   runs/${RELEASE_RUNS}tax${INPUT_YEAR}-$TIMESTAMP_DATE-checkpoint.ron \
    || exit 1
