#!/usr/bin/env bash
export ESPLORA_URL='http://localhost:3001'

echo 'blocks: {' >mock_blocks.ron

for block_hash in \
    00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435 \
    00000000f58b89500c0a7569ab6b9525a574df1f1ac8baf96905de92aad88dbd \
    0000000000000002bbe142a54b29e9d84c307586fa855a17004a19f312dad2c5 \
; do
    cargo run -p esploda --example get_blocks_cli "$block_hash" >>mock_blocks.ron ;
done

echo '},' >>mock_blocks.ron
