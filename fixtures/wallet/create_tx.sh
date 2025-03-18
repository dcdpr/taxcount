#!/usr/bin/env bash
export ESPLORA_URL='http://localhost:3001'

echo 'tx: {' >mock_tx.ron

for txid in \
    7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74 \
    5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9 \
    54fd32320b7715d5a45f692af73b6c179be30392d67a04fba9110bf3436a1208 \
    940539548baeec9f761e1016b29347aeefe2803f6e3a4a14fadd859fd7076630 \
    aa8d28251c5594df72248dbe914208149fdf45a96fcebc78729cd4464fb00694 \
    a3eef08bef357e32d4a606a341538b578239e278b09e9198962b53757ca6ca1d \
; do
    cargo run -p esploda --example get_tx_cli "$txid" >>mock_tx.ron ;
done

echo '},' >>mock_tx.ron
