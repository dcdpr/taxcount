# taxcount
Taxcount is open source software that helps Bitcoin and other crypto traders fill out US IRS tax worksheet Form 8949, "Sales and other Dispositions of Capital Assets", for 1040 Schedule D, "Capital Gains and Losses".  

You can point it at a local bitcoind or Esplora - it needs no cloud. 

Currently supported exchanges include Kraken.com.

Written 100% in Rust. 

On-chain binning is UTXO-level, and on-exchange binning is FIFO.           
