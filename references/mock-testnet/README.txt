## [rgrant 20240204 18:18 GMT] The mock testnet wallet

This wallet is using Taxcount's "manual input wallet format".
It has enough fields to supply all the usual data.

We used to add a TxType field on this format, but have realized that
such annotations should be in their own tx-tags input files, and
independent of the wallet type that we have the transaction from.

## [rgrant 20240220 17:55 UTC] When we don't have an xpub,

then it's the user's job to manually supply addresses that the Auditor
needs to know about.
(see `src/imports/wallet.rs:109:pub struct Auditor` ...)

All internal moves (ie. non-taxable) must be handled by teaching the
Auditor about wallet addresses that belong to the user.
