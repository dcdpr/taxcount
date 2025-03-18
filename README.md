# README

## What is Taxcount?

Taxcount is an open-source airgapped-network tax calculation tool
designed specifically for Bitcoin traders filing US tax returns.  It
generates data for IRS Form 8949 ("Sales and Other Dispositions of
Capital Assets") which feeds into Schedule D ("Capital Gains and
Losses") on your 1040.

In general, when using Taxcount, your accounting job is to know which
transactions were spends or labor income (which includes mining), and
collect the wallet and exchange exports.  If, in the tax year of
interest, you spent a
[UTXO](https://river.com/learn/terms/u/unspent-transaction-output-utxo/)
that was not labor income, then Taxcount also has facilities for
declaring its original [basis](https://www.irs.gov/taxtopics/tc703).

### Comprehensive Transaction Tracking

- **Multi-source integration** - process exchange trades and on-chain wallet transactions
- **Basis tracking** - follow the purchase price of your Bitcoin through any number of wallet transfers and exchange round-trips
- **Complete simulation** - runs a parallel model of all tax-year transactions across your entire Bitcoin ecosystem

### Key Features

* **No cloud dependency** - connect to local bitcoind or Esplora
* **Precise basis tracking** - with special attention to fees
* **Multiple transaction types** - supported:
  * Spending
  * Trading gains/losses
  * Margin Trading gains/losses
  * Mining income
  * Labor income
  * Lending activities
* **CSV outout** - for Form 8949 fields
* **Non-US residency** - Puerto Rico residency is supported

### Technical Advantages

- **100% Rust implementation** - no unsafe code
- **Local Blockchain caching** - retrieve blocks and transactions over the network exactly once
- **UTXO-level on-chain binning** - maximum accuracy
- **FIFO exchange binning** - as required
- **Handles Margin trades** - all trade types on the exchange are supported
- **Zero need for basis averaging** - tracks every detail for complete accuracy (and satisfies [IRS Rev. Proc. 2024-28](https://www.irs.gov/irb/2024-31_irb#REV-PROC-2024-28)
)
- **Flexible wallet support** - generic wallet format included (for bitcoind spends), as well as Electrum and Ledger Live wallet formats
- **International Currencies** - the archtecture is ready for trades in any fiat quote currency, and several are already included
- **Extensive Testing** - testing strategy includes generative simulated data, but see [limitations](#a-temporary-tragedy)

### Exchange Support

Currently reads exports from Kraken.com.

### Lighning Network

TBD

### Overview slides

See [Taxcount from 40,000 feet](doc/Taxcount-from-40kft.pdf) (PDF).

## Getting Started

### Help

`cargo run -- --help`

### Your bitcoind RPC connection

Unless you have set up Esplora, you will need a bitcoind with
`-txindex`.  See the [Bitcoind Server](#bitcoind-server) section,
below.

### Example invocation

Once your bitcoind can respond to testnet3 queries, try this test
script:

`./bin/taxcount-run-kraken-tests.sh`

## Bitcoin Backends

The Bitcoin backend is used to resolve Txids on the blockchain. The
supported backends are:

- Esplora (slightly more efficient) - configured with `ESPLORA_URL`
  environment variable, defaults to `http://localhost:3000`
- Bitcoind JSON-RPC - configured with `BITCOIND_URL` and
  `BITCOIND_CREDENTIALS` environment variables.  If `BITCOIND_URL` is
  set, it takes priority over Esplora. Unset it to use the Esplora
  backend.

### Bitcoind Server

Unless all your bitcoin transactions run throught your bitcoind, you
are going to want `-txindex` on your bitcoind.  That index takes some
time to build, but as of early 2025 only costs about 65GB of disk space.
The main downside from having it turned on is that you cannot also
`-prune`.

The bitcoind server requires a username and password, configured by
the `BITCOIND_CREDENTIALS='username:password'` environment variable.

Taxcount does not currently support local cookie authentication.  Note
that there is now a python script for generating the credentials:

https://raw.githubusercontent.com/bitcoin/bitcoin/master/share/rpcauth/rpcauth.py

If your bitcoind is remote, you will want to set up a local port using
ssh port reditrection.

After waiting for any local port redirection to be established, check
the status.  Here is an invocation on the testnet 18332 port:

```bash
curl --user "${BITCOIND_CREDENTIALS}"                                                           \
  --data-binary '{"jsonrpc": "1.0", "id":"curltest", "method": "getblockcount", "params": [] }' \
  -H 'content-type: application/json;'                                                          \
  http://127.0.0.1:18332/
```

#### If You Are Not Connected

If you have not set up any networking, then the network error will look like this:

```
Error: Wallet transaction resolution error
  Caused by: Client error
  Caused by: Esplora client error
  Caused by: Error requesting TxId `feceb335210ee31662a8f251cfac24b605b51db3d53d10f436470e5f473a6fa3`: io: Connection refused
```

### Bitcoin Backend Cache

The backend APIs can be rather slow to resolve a large number of transactions.
To avoid requesting the same information over and over, taxcount maintains
a request cache for the chosen backend.

After a successful taxcount invocation, the cache is written to the user's
cache directory (shown in the table below), and the client is initialized
with the cache at the start of each invocation.

The cache accumulates backend responses across all invocations.

Cache directories:

| Platform | Directory                                           |
|----------|-----------------------------------------------------|
| Linux    | `$XDG_CONFIG_HOME/.cache/taxcount`                  |
| macOS    | `$HOME/Library/Caches/design.contract.DCD.taxcount` |
| Windows  | `%LOCALAPPDATA%\DCD\taxcount\cache`                 |

One memo file will be created for each backend (depending on which backend is used):

- `esplora_memo.ron`: Esplora backend.
- `bitcoind_memo.ron`: Bitcoind backend.

We have an open ticket to let you relocate these to your favorite
encrypted volume, since they will leak toxic information about your
transaction history.  For now shuffling the files in and out of
encrypted storage is your job.

### Mock Client

The `struct MockClient` example in `src/imports/wallet/rs`
demonstrates how to connet other blockchain providers to Taxcount.

## Data Files

At the moment, input files are loaded relative to the current working
directory.  Run `cargo test` and `cargo run` from the project root.

If you introduce old skool UTXOs that have no associated exchange
trade data, then you need to supply taxcount with their basis
information using a bootstrapping process.

When you spend coins or receive UTXOs as income from either mining or
labor, mark the approprate transaction in the tx-tags file.

### Kraken Ledgers CSV

When you export historical data from Kraken, you can pick a "ledgers"
and a "trades" CSV (beware the PDF option they just made the default -
you need the CSV).  Taxcount primarily uses the ledger rows, since
that is more declarative regarding assets entering and leaving your
account.  However, some margin trades are underspecifed and also very
small amounts can cause Kraken to "helpfully" [/s] elide rows, which
further confuses parsing.  In order to resolve these matters, Taxcount
also refers back to the user's intent as recorded in the trades file.

The fields are:

    "txid","refid","time","type","subtype","aclass","asset","amount","fee","balance"

Since we standardized on those fields more have been added, but they don't affect parsing.

### Kraken Trades CSV

The fields are:

    "txid","ordertxid","pair","time","type","ordertype","price","cost","fee","vol","margin","misc","ledgers"

Since we standardized on those fields more have been added, but they don't affect parsing.

### Basis Information

Deposits to the exchanges from pre-existing UTXOs, as well as assets
directly sent to wallets from sources that Taxcount does not know
about, need Basis Information.

There is a critical distinction to make between solving basis problems
using the three tactical tools:
  - with bootstrapping for UTXOs existing before the first year you
    get a checkpoint from taxcount (see tools/bootstrap-checkpoint);
  - with `--input-tx-tags` as used for transactions in the current year; and
  - with the manual `--input-basis` override for special situations.

The only special situations we are aware of at the moment that might
require `--input-basis` are airdrops and accounting reconciliation for
UTXOs that you had lost and now don't want to feed back through
bootstrapping.  If you think you need it then clean up your act; get
away from airdrops and fix your bootstrap so you're not doing
reconciliations all the time.

### Wallet Information

You need to offer, at minimum, an xpub.

Unless you transferred from the exchange to your wallet first, you are
also responsible for offering some amount of Basis Information.

### Example Release Run

Once you have your files together, the command line is going to get
pretty busy.

```zsh
#!/usr/bin/env zsh
TIMESTAMP_DATE=`date +"%Y%m%d"`
RUN_YEAR=2020
CHECKPOINT_INPUT_YEAR=2019

mkdir -p references/test-runs || exit 1

env RUST_BACKTRACE=1 cargo run --no-default-features --                                         \
  --input-ledger references/ledgers-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                         \
  --input-trades references/trades-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                          \
  --input-tx-tags references/tx-tags-${RUN_YEAR}0101-${RUN_YEAR}1231.csv                        \
  --worksheet-path runs/                                                                        \
  --worksheet-prefix "tax${RUN_YEAR}-${TIMESTAMP_DATE}-"                                        \
  --input-checkpoint references/test-runs/checkpoint-$CHECKPOINT_INPUT_YEAR-$TIMESTAMP_DATE.ron \
  --output-checkpoint references/test-runs/checkpoint-$RUN_YEAR-$TIMESTAMP_DATE.ron             \
  ;
```

### Exchange Balance Checkpoints

If you are making a report for a year that started with any asset or
fiat balances already on the exchange, you will need to generate a
checkpoint file to load that state.  The initial checkpoint can come
from the bootstrap tool.  Run the program for all years that you had
any balances on the exchange, saving appropriate checkpoint files to
load as input for each successive year.

### Exchange Rates Database

Historical exchange rates are required for three purposes:

1. When the cost basis is not declared, the exchange rate at the time of
   asset acquisition is used as the cost basis.

2. US territory Bona Fide Residency Special Election rules allow the
   declaration of US-sourced and territory-sourced gains. The declared
   cost basis is used to calculate US-sourced gains. The historical
   exchange rate is used when a basis split must occur due to residency
   status changing since asset acquisition.

3. Trades that do not involve USD require a value in USD for the asset
   sold. The historical exchange rate is used to calculate the value at
   time of transaction.

The exchange rates are stored in a flat database of RON files, in the
`./references/exchange-rates-db/` directory. Each database is organized
by granularity. For now, we have the `daily-vwap` database with daily
granularity and Volume-Weighted Average Prices calculated.

An `hourly-vwap` database would provide finer-grained hourly prices.

Exchange rate lookups in the database are agnostic to database
granularity. The lookup simply finds the closest timestamp in the
database that appears before the query's date-time.

#### Creating an Exchange Rates DB

Creating a new DB is a two-step process using the `ohlc-ness` tool. The
[documentation](./tools/ohlc-ness/README.md) has some examples.

1. First, download the data. You have a choice of provider, currently
   Kraken and Bitstamp.

   - There is also a choice between OHLC and trades. OHLC is usually
     coarse grained and faster to download. Trades are very fine-grained
     (every single trade/transaction) and can take a very long time to
     download.

2. After downloading a complete range for an entire year for your trade
   pair, convert the raw data to a database with the `--mode taxcount`
   argument to `ohlc-ness`. The outputs can be copied to the reference
   database directory using the existing naming convention:

   `{PERIOD}-vwap/{YEAR}-{PROVIDER}-{TRADE_PAIR}.ron`

   - TBD: `--mode taxcount` is currently hardcoded to use a daily
     aggregation period. It is capable of hourly (and others), but would
     require some minor user interface changes to make it optional.

Finally, the `--exchange-rates-db` CLI argument can be passed to
`taxcount` to specify the database path.

## Background

### Incoming Data from Kraken

Taxcount's initial features support CSV export data from the
venerable Kraken (https://www.kraken.com/).  The exchange offers separate
records of (a) trades and (b) entries on its ledger.

The _Ledger_ entries are sufficient for calculating tax obligations,
but their low-level double-entry accounting format can be confusing
for people expecting something else.  _Orders_ are decisions by a
person, as may have been entered on the website.  Orders are not
included in any of the extracts available, other than implicitly by
their foreign key.  The _Trades_ are formatted closer to how one would
expect a user order to look, but there will still be many trades with
the same `ordertxid`.  That's because trades *record how the order was
executed*, which includes the execution engine splitting up the order
to match with the available counterparty lineup; such as how a large
buy will match with and execute against multiple counterparties
selling amounts smaller than the whole order.

All assets deposited on the exchange have a tax basis unknown to the
exchange, so there are facilities for making Taxcount aware of
historical tax basis.

### Internal Structure

Taxcount must merge trading activity with basis-tracked assets.  Its
code must communicate what it does in clear stages, to increase
confidence in reviews of the code.

#### Parsing Stages

Taxcount's types are strongly influence from initial fields offered by
the exchange's data export.  Original fields are not modified once
minimally parsed.  Instead, higher level types aggregate the prior
information with derived computation.

Despite the goal to not modify data on given ledger entries, there is
a need to perform basis computations using a FIFO ordering notion
foreign to any requirements inherent in the exchange's design.  The
simplest possible architectural join of these requirements was chosen,
using two constructions that will be described next: the `PoolAsset`
and `Event`.

#### `PoolAsset`

Let us consider the case of bitcoins.  On the exchange, there is only
one scalar value representing all the BTC.  However, in the world of
US taxes, it is the responsibility of the taxpayer to track separately
commodity assets that may have been purchased at different times and
with a different basis (ie. different unit price at acquisition).  Tax
rules consider these to be sold from the exchange in FIFO order.  So,
Taxcount logically maintains a FIFO ordering on all assets/UTXOs
deposited and asset purchases made on the exchange, using a structure
called `PoolAsset`.  Each purchase of BTC on the exchange appends a
`PoolAsset` to the back of the logical FIFO.  That tracking
information also maintains the basis of those trades so that continual
buying and selling does not result in `PoolAsset`s with unknown basis
information.

The `PoolAsset` FIFOs are stored in two logical models:

1. `State::exchange_balances` representing balances held within an
   exchange. Constructed from exchange transaction ledgers.
2. `State::on_chain_balances` representing the on-chain transactions for
   UTXO-based assets like BTC and account-based assets like ETH.
   Constructed from wallet transaction ledgers.

#### `Event`

Taxable events are inferred from the user's transaction ledger
history.  Suppose 1 BTC is purchased for $50,000.  This transaction is
not considered a taxable event under US tax law.  It sets the cost
basis for the 1 BTC asset acquired.  Now suppose 0.5 BTC is sold at a
later date for $30,000.  This is a taxable event (with capital gain of
$5,000).

This is the kind of information that the `Event` type is tracking.
Each `Event` references the parsed ledger row that spawned it, as well
as all cost basis information from `PoolAsset`s required to fulfill
the sell.  These are stored in the `Event::trade_details` field as a
vector of `EventTradeAtom`.

There is [a diagram](doc/CapGainsWorksheet_diagrams-new_way.png) that
shows how `EventTradeAtom`s are used in the code.

##### Splitting `PoolAsset`s

The 1 -> 0.5 BTC split from the example above is easy to represent in
the `Event` as a single `EventTradeAtom`.  A more complex scenario
arises when many smaller buys accumulate into a larger single sell
event.

Consider three transactions that purchase 0.2 BTC, 0.3 BTC, and 0.5
BTC.  Your BTC balance will have increased by 1 BTC.  When this 1 BTC
is sold in a single transaction, the `Event::trade_details` will
contain three `EventTradeAtom`s: one for each buy.

The most complex scenario involves multiple buys and multiple sells
with varying BTC amounts.  One count imagine many examples where the
`PoolAsset` received from a buy splits into smaller pieces over time
with several other trades (buys and sells) interleaved throughout the
ledger.  `EventTradeAtom` handles all scenarios gracefully by
splitting `PoolAsset`s.

This structure completely separates the concerns of FIFO rules from
the final report-generating loop.

### Gitver

A small "git version" utility is included that lists the exact
versions of all code files used to generate output.  This helps
reproduce prior answers with more debugging information if desired,
and helps track where now-known software bugs may have affected old
reports.

This creates a dependency on `git`, to be found somewhere in the path.

You can check input data files into a repository for similar
guarantees.

This can be slow on Windows, because we haven't switched to `gix` yet,
but it only runs in `--release` builds.  If you are submitting a bug
report, we will ask you for the hashes of the source files.

## Rebuilds

### Why does cargo always rebuild?

It's [Gitver](#gitver).

#### build.rs does this

```rust
gitver::cargotime_init();
```

#### find out more with this debug info

```zsh
CARGO_LOG=cargo::core::compiler::fingerprint=info cargo build
```

## Development and Testing

### A Temporary Tragedy

Due to path-dependent circumstances, in March 2025, we had some
imperfectly sanitized test data still in the repo moments before our
final push for release to Github.  In order to get the software to
other people before April 15 of this year, **we deleted all of the test
data with any kind of trade-id**.  That is almost all of the test data.

We are still perfecting the generative test data methods described
below.  There has been plenty of testing on live data and generated
data, and the test data will come back bigger and better than ever.

This is also why you will not see any history on Github.  In real
life, the first commit goes back to Sat Feb 19th, 2022.

### In General

Our testing strategy is split between unit testing with static test
[fixtures](./fixtures/) and property testing with dynamically
generated test data.  Tests can be filtered with a `prop_test_` prefix
to only run property tests, or a `test_` prefix to only run unit
tests.

Running only property tests across the entire workspace:

```zsh
cargo test --workspace prop_test_
```

Any test failures will include runtime logs captured by the
[`tracing-test`](https://docs.rs/tracing-test) crate.  When writing a new
test, be sure to include the `#[traced_test]` attribute on it.

Some (but not all) assertions in the test suite use the
[`similar-asserts`](https://docs.rs/similar-asserts) crate, which can be
configured with a few environment variables.  The most prominent of
these is `SIMILAR_ASSERTS_CONTEXT_SIZE` for increasing the unified
diff context.  This crate is usually only used when asserting the
equality of large structures, keeping failure noise to a minimum.

### Regression Testing

Property tests may occasionally discover bugs missed by previous runs.
[`arbtest`](https://docs.rs/arbtest/), which is used for stateful property
testing, supports regression testing through seeding its PRNG.  When a
property test fails, it will print a seed like this:

```text
arbtest failed!
    Seed: 0x6dd392720000eecc
```

This seed can be fed back to the failing property test to replay the
bug.  This seed was discovered by a real property test that found a
real bug (the bug was in the generator used by the property test).  It
is now used as a regression test like this:

```rust
// Run regression tests.
let regression_seeds = [0x6dd392720000eecc];
for seed in regression_seeds {
    arbtest(&test).seed(seed).size_max(entropy_max).run();
}
```

Where `test` is a closure that implements the property test.

This process allows reproducing the bug until it is fixed.  And
keeping it around as a regression test will ensure that the bug fix
does not regress over future development iterations.

### Property Testing with a Finite PRNG

`max_entropy` in the sample code above is unique to "the `arbtest`
way" of property testing.  The strategy employed by this crate is to
give your property test a finite amount of entropy that it can use for
producing random numbers.

It is easiest to think of finite entropy in property testing as
clamping the length of a sequence of random bytes.  This byte slice is
deterministically converted into a value of some type having
randomness.  The property test asserts that properties of the system
under test hold when given this randomized value.

Complicating matters, property testing typically involves generating
randomized _test cases_, not just randomized values.  The same PRNG,
thus the same pool of entropy, is used for generating both test cases
and values.

The amount of `entropy` starts small and grows as the test is
repeated.  This allows the code to be exposed to a wide range of
inputs, from very small data structures to the very large.

The only criticism we have of this approach to entropy is that
minimization does not work the same way as with traditional property
testing libraries.  `arbtest` interprets minimization to be the
smallest amount of entropy that reproduces the bug, while also
changing the seed.  Traditional interpretations consider mechanisms
like bisection of lists to find the smallest such list that exhibits
the bug.  The smallest string, the smallest integer, and so on.  This
difference in interpretation means that `arbtest` minimization is very
simple, but it cannot intelligently find optimally small
counterexamples.

The minimization strategy is described by the author in [Random Fuzzy
Thoughts](https://tigerbeetle.com/blog/2023-03-28-random-fuzzy-thoughts/#finite-prng).
And while it probably works exceptionally well for minimizing randomly
injected faults, it's really bad at minimizing randomly generated
values.  If a bug is only triggered by a rare value in a large and
complex data structure, this minimization strategy will not be able to
reduce the size of the counterexample.

#### Practical Property Testing in taxcount

Randomized values are easy to intuit, but randomized test cases
deserve some more exploration.  Such a test case might be a list of
operations to apply to the system under test.  Here is an example of a
test case for a simple "Book collection" CRUD interface:

```rust
// Operations that can be applied to the book collection.
enum Op {
    CreateBook {
        title: String,
        author: String,
    },
    ReadBook {
        book_id: u64,
    },
    UpdateBookTitle {
        book_id: u64,
        title: String,
    },
    UpdateBookAuthor {
        book_id: u64,
        author: String,
    },
    DeleteBook {
        book_id: u64,
    },
}

// Assume this vector is returned by a test case generator.
let generated_test = vec![
    Op::CreateBook { name: "Alice in Wonderland".into(), author: "Louis Carol".into(), },
    Op::UpdateBookTitle { book_id: 1, title: "Alice's Adventures in Wonderland".into(), },
    Op::ReadBook { book_id: 1, },
    Op::UpdateBookAuthor { book_id: 1, title: "Lewis Carroll".into(), },
    Op::DeleteBook { book_id: 1, },
];
```

In addition to the test cases, the generator also runs these
operations against a model of the system under test, using the model's
behavior to prove properties of the real system.  For instance, if the
generator produces a `DeleteBook` operation with a `book_id` that does
not exist, the model and system under test should agree on the outcome
of this operation.  Namely that an error is returned.

There exists a "Goldilocks zone" where the model cannot be too simple
nor too complex.  Too simple and the model will be inadequate for
testing the entire problem space.  Too complex and it will reinvent
the system a second time.

In taxcount's ledger parser property tests, there is an `ExchangeGen`
that generates a sequence of ledger rows (the input type for the
system under test) and simultaneously generates a sequence of expected
rows (this is the model providing the expected outcome for the
operations that produce ledger rows).

The operations selected by `ExchangeGen` are for generating values
rather than generating test cases.  Mostly this is because the parser
being tested is stateless.

We do not need much `ExchangeGen` complexity to test the parser, but
we want to reuse the generator for testing the resolver.  And for that
task, the generated ledger rows require: proper running balances;
balances must not run negative; you can only trade what you have;
multiple executions for a single order must allow for some slippage;
prices defined in a wallet needs to be reasonably similar to the
market rate; deposits need to come from a known wallet; and
withdrawals need to go to a known wallet.

There are several other invariants that must be considered, but this
gives a good high level overview of the system complexity that the
needs to be modeled.

## Fees

### Margin Trades

MarginPositionOpen and MarginRollover fees are investment interest
expenses.  IRS Form 4952 assigns a limit to how these are applied
(the deduction cannot exceed capital gains for the year).

### Other Fees

All the other fees are tracked and applied as capital losses in the
final summary.

- TradeSell
- Withdrawl
- MarginPositionClose
- MarginPositionSettle

## Historical Notes

### 2022-11-23 Major milestone reached: Taxcount's reports match ground truth data!

### 2022-12-01 Another major milestone reached: Taxcount's reports are ready for an internal release!

### 2024-02-23 Wallet feature merged into main.

### 2024-03-05 Bootstrap feature merged into main.

### 2025-03-18 Release to Github.

## Special Thanks

Special thanks to Jay at Blipjoy, who brought a new Rustacean along on
a much longer ride than anticipated.
