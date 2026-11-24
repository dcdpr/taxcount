//! Command Line Interface.

use onlyargs_derive::OnlyArgs;
use std::path::PathBuf;

#[derive(Debug, OnlyArgs)]
#[footer = "Additional environment variables:"]
#[footer = "  - ADDRESS_GAP is the number of consecutive unused receiving addressees to search"]
#[footer = "      through for on-chain transactions. Default is 25."]
#[footer = "  - BITCOIN_NETWORK accepts {bitcoin (default), testnet, signet, regtest}"]
#[footer = "      https://docs.rs/bitcoin/latest/bitcoin/network/enum.Network.html"]
#[footer = "  - ESPLORA_URL accepts a http: or https: URL"]
#[footer = "      default is \"http://localhost:3000\""]
#[footer = "  - BITCOIND_URL accepts a http: or https: URL"]
#[footer = "      Selects the Bitcoind client if set, overriding any ESPLORA_URL setting"]
#[footer = "  - BITCOIND_CREDENTIALS accepts a `username:password` for HTTP Authorization"]
#[footer = "  - RAYON_NUM_THREADS sets the connection concurrency for the Esplora/Bitcoind client"]
#[footer = "      default is 32"]
#[footer = "  - TERM_COLOR accepts \"always\" to override automatic terminal sensing"]
pub struct Args {
    /// Read Kraken Ledger CSV from a file.
    #[long]
    pub input_ledger: Vec<PathBuf>,

    /// Read Kraken Trades CSV from a file.
    #[long]
    pub input_trades: Vec<PathBuf>,

    /// Read Basis Event Lookup CSV from a file.
    #[long]
    pub input_basis: Vec<PathBuf>,

    /// Read Tx Tags CSV from a file.
    #[long]
    pub input_tx_tags: Vec<PathBuf>,

    /// Read Generic Wallet history CSV from a file.
    #[long]
    pub input_wallet: Vec<PathBuf>,

    /// Read Electrum Wallet history CSV from a file.
    #[long]
    pub input_electrum: Vec<PathBuf>,

    /// Read LedgerLive Wallet history CSV from a file.
    ///
    #[long]
    pub input_ledgerlive: Vec<PathBuf>,

    /// An extended public key.
    ///   Primarily used for automatically claiming change addresses
    ///   in transactions. Also informs the blockchain analysis to
    ///   differentiate transactions between income and spends.
    ///
    #[long]
    pub input_xpub: Vec<String>,

    /// Read raw addresses from a file.
    ///   This is intended as an alternative to providing extended
    ///   public keys. Prefer using xPubs if they are available.
    ///
    #[long]
    pub input_addresses: Vec<PathBuf>,

    /// Read checkpoint from a file.
    #[long]
    pub input_checkpoint: Option<PathBuf>,

    /// Write checkpoint to a file.
    ///
    #[long]
    pub output_checkpoint: Option<PathBuf>,

    /// Date of US territory Bona Fide Residency Special Election.
    ///   This allows you to take a Special Election for allocating
    ///   gains by figuring the appreciation separately for your
    ///   territory and US holding periods.
    ///
    #[short('r')]
    pub bona_fide_residency: Option<String>,

    /// Override default OHLC-ness Exchange Rates database directory.
    ///   Default is "./references/exchange-rates-db/daily-vwap/".
    ///
    // TODO: DRY wrt exchange_rates_db constant.
    pub exchange_rates_db: Option<PathBuf>,

    /// Write worksheet CSVs to this output directory.
    #[short('o')]
    pub worksheet_path: Option<PathBuf>,

    /// Worksheet CSVs written to an output directory will be given
    ///   this prefix.
    #[short('p')]
    #[default("")]
    pub worksheet_prefix: String,

    /// Enable verbose output.
    /// Prints details CSV tables to stdout when not written to a file.
    pub verbose: bool,
}
