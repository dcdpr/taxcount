//! Environment variable handling.

use is_terminal::IsTerminal as _;
use std::{env, str::FromStr, sync::LazyLock};
use thiserror::Error;

// Environment variable parsing errors.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid TERM_COLOR
    #[error("Invalid TERM_COLOR")]
    InvalidTermColor,
}

/// Parsed environment variables.
pub static ENV: LazyLock<Env> = LazyLock::new(Env::parse_from_env);

/// Environment variables used throughout the program.
#[derive(Debug)]
pub struct Env {
    /// `AddressCache` maintains a minimum number of "gap" addresses for `Auditor::is_mine()`. This
    /// default value can be overridden with the `ADDRESS_GAP` environment variable. It cannot be
    /// set to a value less than 25.
    ///
    /// See [`AddressCache::is_mine`] for operation details.
    ///
    /// [`AddressCache::is_mine`]: crate::imports::address::AddressCache::is_mine
    pub address_gap: u32,
    /// Accepts {bitcoin (default), testnet, signet, regtest}
    /// https://docs.rs/bitcoin/latest/bitcoin/network/enum.Network.html
    pub bitcoin_network: Option<String>,
    /// Accepts an `http:` or `https:` URL. Default is "http://localhost:3000"
    pub esplora_url: Option<String>,
    /// Accepts an `http:` or `https:` URL.
    /// Selects the Bitcoind client if set, overriding any ESPLORA_URL setting.
    pub bitcoind_url: Option<String>,
    /// Accepts a `username:password` for HTTP Authorization.
    pub bitcoind_credentials: Option<String>,
    /// Sets the connection concurrency for the Esplora/Bitcoind client. Default is 32.
    pub rayon_num_threads: Option<usize>,
    /// Accepts `Always` to override automatic terminal sensing.
    pub term_color: TermColor,
}

impl Env {
    fn parse_from_env() -> Self {
        Self {
            address_gap: env::var("ADDRESS_GAP")
                .map(|value| {
                    value
                        .parse::<u32>()
                        .expect("Unable to parse ADDRESS_GAP env var")
                })
                .unwrap_or_default()
                .max(25),
            bitcoin_network: env::var("BITCOIN_NETWORK").ok(),
            esplora_url: env::var("ESPLORA_URL").ok(),
            bitcoind_url: env::var("BITCOIND_URL").ok(),
            bitcoind_credentials: env::var("BITCOIND_CREDENTIALS").ok(),
            rayon_num_threads: env::var("RAYON_NUM_THREADS").map_or_default(|num| num.parse().ok()),
            term_color: env::var("TERM_COLOR")
                .map_or_default(|color| color.parse().unwrap_or_default()),
        }
    }
}

/// Terminal coloring.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum TermColor {
    #[default]
    Auto,
    Always,
    Never,
}

impl FromStr for TermColor {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "always" => Ok(Self::Always),
            "never" => Ok(Self::Never),
            "auto" => Ok(Self::Auto),
            _ => Err(Error::InvalidTermColor),
        }
    }
}

impl TermColor {
    pub fn is_enabled(self) -> bool {
        match (self, std::io::stdout().is_terminal()) {
            (Self::Always, _) | (Self::Auto, true) => true,
            _ => false,
        }
    }
}
