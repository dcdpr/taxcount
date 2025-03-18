//! An [Esplora API] client, [sans I/O]. (Bring your own sync/async HTTP client!)
//!
//! This library handles the protocol-layer aspects of Esplora, including ser-de and
//! request-response abstractions.
//!
//! [Esplora API]: https://github.com/Blockstream/esplora/blob/master/API.md
//! [sans I/O]: https://sans-io.readthedocs.io/how-to-sans-io.html
//!
//! # Async example with `reqwest`
//!
//! ```no_run
//! use esploda::esplora::{Esplora, Transaction};
//! use reqwest::Client;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let client = Client::new();
//!     let esplora = Esplora::new("https://blockstream.info/testnet/api/")?;
//!     let txid = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74";
//!
//!     let resp = client.execute(esplora.get_tx(txid.parse()?).map(|_| "").try_into()?).await?;
//!
//!     let tx: Transaction = resp.json().await?;
//!
//!     println!("{tx:#?}");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Sync example with `ureq`
//!
//! ```no_run
//! use esploda::esplora::{Esplora, Transaction};
//!
//! fn main() -> anyhow::Result<()> {
//!     let agent = ureq::agent();
//!     let esplora = Esplora::new("https://blockstream.info/testnet/api/")?;
//!     let txid = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74";
//!
//!     let mut resp = agent.run(esplora.get_tx(txid.parse()?))?;
//!
//!     let tx: Transaction = resp.body_mut().read_json()?;
//!
//!     println!("{tx:#?}");
//!
//!     Ok(())
//! }
//! ```

#![forbid(unsafe_code)]

pub use bitcoin;
pub use chrono;
pub use http;
pub use rust_decimal;

#[cfg(feature = "bitcoind")]
pub mod bitcoind;

pub mod esplora;

pub type Req = http::Request<()>;

/// Append a path to the request.
pub(crate) fn append_path(req: &mut Req, path: String) {
    // The `http` crate has really bad ergonomics for updating paths.
    // SEE: https://github.com/hyperium/http/issues/594
    let req_uri = req.uri_mut();
    let mut uri_parts = req_uri.clone().into_parts();
    let root = req_uri.path();
    uri_parts.path_and_query = Some(format!("{root}{path}").parse().unwrap());
    *req_uri = http::Uri::from_parts(uri_parts).unwrap();
}
