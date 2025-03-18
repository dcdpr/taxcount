//! An implementation of the Esplora protocol. The main type is the [`Esplora`] client.

pub use self::tx::{Error, Status, Transaction, TxIn, TxOut};
use crate::{append_path, Req};
use bitcoin::{BlockHash, Txid};
use http::{Request, Uri};

mod tx;

/// The main Esplora client.
#[derive(Clone, Debug)]
pub struct Esplora {
    req: Req,
}

impl Esplora {
    /// Esplora client constructor.
    ///
    /// The API endpoint string must be a valid [`Uri`].
    ///
    /// # Example
    ///
    /// ```
    /// # use esploda::esplora::Esplora;
    /// # fn main() -> anyhow::Result<()> {
    /// let esplora = Esplora::new("https://blockstream.info/testnet/api/")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// This function asserts that the API server URL has both a scheme and host component. This
    /// disallows the use of relative URIs like `/hello/world` and non-network URIs like `data:` and
    /// `mailto:`.
    pub fn new<U>(api: U) -> Result<Self, http::Error>
    where
        U: TryInto<Uri>,
        <U as TryInto<Uri>>::Error: Into<http::Error>,
    {
        let req = Request::get(api).body(())?;
        assert!(req.uri().scheme().is_some());
        assert!(req.uri().host().is_some());

        Ok(Self { req })
    }

    /// Get a list of [`Txid`]s by [`BlockHash`].
    ///
    /// Returns a [`Req`] which can be sent by your preferred HTTP client.
    ///
    /// The response can be deserialized from JSON into a `Vec<Txid>`.
    ///
    /// [`Txid`]: bitcoin::Txid
    pub fn get_block_txids(&self, hash: BlockHash) -> Req {
        let mut req = self.req.clone();
        append_path(&mut req, format!("block/{hash}/txids"));

        req
    }

    /// Get a [`Transaction`] by [`Txid`].
    ///
    /// Returns a [`Req`] which can be sent by your preferred HTTP client.
    ///
    /// The response can be deserialized from JSON into a [`Transaction`].
    pub fn get_tx(&self, txid: Txid) -> Req {
        let mut req = self.req.clone();
        append_path(&mut req, format!("tx/{txid}"));

        req
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_block_txids() {
        let client = Esplora::new("https://blockstream.info/testnet/api/").unwrap();
        let block_hash = "00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435";
        let req = client.get_block_txids(block_hash.parse().unwrap());
        let uri = req.uri();

        assert_eq!(uri.scheme_str(), Some("https"));
        assert_eq!(uri.host(), Some("blockstream.info"));
        assert_eq!(
            uri.path(),
            "/testnet/api/block/00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435/txids"
        );
        assert!(uri.query().is_none());
    }

    #[test]
    fn test_get_tx() {
        let client = Esplora::new("https://blockstream.info/testnet/api/").unwrap();
        let txid = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74";
        let req = client.get_tx(txid.parse().unwrap());
        let uri = req.uri();

        assert_eq!(uri.scheme_str(), Some("https"));
        assert_eq!(uri.host(), Some("blockstream.info"));
        assert_eq!(
            uri.path(),
            "/testnet/api/tx/7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74"
        );
        assert!(uri.query().is_none());
    }

    #[test]
    fn test_empty_path() {
        let client = Esplora::new("http://localhost:3001").unwrap();
        let txid = "5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9";
        let req = client.get_tx(txid.parse().unwrap());
        let uri = req.uri();

        assert_eq!(uri.scheme_str(), Some("http"));
        assert_eq!(uri.host(), Some("localhost"));
        assert_eq!(uri.port_u16(), Some(3001));
        assert_eq!(
            uri.path(),
            "/tx/5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9"
        );
        assert!(uri.query().is_none());
    }
}
