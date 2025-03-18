//! An implementation of the bitcoind JSON RPC protocol. The main type is the [`Bitcoind`] client.

pub use self::tx::{Block, BlockHeader, Error, Status, Transaction};
pub use crate::esplora::{TxIn, TxOut};
use bitcoin::{BlockHash, Txid};
use http::{Request, Uri};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod tx;

const VERSION: &str = "2.0"; // JSON RPC version
const GET_BLOCK: &str = "getblock"; // get_block() JSON RPC method
const GET_BLOCK_HEADER: &str = "getblockheader"; // get_block_header() JSON RPC method
const GET_TX: &str = "getrawtransaction"; // get_tx() JSON RPC method

pub type Req = Request<String>;

/// The main bitcoind JSON RPC client.
#[derive(Clone, Debug)]
pub struct Bitcoind {
    req: Req,
    id: Arc<AtomicU64>,
}

/// JSON RPC request body for [`Bitcoind::get_block`].
#[derive(Clone, Debug, Deserialize, Serialize)]
struct BlockRpc<'req> {
    jsonrpc: &'req str,
    method: &'req str,
    params: BlockParams,
    id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BlockParams {
    blockhash: BlockHash,
    verbosity: u8,
}

/// JSON RPC request body for [`Bitcoind::get_block_header`].
#[derive(Clone, Debug, Deserialize, Serialize)]
struct BlockHeaderRpc<'req> {
    jsonrpc: &'req str,
    method: &'req str,
    params: BlockHeaderParams,
    id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BlockHeaderParams {
    blockhash: BlockHash,
    verbose: bool,
}

/// JSON RPC request body for [`Bitcoind::get_tx`].
#[derive(Clone, Debug, Deserialize, Serialize)]
struct TxRpc<'req> {
    jsonrpc: &'req str,
    method: &'req str,
    params: TxParams,
    id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TxParams {
    txid: Txid,
    verbose: u8,
}

impl Bitcoind {
    /// Bitcoind client constructor.
    ///
    /// The API endpoint string must be a valid [`Uri`].
    ///
    /// # Example
    ///
    /// ```
    /// # use esploda::bitcoind::Bitcoind;
    /// # fn main() -> anyhow::Result<()> {
    /// let esplora = Bitcoind::new("http://electrum.blockstream.info:60002")?;
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
        let req = Request::post(api).body(String::new())?;
        assert!(req.uri().scheme().is_some());
        assert!(req.uri().host().is_some());
        let id = Arc::new(AtomicU64::new(0));

        Ok(Self { req, id })
    }

    /// Get a block by [`BlockHash`].
    ///
    /// Returns a [`Req`] which can be sent by your preferred HTTP client.
    ///
    /// The response can be deserialized from JSON into a [`Block`].
    pub fn get_block(&self, blockhash: BlockHash) -> Req {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let params = BlockParams {
            blockhash,
            verbosity: 1,
        };
        let rpc = BlockRpc {
            jsonrpc: VERSION,
            method: GET_BLOCK,
            params,
            id,
        };

        let mut req = self.req.clone();
        req.body_mut()
            .push_str(&serde_json::to_string(&rpc).unwrap());

        req
    }

    /// Get a block header by [`BlockHash`].
    ///
    /// Returns a [`Req`] which can be sent by your preferred HTTP client.
    ///
    /// The response can be deserialized from JSON into a [`BlockHeader`].
    pub fn get_block_header(&self, blockhash: BlockHash) -> Req {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let params = BlockHeaderParams {
            blockhash,
            verbose: true,
        };
        let rpc = BlockHeaderRpc {
            jsonrpc: VERSION,
            method: GET_BLOCK_HEADER,
            params,
            id,
        };

        let mut req = self.req.clone();
        req.body_mut()
            .push_str(&serde_json::to_string(&rpc).unwrap());

        req
    }

    /// Get a [`Transaction`] by [`Txid`].
    ///
    /// Returns a [`Req`] which can be sent by your preferred HTTP client.
    ///
    /// The response can be deserialized from JSON into a [`Transaction`].
    pub fn get_tx(&self, txid: Txid) -> Req {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let params = TxParams { txid, verbose: 1 };
        let rpc = TxRpc {
            jsonrpc: VERSION,
            method: GET_TX,
            params,
            id,
        };

        let mut req = self.req.clone();
        req.body_mut()
            .push_str(&serde_json::to_string(&rpc).unwrap());

        req
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_block() {
        let client = Bitcoind::new("https://localhost:60002/").unwrap();
        let block_hash = "00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435";
        let req = client.get_block(block_hash.parse().unwrap());
        let uri = req.uri();

        assert_eq!(uri.scheme_str(), Some("https"));
        assert_eq!(uri.host(), Some("localhost"));
        assert_eq!(uri.port_u16(), Some(60002));
        assert_eq!(uri.path(), "/");
        assert!(uri.query().is_none());

        let rpc: BlockRpc = serde_json::from_str(req.body()).unwrap();

        assert_eq!(rpc.method, GET_BLOCK);
        assert_eq!(
            rpc.params.blockhash.to_string(),
            "00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435",
        );
        assert_eq!(rpc.params.verbosity, 1);
        assert_eq!(rpc.id, 0);
    }

    #[test]
    fn test_get_block_header() {
        let client = Bitcoind::new("https://localhost:60002/").unwrap();
        let block_hash = "00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435";
        let req = client.get_block_header(block_hash.parse().unwrap());
        let uri = req.uri();

        assert_eq!(uri.scheme_str(), Some("https"));
        assert_eq!(uri.host(), Some("localhost"));
        assert_eq!(uri.port_u16(), Some(60002));
        assert_eq!(uri.path(), "/");
        assert!(uri.query().is_none());

        let rpc: BlockHeaderRpc = serde_json::from_str(req.body()).unwrap();

        assert_eq!(rpc.method, GET_BLOCK_HEADER);
        assert_eq!(
            rpc.params.blockhash.to_string(),
            "00000000e2a1946e2c792aa8d763aea1ea70eb3561a484d6cc7a3116d404f435",
        );
        assert!(rpc.params.verbose);
        assert_eq!(rpc.id, 0);
    }

    #[test]
    fn test_get_tx() {
        let client = Bitcoind::new("https://localhost:60002/").unwrap();
        let txid = "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74";
        let req = client.get_tx(txid.parse().unwrap());
        let uri = req.uri();

        assert_eq!(uri.scheme_str(), Some("https"));
        assert_eq!(uri.host(), Some("localhost"));
        assert_eq!(uri.port_u16(), Some(60002));
        assert_eq!(uri.path(), "/");
        assert!(uri.query().is_none());

        let rpc: TxRpc = serde_json::from_str(req.body()).unwrap();

        assert_eq!(rpc.method, GET_TX);
        assert_eq!(
            rpc.params.txid.to_string(),
            "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74",
        );
        assert_eq!(rpc.params.verbose, 1);
        assert_eq!(rpc.id, 0);
    }

    #[test]
    fn test_empty_path() {
        let client = Bitcoind::new("http://localhost:60001").unwrap();
        let txid = "5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9";
        let req = client.get_tx(txid.parse().unwrap());
        let uri = req.uri();

        assert_eq!(uri.scheme_str(), Some("http"));
        assert_eq!(uri.host(), Some("localhost"));
        assert_eq!(uri.port_u16(), Some(60001));
        assert_eq!(uri.path(), "/");
        assert!(uri.query().is_none());

        let rpc: TxRpc = serde_json::from_str(req.body()).unwrap();

        assert_eq!(rpc.method, GET_TX);
        assert_eq!(
            rpc.params.txid.to_string(),
            "5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9",
        );
        assert_eq!(rpc.params.verbose, 1);
        assert_eq!(rpc.id, 0);
    }

    #[test]
    fn test_many_requests() {
        let client = Bitcoind::new("http://localhost:60001").unwrap();
        let txid = "5ad16406d77dfcb36c6a21290fc86771d038f08609efc40ddbf4a1bf2e9d80d9";

        let req = client.get_tx(txid.parse().unwrap());
        let rpc: TxRpc = serde_json::from_str(req.body()).unwrap();
        assert_eq!(rpc.id, 0);

        let req = client.get_tx(txid.parse().unwrap());
        let rpc: TxRpc = serde_json::from_str(req.body()).unwrap();
        assert_eq!(rpc.id, 1);

        let req = client.get_tx(txid.parse().unwrap());
        let rpc: TxRpc = serde_json::from_str(req.body()).unwrap();
        assert_eq!(rpc.id, 2);
    }
}
