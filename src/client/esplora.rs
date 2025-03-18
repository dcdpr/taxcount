use super::{BlockResult, ClientError, TxResult};
use bdk::bitcoin::{BlockHash, Txid};
use esploda::esplora::{Esplora, Transaction};
use fett::Fett;
use rayon::{prelude::*, ThreadPool};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Instant};
use thiserror::Error;
use tracing::{info, trace};
use ureq::tls::{TlsConfig, TlsProvider};
use ureq::Agent;

#[derive(Debug, Error)]
pub enum EsploraClientError {
    #[error("Invalid Esplora URI")]
    EsploraUri(#[from] esploda::http::Error),

    #[error("Thread Pool error")]
    ThreadPool(#[from] super::PoolError),
}

#[derive(Clone, Debug, Error)]
pub enum EsploraError {
    #[error("Error requesting TxId `{0}`: {1}")]
    Tx(Txid, String),

    #[error("Error requesting BlockHash `{0}`: {1}")]
    Block(BlockHash, String),
}

type BlockFetcher = Box<dyn Fn(&BlockHash) -> BlockResult + Sync>;
type TxFetcher = Box<dyn Fn(&Txid) -> TxResult + Sync>;

/// A simple, concurrent Esplora client.
pub struct EsploraClient {
    pool: ThreadPool,

    // Protocol caches
    block_cache: Fett<BlockHash, BlockResult, BlockFetcher>,
    tx_cache: Fett<Txid, TxResult, TxFetcher>,
}

/// Memoized responses from a [`EsploraClient`].
#[derive(Debug, Deserialize, Serialize)]
pub struct EsploraClientMemo {
    block_cache: Vec<(BlockHash, Vec<Txid>)>,
    tx_cache: Vec<(Txid, Transaction)>,
}

/// This macro is similar to [`std::try`] or the `?` operator. Its specific purpose is to wrap the
/// `Result` in `Arc` when it early-returns from the enclosing scope.
macro_rules! try_arc_result {
    ($expr:expr, $variant:ident, $txid:expr) => {
        match $expr {
            Ok(value) => value,
            Err(err) => {
                return Arc::new(Err(ClientError::Esplora(EsploraError::$variant(
                    $txid,
                    err.to_string(),
                ))))
            }
        }
    };
}

impl EsploraClient {
    /// Create a new Esplora client with the provided API server URI.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), taxcount::client::esplora::EsploraClientError> {
    /// # use taxcount::client::esplora::EsploraClient;
    /// let client = EsploraClient::new("https://blockstream.info/testnet/api/")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(api_server: &str) -> Result<Self, EsploraClientError> {
        let (pool, block_fetcher, tx_fetcher) = Self::new_inner(api_server)?;

        Ok(Self {
            pool,
            block_cache: Fett::new(block_fetcher),
            tx_cache: Fett::new(tx_fetcher),
        })
    }

    // Create a new Esplora client with the provided API server URI and memoized responses.
    pub fn from_memo(
        api_server: &str,
        memo: EsploraClientMemo,
    ) -> Result<Self, EsploraClientError> {
        let (pool, block_fetcher, tx_fetcher) = Self::new_inner(api_server)?;

        // Transform `(K, V)` to `(K, Arc<Result<V, E>>)`
        let block_cache = memo
            .block_cache
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Ok(v))))
            .collect::<Vec<(_, Arc<Result<_, _>>)>>();
        let tx_cache = memo
            .tx_cache
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Ok(v))))
            .collect::<Vec<(_, Arc<Result<_, _>>)>>();

        Ok(Self {
            pool,
            block_cache: Fett::from((block_fetcher, block_cache)),
            tx_cache: Fett::from((tx_fetcher, tx_cache)),
        })
    }

    /// Convert this Esplora client into its inner memoized responses.
    pub fn into_memo(self) -> Result<EsploraClientMemo, ClientError> {
        let (_, _, block_cache) = self.block_cache.into_inner();
        let (_, _, tx_cache) = self.tx_cache.into_inner();

        // Transform `(K, Arc<Result<V, E>>)` to `(K, V)`
        let block_cache = block_cache
            .into_iter()
            .map(|(k, v)| {
                let v = Arc::try_unwrap(v).unwrap_or_else(|v| v.as_ref().clone())?;
                Ok((k, v))
            })
            .collect::<Result<_, ClientError>>()?;
        let tx_cache = tx_cache
            .into_iter()
            .map(|(k, v)| {
                let v = Arc::try_unwrap(v).unwrap_or_else(|v| v.as_ref().clone())?;
                Ok((k, v))
            })
            .collect::<Result<_, ClientError>>()?;

        Ok(EsploraClientMemo {
            block_cache,
            tx_cache,
        })
    }

    fn new_inner(
        api_server: &str,
    ) -> Result<(ThreadPool, BlockFetcher, TxFetcher), EsploraClientError> {
        let (num_threads, pool) = super::create_thread_pool()?;

        let agent = Agent::from(
            Agent::config_builder()
                .max_idle_connections_per_host(num_threads)
                .tls_config(
                    TlsConfig::builder()
                        .provider(TlsProvider::NativeTls)
                        .build(),
                )
                .build(),
        );
        let esplora = Arc::new(Esplora::new(api_server)?);

        let tx_fetcher = {
            let agent = agent.clone();
            let esplora = esplora.clone();
            Box::new(move |key: &_| fetch_tx(&agent, &esplora, key)) as TxFetcher
        };
        let block_fetcher =
            Box::new(move |key: &_| fetch_block(&agent, &esplora, key)) as BlockFetcher;

        Ok((pool, block_fetcher, tx_fetcher))
    }

    pub(crate) fn get_transactions(&self, txids: &[Txid]) -> HashMap<Txid, TxResult> {
        self.pool.in_place_scope(|_scope| {
            txids
                .par_iter()
                .map(|txid| (*txid, self.tx_cache.get(*txid)))
                .collect()
        })
    }

    pub(crate) fn get_blocks(&self, block_hashes: &[BlockHash]) -> HashMap<BlockHash, BlockResult> {
        self.pool.in_place_scope(|_scope| {
            block_hashes
                .par_iter()
                .map(|block_hash| (*block_hash, self.block_cache.get(*block_hash)))
                .collect()
        })
    }
}

/// This is the Transaction constructor for the memoizing Esplora client.
/// It does error handling in a special way because the return value needs to be wrapped in `Arc`.
/// See `try_arc_result!()` for info on how errors are handled.
fn fetch_tx(agent: &Agent, esplora: &Arc<Esplora>, txid: &Txid) -> TxResult {
    let thread_id = std::thread::current().id();
    let txid = *txid;

    info!("Fetching TxId `{txid}` on {thread_id:?}");

    let start = Instant::now();
    let req = esplora.get_tx(txid);
    let mut resp = try_arc_result!(agent.run(req), Tx, txid);
    let tx: Transaction = try_arc_result!(resp.body_mut().read_json(), Tx, txid);
    let dur = start.elapsed();

    info!("TxId `{txid}` received in {dur:?}");
    trace!("{tx:#?}");

    Arc::new(Ok(tx))
}

/// This is the "Block TxIds" constructor for the memoizing Esplora client.
/// Same idea as `fetch_tx` above.
fn fetch_block(agent: &Agent, esplora: &Arc<Esplora>, block_hash: &BlockHash) -> BlockResult {
    let thread_id = std::thread::current().id();
    let block_hash = *block_hash;

    info!("Fetching BlockHash `{block_hash}` on {thread_id:?}");

    let start = Instant::now();
    let req = esplora.get_block_txids(block_hash);
    let mut resp = try_arc_result!(agent.run(req), Block, block_hash);
    let txids: Vec<Txid> = try_arc_result!(resp.body_mut().read_json(), Block, block_hash);
    let dur = start.elapsed();

    info!("BlockHash `{block_hash}` received in {dur:?}");
    trace!("{txids:#?}");

    Arc::new(Ok(txids))
}
