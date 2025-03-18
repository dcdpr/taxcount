use base64::engine::{general_purpose::URL_SAFE, Engine as _};
use bdk::bitcoin::{BlockHash, Txid};
use esploda::bitcoind::{Bitcoind, Block, Status, Transaction};
use esploda::http::header::AUTHORIZATION;
use fett::Fett;
use rayon::{prelude::*, ThreadPool};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::{collections::HashMap, env, time::Instant};
use thiserror::Error;
use tracing::{info, trace};
use ureq::tls::{TlsConfig, TlsProvider};
use ureq::Agent;

#[derive(Debug, Error)]
pub enum BitcoindClientError {
    #[error("Invalid Bitcoind URI")]
    BitcoindUri(#[from] esploda::http::Error),

    #[error("Thread Pool error")]
    ThreadPool(#[from] super::PoolError),
}

#[derive(Clone, Debug, Error)]
pub enum BitcoindError {
    #[error("Error requesting TxId `{0}`: {1}")]
    Tx(Txid, String),

    #[error("Error requesting Block `{0}`: {1}")]
    Block(BlockHash, String),

    #[error("Error requesting BlockHeader `{0}`: {1}")]
    Header(BlockHash, String),
}

type BlockResult = Arc<Result<Block, BitcoindError>>;
type BlockFetcher = Box<dyn Fn(&BlockHash) -> BlockResult + Sync>;

type TxResult = Arc<Result<Transaction, BitcoindError>>;
type TxFetcher = Box<dyn Fn(&Txid) -> TxResult + Sync>;

/// A simple, concurrent Bitcoind client.
pub struct BitcoindClient {
    pool: ThreadPool,

    // Protocol caches
    block_cache: Fett<BlockHash, BlockResult, BlockFetcher>,
    tx_cache: Fett<Txid, TxResult, TxFetcher>,
}

/// Memoized responses from a [`BitcoindClient`].
#[derive(Debug, Deserialize, Serialize)]
pub struct BitcoindClientMemo {
    block_cache: Vec<(BlockHash, Block)>,
    tx_cache: Vec<(Txid, Transaction)>,
}

/// This macro is similar to [`std::try`] or the `?` operator. Its specific purpose is to wrap the
/// `Result` in `Arc` when it early-returns from the enclosing scope.
macro_rules! try_arc_result {
    ($expr:expr, $variant:ident, $txid:expr) => {
        match $expr {
            Ok(value) => value,
            Err(err) => return Arc::new(Err(BitcoindError::$variant($txid, err.to_string()))),
        }
    };
}

impl BitcoindClient {
    /// Create a new Bitcoind client with the provided API server URI.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), taxcount::client::bitcoind::BitcoindClientError> {
    /// # use taxcount::client::bitcoind::BitcoindClient;
    /// let client = BitcoindClient::new("http://localhost:8332")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(api_server: &str) -> Result<Self, BitcoindClientError> {
        let (pool, block_fetcher, tx_fetcher) = Self::new_inner(api_server)?;

        Ok(Self {
            pool,
            block_cache: Fett::new(block_fetcher),
            tx_cache: Fett::new(tx_fetcher),
        })
    }

    // Create a new Bitcoind client with the provided API server URI and memoized responses.
    pub fn from_memo(
        api_server: &str,
        memo: BitcoindClientMemo,
    ) -> Result<Self, BitcoindClientError> {
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

    /// Convert this Bitcoind client into its inner memoized responses.
    pub fn into_memo(self) -> Result<BitcoindClientMemo, super::ClientError> {
        let (_, _, block_cache) = self.block_cache.into_inner();
        let (_, _, tx_cache) = self.tx_cache.into_inner();

        // Transform `(K, Arc<Result<V, E>>)` to `(K, V)`
        let block_cache = block_cache
            .into_iter()
            .map(|(k, v)| {
                let v = Arc::try_unwrap(v).unwrap_or_else(|v| v.as_ref().clone())?;
                Ok((k, v))
            })
            .collect::<Result<_, super::ClientError>>()?;
        let tx_cache = tx_cache
            .into_iter()
            .map(|(k, v)| {
                let v = Arc::try_unwrap(v).unwrap_or_else(|v| v.as_ref().clone())?;
                Ok((k, v))
            })
            .collect::<Result<_, super::ClientError>>()?;

        Ok(BitcoindClientMemo {
            block_cache,
            tx_cache,
        })
    }

    fn new_inner(
        api_server: &str,
    ) -> Result<(ThreadPool, BlockFetcher, TxFetcher), BitcoindClientError> {
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
        let bitcoind = Arc::new(Bitcoind::new(api_server)?);

        let block_fetcher = {
            let agent = agent.clone();
            let bitcoind = bitcoind.clone();
            Box::new(move |key: &_| fetch_block(&agent, &bitcoind, key)) as BlockFetcher
        };
        let tx_fetcher = Box::new(move |key: &_| fetch_tx(&agent, &bitcoind, key)) as TxFetcher;

        Ok((pool, block_fetcher, tx_fetcher))
    }

    pub(crate) fn get_transactions(&self, txids: &[Txid]) -> HashMap<Txid, super::TxResult> {
        self.pool.in_place_scope(|_scope| {
            txids
                .par_iter()
                .map(|txid| {
                    // This "outer product" part cannot be memoized (easily) because it requires
                    // self-references.

                    // TODO: Use `?` instead of `expect`?
                    let tx = self.tx_cache.get(*txid);
                    let tx = (*tx).clone().expect("Invalid Txid");

                    // Get block height.
                    let block_hash = match tx.status {
                        Status::Confirmed { block_hash, .. } => block_hash,
                        Status::Unconfirmed => panic!("Unconfirmed transactions are not supported"),
                    };
                    let block = self.block_cache.get(block_hash);
                    let block = (*block).as_ref().expect("Invalid BlockHash");

                    // Get previous transaction outputs.
                    let previous_outputs = tx
                        .inputs
                        .par_iter()
                        .filter_map(|txi| {
                            // Only fetch non-coinbase TxIds.
                            if !txi.is_coinbase() {
                                let parent_tx = self.tx_cache.get(txi.txid);
                                let parent_tx = (*parent_tx).as_ref().expect("Invalid Txid");

                                Some(parent_tx.outputs[txi.index as usize].clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    let tx = tx.into_esplora(block.header.height, previous_outputs);

                    (*txid, Arc::new(Ok(tx)))
                })
                .collect()
        })
    }

    pub(crate) fn get_blocks(
        &self,
        block_hashes: &[BlockHash],
    ) -> HashMap<BlockHash, super::BlockResult> {
        self.pool.in_place_scope(|_scope| {
            block_hashes
                .par_iter()
                .map(|block_hash| {
                    // This "outer product" part cannot be memoized (easily) because it requires
                    // self-references.
                    let txids = self.block_cache.get(*block_hash);
                    let txids = (*txids)
                        .clone()
                        .map(|block| block.txids)
                        .map_err(super::ClientError::from);

                    (*block_hash, Arc::new(txids))
                })
                .collect()
        })
    }
}

/// Get HTTP Basic Authorization header value if provided by the user.
fn get_basic_auth() -> Option<&'static str> {
    static USER_PASS: OnceLock<Option<String>> = OnceLock::new();

    USER_PASS
        .get_or_init(|| {
            env::var("BITCOIND_CREDENTIALS")
                .ok()
                .map(|user_pass| format!("Basic {}", URL_SAFE.encode(user_pass)))
        })
        .as_deref()
}

/// This is the "Block" constructor for the memoizing Bitcoind client.
/// It does error handling in a special way because the return value needs to be wrapped in `Arc`.
/// See `try_arc_result!()` for info on how errors are handled.
fn fetch_block(agent: &Agent, bitcoind: &Arc<Bitcoind>, block_hash: &BlockHash) -> BlockResult {
    let thread_id = std::thread::current().id();
    let block_hash = *block_hash;

    info!("Fetching Block `{block_hash}` on {thread_id:?}");

    let start = Instant::now();
    let mut req = bitcoind.get_block(block_hash);

    if let Some(auth) = get_basic_auth() {
        let auth = auth.parse().unwrap();
        req.headers_mut().insert(AUTHORIZATION, auth);
    }
    let mut resp = try_arc_result!(agent.run(req), Block, block_hash);

    let block = try_arc_result!(
        Block::from_bitcoind_reader(resp.body_mut().as_reader()),
        Block,
        block_hash
    );
    let dur = start.elapsed();

    info!("Block `{block_hash}` received in {dur:?}");
    trace!("{block:#?}");

    Arc::new(Ok(block))
}

/// This is the "Transaction" constructor for the memoizing Bitcoind client.
/// Same idea as `fetch_block` above.
fn fetch_tx(agent: &Agent, bitcoind: &Arc<Bitcoind>, txid: &Txid) -> TxResult {
    let thread_id = std::thread::current().id();
    let txid = *txid;

    info!("Fetching TxId `{txid}` on {thread_id:?}");

    let start = Instant::now();
    let mut req = bitcoind.get_tx(txid);

    if let Some(auth) = get_basic_auth() {
        let auth = auth.parse().unwrap();
        req.headers_mut().insert(AUTHORIZATION, auth);
    }
    let mut resp = try_arc_result!(agent.run(req), Tx, txid);

    let tx = try_arc_result!(
        Transaction::from_bitcoind_reader(resp.body_mut().as_reader()),
        Tx,
        txid
    );
    let dur = start.elapsed();

    info!("TxId `{txid}` received in {dur:?}");
    trace!("{tx:#?}");

    Arc::new(Ok(tx))
}
