use bdk::bitcoin::{BlockHash, Txid};
use esploda::esplora::Transaction;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::{collections::HashMap, env, sync::Arc};
use thiserror::Error;

pub mod bitcoind;
pub mod esplora;

// We are choosing 32 threads by default, but really we can use as many as the Esplora server can
// handle.
const DEFAULT_THREADPOOL_SIZE: usize = 32;

pub(crate) type TxResult = Arc<Result<Transaction, ClientError>>;
pub(crate) type BlockResult = Arc<Result<Vec<Txid>, ClientError>>;

/// Memoizing client that retrieves Bitcoin transactions and blocks.
///
/// All supported backends are guaranteed to memoize requests and provide per-resource concurrency,
/// limited by global threadpool configuration. See `cargo run -- --help`
pub enum Client {
    Bitcoind(bitcoind::BitcoindClient),
    Esplora(esplora::EsploraClient),
}

impl From<bitcoind::BitcoindClient> for Client {
    fn from(value: bitcoind::BitcoindClient) -> Self {
        Self::Bitcoind(value)
    }
}

impl From<esplora::EsploraClient> for Client {
    fn from(value: esplora::EsploraClient) -> Self {
        Self::Esplora(value)
    }
}

/// The public interface for the client API.
///
/// Exists as a trait so that unit tests can mock the client responses.
pub trait ClientApi {
    /// Get a list of transactions by [`Txid`].
    fn get_transactions(&self, txids: &[Txid]) -> HashMap<Txid, TxResult>;

    /// Get a list of blocks by [`BlockHash`].
    fn get_blocks(&self, block_hashes: &[BlockHash]) -> HashMap<BlockHash, BlockResult>;
}

impl ClientApi for Client {
    fn get_transactions(&self, txids: &[Txid]) -> HashMap<Txid, TxResult> {
        match self {
            Self::Bitcoind(bitcoind) => bitcoind.get_transactions(txids),
            Self::Esplora(esplora) => esplora.get_transactions(txids),
        }
    }

    fn get_blocks(&self, block_hashes: &[BlockHash]) -> HashMap<BlockHash, BlockResult> {
        match self {
            Self::Bitcoind(bitcoind) => bitcoind.get_blocks(block_hashes),
            Self::Esplora(esplora) => esplora.get_blocks(block_hashes),
        }
    }
}

#[derive(Debug, Error)]
pub enum PoolError {
    #[error("Error parsing RAYON_NUM_THREADS")]
    RayonThreadPoolSize(#[source] std::num::ParseIntError),

    #[error("Rayon thread pool error")]
    RayonThreadPoolInit(#[from] rayon::ThreadPoolBuildError),
}

#[derive(Clone, Debug, Error)]
pub enum ClientError {
    #[error("Bitcoind client error")]
    Bitcoind(#[from] bitcoind::BitcoindError),

    #[error("Esplora client error")]
    Esplora(#[from] esplora::EsploraError),
}

pub(crate) fn create_thread_pool() -> Result<(usize, ThreadPool), PoolError> {
    // Configure the Rayon thread pool for high I/O concurrency.
    let num_threads = env::var("RAYON_NUM_THREADS")
        .unwrap_or_else(|_| DEFAULT_THREADPOOL_SIZE.to_string())
        .parse()
        .map_err(PoolError::RayonThreadPoolSize)?;

    let pool = ThreadPoolBuilder::new().num_threads(num_threads).build()?;

    Ok((num_threads, pool))
}

/// Transpose a `HashMap` of `Arc<Result>`s into a `Result<HashMap>`, borrowing through the `Arc`.
pub(crate) fn transpose_arc_result<K, V, E>(
    value: &HashMap<K, Arc<Result<V, E>>>,
) -> Result<HashMap<K, &V>, E>
where
    K: Eq + std::hash::Hash + Copy,
    E: Clone,
{
    // Sorry for the mess, here!
    //
    // This whole thing is for unwrapping possible errors from the HTTP requests. This is hard
    // because the results are wrapped in `Arc<T>` for cheap clones while memoizing responses.
    //
    // The map function transposes the `(K, Arc<Result<V, E>>)` into `Result<(K, &V), E>` which
    // allows `collect()` to give us the first error. Then we need to clone that single error
    // because we aren't allowed to move out of the Arc.
    value
        .iter()
        .map(|(key, arc_tx)| match arc_tx.as_ref() {
            Ok(tx) => Ok((*key, tx)),
            Err(err) => Err(err),
        })
        .collect::<Result<_, _>>()
        .map_err(|err| err.clone())
}
