use crate::esplora::{TxIn, TxOut};
use bitcoin::{hash_types::TxMerkleNode, locktime::absolute::LockTime, pow::CompactTarget};
use bitcoin::{BlockHash, ScriptBuf, Sequence, Txid, Witness};
use chrono::{DateTime, Utc};
use data_encoding::HEXLOWER;
use rust_decimal::{prelude::FromPrimitive as _, Decimal};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use thiserror::Error;

/// Bitcoind JSON Parsing errors for [`Transaction`].
#[derive(Debug, Error)]
pub enum Error {
    /// JSON parsing error.
    #[error("Unable to parse JSON")]
    Json(#[from] serde_json::Error),

    /// Response error.
    #[error("Bitcoind response error: {0}")]
    Response(serde_json::Value),

    /// Response is missing a field.
    #[error("Bitcoind response is missing field `{0}`")]
    MissingField(&'static str),

    /// Response has the wrong type for a field.
    #[error("Bitcoind response has wrong type for field `{0}`")]
    WrongFieldType(&'static str),

    /// Response field cannot be parsed.
    #[error("Bitcoind response field `{0}` parse error")]
    Parse(
        &'static str,
        #[source] Box<dyn std::error::Error + Send + Sync>,
    ),
}

/// This is the same as [`esplora::Transaction`] with a few differences:
///
/// - `inputs[].previous_output` is always `None`.
/// - `fees` is unavailable.
/// - `status.block_height` is unavailable.
///
/// It can be converted into [`esplora::Transaction`] with [`Transaction::into_esplora`].
///
/// [`esplora::Transaction`]: crate::esplora::Transaction
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, PartialOrd)]
pub struct Transaction {
    /// Transaction ID.
    pub txid: Txid,

    /// Transaction version.
    ///
    /// Only versions 1 and 2 are currently valid, according to the protocol.
    pub version: u32,

    /// Block height or timestamp for transaction finalization.
    pub lock_time: LockTime,

    /// Transaction inputs.
    pub inputs: Vec<TxIn>,

    /// Transaction outputs.
    pub outputs: Vec<TxOut>,

    pub size: u32,
    pub weight: u32,

    /// Indicates whether the transaction has been confirmed by the network, and information about
    /// which block it exists in (if confirmed).
    pub status: Status,
}

/// Network consensus status for [`Transaction`].
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, Ord, PartialEq, PartialOrd)]
pub enum Status {
    /// Transaction has not yet been confirmed by the network.
    Unconfirmed,

    /// Transaction has been confirmed by consensus.
    Confirmed {
        /// The block hash that uniquely identifies the block.
        block_hash: BlockHash,

        /// Absolute timestamp for the block, as agreed upon by the network.
        block_time: DateTime<Utc>,
    },
}

/// Block header.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, PartialOrd)]
pub struct BlockHeader {
    pub block_hash: BlockHash,
    pub confirmations: u32,
    pub height: u32,
    pub version: u32,
    pub merkle_root: TxMerkleNode,
    pub time: DateTime<Utc>,
    pub median_time: DateTime<Utc>,
    pub nonce: u32,
    pub bits: CompactTarget,
    pub difficulty: f32,
    // pub chain_work: String, // TODO: What type is this?
    pub num_tx: u32,
    pub prev_block_hash: BlockHash,
    pub next_block_hash: Option<BlockHash>,
}

/// Block.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, PartialOrd)]
pub struct Block {
    pub header: BlockHeader,
    pub size: u32,
    pub stripped_size: u32,
    pub weight: u32,
    pub txids: Vec<Txid>,
}

impl Transaction {
    /// Create a `Transaction` from any `bitcoind`-serialized type that implements [`Read`].
    ///
    /// [`Read`]: std::io::Read
    pub fn from_bitcoind_reader<R>(value: R) -> Result<Self, Error>
    where
        R: std::io::Read,
    {
        Self::from_bitcoind_value(&serde_json::from_reader(value)?)
    }

    /// Create a `Transaction` from a `bitcoind`-serialized string.
    pub fn from_bitcoind_str(value: &str) -> Result<Self, Error> {
        Self::from_bitcoind_value(&serde_json::from_str(value)?)
    }

    /// Create a `Transaction` from a `bitcoind`-serialized [`Value`].
    pub fn from_bitcoind_value(value: &Value) -> Result<Self, Error> {
        use Error::*;

        if !value["error"].is_null() {
            return Err(Response(value["error"].clone()));
        }

        let value = &value["result"];

        Ok(Transaction {
            txid: parse_field_str(value, "txid")?,
            version: parse_field_u32(value, "version")?,
            lock_time: LockTime::from_consensus(parse_field_u32(value, "locktime")?),
            inputs: parse_field_array(value, "vin", |value| {
                Ok(TxIn {
                    // Coinbase (newly generated coins) transaction do not have a `txid`, `index`,
                    // or `previous_output`. And the `script_sig` is parsed from the `coinbase`
                    // field.
                    txid: parse_field_str(value, "txid").unwrap_or(coinbase_txid()),
                    index: parse_field_u32(value, "vout").unwrap_or(0xffff_ffff),
                    previous_output: None,
                    script_sig: parse_script(value, "scriptSig")
                        .or_else(|_| parse_script_hex(value, "coinbase"))?,
                    witness: parse_field_array(value, "txinwitness", |bytes| {
                        decode_hex(bytes, "txinwitness")
                    })
                    .ok()
                    .map(|slice| Witness::from_slice(&slice)),
                    sequence: Sequence(parse_field_u32(value, "sequence")?),
                })
            })?,
            outputs: parse_field_array(value, "vout", |value| {
                Ok(TxOut {
                    script_pubkey: parse_script(value, "scriptPubKey")?,
                    value: value
                        .get("value")
                        .and_then(|value| value.as_f64())
                        .and_then(|value| {
                            Decimal::from_f64(value).map(|mut value| {
                                value.rescale(8);
                                value
                            })
                        })
                        .ok_or_else(|| WrongFieldType("value"))?,
                })
            })?,
            size: parse_field_u32(value, "size")?,
            weight: parse_field_u32(value, "weight")?,
            status: match (
                parse_field_str(value, "blockhash"),
                parse_field_i64(value, "blocktime"),
            ) {
                (Ok(block_hash), Ok(block_time)) => Status::Confirmed {
                    block_hash,
                    block_time: DateTime::from_timestamp(block_time, 0).unwrap(),
                },
                _ => Status::Unconfirmed,
            },
        })
    }

    /// Convert `self` into the more complete [`esplora::Transaction`] type by filling in
    /// the missing details.
    ///
    /// # Panics
    ///
    /// Asserts that `previous_outputs` and `self.inputs` have the same number of non-coinbase TXOs.
    ///
    /// [`esplora::Transaction`]: crate::esplora::Transaction
    pub fn into_esplora(
        self,
        block_height: u32,
        previous_outputs: Vec<TxOut>,
    ) -> crate::esplora::Transaction {
        let fee = previous_outputs.iter().map(|txo| txo.value).sum();
        let mut previous_outputs = previous_outputs.into_iter();
        let inputs = self
            .inputs
            .into_iter()
            .map(|mut txi| {
                if !txi.is_coinbase() {
                    let txo = previous_outputs
                        .next()
                        .expect("Constructing a Transaction with differing inputs");
                    txi.previous_output = Some(txo);
                }
                txi
            })
            .collect();

        assert!(
            previous_outputs.next().is_none(),
            "Constructing a Transaction with differing inputs",
        );

        crate::esplora::Transaction {
            txid: self.txid,
            version: self.version,
            lock_time: self.lock_time,
            inputs,
            outputs: self.outputs,
            size: self.size,
            weight: self.weight,
            fee,
            status: match self.status {
                Status::Unconfirmed => crate::esplora::Status::Unconfirmed,
                Status::Confirmed {
                    block_hash,
                    block_time,
                } => crate::esplora::Status::Confirmed {
                    block_height,
                    block_hash,
                    block_time,
                },
            },
        }
    }
}

impl BlockHeader {
    /// Create a `BlockHeader` from any `bitcoind`-serialized type that implements [`Read`].
    ///
    /// [`Read`]: std::io::Read
    pub fn from_bitcoind_reader<R>(value: R) -> Result<Self, Error>
    where
        R: std::io::Read,
    {
        Self::from_bitcoind_value(&serde_json::from_reader(value)?)
    }

    /// Create a `BlockHeader` from a `bitcoind`-serialized string.
    pub fn from_bitcoind_str(value: &str) -> Result<Self, Error> {
        Self::from_bitcoind_value(&serde_json::from_str(value)?)
    }

    /// Create a `BlockHeader` from a `bitcoind`-serialized [`Value`].
    pub fn from_bitcoind_value(value: &Value) -> Result<Self, Error> {
        use Error::*;

        if !value["error"].is_null() {
            return Err(Response(value["error"].clone()));
        }

        let value = &value["result"];

        Ok(BlockHeader {
            block_hash: parse_field_str(value, "hash")?,
            confirmations: parse_field_u32(value, "confirmations")?,
            height: parse_field_u32(value, "height")?,
            version: parse_field_u32(value, "version")?,
            merkle_root: parse_field_str(value, "merkleroot")?,
            time: DateTime::from_timestamp(parse_field_i64(value, "time")?, 0).unwrap(),
            median_time: DateTime::from_timestamp(parse_field_i64(value, "mediantime")?, 0)
                .unwrap(),
            nonce: parse_field_u32(value, "nonce")?,
            bits: CompactTarget::from_consensus(u32::from_be_bytes(
                get_field(value, "bits", |data| decode_hex(data, "bits"))?
                    .try_into()
                    .map_err(|_| WrongFieldType("bits"))?,
            )),
            difficulty: parse_field_f32(value, "difficulty")?,
            // chain_work: String,
            num_tx: parse_field_u32(value, "nTx")?,
            prev_block_hash: parse_field_str(value, "previousblockhash")?,
            next_block_hash: parse_field_str(value, "nextblockhash").ok(),
        })
    }
}

impl Block {
    /// Create a `Block` from any `bitcoind`-serialized type that implements [`Read`].
    ///
    /// [`Read`]: std::io::Read
    pub fn from_bitcoind_reader<R>(value: R) -> Result<Self, Error>
    where
        R: std::io::Read,
    {
        Self::from_bitcoind_value(&serde_json::from_reader(value)?)
    }

    /// Create a `Block` from a `bitcoind`-serialized string.
    pub fn from_bitcoind_str(value: &str) -> Result<Self, Error> {
        Self::from_bitcoind_value(&serde_json::from_str(value)?)
    }

    /// Create a `Block` from a `bitcoind`-serialized [`Value`].
    pub fn from_bitcoind_value(value: &Value) -> Result<Self, Error> {
        use Error::*;

        if !value["error"].is_null() {
            return Err(Response(value["error"].clone()));
        }

        let header = BlockHeader::from_bitcoind_value(value)?;
        let value = &value["result"];

        Ok(Block {
            header,
            size: parse_field_u32(value, "size")?,
            stripped_size: parse_field_u32(value, "strippedsize")?,
            weight: parse_field_u32(value, "weight")?,
            txids: parse_field_array(value, "tx", |txid| parse_str(txid, "tx"))?,
        })
    }
}

fn get_field<F, T>(value: &Value, field: &'static str, map: F) -> Result<T, Error>
where
    F: Fn(&Value) -> Result<T, Error>,
{
    value
        .get(field)
        .ok_or_else(|| Error::MissingField(field))
        .and_then(map)
}

fn parse_str<T>(value: &Value, field: &'static str) -> Result<T, Error>
where
    T: FromStr,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    use Error::*;

    value
        .as_str()
        .ok_or_else(|| WrongFieldType(field))
        .and_then(|value| value.parse().map_err(|err| Parse(field, Box::new(err))))
}

fn parse_field_str<T>(value: &Value, field: &'static str) -> Result<T, Error>
where
    T: FromStr,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    get_field(value, field, |value| parse_str(value, field))
}

fn parse_field_u32(value: &Value, field: &'static str) -> Result<u32, Error> {
    get_field(value, field, |value| {
        value
            .as_u64()
            .and_then(|num| u32::try_from(num).ok())
            .ok_or_else(|| Error::WrongFieldType(field))
    })
}

fn parse_field_i64(value: &Value, field: &'static str) -> Result<i64, Error> {
    get_field(value, field, |value| {
        value.as_i64().ok_or_else(|| Error::WrongFieldType(field))
    })
}

fn parse_field_f32(value: &Value, field: &'static str) -> Result<f32, Error> {
    get_field(value, field, |value| {
        value
            .as_f64()
            .map(|num| num as f32)
            .ok_or_else(|| Error::WrongFieldType(field))
    })
}

fn parse_field_array<F, T>(value: &Value, field: &'static str, map: F) -> Result<Vec<T>, Error>
where
    F: Fn(&Value) -> Result<T, Error> + Copy,
{
    get_field(value, field, |value| {
        value
            .as_array()
            .ok_or_else(|| Error::WrongFieldType(field))
            .and_then(|items| items.iter().map(map).collect())
    })
}

fn parse_script(value: &Value, field: &'static str) -> Result<ScriptBuf, Error> {
    get_field(value, field, |value| parse_script_hex(value, "hex"))
}

fn parse_script_hex(value: &Value, field: &'static str) -> Result<ScriptBuf, Error> {
    use Error::*;

    get_field(value, field, |value| {
        value
            .as_str()
            .ok_or_else(|| WrongFieldType(field))
            .and_then(|data| ScriptBuf::from_hex(data).map_err(|err| Parse(field, Box::new(err))))
    })
}

fn decode_hex(value: &Value, field: &'static str) -> Result<Vec<u8>, Error> {
    use Error::*;

    value
        .as_str()
        .ok_or_else(|| WrongFieldType(field))
        .and_then(|data| {
            HEXLOWER
                .decode(data.as_bytes())
                .map_err(|err| Parse(field, Box::new(err)))
        })
}

fn coinbase_txid() -> Txid {
    "0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .unwrap()
}
