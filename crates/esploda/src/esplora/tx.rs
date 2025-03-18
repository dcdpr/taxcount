//! Bitcoin transactions.

use bitcoin::{locktime::absolute::LockTime, BlockHash, ScriptBuf, Sequence, Txid, Witness};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// JSON Parsing errors for [`Transaction`].
#[derive(Debug, Error)]
pub enum Error {
    /// Missing `status.block_height` field.
    #[error("Missing `status.block_height` field")]
    Height,

    /// Missing `status.block_hash` field.
    #[error("Missing `status.block_hash` field")]
    Hash,

    /// Missing or invalid `status.block_time` field.
    #[error("Missing or invalid `status.block_time` field")]
    Time,
}

/// A transaction is a transfer of Bitcoin value.
///
/// See: [Bitcoin Wiki: Transaction](https://en.bitcoin.it/wiki/Transaction)
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, PartialOrd)]
pub struct Transaction {
    /// Transaction ID.
    pub txid: Txid,

    /// Transaction version.
    ///
    /// Only versions 1 and 2 are currently valid, according to the protocol.
    pub version: u32,

    /// Block height or timestamp for transaction finalization.
    #[serde(rename = "locktime")]
    pub lock_time: LockTime,

    /// Transaction inputs.
    #[serde(rename = "vin")]
    pub inputs: Vec<TxIn>,

    /// Transaction outputs.
    #[serde(rename = "vout")]
    pub outputs: Vec<TxOut>,

    pub size: u32,
    pub weight: u32,

    /// Fee paid for the transaction. Denominated in Bitcoin.
    #[serde(deserialize_with = "from_sats", serialize_with = "to_sats")]
    pub fee: Decimal,

    /// Indicates whether the transaction has been confirmed by the network, and information about
    /// which block it exists in (if confirmed).
    pub status: Status,
}

/// [`Transaction`] input.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct TxIn {
    /// Previous transaction output ID.
    ///
    /// When all zeros, this input is declared as coinbase (newly generated coins).
    pub txid: Txid,

    /// Previous transaction output index.
    ///
    /// When `0xffff_ffff`, this input is declared as coinbase (newly generated coins).
    #[serde(rename = "vout")]
    pub index: u32,

    /// Previous transaction output.
    ///
    /// When `None`, this input is declared as coinbase (newly generated coins).
    #[serde(rename = "prevout")]
    pub previous_output: Option<TxOut>,

    /// Script signature.
    #[serde(rename = "scriptsig")]
    pub script_sig: ScriptBuf,

    /// Witness data.
    ///
    /// When `None`, this input is a pre-[BIP-144] transaction.
    ///
    /// [BIP-144]: https://github.com/bitcoin/bips/blob/master/bip-0144.mediawiki
    pub witness: Option<Witness>,

    /// Sequence number.
    pub sequence: Sequence,
}

/// [`Transaction`] output.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct TxOut {
    /// Script public key.
    #[serde(rename = "scriptpubkey")]
    pub script_pubkey: ScriptBuf,

    /// Transaction output value. Denominated in Bitcoin.
    #[serde(deserialize_with = "from_sats", serialize_with = "to_sats")]
    pub value: Decimal,
}

/// Network consensus status for [`Transaction`].
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(try_from = "JsonStatus")]
#[serde(into = "JsonStatus")]
pub enum Status {
    /// Transaction has not yet been confirmed by the network.
    Unconfirmed,

    /// Transaction has been confirmed by consensus.
    Confirmed {
        /// The block height containing the transaction.
        block_height: u32,

        /// The block hash that uniquely identifies the block.
        block_hash: BlockHash,

        /// Absolute timestamp for the block, as agreed upon by the network.
        block_time: DateTime<Utc>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct JsonStatus {
    confirmed: bool,
    block_height: Option<u32>,
    block_hash: Option<BlockHash>,
    block_time: Option<i64>,
}

impl TryFrom<JsonStatus> for Status {
    type Error = Error;

    fn try_from(value: JsonStatus) -> Result<Self, Self::Error> {
        match value.confirmed {
            false => Ok(Status::Unconfirmed),
            true => Ok(Status::Confirmed {
                block_height: value.block_height.ok_or(Error::Height)?,
                block_hash: value.block_hash.ok_or(Error::Hash)?,
                block_time: value
                    .block_time
                    .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
                    .ok_or(Error::Time)?,
            }),
        }
    }
}

impl From<Status> for JsonStatus {
    fn from(value: Status) -> Self {
        match value {
            Status::Unconfirmed => JsonStatus {
                confirmed: false,
                block_height: None,
                block_hash: None,
                block_time: None,
            },
            Status::Confirmed {
                block_height,
                block_hash,
                block_time,
            } => JsonStatus {
                confirmed: true,
                block_height: Some(block_height),
                block_hash: Some(block_hash),
                block_time: Some(block_time.timestamp()),
            },
        }
    }
}

impl TxIn {
    /// Returns `true` if this transaction input is coinbase (newly generated coins).
    pub fn is_coinbase(&self) -> bool {
        let txid_bytes = AsRef::<[u8; 32]>::as_ref(&self.txid);

        txid_bytes.iter().all(|byte| *byte == 0) && self.index == 0xffff_ffff
    }
}

fn from_sats<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_u64(DecimalVisitor)
}

fn to_sats<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    assert!(
        !value.is_sign_negative(),
        "Unexpected negative Decimal value"
    );
    assert_eq!(value.scale(), 8, "Unexpected Decimal scale");

    let bytes = value.serialize();
    let lo = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as u64;
    let mid = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as u64;
    let hi = u32::from_le_bytes(bytes[12..16].try_into().unwrap());

    assert_eq!(hi, 0, "Unexpected serialization for Decimal value");

    serializer.serialize_u64((mid << 32) | lo)
}

struct DecimalVisitor;

impl serde::de::Visitor<'_> for DecimalVisitor {
    type Value = Decimal;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a BTC type in satoshis")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Decimal, E>
    where
        E: serde::de::Error,
    {
        Ok(Decimal::from_i128_with_scale(value as i128, 8))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_coinbase() {
        let mut tx_in = TxIn {
            txid: "7a23e9ffacfe08ad6c942aeb0eb94a1653804e40c12babdbd10468d3886f3e74"
                .parse()
                .unwrap(),
            index: 0,
            previous_output: None,
            script_sig: ScriptBuf::default(),
            witness: None,
            sequence: Sequence(0),
        };
        assert!(!tx_in.is_coinbase());

        tx_in.txid = "0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap();
        tx_in.index = 0xffff_ffff;
        assert!(tx_in.is_coinbase());
    }
}
