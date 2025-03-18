use crate::model::kraken_amount::{KrakenAmount, UsdAmount};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// spends to/from blockchain
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Bucket {
    pub synthetic_id: String, // exchange UUID or blockchain transaction ID
    pub time: DateTime<Utc>,  // basis-time, not time of external_id tx
    pub amount: KrakenAmount,
    pub exchange_rate: UsdAmount,
}
