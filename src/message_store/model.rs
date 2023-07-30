use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

#[derive(Clone, Debug, Display, EnumString, PartialEq, Eq)]
pub enum MessageStatus {
    Available,
    Acknowledged,
    Reserved(DateTime<Utc>),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Message {
    pub id: String,
    pub value: String,
    #[serde(with = "ts_seconds")]
    pub expiry: DateTime<Utc>,
}

#[derive(Clone, Debug, Display, EnumString, PartialEq, Eq)]
pub enum DurabilityEngine {
    #[strum(ascii_case_insensitive)]
    Memory,
    #[strum(ascii_case_insensitive)]
    Postgres,
}
