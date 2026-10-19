//! The JSON body the driver writes for every message.
//!
//! AMQP keeps no attempt count on a classic queue, so the driver carries its
//! own: each retry republishes the envelope with `attempt` raised.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_core::{Result, StorageError};

/// Reason recorded for a message whose attempts ran out.
pub(crate) const MAX_ATTEMPTS: &str = "max attempts exceeded";
/// Reason recorded for a body the driver did not write.
pub(crate) const FOREIGN: &str = "not a tinystoragedrivers message";

/// One message as stored in the broker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Envelope {
    /// The message id, also the AMQP `message_id`.
    pub(crate) id: String,
    /// The caller's payload.
    pub(crate) payload: Value,
    /// Deliveries already counted.
    pub(crate) attempt: u32,
    /// Deliveries allowed before dead-lettering.
    pub(crate) max_attempts: u32,
    /// When the message was first enqueued, epoch milliseconds.
    pub(crate) enqueued_ms: i64,
    /// Why it was dead-lettered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

impl Envelope {
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(StorageError::from)
    }

    /// Decode a body. A body the driver did not write becomes an envelope
    /// holding the raw text (lossily), already dead with [`FOREIGN`].
    pub(crate) fn from_bytes(bytes: &[u8], id: Option<&str>, now_ms: i64) -> Self {
        serde_json::from_slice(bytes).unwrap_or_else(|_| Self {
            id: id.map_or_else(
                || tinystoragedrivers_core::Key::ulid().as_str().to_owned(),
                str::to_owned,
            ),
            payload: Value::String(String::from_utf8_lossy(bytes).into_owned()),
            attempt: 0,
            max_attempts: 1,
            enqueued_ms: now_ms,
            reason: Some(FOREIGN.to_owned()),
        })
    }

    /// Whether [`from_bytes`](Self::from_bytes) found a body it did not write.
    pub(crate) fn is_foreign(&self) -> bool {
        self.reason.as_deref() == Some(FOREIGN)
    }
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
