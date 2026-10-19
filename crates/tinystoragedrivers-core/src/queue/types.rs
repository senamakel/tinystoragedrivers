//! The value types of the [`QueueStore`](super::QueueStore) port.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, StorageError};

/// The longest queue name, in bytes.
pub const MAX_QUEUE_NAME_LEN: usize = 64;

/// The highest message priority. Priorities run `0..=MAX_PRIORITY`, higher
/// first, matching `RabbitMQ`'s `x-max-priority` range.
pub const MAX_PRIORITY: u8 = 9;

/// Validate a queue name: 1 to [`MAX_QUEUE_NAME_LEN`] bytes of lowercase
/// ASCII letters, digits, `.`, `_` and `-`, so the name is safe as a
/// collection suffix, a key segment and a broker queue name alike.
///
/// # Errors
///
/// `InvalidInput` naming the rule the name breaks.
pub fn validate_queue_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_QUEUE_NAME_LEN {
        return Err(StorageError::invalid_input(format!(
            "a queue name must be 1 to {MAX_QUEUE_NAME_LEN} bytes"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(StorageError::invalid_input(
            "a queue name holds only lowercase letters, digits, '.', '_' and '-'",
        ));
    }
    Ok(())
}

/// How a queue treats its messages, declared once with
/// [`ensure_queue`](super::QueueStore::ensure_queue).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSpec {
    /// Deliveries a message gets before it is dead-lettered. A delivery whose
    /// lease lapses counts, so a consumer that crashes on a message cannot
    /// loop on it forever. At least 1.
    pub max_attempts: u32,
    /// How long an acknowledged message with a dedupe key keeps rejecting a
    /// re-enqueue of the same key, in milliseconds. `0` forgets the key at
    /// acknowledgement.
    pub dedupe_window_ms: u64,
}

impl Default for QueueSpec {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            dedupe_window_ms: 0,
        }
    }
}

impl QueueSpec {
    /// Check the spec is usable.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for `max_attempts == 0`.
    pub fn validate(&self) -> Result<()> {
        if self.max_attempts == 0 {
            return Err(StorageError::invalid_input(
                "a queue needs at least one attempt per message",
            ));
        }
        Ok(())
    }
}

/// What a queue backend can do beyond the at-least-once core every driver
/// gives. A caller that needs a missing feature builds it itself (the
/// documented fallback for dedupe is a document store keyed by the dedupe key).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent feature flags, one per optional semantic"
)]
pub struct QueueFeatures {
    /// [`NewMessage::dedupe_key`] is honored: a second enqueue of a live or
    /// recently acknowledged key is reported as a duplicate, not stored.
    pub dedupe: bool,
    /// [`NewMessage::group`] is honored: at most one message per group is
    /// leased at a time, and a nack with delay holds the group until the
    /// retry is due.
    pub groups: bool,
    /// [`NewMessage::priority`] orders delivery.
    pub priorities: bool,
    /// [`NewMessage::delay_ms`] and [`Retry::After`] hold a message back.
    pub delay: bool,
}

/// One message to enqueue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewMessage {
    /// The message body.
    pub payload: Value,
    /// `0..=`[`MAX_PRIORITY`], higher delivered first. Equal priorities are
    /// delivered in enqueue order.
    #[serde(default)]
    pub priority: u8,
    /// Hold the message back for this long after enqueueing, in milliseconds.
    #[serde(default)]
    pub delay_ms: u64,
    /// Messages sharing a group are leased one at a time, in priority then
    /// enqueue order (a conversation, a thread, a sender).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// A second enqueue with the same key, while the first is still queued or
    /// within the queue's dedupe window after it was acknowledged, is
    /// reported as a duplicate and not stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
}

impl NewMessage {
    /// A message with `payload` and every option at its default.
    #[must_use]
    pub fn new(payload: Value) -> Self {
        Self {
            payload,
            priority: 0,
            delay_ms: 0,
            group: None,
            dedupe_key: None,
        }
    }

    /// Set [`priority`](Self::priority).
    #[must_use]
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Set [`delay_ms`](Self::delay_ms).
    #[must_use]
    pub fn delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    /// Set [`group`](Self::group).
    #[must_use]
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Set [`dedupe_key`](Self::dedupe_key).
    #[must_use]
    pub fn dedupe_key(mut self, key: impl Into<String>) -> Self {
        self.dedupe_key = Some(key.into());
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.priority > MAX_PRIORITY {
            return Err(StorageError::invalid_input(format!(
                "a message priority runs 0 to {MAX_PRIORITY}"
            )));
        }
        for (what, value) in [("group", &self.group), ("dedupe key", &self.dedupe_key)] {
            if value.as_deref().is_some_and(str::is_empty) {
                return Err(StorageError::invalid_input(format!(
                    "a message {what} must not be empty"
                )));
            }
        }
        Ok(())
    }
}

/// The outcome of enqueueing one [`NewMessage`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Enqueued {
    /// The message id. For a duplicate, the id of the message already held.
    pub id: String,
    /// The dedupe key was already live: nothing new was stored.
    pub duplicate: bool,
}

/// Proof of one lease on one message. Acknowledging, nacking or extending
/// needs it; once the lease lapses and the message is delivered again, the old
/// receipt no longer works.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Receipt {
    /// The message id.
    pub id: String,
    /// The lease token, unique to this delivery.
    pub token: String,
}

/// One leased message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delivery {
    /// What the consumer hands back to settle the lease.
    pub receipt: Receipt,
    /// The message body.
    pub payload: Value,
    /// Which delivery this is, starting at 1.
    pub attempt: u32,
    /// The message priority.
    pub priority: u8,
    /// The message group, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// When the message was enqueued, Unix epoch milliseconds.
    pub enqueued_ms: i64,
    /// When the lease lapses, Unix epoch milliseconds.
    pub lease_until_ms: i64,
}

/// What [`nack`](super::QueueStore::nack) does with a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Retry {
    /// Deliver it again after this many milliseconds, unless it has used up
    /// its attempts, in which case it is dead-lettered.
    After(u64),
    /// Dead-letter it now, recording why.
    DeadLetter(String),
}

/// A dead-lettered message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// The message id.
    pub id: String,
    /// The message body.
    pub payload: Value,
    /// Deliveries it got.
    pub attempt: u32,
    /// Why it was dead-lettered.
    pub reason: String,
    /// The message group, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// When it was enqueued, Unix epoch milliseconds.
    pub enqueued_ms: i64,
}

/// How many messages a queue holds, by state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct QueueStats {
    /// Deliverable now.
    pub ready: u64,
    /// Waiting for a delay or a retry backoff.
    pub delayed: u64,
    /// Leased to a consumer, lease still running.
    pub leased: u64,
    /// Dead-lettered.
    pub dead: u64,
}
