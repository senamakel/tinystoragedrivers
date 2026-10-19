//! Wire shapes for the Queues object (durable at-least-once message queues).
//!
//! Every selector mirrors one `QueueStore` method. Payloads are inline JSON
//! text, like document bodies and stream records. Scope is host-captured: a
//! queue name addresses a queue inside the caller's grant and nothing else.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Queues object name under [`crate::STORAGE_INTERFACE`].
pub const QUEUES_OBJECT: &str = "Queues";
/// Finite selector set; servers must reject every other selector.
pub const QUEUES_SELECTORS: [&str; 11] = [
    "Features",
    "EnsureQueue",
    "Enqueue",
    "Receive",
    "Ack",
    "Nack",
    "Extend",
    "Stats",
    "DeadLetters",
    "Redrive",
    "Purge",
];

/// Return whether `selector` belongs to the fixed Queues selector set.
#[must_use]
pub fn is_queues_selector(selector: &str) -> bool {
    QUEUES_SELECTORS.contains(&selector)
}

/// Reply to `Features`: the optional semantics the host's queue honors.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent feature flags, one per optional semantic"
)]
pub struct QueueFeaturesReply {
    /// Dedupe keys are honored.
    pub dedupe: bool,
    /// Groups are honored.
    pub groups: bool,
    /// Priorities order delivery.
    pub priorities: bool,
    /// Delays and delayed retries hold messages back.
    pub delay: bool,
}

/// How a queue treats its messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSpec {
    /// Deliveries before a message is dead-lettered; at least 1.
    pub max_attempts: u32,
    /// How long an acknowledged dedupe key keeps rejecting re-enqueues, in
    /// milliseconds.
    #[serde(default)]
    pub dedupe_window_ms: u64,
}

/// `EnsureQueue`: declare a queue or update its spec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsureQueueRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// The queue's spec.
    pub spec: QueueSpec,
}

/// One message to enqueue.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueMessage {
    /// Message body, encoded as JSON text on the wire; at most
    /// [`MAX_QUEUE_MESSAGE_BYTES`](crate::MAX_QUEUE_MESSAGE_BYTES).
    #[serde(rename = "payload_json", with = "crate::inline_json")]
    pub payload: Value,
    /// `0..=9`, higher delivered first.
    #[serde(default)]
    pub priority: u8,
    /// Hold back for this long after enqueueing, in milliseconds.
    #[serde(default)]
    pub delay_ms: u64,
    /// Messages sharing a group are leased one at a time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Rejects re-enqueues of a held key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
}

/// `Enqueue`: add messages in order; at most [`MAX_BATCH_OPS`](crate::MAX_BATCH_OPS).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnqueueRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// Messages in enqueue order.
    pub messages: Vec<QueueMessage>,
}

/// The outcome of enqueueing one message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnqueuedMessage {
    /// The message id; for a duplicate, the held message's id.
    pub id: String,
    /// The dedupe key was live and nothing was stored.
    pub duplicate: bool,
}

/// Reply to `Enqueue`, one entry per message in request order.
pub type EnqueueResponse = Vec<EnqueuedMessage>;

/// `Receive`: lease up to `max` deliverable messages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiveRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// Most messages to lease; servers clamp it to
    /// [`MAX_PAGE_ITEMS`](crate::MAX_PAGE_ITEMS) and the inline byte cap.
    pub max: u32,
    /// Lease length in milliseconds.
    pub visibility_ms: u64,
    /// Consumer label recorded with the lease.
    pub consumer: String,
}

/// Proof of one lease on one message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueReceipt {
    /// The message id.
    pub id: String,
    /// The lease token, unique to this delivery.
    pub token: String,
}

/// One leased message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDelivery {
    /// What the caller hands back to settle the lease.
    pub receipt: QueueReceipt,
    /// Message body.
    #[serde(rename = "payload_json", with = "crate::inline_json")]
    pub payload: Value,
    /// Which delivery this is, starting at 1.
    pub attempt: u32,
    /// The message priority.
    pub priority: u8,
    /// The message group, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// When the message was enqueued, epoch milliseconds (host clock).
    pub enqueued_ms: i64,
    /// When the lease lapses, epoch milliseconds (host clock).
    pub lease_until_ms: i64,
}

/// Reply to `Receive`. A short reply is not evidence of an empty queue when
/// the server cut it at its byte cap.
pub type ReceiveResponse = Vec<QueueDelivery>;

/// `Ack`: settle a delivery as done. Replies `bool`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// The delivery's receipt.
    pub receipt: QueueReceipt,
}

/// What a nack does with a message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueRetry {
    /// Deliver again after `delay_ms`, or dead-letter when out of attempts.
    After {
        /// Backoff in milliseconds.
        delay_ms: u64,
    },
    /// Dead-letter now.
    DeadLetter {
        /// Why.
        reason: String,
    },
}

/// `Nack`: hand a delivery back. Replies `bool`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NackRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// The delivery's receipt.
    pub receipt: QueueReceipt,
    /// Retry later or dead-letter.
    pub retry: QueueRetry,
}

/// `Extend`: push a lease out. Replies `bool`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtendRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// The delivery's receipt.
    pub receipt: QueueReceipt,
    /// New lease length from now, in milliseconds.
    pub visibility_ms: u64,
}

/// Request addressing one queue (`Stats`, `Purge`). `Purge` replies the
/// number of messages removed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
}

/// Reply to `Stats`: messages by state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueStatsReply {
    /// Deliverable now.
    pub ready: u64,
    /// Waiting for a delay or a retry backoff.
    pub delayed: u64,
    /// Leased to a consumer.
    pub leased: u64,
    /// Dead-lettered.
    pub dead: u64,
}

/// `DeadLetters`: up to `limit` dead letters, oldest first.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadLettersRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// Most dead letters to return; servers clamp it to
    /// [`MAX_PAGE_ITEMS`](crate::MAX_PAGE_ITEMS) and the inline byte cap.
    pub limit: usize,
}

/// One dead-lettered message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDeadLetter {
    /// The message id.
    pub id: String,
    /// Message body.
    #[serde(rename = "payload_json", with = "crate::inline_json")]
    pub payload: Value,
    /// Deliveries it got.
    pub attempt: u32,
    /// Why it was dead-lettered.
    pub reason: String,
    /// The message group, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// When it was enqueued, epoch milliseconds (host clock).
    pub enqueued_ms: i64,
}

/// Reply to `DeadLetters`.
pub type DeadLettersResponse = Vec<QueueDeadLetter>;

/// `Redrive`: put a dead letter back with a fresh attempt count. Replies
/// `bool`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedriveRequest {
    /// Queue name inside the captured grant.
    pub queue: String,
    /// The dead letter's id.
    pub id: String,
}

#[cfg(test)]
#[path = "queues_tests.rs"]
mod tests;
