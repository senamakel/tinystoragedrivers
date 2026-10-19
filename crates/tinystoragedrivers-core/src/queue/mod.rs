//! [`QueueStore`]: durable at-least-once message queues.
//!
//! A queue holds messages until a consumer settles them. A consumer
//! [`receive`](QueueStore::receive)s a batch, each message leased to it for a
//! visibility window, and then [`ack`](QueueStore::ack)s it (done),
//! [`nack`](QueueStore::nack)s it (retry later, or dead-letter), or
//! [`extend`](QueueStore::extend)s the lease while it works. A lease that
//! lapses hands the message to the next receiver with its attempt count
//! raised, and a message that runs out of attempts is dead-lettered instead of
//! looping. Delivery is at least once: a consumer that crashes after acting
//! but before acknowledging sees the message again, so effects must be
//! idempotent.
//!
//! Optional semantics a backend reports in [`QueueFeatures`]:
//!
//! - **groups**: one leased message per group at a time, in priority then
//!   enqueue order, which serializes a conversation without serializing the
//!   whole queue;
//! - **dedupe**: a dedupe key rejects re-enqueues while the message is held
//!   and for a window after it is acknowledged, so a platform redelivery after
//!   a restart is recognized;
//! - **priorities** and **delay**.
//!
//! Queues are declared in a JSON file, not in code: [`QueueManifest`] loads
//! `queues.json` (format in `schemas/queues.schema.json`) and
//! [`apply`](QueueManifest::apply) declares every queue it lists, so people
//! and agents add or tune a queue by editing the file.
//!
//! [`DocumentQueue`] implements every feature over any
//! [`DocumentStore`](crate::DocumentStore), so every document driver (memory,
//! `SQLite`, file, `MongoDB`) is a queue backend. A broker driver implements
//! the trait natively and reports what it lacks.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers_core::queue::{DocumentQueue, NewMessage, QueueSpec, QueueStore};
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//!
//! let storage = MemoryStorage::new().for_scope(&Scope::local())?;
//! let queue = DocumentQueue::new(storage.documents());
//! queue.ensure_queue("inbound", &QueueSpec::default()).await?;
//! queue.enqueue("inbound", &[NewMessage::new(json!({"text": "hi"})).group("chat-1")]).await?;
//!
//! let batch = queue.receive("inbound", 10, 30_000, "worker-a").await?;
//! assert_eq!(batch[0].payload["text"], "hi");
//! assert!(queue.ack("inbound", &batch[0].receipt).await?);
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```

mod document;
mod manifest;
mod types;

use async_trait::async_trait;

use crate::error::Result;

pub use document::{DocumentQueue, GROUPS, MESSAGES, SPECS, collection_specs, ensure};
pub use manifest::{QUEUE_MANIFEST_VERSION, QueueDeclaration, QueueManifest};
pub use types::{
    DeadLetter, Delivery, Enqueued, MAX_PRIORITY, MAX_QUEUE_NAME_LEN, NewMessage, QueueFeatures,
    QueueSpec, QueueStats, Receipt, Retry, validate_queue_name,
};

/// A durable at-least-once message queue port. See the [module](self) docs.
///
/// Every method takes the queue name, which must pass
/// [`validate_queue_name`]. A handle is bound to one scope, like every other
/// port, so two tenants' queues of the same name never meet.
#[async_trait]
pub trait QueueStore: Send + Sync + std::fmt::Debug {
    /// The optional semantics this backend honors.
    fn features(&self) -> QueueFeatures;

    /// Declare `queue` with `spec`, or update the spec of an existing one.
    /// Enqueueing into an undeclared queue uses [`QueueSpec::default`].
    ///
    /// # Errors
    ///
    /// An invalid name or spec, or a backend error.
    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()>;

    /// Add `messages`, in order, and report each one's id and whether it was a
    /// duplicate. Messages are stored independently: a failure part way leaves
    /// the earlier ones stored, and enqueueing again with dedupe keys is the
    /// safe retry.
    ///
    /// # Errors
    ///
    /// An invalid name or message, or a backend error.
    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>>;

    /// Lease up to `max` deliverable messages to `consumer` for
    /// `visibility_ms` milliseconds, highest priority first, then oldest. An
    /// empty result means nothing is deliverable now.
    ///
    /// # Errors
    ///
    /// An invalid name, a zero `max` or `visibility_ms`, or a backend error.
    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> Result<Vec<Delivery>>;

    /// Settle a delivery as done. Returns `false` when the receipt no longer
    /// holds the message (its lease lapsed and it went to another consumer, or
    /// it was already settled).
    ///
    /// # Errors
    ///
    /// An invalid name or a backend error.
    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool>;

    /// Hand a delivery back: retry it later or dead-letter it. Returns `false`
    /// when the receipt no longer holds the message.
    ///
    /// # Errors
    ///
    /// An invalid name or a backend error.
    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool>;

    /// Push a delivery's lease out to `visibility_ms` from now. Returns
    /// `false` when the receipt no longer holds the message.
    ///
    /// # Errors
    ///
    /// An invalid name, a zero `visibility_ms`, or a backend error.
    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool>;

    /// Count the queue's messages by state.
    ///
    /// # Errors
    ///
    /// An invalid name or a backend error.
    async fn stats(&self, queue: &str) -> Result<QueueStats>;

    /// Up to `limit` dead-lettered messages, oldest first.
    ///
    /// # Errors
    ///
    /// An invalid name or a backend error.
    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>>;

    /// Put a dead-lettered message back on the queue with a fresh attempt
    /// count. Returns `false` when no dead letter has that id.
    ///
    /// # Errors
    ///
    /// An invalid name or a backend error.
    async fn redrive(&self, queue: &str, id: &str) -> Result<bool>;

    /// Remove every message of `queue`, in any state, and report how many.
    ///
    /// # Errors
    ///
    /// An invalid name or a backend error.
    async fn purge(&self, queue: &str) -> Result<u64>;
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
