//! The `RabbitMQ` queue backend: [`QueueStore`] over AMQP 0-9-1.
//!
//! `RabbitMQ` is a queue, not a document store, so this crate implements only
//! the queue port and is selected by its own `amqp://` / `amqps://` URL. The
//! mapping (see `docs/specs/queue-rabbitmq.md`):
//!
//! - a queue `q` of scope `s` is the durable classic queue `tsd/<h>/q`, `<h>`
//!   a stable hash of `s`, with `x-max-priority: 9`;
//! - dead letters wait in `tsd/<h>/q/dead`;
//! - delays round up to a power of two between 256 ms and [`MAX_DELAY_MS`] and
//!   wait in a TTL queue that dead-letters back into `tsd/<h>/q`;
//! - `receive` is `basic.get`; the lease, its timer and the delivery tag stay
//!   in the handle, so a [`Receipt`](tinystoragedrivers_core::queue::Receipt)
//!   works only on the handle that issued it;
//! - the driver counts attempts in a JSON envelope around each payload, and
//!   moves a message by publishing its next state (with publisher confirms)
//!   before acknowledging it, so a crash duplicates and never loses.
//!
//! [`QueueFeatures`](tinystoragedrivers_core::queue::QueueFeatures) reports
//! priorities and delay; groups and dedupe are absent, and a message asking
//! for either is refused rather than silently stored.
//!
//! ```no_run
//! # async fn run() -> tinystoragedrivers_core::Result<()> {
//! use serde_json::json;
//! use tinystoragedrivers_core::queue::{NewMessage, QueueStore};
//! use tinystoragedrivers_core::Scope;
//! use tinystoragedrivers_rabbitmq::RabbitQueues;
//!
//! let queues = RabbitQueues::connect("amqp://guest:guest@localhost:5672/%2f", &Scope::local()).await?;
//! queues.enqueue("inbound", &[NewMessage::new(json!({"text": "hi"}))]).await?;
//! let batch = queues.receive("inbound", 10, 30_000, "worker").await?;
//! queues.ack("inbound", &batch[0].receipt).await?;
//! # Ok(())
//! # }
//! ```
//!
//! This crate holds no document, stream or blob port; pair it with any
//! storage backend through `ScopedStorage::with_queues`.

mod envelope;
mod link;
mod names;
mod queues;

pub use names::MAX_DELAY_MS;
pub use queues::RabbitQueues;
pub use tinystoragedrivers_core::QueueStore;
