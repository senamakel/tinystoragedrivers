//! Pluggable storage for OpenHuman and the libraries it hosts.
//!
//! One set of ports ([`DocumentStore`], [`StreamStore`], [`BlobStore`]) with a
//! driver per backend, chosen by URL at boot and compiled in by Cargo feature.
//! Every crate that persists data builds its typed repository on the ports and
//! stays unaware of which backend the host picked.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers::{Precondition, Scope, StorageConfig};
//!
//! let backend = tinystoragedrivers::open(&StorageConfig::parse("memory")?).await?;
//! let approvals = backend.for_scope(&Scope::local())?;
//! approvals
//!     .documents()
//!     .put("pending", "req-1", json!({"tool": "shell"}), Precondition::Absent)
//!     .await?;
//! # Ok::<(), tinystoragedrivers::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! # Drivers
//!
//! | URL | Driver | Feature |
//! | --- | --- | --- |
//! | `memory` | in-process maps | always |
//! | `sqlite:<dir>`, `sqlite:<file>.db`, `/abs/path` | SQLite ([`sqlite`]) | `sqlite` |
//! | `file:<dir>` | JSON, JSONL and raw files under a directory | `file` |
//! | `mongodb://…/<db>`, `mongodb+srv://…/<db>` | MongoDB, every record scoped to its tenant | `mongodb` |
//!
//! # Queues
//!
//! Every scope has a [`QueueStore`] at [`ScopedStorage::queues`]: by default
//! durable queues in its own documents. [`QueueConfig`] and [`open_queues`]
//! move them to a broker with a separate queue URL (`amqp://…`, feature
//! `rabbitmq`) and declare them from a
//! [`QueueManifest`](queue::QueueManifest) file.
//!
//! # Secrets
//!
//! With the `secrets` feature, [`secrets`] is the `tinystoragedrivers-secrets`
//! crate: the `SecretStore` port for API keys and tokens, with drivers for
//! memory, an encrypted `secrets.enc` file, and `enc2:` ciphertext in any
//! [`DocumentStore`] under a per-scope key. The `keyring` feature adds the OS
//! credential store. It has no URL form; a host builds the store it wants.
//!
//! This crate holds no repository for any particular record type; sessions,
//! approvals and workflow runs belong to the crates that own them.

mod blobs;
mod config;
mod queues;
pub use blobs::{BlobConfig, open_blobs, open_composite};
pub use queues::{QueueConfig, open_queues};

pub use config::{StorageConfig, open, redact_url};
pub use tinystoragedrivers_core::*;
/// The SQLite driver, including raw access for owners that keep their own
/// tables.
#[cfg(feature = "sqlite")]
pub use tinystoragedrivers_sqlite as sqlite;

/// Encrypted secret storage (feature `secrets`).
#[cfg(feature = "secrets")]
pub use tinystoragedrivers_secrets as secrets;

/// The MongoDB backend, for hosts that construct it directly (an injected
/// clock, transactions turned off, expiry sweeps).
#[cfg(feature = "mongodb")]
pub use tinystoragedrivers_mongodb::MongoStorage;

/// The `RabbitMQ` queue backend (feature `rabbitmq`).
#[cfg(feature = "rabbitmq")]
pub use tinystoragedrivers_rabbitmq::RabbitQueues;

/// The S3-compatible scoped blob driver (feature s3).
#[cfg(feature = "s3")]
pub use tinystoragedrivers_s3 as s3;
