//! Where a scope's queues live, chosen by a URL separate from the storage URL.
//!
//! By default a scope's [`QueueStore`](crate::QueueStore) is the
//! `DocumentQueue` over its own documents ([`ScopedStorage::queues`]). A
//! deployment with a broker points the queues elsewhere with an `amqp://` or
//! `amqps://` URL (feature `rabbitmq`), and may declare them from a
//! [`QueueManifest`] file at the same time.

use std::fmt;
use std::path::Path;

use crate::queue::QueueManifest;
use crate::{Result, ScopedStorage, StorageError};

/// The queue location of a deployment.
#[derive(Clone, PartialEq, Eq)]
pub enum QueueConfig {
    /// The scope's own documents: `ScopedStorage::queues`, unchanged.
    Storage,
    /// A `RabbitMQ` broker (`amqp://` or `amqps://`).
    RabbitMq {
        /// The broker URL, credentials included; never printed.
        url: String,
    },
}

impl QueueConfig {
    /// Parse a queue URL: `storage` (or an empty string) keeps the queues in
    /// the storage backend; `amqp://…` and `amqps://…` select `RabbitMQ`.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for any other form, or for an AMQP URL when the
    /// `rabbitmq` feature is not compiled in.
    pub fn parse(url: &str) -> Result<Self> {
        let url = url.trim();
        if url.is_empty() || url == "storage" {
            return Ok(Self::Storage);
        }
        if url.starts_with("amqp://") || url.starts_with("amqps://") {
            #[cfg(feature = "rabbitmq")]
            return Ok(Self::RabbitMq {
                url: url.to_owned(),
            });
            #[cfg(not(feature = "rabbitmq"))]
            return Err(StorageError::invalid_input(
                "an amqp queue URL needs the rabbitmq feature",
            ));
        }
        Err(StorageError::invalid_input(
            "a queue URL is `storage`, `amqp://…` or `amqps://…`",
        ))
    }
}

/// `amqp://user:secret@host/vhost` printed as `amqp://***@host/vhost`.
fn redacted(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{scheme}://***{}", &rest[at..]),
        None => url.to_owned(),
    }
}

impl fmt::Display for QueueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage => f.write_str("storage"),
            Self::RabbitMq { url } => f.write_str(&redacted(url)),
        }
    }
}

impl fmt::Debug for QueueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QueueConfig({self})")
    }
}

/// Install the queues `config` names on `storage`, then declare every queue
/// of the manifest at `manifest`, if given, on them.
///
/// The returned handles are `storage`'s with [`ScopedStorage::queues`]
/// replaced for a broker, so a host serves them over the storage bus like any
/// other port.
///
/// # Errors
///
/// An unreachable broker, a missing feature, an unreadable or invalid
/// manifest, or a failing [`ensure_queue`](crate::QueueStore::ensure_queue).
pub async fn open_queues(
    config: &QueueConfig,
    storage: ScopedStorage,
    manifest: Option<&Path>,
) -> Result<ScopedStorage> {
    let manifest = manifest.map(QueueManifest::load).transpose()?;
    let storage = match config {
        QueueConfig::Storage => storage,
        #[cfg(feature = "rabbitmq")]
        QueueConfig::RabbitMq { url } => {
            let queues =
                tinystoragedrivers_rabbitmq::RabbitQueues::connect(url, storage.scope()).await?;
            storage.with_queues(std::sync::Arc::new(queues))
        }
        #[cfg(not(feature = "rabbitmq"))]
        QueueConfig::RabbitMq { .. } => {
            return Err(StorageError::invalid_input(
                "an amqp queue URL needs the rabbitmq feature",
            ));
        }
    };
    if let Some(manifest) = manifest {
        manifest.apply(storage.queues().as_ref()).await?;
    }
    Ok(storage)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
