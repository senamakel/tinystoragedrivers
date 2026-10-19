//! Reference host-side adapter: answers storage bus messages from an installed
//! backend.
//!
//! The adapter is generic over [`ScopedStorage`], already bound by the host to
//! one scope. Requests carry none of that, and they reject unknown
//! fields, so there is no way to name another scope. The adapter enforces the
//! wire limits before native I/O and maps native errors to the sanitized
//! [`StorageFailure`] category.
mod blobs;
mod documents;
mod queues;
mod secrets;
mod streams;
mod watch;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tinystoragedrivers_bus::{
    BlobInfo, BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageFailure, StorageKind,
    WatchChanged,
};
use tinystoragedrivers_core::queue::QueueManifest;
use tinystoragedrivers_core::{ScopedStorage, StorageError};
use tinystoragedrivers_secrets::SecretStore;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::convert::failure_of;
use crate::transport::{Admission, BlobBody, Object};

/// Most watch subscriptions one caller binding may hold open.
pub(crate) const MAX_WATCHES: usize = 64;
const SIGNAL_BUFFER: usize = 256;

/// Open watch tasks plus the slots reserved by opens still awaiting the native
/// watch, so concurrent opens cannot together exceed [`MAX_WATCHES`].
#[derive(Debug, Default)]
pub(crate) struct WatchRegistry {
    pub(crate) tasks: HashMap<String, JoinHandle<()>>,
    pub(crate) reserved: usize,
}

impl WatchRegistry {
    pub(crate) fn used(&self) -> usize {
        self.tasks.len() + self.reserved
    }
}

pub(crate) type Reply = Result<Value, StorageFailure>;

pub(crate) fn invalid() -> StorageFailure {
    StorageFailure {
        kind: StorageKind::InvalidInput,
    }
}

pub(crate) fn decode<T: DeserializeOwned>(request: Value) -> Result<T, StorageFailure> {
    serde_json::from_value(request).map_err(|_| invalid())
}

pub(crate) fn encode<T: Serialize>(reply: &T) -> Reply {
    serde_json::to_value(reply).map_err(|_| StorageFailure {
        kind: StorageKind::Serialization,
    })
}

pub(crate) fn native<T>(result: Result<T, StorageError>) -> Result<T, StorageFailure> {
    result.map_err(|error| failure_of(&error))
}

/// The host side of one bound caller.
pub struct StorageServer {
    pub(crate) storage: ScopedStorage,
    pub(crate) secrets: Option<std::sync::Arc<dyn SecretStore>>,
    pub(crate) signals: broadcast::Sender<WatchChanged>,
    pub(crate) watches: Arc<Mutex<WatchRegistry>>,
    pub(crate) next_watch: AtomicU64,
    pub(crate) queue_manifest: Option<Arc<QueueManifest>>,
    pub(crate) queues_declared: tokio::sync::OnceCell<()>,
}

impl std::fmt::Debug for StorageServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageServer")
            .field("storage", &self.storage)
            .field("secrets", &self.secrets.is_some())
            .field("queue_manifest", &self.queue_manifest.is_some())
            .finish_non_exhaustive()
    }
}

impl StorageServer {
    /// Serve `storage`, whose scope (and any fence or generation) the host
    /// already chose.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        let (signals, _) = broadcast::channel(SIGNAL_BUFFER);
        Self {
            storage,
            secrets: None,
            signals,
            watches: Arc::new(Mutex::new(WatchRegistry::default())),
            next_watch: AtomicU64::new(1),
            queue_manifest: None,
            queues_declared: tokio::sync::OnceCell::new(),
        }
    }

    /// Serve exactly the queues `manifest` declares. The host declares them
    /// (with [`QueueManifest::apply`]) on the caller's first `Queues` call;
    /// a request naming any other queue is refused `NotFound`, and an
    /// `EnsureQueue` is accepted only when it asks for the declared spec
    /// (`Conflict` otherwise), so the manifest stays the one source of truth.
    /// Without a manifest the caller may declare any queue in its scope.
    #[must_use]
    pub fn with_queue_manifest(mut self, manifest: Arc<QueueManifest>) -> Self {
        self.queue_manifest = Some(manifest);
        self
    }

    /// Also serve `Secrets` from `secrets`, to confidential callers only.
    #[must_use]
    pub fn with_secrets(mut self, secrets: std::sync::Arc<dyn SecretStore>) -> Self {
        self.secrets = Some(secrets);
        self
    }

    /// Number of watch subscriptions currently held open for this caller.
    #[must_use]
    pub fn open_watches(&self) -> usize {
        self.watches.lock().map_or(0, |watches| watches.tasks.len())
    }

    /// A receiver of the `WatchChanged` signals this server emits.
    #[must_use]
    pub fn signals(&self) -> broadcast::Receiver<WatchChanged> {
        self.signals.subscribe()
    }

    /// Answer one request/reply message. Unknown objects and selectors fail
    /// closed with `InvalidInput`; `Secrets` needs [`Admission::Confidential`].
    ///
    /// # Errors
    /// A sanitized storage failure category.
    pub async fn handle(
        &self,
        admission: Admission,
        object: &str,
        selector: &str,
        request: Value,
    ) -> Reply {
        let Some(object) = Object::parse(object) else {
            return Err(invalid());
        };
        match object {
            Object::Documents => self.documents(selector, request).await,
            Object::Streams => self.streams(selector, request).await,
            Object::Blobs => self.blobs(selector, request).await,
            Object::Queues => self.queues(selector, request).await,
            Object::Secrets => {
                if admission != Admission::Confidential {
                    return Err(StorageFailure {
                        kind: StorageKind::Unsupported,
                    });
                }
                self.secrets(selector, request).await
            }
        }
    }

    /// `Blobs.PutStream`: store `body`, which must be exactly `head.len` bytes.
    ///
    /// # Errors
    /// A sanitized storage failure category.
    pub async fn put_blob_stream(
        &self,
        head: PutBlobStreamRequest,
        body: BlobBody,
    ) -> Result<BlobInfo, StorageFailure> {
        self.put_stream(head, body).await
    }

    /// `Blobs.GetStream`: the blob's metadata and body, or `None`.
    ///
    /// # Errors
    /// A sanitized storage failure category.
    pub async fn get_blob_stream(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobBody)>, StorageFailure> {
        self.get_stream(request).await
    }
}

impl Drop for StorageServer {
    fn drop(&mut self) {
        if let Ok(mut watches) = self.watches.lock() {
            for (_, task) in watches.tasks.drain() {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
