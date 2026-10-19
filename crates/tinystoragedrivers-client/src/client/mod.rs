//! The client: native port traits over a [`StorageTransport`].
mod blobs;
mod documents;
mod queues;
mod secrets;
mod streams;
mod watch;

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tinystoragedrivers_bus::{BlobCapabilities, CapabilitiesReply, QueueFeaturesReply};
use tinystoragedrivers_core::queue::QueueFeatures;
use tinystoragedrivers_core::{
    Capabilities, Capability, Result, Scope, ScopedStorage, StorageBackend, StorageError,
};

use crate::convert::queues::features_from_wire;
use crate::convert::{HOST_ONLY, capabilities_of, error_of};
use crate::transport::{Object, StorageTransport, WireError};

pub use blobs::BusBlobs;
pub use documents::BusDocuments;
pub use queues::BusQueues;
pub use secrets::BusSecrets;
pub use streams::BusStreams;

/// Shared request plumbing of every port handle.
#[derive(Clone)]
pub(crate) struct Wire {
    transport: Arc<dyn StorageTransport>,
}

impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wire").finish_non_exhaustive()
    }
}

impl Wire {
    pub(crate) fn new(transport: Arc<dyn StorageTransport>) -> Self {
        Self { transport }
    }

    pub(crate) fn transport(&self) -> &Arc<dyn StorageTransport> {
        &self.transport
    }

    /// Call `selector` and decode the reply. `unsupported_as` names the
    /// capability a server `Unsupported` refusal stands for.
    pub(crate) async fn call<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        object: Object,
        selector: &str,
        request: &Req,
        unsupported_as: Option<Capability>,
    ) -> Result<Resp> {
        let request = serde_json::to_value(request)?;
        let reply = self
            .transport
            .call(object, selector, request)
            .await
            .map_err(|error| wire_error(error, unsupported_as))?;
        Ok(serde_json::from_value(reply)?)
    }
}

pub(crate) fn wire_error(error: WireError, unsupported_as: Option<Capability>) -> StorageError {
    match error {
        WireError::Storage(failure) => error_of(failure, unsupported_as),
        WireError::Transport(reason) => StorageError::unavailable(reason),
    }
}

/// A storage backend reached over a bus, bound to the one scope its host chose.
///
/// There is no way to select another scope, fence or generation from
/// here: those are fixed by the host's grant, so [`StorageBackend::for_scope`]
/// accepts only the bound scope and the fenced and generation variants stay
/// unsupported.
#[derive(Clone)]
pub struct BusStorage {
    wire: Wire,
    scope: Scope,
    capabilities: Capabilities,
    blob_capabilities: BlobCapabilities,
    queue_features: QueueFeatures,
}

impl std::fmt::Debug for BusStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusStorage")
            .field("scope", &self.scope)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl BusStorage {
    /// Connect over `transport` and learn the admitted backend's capabilities
    /// and queue features.
    /// `scope` is a label for the scope the server end is bound to; the server
    /// never reads it.
    ///
    /// # Errors
    /// Transport failures, or a server that refuses the capability calls.
    pub async fn connect(transport: Arc<dyn StorageTransport>, scope: Scope) -> Result<Self> {
        let wire = Wire::new(transport);
        let documents: CapabilitiesReply = wire
            .call(Object::Documents, "Capabilities", &(), None)
            .await?;
        let blob_capabilities: BlobCapabilities =
            wire.call(Object::Blobs, "Capabilities", &(), None).await?;
        let features: QueueFeaturesReply = wire.call(Object::Queues, "Features", &(), None).await?;
        let mut capabilities = capabilities_of(&documents.capabilities);
        for capability in HOST_ONLY {
            capabilities = capabilities.without(capability);
        }
        Ok(Self {
            wire,
            scope,
            capabilities,
            blob_capabilities,
            queue_features: features_from_wire(features),
        })
    }

    /// The `Secrets` handle. Calls succeed only when the server admitted this
    /// caller on the confidential route.
    #[must_use]
    pub fn secrets(&self) -> Arc<BusSecrets> {
        Arc::new(BusSecrets::new(self.wire.clone()))
    }
}

#[async_trait::async_trait]
impl StorageBackend for BusStorage {
    fn driver(&self) -> &'static str {
        "bus"
    }

    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        if scope != &self.scope {
            return Err(StorageError::invalid_input(
                "a bus caller is bound to one host-chosen scope",
            ));
        }
        Ok(ScopedStorage::new(
            self.scope.clone(),
            "bus",
            Arc::new(BusDocuments::new(self.wire.clone(), self.capabilities)),
            Arc::new(BusStreams::new(self.wire.clone())),
            Arc::new(BusBlobs::new(self.wire.clone(), self.blob_capabilities)),
        )
        .with_queues(Arc::new(BusQueues::new(
            self.wire.clone(),
            self.queue_features,
        ))))
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use tinystoragedrivers_core::DocumentWatch;
    use tokio::sync::broadcast::Receiver;

    use super::{Wire, watch::BusWatch};

    /// A watch for id `mine`, fed by `signals`, with a transport that is
    /// never used (closing is best effort).
    pub(crate) fn watch(
        signals: Receiver<tinystoragedrivers_bus::WatchChanged>,
    ) -> Box<dyn DocumentWatch> {
        let backend = tinystoragedrivers_core::MemoryStorage::new();
        let storage = tinystoragedrivers_core::StorageBackend::for_scope(
            &backend,
            &tinystoragedrivers_core::Scope::local(),
        )
        .unwrap();
        let server = std::sync::Arc::new(crate::StorageServer::new(storage));
        let transport =
            std::sync::Arc::new(crate::Loopback::new(server, crate::Admission::Private));
        Box::new(BusWatch::new(Wire::new(transport), "mine".into(), signals))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
