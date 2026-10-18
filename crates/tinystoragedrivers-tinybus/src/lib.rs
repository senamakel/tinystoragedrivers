//! Confined native document callbacks for compiled `TinyBus` modules.
//!
//! Registration captures an already-admitted store and fixed collection.
//! It never opens a backend or declares schema. Native storage owns atomic
//! conditions, validation, leases and generations. Native messages and source
//! chains never cross this wire boundary.
//!
//! ```no_run
//! use std::sync::Arc;
//! use tinybus::module::HostCallbackBinding;
//! use tinystoragedrivers_core::DocumentStore;
//! use tinystoragedrivers_tinybus::serve_collection;
//! # async fn register(binding: &HostCallbackBinding, store: Arc<dyn DocumentStore>) -> tinybus::Result<()> {
//! // The host already selected this store's scope, admission and schema.
//! serve_collection(binding, "/documents".parse()?, store, "learned").await?;
//! // Load the verified module with this same binding before it can call back.
//! # Ok(())
//! # }
//! ```
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tinybus::module::{HostCallbackBinding, HostCallbackEndpoint, HostCallbackGuard};
use tinybus::{CallContext, Interface, InterfaceName, MemberName, ObjectPath};
use tinystoragedrivers_core::DocumentStore;

mod storage_host;
pub use storage_host::{StorageRegistration, attach_storage};

/// Declare the exact callback interface and finite member allowlist.
///
/// # Errors
/// Refuses an invalid `TinyBus` path or name.
pub fn endpoint(path: &str) -> tinybus::Result<HostCallbackEndpoint> {
    Ok(HostCallbackEndpoint {
        path: ObjectPath::new(path)?,
        interface: InterfaceName::new(tinystoragedrivers_bus::INTERFACE)?,
        members: tinystoragedrivers_bus::MEMBERS
            .iter()
            .map(|member| MemberName::new(*member))
            .collect::<tinybus::Result<_>>()?,
    })
}

/// Serve only on a private SDK-created callback binding.
///
/// Collection authority is fixed by the host before module initialization.
/// The store must already carry the intended native admission.
///
/// # Errors
/// Refuses invalid collection names, undeclared endpoints, duplicate registration,
/// or a revoked/used binding. Never declares schema or changes admission.
pub async fn serve_collection(
    binding: &HostCallbackBinding,
    path: ObjectPath,
    documents: Arc<dyn DocumentStore>,
    collection: impl Into<String>,
) -> tinybus::Result<()> {
    let collection = collection.into();
    tinystoragedrivers_core::validate_collection(&collection)
        .map_err(|_| tinybus::Error::failed("invalid document callback collection"))?;
    let declaration = endpoint(path.as_str())?;
    binding
        .serve_at(
            path,
            DocumentService {
                documents,
                collection,
                guard: binding.guard(),
                interface: declaration.interface,
                members: declaration.members,
            },
        )
        .await
}

struct DocumentService {
    documents: Arc<dyn DocumentStore>,
    collection: String,
    guard: HostCallbackGuard,
    interface: InterfaceName,
    members: Vec<MemberName>,
}
impl DocumentService {
    async fn dispatch(&self, member: &MemberName, args: Value) -> tinybus::Result<Value> {
        match member.as_str() {
            "Get" => {
                let (request,): (tinystoragedrivers_bus::GetRequest,) = decode(args)?;
                let outcome = self
                    .documents
                    .get(&self.collection, &request.id)
                    .await
                    .map(|document| {
                        document.map(|document| tinystoragedrivers_bus::Versioned {
                            id: document.id,
                            version: tinystoragedrivers_bus::Version(document.version.0),
                            doc: document.doc,
                            // The legacy callback never carried metadata.
                            meta: None,
                        })
                    })
                    .map_err(|error| storage_failure(&error));
                encode(outcome)
            }
            "Put" => {
                let (request,): (tinystoragedrivers_bus::PutRequest,) = decode(args)?;
                encode(
                    self.documents
                        .put(
                            &self.collection,
                            &request.id,
                            request.doc,
                            native_condition(request.precondition),
                        )
                        .await
                        .map(|version| tinystoragedrivers_bus::Version(version.0))
                        .map_err(|error| storage_failure(&error)),
                )
            }
            "Delete" => {
                let (request,): (tinystoragedrivers_bus::DeleteRequest,) = decode(args)?;
                encode(
                    self.documents
                        .delete(
                            &self.collection,
                            &request.id,
                            native_condition(request.precondition),
                        )
                        .await
                        .map_err(|error| storage_failure(&error)),
                )
            }
            _ => Err(tinybus::Error::failed("unknown document callback member")),
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(args: Value) -> tinybus::Result<T> {
    serde_json::from_value(args)
        .map_err(|_| tinybus::Error::failed("invalid document callback arguments"))
}

fn encode<T: serde::Serialize>(outcome: T) -> tinybus::Result<Value> {
    serde_json::to_value(outcome)
        .map_err(|_| tinybus::Error::failed("document callback serialization failed"))
}

fn native_condition(
    condition: tinystoragedrivers_bus::Precondition,
) -> tinystoragedrivers_core::Precondition {
    use tinystoragedrivers_bus::Precondition;
    match condition {
        Precondition::None => tinystoragedrivers_core::Precondition::None,
        Precondition::Absent => tinystoragedrivers_core::Precondition::Absent,
        Precondition::Version(version) => tinystoragedrivers_core::Precondition::Version(
            tinystoragedrivers_core::Version(version.0),
        ),
    }
}

fn storage_failure(
    error: &tinystoragedrivers_core::StorageError,
) -> tinystoragedrivers_bus::StorageFailure {
    use tinystoragedrivers_bus::{StorageFailure, StorageKind};
    use tinystoragedrivers_core::ErrorKind;
    let kind = match error.kind() {
        ErrorKind::NotFound => StorageKind::NotFound,
        ErrorKind::Conflict => StorageKind::Conflict,
        ErrorKind::AlreadyExists => StorageKind::AlreadyExists,
        ErrorKind::Unsupported(_) => StorageKind::Unsupported,
        ErrorKind::InvalidInput => StorageKind::InvalidInput,
        ErrorKind::Serialization => StorageKind::Serialization,
        ErrorKind::Crypto => StorageKind::Crypto,
        ErrorKind::Unavailable => StorageKind::Unavailable,
        ErrorKind::Fenced => StorageKind::Fenced,
        ErrorKind::ScopeGenerationRejected => StorageKind::ScopeGenerationRejected,
        // Future native error kinds safely retain the generic driver category.
        _ => StorageKind::Backend,
    };
    StorageFailure { kind }
}
#[async_trait]
impl Interface for DocumentService {
    fn name(&self) -> InterfaceName {
        self.interface.clone()
    }
    fn members(&self) -> Vec<MemberName> {
        self.members.clone()
    }
    fn requires_confidential(&self, _: &MemberName) -> bool {
        true
    }
    async fn call(&self, _: &MemberName, _: Value) -> tinybus::Result<Value> {
        Err(tinybus::Error::failed(
            "authenticated document callback required",
        ))
    }
    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> tinybus::Result<Value> {
        if !self.guard.authorizes(context) {
            return Err(tinybus::Error::failed(
                "document callback authority unavailable",
            ));
        }
        self.dispatch(member, args).await
    }
}
#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "storage_host_tests.rs"]
mod storage_host_tests;
