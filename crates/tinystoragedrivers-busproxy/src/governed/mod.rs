//! Host-owned storage contexts, leases, and per-call governance.

mod endpoint;
mod types;

pub use endpoint::{
    AuditEvent, AuditSink, CallerDirectory, Environment, GovernedEndpoint, GovernedEndpointLimits,
    GovernedStorage, ResolvedStorage, StorageRegistration, TinyBusCallerDirectory,
};
pub use types::{HostContext, HostStorageContext, StorageDescriptor, StorageLease};
