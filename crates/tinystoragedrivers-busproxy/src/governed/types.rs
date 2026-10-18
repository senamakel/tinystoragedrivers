//! Opaque contexts and routing handles for governed storage leases.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use serde::{Deserialize, Serialize};
use tinybus::{BusName, module::ModuleId};

/// A typed host-owned context that resolves a lease's canonical storage scope.
///
/// The context is stored by the host. It is never serialized into a storage
/// request or copied into a [`StorageDescriptor`].
pub trait HostContext: Send + Sync + 'static {}

/// An opaque host context captured when a storage lease is registered.
pub struct HostStorageContext<C>(C);

impl<C> HostStorageContext<C> {
    /// Capture a host-owned context for a storage lease.
    #[must_use]
    pub const fn new(context: C) -> Self {
        Self(context)
    }

    /// Borrow the captured context for trusted environment resolution.
    #[must_use]
    pub const fn as_ref(&self) -> &C {
        &self.0
    }

    pub(crate) fn into_inner(self) -> C {
        self.0
    }
}

impl<C> fmt::Debug for HostStorageContext<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HostStorageContext(..)")
    }
}

/// A host-issued route to one registration's exact storage object paths.
///
/// This value is routing configuration, not authority. The server registry
/// and the authenticated caller peer determine whether it may be used.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct StorageDescriptor {
    pub(crate) service_peer: String,
    pub(crate) registration_id: u64,
    pub(crate) object_root: String,
}

impl StorageDescriptor {
    /// The broker-assigned unique peer that serves this lease's objects.
    #[must_use]
    pub fn service_peer(&self) -> &str {
        &self.service_peer
    }

    /// The exact object root assigned to this lease.
    #[must_use]
    pub fn object_root(&self) -> &str {
        &self.object_root
    }
}

/// A host-only handle that can revoke one registered storage lease.
#[derive(Clone)]
pub struct StorageLease {
    pub(crate) registration_id: u64,
    pub(crate) owner: Arc<()>,
    pub(crate) epoch: u64,
    pub(crate) caller_peer: BusName,
    pub(crate) module: ModuleId,
    pub(crate) revoked: Arc<AtomicBool>,
    pub(crate) published: Arc<AtomicBool>,
    pub(crate) _drop_guard: Arc<LeaseDropGuard>,
}

pub(crate) struct LeaseDropGuard {
    cleanup: std::sync::Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>,
}

impl LeaseDropGuard {
    pub(crate) fn new(cleanup: impl FnOnce() + Send + 'static) -> Self {
        Self {
            cleanup: std::sync::Mutex::new(Some(Box::new(cleanup))),
        }
    }
}

impl fmt::Debug for LeaseDropGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LeaseDropGuard(..)")
    }
}

impl Drop for LeaseDropGuard {
    fn drop(&mut self) {
        if let Ok(cleanup) = self.cleanup.get_mut()
            && let Some(cleanup) = cleanup.take()
        {
            cleanup();
        }
    }
}

pub(crate) struct LeaseClaims {
    pub(crate) registration_id: u64,
    pub(crate) epoch: u64,
    pub(crate) caller_peer: BusName,
    pub(crate) module: ModuleId,
    pub(crate) revoked: Arc<AtomicBool>,
    pub(crate) published: Arc<AtomicBool>,
}

impl From<&StorageLease> for LeaseClaims {
    fn from(lease: &StorageLease) -> Self {
        Self {
            registration_id: lease.registration_id,
            epoch: lease.epoch,
            caller_peer: lease.caller_peer.clone(),
            module: lease.module.clone(),
            revoked: Arc::clone(&lease.revoked),
            published: Arc::clone(&lease.published),
        }
    }
}

impl fmt::Debug for StorageLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StorageLease(..)")
    }
}
