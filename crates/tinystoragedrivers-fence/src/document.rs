//! Adapter from the core document lease primitive to the generic lease store.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tinystoragedrivers_core::{
    Acquire, Acquisition, DocumentStore, Fence, Filter, Key, LeaseRecord, Leases, Scope,
    StorageError, Version, primitives::LEASES,
};
use tokio::sync::Mutex;

use crate::{LeaseError, LeaseGrant, LeaseStore};

/// Lease store backed by `tinystoragedrivers_core::Leases`.
///
/// The caller supplies the document view already bound to the scope where
/// lease records live. The owner, holder identity, and TTL are explicit; the
/// adapter does not choose profile scope or host policy.
pub struct DocumentLeases {
    leases: Leases,
    guard_scope: Scope,
    owner: String,
    holder: String,
    endpoint: Option<String>,
    ttl_ms: i64,
    now_ms: Arc<AtomicU64>,
    grants: Mutex<HashMap<String, (u64, Version)>>,
}

impl std::fmt::Debug for DocumentLeases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentLeases")
            .field("guard_scope", &self.guard_scope)
            .field("owner", &self.owner)
            .field("holder", &self.holder)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl DocumentLeases {
    /// Create a document-backed store using the injected clock passed to its
    /// [`LeaseStore`] operations.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an empty owner or holder, or a non-positive
    /// TTL.
    pub fn new(
        documents: &Arc<dyn DocumentStore>,
        guard_scope: Scope,
        owner: impl Into<String>,
        holder: impl Into<String>,
        ttl_ms: i64,
    ) -> Result<Self, StorageError> {
        let owner = owner.into();
        let holder = holder.into();
        if owner.is_empty() || holder.is_empty() || ttl_ms <= 0 {
            return Err(StorageError::invalid_input(
                "lease owner and holder must be non-empty and ttl positive",
            ));
        }
        let now_ms = Arc::new(AtomicU64::new(0));
        let clock_value = now_ms.clone();
        let leases =
            Leases::new(documents).with_clock(Arc::new(move || clock_value.load(Ordering::SeqCst)));
        Ok(Self {
            leases,
            guard_scope,
            owner,
            holder,
            endpoint: None,
            ttl_ms,
            now_ms,
            grants: Mutex::new(HashMap::new()),
        })
    }

    /// Set the endpoint written into lease records and returned to contenders.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }

    fn set_now(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }

    fn record_fence(&self, key: &str, holder: &str, epoch: u64) -> Option<Fence> {
        let id = Key::of([self.owner.as_str(), key])
            .ok()?
            .as_str()
            .to_owned();
        Some(Fence::new(
            self.guard_scope.clone(),
            LEASES,
            id,
            Filter::eq("epoch", epoch)
                .and(Filter::eq("holder", holder))
                .and(Filter::exists("released", false)),
        ))
    }
}

#[async_trait::async_trait]
impl LeaseStore for DocumentLeases {
    async fn acquire(&self, key: &str, now_ms: u64) -> Result<LeaseGrant, LeaseError> {
        let mut grants = self.grants.lock().await;
        self.set_now(now_ms);
        let held_epoch = grants.get(key).map(|(epoch, _)| *epoch);
        match self
            .leases
            .acquire_with(Acquire {
                owner: &self.owner,
                key,
                holder: &self.holder,
                ttl_ms: self.ttl_ms,
                endpoint: self.endpoint.as_deref(),
                held_epoch,
            })
            .await?
        {
            Acquisition::Held(record) => Err(LeaseError::Held(record)),
            Acquisition::Granted {
                lease,
                previous_unclean,
            } => {
                let version = grants
                    .get(key)
                    .filter(|(epoch, _)| *epoch == lease.epoch)
                    .map_or(Ok(Version::FIRST), |(_, version)| {
                        version.next().ok_or_else(|| {
                            StorageError::backend("lease grant version space exhausted")
                        })
                    })?;
                grants.insert(key.to_owned(), (lease.epoch, version));
                Ok(LeaseGrant {
                    key: key.to_owned(),
                    holder: self.holder.clone(),
                    epoch: lease.epoch,
                    version,
                    expires_ms: lease.expires_ms,
                    previous_unclean,
                })
            }
        }
    }

    async fn renew(&self, grant: &LeaseGrant, now_ms: u64) -> Result<LeaseGrant, LeaseError> {
        let mut grants = self.grants.lock().await;
        self.set_now(now_ms);
        if grants.get(&grant.key) != Some(&(grant.epoch, grant.version))
            || grant.holder != self.holder
        {
            return Err(LeaseError::Lost);
        }
        let Some(lease) = self
            .leases
            .renew(
                &self.owner,
                &grant.key,
                &self.holder,
                grant.epoch,
                self.ttl_ms,
            )
            .await?
        else {
            return Err(LeaseError::Lost);
        };
        let version = grant
            .version
            .next()
            .ok_or_else(|| StorageError::backend("lease grant version space exhausted"))?;
        grants.insert(grant.key.clone(), (grant.epoch, version));
        Ok(LeaseGrant {
            key: grant.key.clone(),
            holder: self.holder.clone(),
            epoch: grant.epoch,
            version,
            expires_ms: lease.expires_ms,
            previous_unclean: grant.previous_unclean,
        })
    }

    async fn release(&self, grant: LeaseGrant) -> Result<(), LeaseError> {
        let mut grants = self.grants.lock().await;
        if grants.get(&grant.key) != Some(&(grant.epoch, grant.version))
            || grant.holder != self.holder
        {
            return Err(LeaseError::Lost);
        }
        self.set_now(
            u64::try_from(grant.expires_ms.saturating_sub(self.ttl_ms)).unwrap_or_default(),
        );
        let released = self
            .leases
            .release(&self.owner, &grant.key, &self.holder, grant.epoch)
            .await?;
        if !released {
            return Err(LeaseError::Lost);
        }
        grants.remove(&grant.key);
        Ok(())
    }

    async fn inspect(&self, key: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        Ok(self.leases.inspect(&self.owner, key).await?)
    }

    fn driver_fence(&self, key: &str, holder: &str, epoch: u64) -> Option<Fence> {
        self.record_fence(key, holder, epoch)
    }
}

#[cfg(test)]
#[path = "document_tests.rs"]
mod tests;
