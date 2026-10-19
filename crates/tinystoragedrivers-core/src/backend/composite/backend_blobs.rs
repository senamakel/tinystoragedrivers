//! Generation-isolated blob adapter for a complete storage backend.
//!
//! Generation zero preserves the backend's historical scope names. Later
//! generations use bounded reserved physical scopes, with a CAS-maintained
//! mapping index in a separate reserved scope so cleanup works even when the
//! backend cannot enumerate scopes. Existing unmapped data at a generated
//! physical name is treated as a collision and is never claimed or deleted.
use super::BlobBackend;
use crate::{
    Blob, BlobMeta, BlobReader, BlobStore, BlobStream, Capabilities, Capability, Precondition,
    Result, Scope, ScopeGeneration, StorageBackend, StorageError, async_trait,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{ops::Range, sync::Arc, time::Duration};

const INTERNAL_PREFIX: &str = "__tsd_composite_blob_generation";
const MAP_SCOPE: &str = "__tsd_composite_blob_scope_map";
const MAP_COLLECTION: &str = "scopes";
pub(super) const RESERVED_SCOPE_PREFIX: &str = INTERNAL_PREFIX;
pub(super) const RESERVED_MAP_SCOPE: &str = MAP_SCOPE;

/// Use the blob port of an existing complete backend independently.
///
/// Generation zero reads and writes the legacy scope. Later generations map
/// to bounded internal scope names so a record backend's generation counter
/// does not need to match this backend's counter. The mapping index is stored
/// in a reserved scope and is maintained before writes, allowing whole-scope
/// cleanup without scope enumeration.
#[derive(Debug)]
pub struct BackendBlobs(Arc<dyn StorageBackend>);

impl BackendBlobs {
    /// Wrap an existing backend's blob port.
    #[must_use]
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self(backend)
    }

    fn map_scope() -> Result<Scope> {
        Scope::new(MAP_SCOPE)
    }

    fn digest(scope: &Scope) -> String {
        let digest = Sha256::digest(scope.as_str().as_bytes());
        let mut encoded = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(encoded, "{byte:02x}");
        }
        encoded
    }

    fn physical_scope(scope: &Scope, generation: ScopeGeneration) -> Result<Scope> {
        Scope::new(format!(
            "{INTERNAL_PREFIX}/{}/{}",
            Self::digest(scope),
            generation.0
        ))
    }

    async fn mapping_entry(
        &self,
        digest: &str,
    ) -> Result<Option<(String, Vec<u64>, crate::Version)>> {
        let scoped = self.0.for_scope(&Self::map_scope()?)?;
        let Some(record) = scoped.documents().get(MAP_COLLECTION, digest).await? else {
            return Ok(None);
        };
        if record.doc.get("schema").and_then(Value::as_u64) != Some(1) {
            return Err(StorageError::backend("blob generation mapping is corrupt"));
        }
        let logical = record
            .doc
            .get("scope")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| StorageError::backend("blob generation mapping is corrupt"))?;
        let scope = Scope::new(&logical)
            .map_err(|_| StorageError::backend("blob generation mapping is corrupt"))?;
        if Self::digest(&scope) != digest || Self::internal_scope(&scope) {
            return Err(StorageError::backend("blob generation mapping is corrupt"));
        }
        let generations = record
            .doc
            .get("generations")
            .and_then(Value::as_array)
            .ok_or_else(|| StorageError::backend("blob generation mapping is corrupt"))?
            .iter()
            .map(|generation| {
                generation
                    .as_u64()
                    .ok_or_else(|| StorageError::backend("blob generation mapping is corrupt"))
            })
            .collect::<Result<Vec<_>>>()?;
        if generations.contains(&0) || generations.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(StorageError::backend("blob generation mapping is corrupt"));
        }
        Ok(Some((logical, generations, record.version)))
    }

    async fn ensure_mapping(&self, scope: &Scope, generation: ScopeGeneration) -> Result<()> {
        let digest = Self::digest(scope);
        let map = self.0.for_scope(&Self::map_scope()?)?;
        let documents = map.documents();
        let spec = crate::CollectionSpec::new(MAP_COLLECTION);
        match documents.ensure_collection(&spec).await {
            Ok(()) => {}
            Err(error)
                if error.kind() == crate::ErrorKind::Unsupported(Capability::ExistingSchema) =>
            {
                documents.validate_collection_spec(&spec).await?;
            }
            Err(error) => return Err(error),
        }
        loop {
            let current = self.mapping_entry(&digest).await?;
            if let Some((existing, generations, _)) = &current {
                if existing != scope.as_str() {
                    return Err(StorageError::backend("blob generation mapping collision"));
                }
                if generations.contains(&generation.0) {
                    return Ok(());
                }
            }
            let physical = self
                .0
                .for_scope(&Self::physical_scope(scope, generation)?)?;
            if !physical.blobs().list("").await?.is_empty() {
                return Err(StorageError::backend(
                    "unmapped blob generation namespace already contains data",
                ));
            }
            let mut generations = current
                .as_ref()
                .map_or_else(Vec::new, |(_, generations, _)| generations.clone());
            generations.push(generation.0);
            generations.sort_unstable();
            generations.dedup();
            let value = json!({"schema": 1, "scope": scope.as_str(), "generations": generations});
            let precondition = match &current {
                Some((_, _, version)) => Precondition::Version(*version),
                None => Precondition::Absent,
            };
            match documents
                .put(MAP_COLLECTION, &digest, value, precondition)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == crate::ErrorKind::Conflict => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn internal_scope(scope: &Scope) -> bool {
        scope.as_str() == MAP_SCOPE
            || scope.as_str().starts_with(&format!("{MAP_SCOPE}/"))
            || scope.as_str() == super::purge::COMPOSITE_PURGE_SCOPE
            || scope
                .as_str()
                .starts_with(&format!("{}/", super::purge::COMPOSITE_PURGE_SCOPE))
            || scope.as_str() == INTERNAL_PREFIX
            || scope.as_str().starts_with(&format!("{INTERNAL_PREFIX}/"))
    }
}

#[async_trait]
impl BlobBackend for BackendBlobs {
    fn capabilities(&self) -> Capabilities {
        let source = self.0.capabilities();
        let mut caps = Capabilities::none();
        for cap in [
            Capability::StreamingBlobs,
            Capability::PresignedUrls,
            Capability::BlobTtl,
        ] {
            if source.contains(cap) {
                caps = caps.with(cap);
            }
        }
        if source.contains(Capability::ScopeEnumeration) {
            caps = caps.with(Capability::ScopeEnumeration);
        }
        if source.contains(Capability::ScopePurge) {
            caps = caps.with(Capability::ScopePurge);
        }
        caps
    }

    fn supports_generation_purge(&self) -> bool {
        self.0.capabilities().contains(Capability::ScopePurge)
    }

    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        if Self::internal_scope(scope) {
            return Err(StorageError::invalid_input(
                "scope is reserved for composite storage",
            ));
        }
        Ok(self.0.for_scope(scope)?.blobs().clone())
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<Arc<dyn BlobStore>> {
        if Self::internal_scope(scope) {
            return Err(StorageError::invalid_input(
                "scope is reserved for composite storage",
            ));
        }
        if generation == ScopeGeneration(0) {
            return self.for_scope(scope);
        }
        let physical = Self::physical_scope(scope, generation)?;
        let inner = self.0.for_scope(&physical)?.blobs().clone();
        Ok(Arc::new(GenerationNamespaceBlobs {
            backend: Arc::clone(&self.0),
            inner,
            scope: scope.clone(),
            generation,
        }))
    }

    async fn purge_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<()> {
        if Self::internal_scope(scope) {
            return Err(StorageError::invalid_input(
                "scope is reserved for composite storage",
            ));
        }
        if generation == ScopeGeneration(0) {
            let blobs = self.0.for_scope(scope)?.blobs().clone();
            for item in blobs.list("").await? {
                blobs.delete(&item.key).await?;
            }
            blobs.sweep_expired().await?;
            return Ok(());
        }
        let digest = Self::digest(scope);
        match self.mapping_entry(&digest).await? {
            Some((existing, generations, _))
                if existing == scope.as_str() && generations.contains(&generation.0) => {}
            Some(_) => return Err(StorageError::backend("blob generation mapping collision")),
            None => {
                let physical = self
                    .0
                    .for_scope(&Self::physical_scope(scope, generation)?)?;
                physical.blobs().sweep_expired().await?;
                if physical.blobs().list("").await?.is_empty() {
                    return Ok(());
                }
                return Err(StorageError::backend(
                    "unmapped blob generation namespace already contains data",
                ));
            }
        }
        let blobs = self
            .0
            .for_scope(&Self::physical_scope(scope, generation)?)?
            .blobs()
            .clone();
        for item in blobs.list("").await? {
            blobs.delete(&item.key).await?;
        }
        blobs.sweep_expired().await?;
        Ok(())
    }

    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        if Self::internal_scope(scope) {
            return Err(StorageError::invalid_input(
                "scope is reserved for composite storage",
            ));
        }
        let digest = Self::digest(scope);
        let mapping = self.mapping_entry(&digest).await?;
        if mapping
            .as_ref()
            .is_some_and(|(existing, _, _)| existing != scope.as_str())
        {
            return Err(StorageError::backend("blob generation mapping collision"));
        }
        let legacy = self.0.for_scope(scope)?.blobs().clone();
        for item in legacy.list("").await? {
            legacy.delete(&item.key).await?;
        }
        legacy.sweep_expired().await?;
        if let Some((_, generations, _)) = mapping {
            for generation in generations
                .into_iter()
                .filter(|generation| *generation != 0)
            {
                let physical = Self::physical_scope(scope, ScopeGeneration(generation))?;
                let blobs = self.0.for_scope(&physical)?.blobs().clone();
                for item in blobs.list("").await? {
                    blobs.delete(&item.key).await?;
                }
                blobs.sweep_expired().await?;
            }
        }
        Ok(())
    }

    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let mut result = std::collections::BTreeSet::new();
        for raw in self.0.scopes("").await? {
            if Self::internal_scope(&raw) {
                if let Some((digest, generation)) = raw
                    .as_str()
                    .strip_prefix(&format!("{INTERNAL_PREFIX}/"))
                    .and_then(|s| s.split_once('/'))
                {
                    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        return Err(StorageError::backend("invalid blob generation namespace"));
                    }
                    let generation_number = generation
                        .parse::<u64>()
                        .map_err(|_| StorageError::backend("invalid blob generation namespace"))?;
                    if generation_number == 0 || generation_number.to_string() != generation {
                        return Err(StorageError::backend("invalid blob generation namespace"));
                    }
                    let (logical, generations, _) =
                        self.mapping_entry(digest).await?.ok_or_else(|| {
                            StorageError::backend(
                                "unmapped blob generation namespace contains data",
                            )
                        })?;
                    if !generations.contains(&generation_number) {
                        return Err(StorageError::backend(
                            "unmapped blob generation namespace contains data",
                        ));
                    }
                    if logical.starts_with(prefix) {
                        let scope = Scope::new(logical)?;
                        if !self.0.for_scope(&raw)?.blobs().list("").await?.is_empty() {
                            result.insert(scope);
                        }
                    }
                }
            } else if raw.as_str().starts_with(prefix)
                && !self.0.for_scope(&raw)?.blobs().list("").await?.is_empty()
            {
                result.insert(raw);
            }
        }
        Ok(result.into_iter().collect())
    }
}

#[derive(Debug)]
struct GenerationNamespaceBlobs {
    backend: Arc<dyn StorageBackend>,
    inner: Arc<dyn BlobStore>,
    scope: Scope,
    generation: ScopeGeneration,
}

impl GenerationNamespaceBlobs {
    async fn map_before_write(&self) -> Result<()> {
        BackendBlobs::new(Arc::clone(&self.backend))
            .ensure_mapping(&self.scope, self.generation)
            .await
    }

    async fn validate_claim(&self) -> Result<bool> {
        let adapter = BackendBlobs::new(Arc::clone(&self.backend));
        match adapter
            .mapping_entry(&BackendBlobs::digest(&self.scope))
            .await?
        {
            Some((logical, generations, _))
                if logical == self.scope.as_str() && generations.contains(&self.generation.0) =>
            {
                Ok(true)
            }
            Some((logical, _, _)) if logical != self.scope.as_str() => {
                Err(StorageError::backend("blob generation mapping collision"))
            }
            Some(_) | None => {
                if self.inner.list("").await?.is_empty() {
                    Ok(false)
                } else {
                    Err(StorageError::backend(
                        "unmapped blob generation namespace contains data",
                    ))
                }
            }
        }
    }
}

#[async_trait]
impl BlobStore for GenerationNamespaceBlobs {
    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn put(&self, key: &str, bytes: Vec<u8>, ct: Option<&str>) -> Result<BlobMeta> {
        self.map_before_write().await?;
        self.inner.put(key, bytes, ct).await
    }
    async fn put_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        ct: Option<&str>,
    ) -> Result<BlobMeta> {
        self.map_before_write().await?;
        self.inner.put_stream(key, reader, len, ct).await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        ct: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.map_before_write().await?;
        self.inner.put_expiring(key, bytes, ct, ttl).await
    }
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        ct: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.map_before_write().await?;
        self.inner
            .put_stream_expiring(key, reader, len, ct, ttl)
            .await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        if !self.validate_claim().await? {
            return Ok(None);
        }
        self.inner.get(key).await
    }
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        if !self.validate_claim().await? {
            return Ok(None);
        }
        self.inner.get_stream(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        if !self.validate_claim().await? {
            return Ok(None);
        }
        self.inner.get_range(key, range).await
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        if !self.validate_claim().await? {
            return Ok(None);
        }
        self.inner.head(key).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        if !self.validate_claim().await? {
            return Ok(false);
        }
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        if !self.validate_claim().await? {
            return Ok(Vec::new());
        }
        self.inner.list(prefix).await
    }
    async fn sweep_expired(&self) -> Result<u64> {
        if !self.validate_claim().await? {
            return Ok(0);
        }
        self.inner.sweep_expired().await
    }
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        if !self.validate_claim().await? {
            return Err(StorageError::backend("unclaimed blob generation namespace"));
        }
        self.inner.presign_get(key, ttl).await
    }
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        self.map_before_write().await?;
        self.inner.presign_put(key, ttl).await
    }
}
