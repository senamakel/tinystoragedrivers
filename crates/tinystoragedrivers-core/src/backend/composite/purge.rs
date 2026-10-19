//! Durable staged scope purge for independent composite storage ports.
use crate::{
    BlobBackend, Capability, PurgeMode, PurgeReceipt, PurgeRequest, PurgeStatus, Result, Scope,
    ScopeGeneration, ScopePurge, ScopedStorage, StorageError, async_trait,
};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct CompositePurge {
    pub(super) records: ScopedStorage,
    pub(super) blobs: Arc<dyn BlobBackend>,
    pub(super) scope: Scope,
    pub(super) generation: ScopeGeneration,
    pub(super) coordinator: ScopedStorage,
    pub(super) purge_lock: Arc<tokio::sync::Mutex<()>>,
}

enum PreparedPurge {
    Replay(PurgeReceipt),
    Pending(crate::Versioned<serde_json::Value>),
}

const COMPOSITE_PURGE_COLLECTION: &str = "__tsd_composite_purges";
const TARGET_GENERATION_FIELD: &str = "target_generation";
pub(super) const COMPOSITE_PURGE_SCOPE: &str = "__tsd_composite_purge_coordinator";

async fn validate_coordinator_schema(coordinator: &ScopedStorage) -> Result<()> {
    let spec = crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION);
    let documents = coordinator.documents();
    match documents.ensure_collection(&spec).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == crate::ErrorKind::Unsupported(Capability::ExistingSchema) => {
            documents.validate_collection_spec(&spec).await
        }
        Err(error) => Err(error),
    }
}

pub(super) fn ensure_public_scope(scope: &Scope) -> Result<()> {
    if scope.as_str() == COMPOSITE_PURGE_SCOPE
        || scope
            .as_str()
            .starts_with(&format!("{COMPOSITE_PURGE_SCOPE}/"))
        || scope.as_str() == super::backend_blobs::RESERVED_MAP_SCOPE
        || scope
            .as_str()
            .starts_with(&format!("{}/", super::backend_blobs::RESERVED_MAP_SCOPE))
        || scope.as_str() == super::backend_blobs::RESERVED_SCOPE_PREFIX
        || scope
            .as_str()
            .starts_with(&format!("{}/", super::backend_blobs::RESERVED_SCOPE_PREFIX))
    {
        return Err(StorageError::invalid_input(
            "reserved composite storage scope",
        ));
    }
    Ok(())
}

pub(super) fn is_public_scope(scope: &Scope) -> bool {
    ensure_public_scope(scope).is_ok()
}

pub(super) fn coordinator_scope() -> Result<Scope> {
    Scope::new(COMPOSITE_PURGE_SCOPE)
}

pub(super) fn coordinator_scope_for_layer(depth: usize) -> Result<Scope> {
    if depth > 32 {
        return Err(StorageError::invalid_input(
            "composite storage nesting is too deep",
        ));
    }
    if depth == 0 {
        coordinator_scope()
    } else {
        Scope::new(format!("{COMPOSITE_PURGE_SCOPE}/{depth}"))
    }
}

pub(super) fn coordinator_id(scope: &Scope) -> Result<String> {
    crate::Key::of(["scope-purge", scope.as_str()]).map(|key| key.to_string())
}
pub(super) fn coordinator_operation_id(scope: &Scope, operation: &str) -> Result<String> {
    crate::Key::of(["scope-purge-history", scope.as_str(), operation]).map(|key| key.to_string())
}

impl CompositePurge {
    async fn read(&self) -> Result<Option<crate::Versioned<serde_json::Value>>> {
        validate_coordinator_schema(&self.coordinator).await?;
        self.coordinator
            .documents()
            .get(COMPOSITE_PURGE_COLLECTION, &coordinator_id(&self.scope)?)
            .await
    }
    async fn write(
        &self,
        current: Option<&crate::Versioned<serde_json::Value>>,
        value: serde_json::Value,
    ) -> Result<()> {
        let precondition = current.map_or(crate::Precondition::Absent, crate::Versioned::unchanged);
        self.coordinator
            .documents()
            .put(
                COMPOSITE_PURGE_COLLECTION,
                &coordinator_id(&self.scope)?,
                value,
                precondition,
            )
            .await?;
        Ok(())
    }
    fn receipt(value: &serde_json::Value) -> Option<PurgeReceipt> {
        serde_json::from_value(value.get("receipt")?.clone()).ok()
    }
    fn target_generation(value: &serde_json::Value) -> Result<ScopeGeneration> {
        let target = value
            .get(TARGET_GENERATION_FIELD)
            .and_then(serde_json::Value::as_u64)
            .map(ScopeGeneration)
            .ok_or_else(|| StorageError::backend("invalid composite purge target generation"))?;
        let receipt = Self::receipt(value)
            .ok_or_else(|| StorageError::backend("invalid composite purge coordinator record"))?;
        if target.0.checked_add(1) != Some(receipt.generation.0) {
            return Err(StorageError::backend(
                "composite purge target does not match its issued generation",
            ));
        }
        Ok(target)
    }
    fn historical_receipt(value: &serde_json::Value, operation: &str) -> Result<PurgeReceipt> {
        let receipt = Self::receipt(value)
            .ok_or_else(|| StorageError::backend("invalid composite purge history record"))?;
        if receipt.operation_id != operation || receipt.status != PurgeStatus::Complete {
            return Err(StorageError::backend(
                "inconsistent composite purge history record",
            ));
        }
        Ok(receipt)
    }
    async fn historical(&self, operation: &str) -> Result<Option<PurgeReceipt>> {
        validate_coordinator_schema(&self.coordinator).await?;
        self.coordinator
            .documents()
            .get(
                COMPOSITE_PURGE_COLLECTION,
                &coordinator_operation_id(&self.scope, operation)?,
            )
            .await?
            .map(|history| Self::historical_receipt(&history.doc, operation))
            .transpose()
    }
    async fn save_history(&self, receipt: &PurgeReceipt) -> Result<()> {
        let docs = self.coordinator.documents();
        let id = coordinator_operation_id(&self.scope, &receipt.operation_id)?;
        let previous = docs.get(COMPOSITE_PURGE_COLLECTION, &id).await?;
        docs.put(
            COMPOSITE_PURGE_COLLECTION,
            &id,
            serde_json::json!({"receipt": receipt}),
            previous
                .as_ref()
                .map_or(crate::Precondition::Absent, crate::Versioned::unchanged),
        )
        .await?;
        Ok(())
    }

    async fn completed_after_conflict(&self, operation: &str) -> Result<Option<PurgeReceipt>> {
        let Some(current) = self.read().await? else {
            return Ok(None);
        };
        let receipt = Self::receipt(&current.doc)
            .ok_or_else(|| StorageError::backend("invalid composite purge coordinator record"))?;
        Ok(
            (receipt.operation_id == operation && receipt.status == PurgeStatus::Complete)
                .then_some(receipt),
        )
    }

    fn generations(
        generation: ScopeGeneration,
        operation: &str,
        native_status: Option<&PurgeReceipt>,
    ) -> Result<(ScopeGeneration, ScopeGeneration)> {
        match native_status {
            Some(receipt) if receipt.operation_id != operation => Err(StorageError::backend(
                "native purge status returned a different operation",
            )),
            Some(receipt) => {
                let target =
                    ScopeGeneration(receipt.generation.0.checked_sub(1).ok_or_else(|| {
                        StorageError::backend("native purge receipt has no prior generation")
                    })?);
                Ok((target, receipt.generation))
            }
            None => {
                let next = generation
                    .0
                    .checked_add(1)
                    .ok_or_else(|| StorageError::backend("scope generation space is exhausted"))?;
                Ok((generation, ScopeGeneration(next)))
            }
        }
    }

    async fn prepare(
        &self,
        request: &PurgeRequest,
        native_status: Option<PurgeReceipt>,
    ) -> Result<PreparedPurge> {
        let (target_generation, receipt_generation) = Self::generations(
            self.generation,
            &request.operation_id,
            native_status.as_ref(),
        )?;
        let mut current = self.read().await?;
        if let Some(value) = &current {
            let prior = Self::receipt(&value.doc).ok_or_else(|| {
                StorageError::backend("invalid composite purge coordinator record")
            })?;
            if prior.operation_id == request.operation_id && prior.status == PurgeStatus::Complete {
                return Ok(PreparedPurge::Replay(prior));
            }
            if prior.status == PurgeStatus::Pending && prior.operation_id != request.operation_id {
                return Err(StorageError::conflict("another composite purge is pending"));
            }
            if prior.status == PurgeStatus::Complete && prior.operation_id != request.operation_id {
                self.save_history(&prior).await?;
                self.write(
                    Some(value),
                    serde_json::json!({
                        "receipt": PurgeReceipt {
                            operation_id: request.operation_id.clone(),
                            generation: receipt_generation,
                            status: PurgeStatus::Pending,
                            stage: crate::PurgeStage::Blobs,
                        },
                        (TARGET_GENERATION_FIELD): target_generation.0
                    }),
                )
                .await?;
                current = self.read().await?;
            }
        } else {
            self.write(
                None,
                serde_json::json!({
                    "receipt": PurgeReceipt {
                        operation_id: request.operation_id.clone(),
                        generation: receipt_generation,
                        status: PurgeStatus::Pending,
                        stage: crate::PurgeStage::Blobs,
                    },
                    (TARGET_GENERATION_FIELD): target_generation.0
                }),
            )
            .await?;
            current = self.read().await?;
        }
        let value = current.ok_or_else(|| {
            StorageError::backend("composite purge coordinator record disappeared")
        })?;
        let receipt = Self::receipt(&value.doc)
            .ok_or_else(|| StorageError::backend("invalid composite purge coordinator record"))?;
        if receipt.operation_id != request.operation_id || receipt.status != PurgeStatus::Pending {
            return Err(StorageError::conflict(
                "composite purge coordinator operation changed",
            ));
        }
        Ok(PreparedPurge::Pending(value))
    }

    async fn execute(
        &self,
        request: &PurgeRequest,
        value: crate::Versioned<serde_json::Value>,
    ) -> Result<PurgeReceipt> {
        let mut receipt = Self::receipt(&value.doc)
            .ok_or_else(|| StorageError::backend("invalid composite purge coordinator record"))?;
        let target_generation = Self::target_generation(&value.doc)?;
        if receipt.stage == crate::PurgeStage::Blobs {
            self.blobs
                .purge_scope_generation(&self.scope, target_generation)
                .await?;
            receipt.stage = crate::PurgeStage::Records;
            if let Err(error) = self
                .write(
                    Some(&value),
                    serde_json::json!({
                        "receipt": receipt,
                        (TARGET_GENERATION_FIELD): target_generation.0
                    }),
                )
                .await
            {
                if error.kind() == crate::ErrorKind::Conflict
                    && let Some(completed) =
                        self.completed_after_conflict(&request.operation_id).await?
                {
                    return Ok(completed);
                }
                return Err(error);
            }
        }
        let record_receipt = self.records.purge(request).await?;
        receipt.generation = record_receipt.generation;
        receipt.stage = record_receipt.stage;
        receipt.status = record_receipt.status;
        let current = self.read().await?;
        let current = current.as_ref().ok_or_else(|| {
            StorageError::backend("composite purge coordinator record disappeared")
        })?;
        let current_receipt = Self::receipt(&current.doc)
            .ok_or_else(|| StorageError::backend("invalid composite purge coordinator record"))?;
        if current_receipt.operation_id != request.operation_id {
            return Err(StorageError::conflict(
                "composite purge coordinator operation changed",
            ));
        }
        if let Err(error) = self
            .write(
                Some(current),
                serde_json::json!({
                    "receipt": receipt,
                    (TARGET_GENERATION_FIELD): target_generation.0
                }),
            )
            .await
        {
            if error.kind() == crate::ErrorKind::Conflict
                && let Some(completed) =
                    self.completed_after_conflict(&request.operation_id).await?
            {
                return Ok(completed);
            }
            return Err(error);
        }
        if receipt.status == PurgeStatus::Complete {
            self.save_history(&receipt).await?;
        }
        Ok(receipt)
    }
}

pub(super) async fn pending(coordinator: &ScopedStorage, scope: &Scope) -> Result<bool> {
    validate_coordinator_schema(coordinator).await?;
    let Some(value) = coordinator
        .documents()
        .get(COMPOSITE_PURGE_COLLECTION, &coordinator_id(scope)?)
        .await?
    else {
        return Ok(false);
    };
    let receipt = CompositePurge::receipt(&value.doc)
        .ok_or_else(|| StorageError::backend("invalid composite purge coordinator record"))?;
    Ok(receipt.status == PurgeStatus::Pending)
}

#[async_trait]
impl ScopePurge for CompositePurge {
    fn supports_binding_validation(&self) -> bool {
        true
    }

    fn supports_generation_validation(&self) -> bool {
        true
    }

    async fn validate_generation(&self) -> Result<()> {
        self.records.validate_purge_generation().await?;
        if pending(&self.coordinator, &self.scope).await? {
            return Err(StorageError::scope_generation_rejected(
                "scope purge is pending",
            ));
        }
        Ok(())
    }

    async fn validate_binding(&self) -> Result<()> {
        self.records.validate_purge_binding().await?;
        if pending(&self.coordinator, &self.scope).await? {
            return Err(StorageError::scope_generation_rejected(
                "scope purge is pending",
            ));
        }
        Ok(())
    }

    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        let _guard = Arc::clone(&self.purge_lock).lock_owned().await;
        request.validate()?;
        if request.mode == PurgeMode::Atomic {
            return Err(StorageError::unsupported(
                Capability::AtomicScopePurge,
                "independent record and blob stores cannot be purged atomically",
            ));
        }
        if ensure_public_scope(&self.scope).is_err() {
            return Err(StorageError::invalid_input(
                "reserved composite purge coordinator scope",
            ));
        }
        // Native status validates an attached fence even when the operation
        // has already completed. Keep historical replay semantics, but never
        // let a stale fenced handle reach coordinator/blob mutations.
        let native_status = self.records.purge_status(&request.operation_id).await?;
        if native_status
            .as_ref()
            .is_some_and(|receipt| receipt.operation_id != request.operation_id)
        {
            return Err(StorageError::backend(
                "native purge status returned a different operation",
            ));
        }
        if let Some(receipt) = self.historical(&request.operation_id).await? {
            return Ok(receipt);
        }
        if native_status.is_none() {
            self.records.validate_purge_binding().await?;
        }
        match self.prepare(request, native_status).await? {
            PreparedPurge::Replay(receipt) => Ok(receipt),
            PreparedPurge::Pending(value) => self.execute(request, value).await,
        }
    }

    async fn status(&self, operation_id: &str) -> Result<Option<PurgeReceipt>> {
        // Even a coordinator-only receipt must honor the records backend's
        // native fence; otherwise stale fenced handles can inspect history.
        let native_status = self.records.purge_status(operation_id).await?;
        if native_status
            .as_ref()
            .is_some_and(|receipt| receipt.operation_id != operation_id)
        {
            return Err(StorageError::backend(
                "native purge status returned a different operation",
            ));
        }
        if native_status.is_none() {
            self.records.validate_purge_binding().await?;
        }
        let Some(value) = self.read().await? else {
            return Ok(None);
        };
        if let Some(receipt) =
            Self::receipt(&value.doc).filter(|receipt| receipt.operation_id == operation_id)
        {
            return Ok(Some(receipt));
        }
        let history = self
            .coordinator
            .documents()
            .get(
                COMPOSITE_PURGE_COLLECTION,
                &coordinator_operation_id(&self.scope, operation_id)?,
            )
            .await?;
        history
            .map(|record| Self::historical_receipt(&record.doc, operation_id))
            .transpose()
    }
}

#[cfg(test)]
#[path = "purge_tests.rs"]
mod tests;
