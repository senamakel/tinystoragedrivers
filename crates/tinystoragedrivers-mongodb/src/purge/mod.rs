//! Durable admission and staged, quiescent GridFS reset.
use crate::{backend::Shared, errors};
use mongodb::{
    ClientSession,
    bson::{Document, doc},
};
use tinystoragedrivers_core::{Result, Scope, ScopeGeneration, StorageError};

pub(crate) const ADMISSIONS: &str = "_tsd_validated_batch";
pub(crate) fn scope_key(scope: &Scope) -> String {
    format!("scope:{}", scope.as_str())
}

fn has_reset_marker(record: &Document) -> bool {
    ["generation", "pending", "operation", "stage"]
        .iter()
        .any(|field| record.contains_key(*field))
}

fn verify(document: Option<&Document>, expected: ScopeGeneration) -> Result<()> {
    // Before the first reset, scope-validation registrations have no reset
    // fields. Any reset marker requires the complete authority record.
    let receipt = document
        .filter(|record| has_reset_marker(record))
        .map(decode)
        .transpose()?;
    let current = receipt
        .as_ref()
        .map_or(ScopeGeneration::default(), |receipt| receipt.generation);
    if receipt
        .as_ref()
        .is_some_and(|receipt| receipt.status == PurgeStatus::Pending)
        || current != expected
    {
        return Err(StorageError::scope_generation_rejected(
            "scope reset is pending or this handle generation has retired",
        ));
    }
    Ok(())
}

/// Transactional mutations check after touching the same scope admission row.
pub(crate) async fn check(
    shared: &Shared,
    scope: &Scope,
    expected: ScopeGeneration,
    mut session: Option<&mut ClientSession>,
) -> Result<()> {
    let collection = shared.raw(ADMISSIONS);
    let action = collection.find_one(doc! {"_id": scope_key(scope)});
    let document = match session.as_mut() {
        Some(session) => action.session(&mut **session).await,
        None => action.await,
    }
    .map_err(errors::failed("read scope admission"))?;
    // A registration recreated after authority loss still has no reset
    // markers. Completed history distinguishes that corruption from a scope
    // that has legitimately never been reset.
    if !document.as_ref().is_some_and(has_reset_marker) {
        let receipts = shared.raw(RECEIPTS);
        let action = receipts.find_one(doc! {"$or": [
            {"_id": format!("scope:{}", scope.as_str())},
            {"_id.s": scope.as_str()},
        ]});
        let history = match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
        .map_err(errors::failed("verify missing scope admission"))?;
        if history.is_some() {
            return Err(StorageError::serialization(
                "scope admission is missing despite reset history",
            ));
        }
    }
    verify(document.as_ref(), expected)
}

/// Read admission before and after a port operation. Primary reads ensure a
/// monotonic generation change cannot expose replacement data.
macro_rules! admitted {
    ($this:expr, $body:expr) => {{
        $crate::purge::check(&$this.shared, &$this.scope, $this.generation, None).await?;
        let result = async { $body }.await;
        $crate::purge::check(&$this.shared, &$this.scope, $this.generation, None).await?;
        result
    }};
}
pub(crate) use admitted;

use crate::{
    fence::in_write,
    naming::{BLOBS, META},
};
use futures_util::TryStreamExt;
use std::sync::Arc;
use tinystoragedrivers_core::{
    Capability, PurgeMode, PurgeReceipt, PurgeRequest, PurgeStage, PurgeStatus, ScopePurge,
    async_trait,
};

pub(crate) const MANIFEST: &str = "_tsd_purge_blobs";
pub(crate) const RECEIPTS: &str = "_tsd_purge_receipts";

#[derive(Debug)]
pub(crate) struct MongoPurge {
    pub(crate) shared: Arc<Shared>,
    pub(crate) scope: Scope,
    pub(crate) generation: ScopeGeneration,
    pub(crate) fenced: bool,
    #[cfg(test)]
    pub(crate) fail_after_chunks: bool,
}

fn decode(record: &Document) -> Result<PurgeReceipt> {
    let bad = || StorageError::serialization("malformed purge receipt");
    let generation = record
        .get_i64("generation")
        .ok()
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(bad)?;
    let stage = match record.get_str("stage").map_err(|_| bad())? {
        "records" => PurgeStage::Records,
        "blob_manifest" => PurgeStage::BlobManifest,
        "blobs" => PurgeStage::Blobs,
        "complete" => PurgeStage::Complete,
        _ => return Err(bad()),
    };
    let pending = record.get_bool("pending").map_err(|_| bad())?;
    if pending == (stage == PurgeStage::Complete) {
        return Err(bad());
    }
    Ok(PurgeReceipt {
        operation_id: record.get_str("operation").map_err(|_| bad())?.to_owned(),
        generation: ScopeGeneration(generation),
        status: if pending {
            PurgeStatus::Pending
        } else {
            PurgeStatus::Complete
        },
        stage,
    })
}

fn owns_records(name: &str) -> bool {
    // A `:` marks a collection a pre-1.0 named database prefixed; it is not ours.
    !name.contains(':')
        && ![META, ADMISSIONS, MANIFEST, RECEIPTS].contains(&name)
        && !name.starts_with(&format!("{BLOBS}."))
        && !name.starts_with("system.")
}

impl MongoPurge {
    async fn archive_complete(&self, record: &Document, session: &mut ClientSession) -> Result<()> {
        let receipt = decode(record)?;
        if receipt.status != PurgeStatus::Complete {
            return Err(StorageError::conflict("cannot archive an incomplete reset"));
        }
        let mut fields = record.clone();
        fields.remove("_id");
        self.shared
            .raw(RECEIPTS)
            .update_one(
                doc! {"_id": {"s": self.scope.as_str(), "o": &receipt.operation_id}},
                doc! {"$setOnInsert": fields},
            )
            .upsert(true)
            .session(session)
            .await
            .map_err(errors::failed("persist completed reset receipt"))?;
        Ok(())
    }

    async fn begin(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        if self.shared.existing {
            for collection in [RECEIPTS, MANIFEST] {
                self.shared.require_collection(collection).await?;
            }
        }
        self.shared.prepare_validation(Some(&self.scope)).await?;
        // Initialize the internal collection before a transaction could need
        // to create it. This is trusted native reset metadata, never tenant data.
        for collection in [RECEIPTS, MANIFEST] {
            let initialized = self
                .shared
                .raw(collection)
                .update_one(
                    doc! {"_id": "metadata"},
                    doc! {"$setOnInsert": {"version": 1_i64}},
                )
                .upsert(true)
                .await;
            if let Err(error) = initialized {
                let error = errors::map(error, "initialize reset metadata");
                if error.kind() != tinystoragedrivers_core::ErrorKind::AlreadyExists {
                    return Err(error);
                }
            }
        }
        in_write!(&self.shared, None, session => {
            self.shared.hold_validation(Some(&self.scope), &mut *session).await?;
            if let Some(record) = self.shared.raw(RECEIPTS).find_one(
                doc! {"_id": {"s": self.scope.as_str(), "o": &request.operation_id}}
            ).session(&mut *session).await.map_err(errors::failed("read completed reset receipt"))? {
                return decode(&record);
            }
            let metadata = self.shared.raw(ADMISSIONS);
            let record = metadata.find_one(doc! {"_id": scope_key(&self.scope)})
                .session(&mut *session).await.map_err(errors::failed("read reset authority"))?
                .ok_or_else(|| StorageError::unavailable("scope admission is missing"))?;
            if record.get_str("operation").ok() == Some(request.operation_id.as_str()) {
                return decode(&record);
            }
            if record.get_bool("pending").unwrap_or(false) {
                return Err(StorageError::conflict("another scope reset remains pending"));
            }
            check(&self.shared, &self.scope, self.generation, Some(&mut *session)).await?;
            // Backfill the latest completed receipt from an earlier driver
            // before replacing its admission record with the next operation.
            if record.contains_key("operation") {
                self.archive_complete(&record, &mut *session).await?;
            }
            let next = self.generation.0.checked_add(1)
                .and_then(|v| i64::try_from(v).ok())
                .ok_or_else(|| StorageError::backend("scope generation space is exhausted"))?;
            metadata.update_one(doc! {"_id": scope_key(&self.scope)}, doc! {"$set": {
                "generation": next, "operation": &request.operation_id,
                "pending": true, "stage": "records",
            }}).session(&mut *session).await.map_err(errors::failed("close reset admission"))?;
            // The first pending operation has no completed receipt. Record a
            // durable witness in the same transaction as admission closure so
            // losing/recreating admission cannot reopen generation zero.
            self.shared.raw(RECEIPTS).update_one(
                doc! {"_id": format!("scope:{}", self.scope.as_str())},
                doc! {"$setOnInsert": {"version": 1_i64}},
            ).upsert(true).session(session).await
                .map_err(errors::failed("persist scope reset witness"))?;
            Ok(PurgeReceipt { operation_id: request.operation_id.clone(),
                generation: ScopeGeneration(u64::try_from(next).map_err(|_| StorageError::serialization("invalid issued generation"))?), status: PurgeStatus::Pending,
                stage: PurgeStage::Records })
        })
    }

    async fn pending(&self, operation: &str, session: &mut ClientSession) -> Result<PurgeReceipt> {
        self.shared
            .hold_validation(Some(&self.scope), &mut *session)
            .await?;
        let record = self
            .shared
            .raw(ADMISSIONS)
            .find_one(doc! {"_id": scope_key(&self.scope)})
            .session(session)
            .await
            .map_err(errors::failed("read pending reset"))?
            .ok_or_else(|| StorageError::unavailable("reset authority is missing"))?;
        let receipt = decode(&record)?;
        if receipt.operation_id != operation || receipt.status != PurgeStatus::Pending {
            return Err(StorageError::conflict(
                "reset operation is no longer pending",
            ));
        }
        Ok(receipt)
    }

    async fn advance(
        &self,
        operation: &str,
        stage: &str,
        session: &mut ClientSession,
    ) -> Result<()> {
        let updated = self
            .shared
            .raw(ADMISSIONS)
            .update_one(
                doc! {"_id": scope_key(&self.scope), "operation": operation, "pending": true},
                doc! {"$set": {"stage": stage, "pending": stage != "complete"}},
            )
            .session(&mut *session)
            .await
            .map_err(errors::failed("advance reset stage"))?;
        if updated.matched_count != 1 {
            return Err(StorageError::conflict(
                "reset operation is no longer pending",
            ));
        }
        if stage == "complete" {
            let completed = self
                .shared
                .raw(ADMISSIONS)
                .find_one(doc! {"_id": scope_key(&self.scope)})
                .session(&mut *session)
                .await
                .map_err(errors::failed("read completed reset authority"))?
                .ok_or_else(|| StorageError::unavailable("reset authority is missing"))?;
            self.archive_complete(&completed, session).await?;
        }
        Ok(())
    }

    async fn records(&self, operation: &str) -> Result<()> {
        let names = self
            .shared
            .db
            .list_collection_names()
            .await
            .map_err(errors::failed("enumerate reset collections"))?;
        in_write!(&self.shared, None, session => {
            let receipt = self.pending(operation, &mut *session).await?;
            if receipt.stage != PurgeStage::Records { return Ok(()); }
            for name in &names {
                if owns_records(name) {
                    self.shared.db.collection::<Document>(name).delete_many(doc! {"_scope": self.scope.as_str()})
                        .session(&mut *session).await.map_err(errors::failed("erase scoped records"))?;
                }
            }
            self.advance(operation, "blob_manifest", session).await
        })
    }

    /// Keep one durable manifest record per file, avoiding BSON document limits.
    async fn plan_blobs(&self, operation: &str) -> Result<()> {
        let admitted = in_write!(&self.shared, None, session => {
            Ok(self.pending(operation, session).await?.stage == PurgeStage::BlobManifest)
        })?;
        if !admitted {
            return Ok(());
        }
        let mut files = self
            .shared
            .raw(&format!("{BLOBS}.files"))
            .find(doc! {"metadata._scope": self.scope.as_str()})
            .await
            .map_err(errors::failed("enumerate reset blobs"))?;
        let manifest = self.shared.raw(MANIFEST);
        while let Some(file) = files
            .try_next()
            .await
            .map_err(errors::failed("enumerate reset blobs"))?
        {
            let id = file
                .get("_id")
                .ok_or_else(|| StorageError::serialization("blob has no identity"))?;
            // Completion and every manifest publication touch the same native
            // authority row. A stale planner cannot commit replacement IDs.
            let recorded = in_write!(&self.shared, None, session => {
                if self.pending(operation, &mut *session).await?.stage != PurgeStage::BlobManifest {
                    return Ok(false);
                }
                manifest.update_one(
                    doc! {"_id": {"s": self.scope.as_str(), "o": operation, "f": id}},
                    doc! {"$setOnInsert": {"scope": self.scope.as_str(), "operation": operation, "file_id": id}},
                ).upsert(true).session(session).await.map_err(errors::failed("persist reset blob manifest"))?;
                Ok(true)
            })?;
            if !recorded {
                break;
            }
        }
        in_write!(&self.shared, None, session => {
            if self.pending(operation, &mut *session).await?.stage == PurgeStage::BlobManifest {
                self.advance(operation, "blobs", session).await?;
            }
            Ok(())
        })
    }

    /// Chunks precede file deletion: on any failure the manifest retains ownership.
    async fn erase_blobs(&self, operation: &str) -> Result<()> {
        in_write!(&self.shared, None, session => {
            if self.pending(operation, session).await?.stage != PurgeStage::Blobs {
                return Err(StorageError::conflict("reset blob stage changed"));
            }
            Ok(())
        })?;
        let manifest = self.shared.raw(MANIFEST);
        let mut entries = manifest
            .find(doc! {"scope": self.scope.as_str(), "operation": operation})
            .await
            .map_err(errors::failed("read reset blob manifest"))?;
        while let Some(entry) = entries
            .try_next()
            .await
            .map_err(errors::failed("read reset blob manifest"))?
        {
            let id = entry
                .get("file_id")
                .ok_or_else(|| StorageError::serialization("manifest has no blob identity"))?;
            // Each destructive commit validates pending authority in the same
            // transaction. Completion cannot race a delayed destructive step.
            in_write!(&self.shared, None, session => {
                if self.pending(operation, &mut *session).await?.stage != PurgeStage::Blobs {
                    return Err(StorageError::conflict("reset blob stage changed"));
                }
                self.shared.raw(&format!("{BLOBS}.chunks"))
                    .delete_many(doc! {"files_id": id}).session(session).await
                    .map_err(errors::failed("erase reset blob chunks"))?;
                Ok(())
            })?;
            #[cfg(test)]
            if self.fail_after_chunks {
                return Err(StorageError::backend(
                    "injected failure after durable chunk erasure",
                ));
            }
            in_write!(&self.shared, None, session => {
                if self.pending(operation, &mut *session).await?.stage != PurgeStage::Blobs {
                    return Err(StorageError::conflict("reset blob stage changed"));
                }
                self.shared.raw(&format!("{BLOBS}.files"))
                    .delete_one(doc! {"_id": id, "metadata._scope": self.scope.as_str()})
                    .session(&mut *session).await.map_err(errors::failed("erase reset blob file"))?;
                manifest.delete_one(doc! {"_id": entry.get("_id").cloned().unwrap_or_default()})
                    .session(session).await.map_err(errors::failed("finish reset blob manifest entry"))?;
                Ok(())
            })?;
        }
        let complete = in_write!(&self.shared, None, session => {
            if self.pending(operation, &mut *session).await?.stage != PurgeStage::Blobs {
                return Err(StorageError::conflict("reset blob stage changed"));
            }
            if manifest.count_documents(doc! {"scope": self.scope.as_str(), "operation": operation})
                .session(&mut *session).await.map_err(errors::failed("verify reset blob manifest"))? != 0 {
                return Err(StorageError::unavailable("reset blob manifest is not empty; resume"));
            }
            if self.shared.raw(&format!("{BLOBS}.files"))
                .count_documents(doc! {"metadata._scope": self.scope.as_str()})
                .session(&mut *session).await.map_err(errors::failed("verify reset blobs"))? != 0 {
                self.advance(operation, "blob_manifest", session).await?;
                return Ok(false);
            }
            self.advance(operation, "complete", session).await?;
            Ok(true)
        })?;
        if !complete {
            return Err(StorageError::unavailable(
                "blob writers were not drained; reset remains pending and must replan",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ScopePurge for MongoPurge {
    fn supports_binding_validation(&self) -> bool {
        self.shared.transactions && !self.fenced
    }

    fn supports_generation_validation(&self) -> bool {
        self.shared.transactions
    }

    async fn validate_generation(&self) -> Result<()> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "public reset capabilities are disabled",
            ));
        }
        check(&self.shared, &self.scope, self.generation, None).await
    }

    async fn validate_binding(&self) -> Result<()> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "public reset capabilities are disabled",
            ));
        }
        if self.fenced {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "fenced GridFS reset is unsupported",
            ));
        }
        check(&self.shared, &self.scope, self.generation, None).await
    }

    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "public transactions are required for reset admission",
            ));
        }
        if request.mode == PurgeMode::Atomic {
            return Err(StorageError::unsupported(
                Capability::AtomicScopePurge,
                "GridFS cannot join an atomic all-port purge",
            ));
        }
        if self.fenced {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "GridFS purge cannot hold a native fence across all stages",
            ));
        }
        let receipt = self.begin(request).await?;
        if receipt.status == PurgeStatus::Complete {
            return Ok(receipt);
        }
        let operation = request.operation_id.as_str();
        if receipt.stage == PurgeStage::Records {
            self.records(operation).await?;
        }
        let receipt = self
            .status(operation)
            .await?
            .ok_or_else(|| StorageError::unavailable("reset authority is missing"))?;
        if receipt.stage == PurgeStage::BlobManifest {
            self.plan_blobs(operation).await?;
        }
        if receipt.status == PurgeStatus::Complete {
            return Ok(receipt);
        }
        self.erase_blobs(operation).await?;
        self.status(operation)
            .await?
            .ok_or_else(|| StorageError::unavailable("reset authority is missing"))
    }
    async fn status(&self, operation: &str) -> Result<Option<PurgeReceipt>> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "public reset capabilities are disabled",
            ));
        }
        if self.fenced {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "fenced GridFS reset is unsupported",
            ));
        }
        if let Some(record) = self
            .shared
            .raw(RECEIPTS)
            .find_one(doc! {"_id": {"s": self.scope.as_str(), "o": operation}})
            .await
            .map_err(errors::failed("read completed reset receipt"))?
        {
            return decode(&record).map(Some);
        }
        self.shared
            .raw(ADMISSIONS)
            .find_one(doc! {"_id": scope_key(&self.scope), "operation": operation})
            .await
            .map_err(errors::failed("read reset receipt"))?
            .as_ref()
            .map(decode)
            .transpose()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
