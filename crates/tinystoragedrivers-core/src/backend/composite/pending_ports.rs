//! Deny every data-port operation while a composite purge is pending.
use crate::{
    Capabilities, DocumentStore, Filter, Query, Result, Scope, ScopedStorage, Sort, StorageError,
    StreamStore, async_trait,
};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct PendingDocuments {
    pub(super) inner: Arc<dyn DocumentStore>,
    pub(super) coordinator: ScopedStorage,
    pub(super) scope: Scope,
}

impl PendingDocuments {
    async fn admit(&self) -> Result<()> {
        if super::purge::pending(&self.coordinator, &self.scope).await? {
            Err(StorageError::scope_generation_rejected(
                "scope purge is pending",
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl DocumentStore for PendingDocuments {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn watch(
        &self,
        c: &str,
        f: &Filter,
        o: &crate::WatchOptions,
    ) -> Result<Box<dyn crate::DocumentWatch>> {
        self.admit().await?;
        self.inner.watch(c, f, o).await
    }
    async fn ensure_collection(&self, s: &crate::CollectionSpec) -> Result<()> {
        self.admit().await?;
        self.inner.ensure_collection(s).await
    }
    async fn validate_collection_spec(&self, s: &crate::CollectionSpec) -> Result<()> {
        self.admit().await?;
        self.inner.validate_collection_spec(s).await
    }
    async fn get(&self, c: &str, id: &str) -> Result<Option<crate::Versioned<Value>>> {
        self.admit().await?;
        self.inner.get(c, id).await
    }
    async fn put(
        &self,
        c: &str,
        id: &str,
        doc: Value,
        p: crate::Precondition,
    ) -> Result<crate::Version> {
        self.admit().await?;
        self.inner.put(c, id, doc, p).await
    }
    async fn put_with(
        &self,
        c: &str,
        id: &str,
        doc: Value,
        p: crate::Precondition,
        o: crate::WriteOptions,
    ) -> Result<crate::Version> {
        self.admit().await?;
        self.inner.put_with(c, id, doc, p, o).await
    }
    async fn delete(&self, c: &str, id: &str, p: crate::Precondition) -> Result<bool> {
        self.admit().await?;
        self.inner.delete(c, id, p).await
    }
    async fn query(&self, c: &str, q: &Query) -> Result<crate::Page<crate::Versioned<Value>>> {
        self.admit().await?;
        self.inner.query(c, q).await
    }
    async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
        self.admit().await?;
        self.inner.count(c, f).await
    }
    async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
        self.admit().await?;
        self.inner.delete_where(c, f).await
    }
    async fn claim(
        &self,
        c: &str,
        f: &Filter,
        s: &[Sort],
        p: &Value,
    ) -> Result<Option<crate::Versioned<Value>>> {
        self.admit().await?;
        self.inner.claim(c, f, s, p).await
    }
    async fn atomic_batch(&self, ops: Vec<crate::WriteOp>) -> Result<Vec<crate::WriteResult>> {
        self.admit().await?;
        self.inner.atomic_batch(ops).await
    }
    async fn atomic_batch_with(
        &self,
        ops: Vec<crate::WriteOp>,
        options: crate::WriteOptions,
    ) -> Result<Vec<crate::WriteResult>> {
        self.admit().await?;
        self.inner.atomic_batch_with(ops, options).await
    }
    async fn atomic_batch_checked(
        &self,
        checks: Vec<crate::ReadCheck>,
        ops: Vec<crate::WriteOp>,
    ) -> Result<Vec<crate::WriteResult>> {
        self.admit().await?;
        self.inner.atomic_batch_checked(checks, ops).await
    }
    async fn atomic_batch_checked_with(
        &self,
        checks: Vec<crate::ReadCheck>,
        ops: Vec<crate::WriteOp>,
        options: crate::WriteOptions,
    ) -> Result<Vec<crate::WriteResult>> {
        self.admit().await?;
        self.inner
            .atomic_batch_checked_with(checks, ops, options)
            .await
    }
    async fn search(&self, c: &str, t: &str, l: usize) -> Result<Vec<crate::SearchHit>> {
        self.admit().await?;
        self.inner.search(c, t, l).await
    }
    async fn drop_collection(&self, c: &str) -> Result<()> {
        self.admit().await?;
        self.inner.drop_collection(c).await
    }
    async fn collections(&self, p: &str) -> Result<Vec<String>> {
        self.admit().await?;
        self.inner.collections(p).await
    }
}

#[derive(Debug)]
pub(super) struct PendingStreams {
    pub(super) inner: Arc<dyn StreamStore>,
    pub(super) coordinator: ScopedStorage,
    pub(super) scope: Scope,
}

impl PendingStreams {
    async fn admit(&self) -> Result<()> {
        if super::purge::pending(&self.coordinator, &self.scope).await? {
            Err(StorageError::scope_generation_rejected(
                "scope purge is pending",
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl StreamStore for PendingStreams {
    async fn append(&self, s: &str, v: Value) -> Result<u64> {
        self.admit().await?;
        self.inner.append(s, v).await
    }
    async fn append_batch(&self, s: &str, v: Vec<Value>) -> Result<u64> {
        self.admit().await?;
        self.inner.append_batch(s, v).await
    }
    async fn append_batch_if_offset(&self, s: &str, o: u64, v: Vec<Value>) -> Result<u64> {
        self.admit().await?;
        self.inner.append_batch_if_offset(s, o, v).await
    }
    async fn read_window(&self, s: &str, f: u64, l: usize) -> Result<Vec<crate::StreamEntry>> {
        self.admit().await?;
        self.inner.read_window(s, f, l).await
    }
    async fn len(&self, s: &str) -> Result<u64> {
        self.admit().await?;
        self.inner.len(s).await
    }
    async fn truncate_before(&self, s: &str, o: u64) -> Result<u64> {
        self.admit().await?;
        self.inner.truncate_before(s, o).await
    }
    async fn delete_stream(&self, s: &str) -> Result<bool> {
        self.admit().await?;
        self.inner.delete_stream(s).await
    }
    async fn seal(&self, s: &str) -> Result<crate::SealOutcome> {
        self.admit().await?;
        self.inner.seal(s).await
    }
    async fn delete_sealed_stream(&self, s: &str, t: i64) -> Result<bool> {
        self.admit().await?;
        self.inner.delete_sealed_stream(s, t).await
    }
    async fn info(&self, s: &str) -> Result<Option<crate::StreamInfo>> {
        self.admit().await?;
        self.inner.info(s).await
    }
    async fn streams(&self, p: &str) -> Result<Vec<String>> {
        self.admit().await?;
        self.inner.streams(p).await
    }
}
