//! `DocumentStore` over the `Documents` object.
use serde_json::Value;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::{MAX_BATCH_OPS, MAX_INLINE_BYTES};
use tinystoragedrivers_core::{
    Capabilities, Capability, CollectionSpec, Cursor, DocumentStore, DocumentWatch, Filter, Page,
    Precondition, Query, ReadCheck, Result, SearchHit, Sort, StorageError, Version, Versioned,
    WatchOptions, WriteOp, WriteOptions, WriteResult, async_trait,
};

use super::Wire;
use super::watch::BusWatch;
use crate::convert::{op_to_wire, versioned_from_wire, via};
use crate::transport::Object;

/// The document port over a bus.
#[derive(Clone, Debug)]
pub struct BusDocuments {
    wire: Wire,
    capabilities: Capabilities,
}

impl BusDocuments {
    pub(super) fn new(wire: Wire, capabilities: Capabilities) -> Self {
        Self { wire, capabilities }
    }

    async fn call<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        selector: &str,
        request: &Req,
        unsupported_as: Option<Capability>,
    ) -> Result<Resp> {
        self.wire
            .call(Object::Documents, selector, request, unsupported_as)
            .await
    }
}

fn batch(checks: &[ReadCheck], ops: Vec<WriteOp>) -> Result<wire::BatchRequest> {
    if ops.len() > MAX_BATCH_OPS || checks.len() > MAX_BATCH_OPS {
        return Err(StorageError::invalid_input(
            "an atomic batch is limited to 1000 operations",
        ));
    }
    Ok(wire::BatchRequest {
        checks: via(checks)?,
        ops: ops.into_iter().map(op_to_wire).collect::<Result<_>>()?,
    })
}

fn validate_module_options(options: WriteOptions) -> Result<()> {
    if options.created_ms.is_some() {
        return Err(StorageError::invalid_input(
            "a module cannot set created_ms: it is a host-only migration option",
        ));
    }
    Ok(())
}

fn batch_with(ops: Vec<WriteOp>, options: WriteOptions) -> Result<wire::BatchWriteRequest> {
    if ops.len() > MAX_BATCH_OPS {
        return Err(StorageError::invalid_input(
            "an atomic batch is limited to 1000 operations",
        ));
    }
    let request = wire::BatchWriteRequest {
        ops: ops.into_iter().map(op_to_wire).collect::<Result<_>>()?,
        options: wire::BatchWriteOptions {
            schema: options.schema,
            expires_ms: options.expires_ms,
        },
    };
    ensure_batch_size(&request)?;
    Ok(request)
}

fn batch_checked_with(
    checks: &[ReadCheck],
    ops: Vec<WriteOp>,
    options: WriteOptions,
) -> Result<wire::BatchCheckedWriteRequest> {
    if ops.len() > MAX_BATCH_OPS || checks.len() > MAX_BATCH_OPS {
        return Err(StorageError::invalid_input(
            "an atomic batch is limited to 1000 operations",
        ));
    }
    let request = wire::BatchCheckedWriteRequest {
        checks: via(checks)?,
        ops: ops.into_iter().map(op_to_wire).collect::<Result<_>>()?,
        options: wire::BatchWriteOptions {
            schema: options.schema,
            expires_ms: options.expires_ms,
        },
    };
    ensure_batch_size(&request)?;
    Ok(request)
}

fn ensure_batch_size(request: &impl serde::Serialize) -> Result<()> {
    let request_size = serde_json::to_vec(request)?.len();
    if request_size > MAX_INLINE_BYTES {
        Err(StorageError::invalid_input(
            "an atomic batch exceeds the inline request limit",
        ))
    } else {
        Ok(())
    }
}

#[async_trait]
impl DocumentStore for BusDocuments {
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    async fn watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> Result<Box<dyn DocumentWatch>> {
        options.validate()?;
        let interval_ms = u64::try_from(options.interval.as_millis())
            .map_err(|_| StorageError::invalid_input("invalid watch interval"))?;
        let request = wire::WatchOpenRequest {
            collection: collection.to_owned(),
            filter: via(filter)?,
            after: options.after.as_ref().map(|c| c.as_bytes().to_vec()),
            interval_ms,
        };
        // Subscribe first so a signal racing the reply is not lost.
        let signals = self.wire.transport().signals();
        let opened: wire::WatchOpenResponse = self
            .call("WatchOpen", &request, Some(Capability::Watch))
            .await?;
        Ok(Box::new(BusWatch::new(
            self.wire.clone(),
            opened.watch_id,
            signals,
        )))
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        let request = wire::EnsureCollectionRequest { spec: via(spec)? };
        self.call("EnsureCollection", &request, None).await
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        let request = wire::ValidateCollectionSpecRequest {
            spec: via(required)?,
        };
        self.call(
            "ValidateCollectionSpec",
            &request,
            Some(Capability::CollectionValidation),
        )
        .await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        let request = wire::GetDocumentRequest {
            collection: collection.to_owned(),
            id: id.to_owned(),
        };
        let found: Option<wire::Versioned> = self.call("Get", &request, None).await?;
        Ok(found.map(versioned_from_wire))
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        let request = wire::PutDocumentRequest {
            collection: collection.to_owned(),
            id: id.to_owned(),
            doc,
            precondition: via(&precondition)?,
            schema: 0,
            expires_ms: None,
        };
        let version: wire::Version = self.call("Put", &request, None).await?;
        Ok(Version(version.0))
    }

    /// `created_ms` is a host-only migration option the wire cannot carry, so
    /// a write that sets it is refused here with `InvalidInput`.
    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        if options.created_ms.is_some() {
            return Err(StorageError::invalid_input(
                "a module cannot set created_ms: it is a host-only migration option",
            ));
        }
        let request = wire::PutDocumentRequest {
            collection: collection.to_owned(),
            id: id.to_owned(),
            doc,
            precondition: via(&precondition)?,
            schema: options.schema,
            expires_ms: options.expires_ms,
        };
        let unsupported_as = (!options.is_default()).then_some(Capability::RecordMetadata);
        let version: wire::Version = self.call("Put", &request, unsupported_as).await?;
        Ok(Version(version.0))
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        let request = wire::DeleteDocumentRequest {
            collection: collection.to_owned(),
            id: id.to_owned(),
            precondition: via(&precondition)?,
        };
        self.call("Delete", &request, None).await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        let request = wire::QueryRequest {
            collection: collection.to_owned(),
            query: via(query)?,
        };
        let page: wire::QueryResponse = self.call("Query", &request, None).await?;
        Ok(Page {
            items: page.items.into_iter().map(versioned_from_wire).collect(),
            next: page.next.map(Cursor),
        })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let request = wire::CountRequest {
            collection: collection.to_owned(),
            filter: via(filter)?,
        };
        self.call("Count", &request, None).await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let request = wire::DeleteWhereRequest {
            collection: collection.to_owned(),
            filter: via(filter)?,
        };
        self.call("DeleteWhere", &request, None).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        let request = wire::ClaimRequest {
            collection: collection.to_owned(),
            filter: via(filter)?,
            sort: via(&sort)?,
            patch: patch.clone(),
        };
        let claimed: Option<wire::Versioned> = self.call("Claim", &request, None).await?;
        Ok(claimed.map(versioned_from_wire))
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        let request = batch(&[], ops)?;
        let results: wire::BatchResponse = self
            .call("AtomicBatch", &request, Some(Capability::Transactions))
            .await?;
        via(&results)
    }

    async fn atomic_batch_with(
        &self,
        ops: Vec<WriteOp>,
        options: WriteOptions,
    ) -> Result<Vec<WriteResult>> {
        validate_module_options(options)?;
        self.capabilities.require(Capability::AtomicWriteOptions)?;
        let request = batch_with(ops, options)?;
        let results: wire::BatchResponse = self
            .call(
                "AtomicBatchWith",
                &request,
                Some(Capability::AtomicWriteOptions),
            )
            .await?;
        via(&results)
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        let request = batch(&checks, ops)?;
        let results: wire::BatchResponse = self
            .call(
                "AtomicBatchChecked",
                &request,
                Some(Capability::ValidatedBatch),
            )
            .await?;
        via(&results)
    }

    async fn atomic_batch_checked_with(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
        options: WriteOptions,
    ) -> Result<Vec<WriteResult>> {
        validate_module_options(options)?;
        self.capabilities.require(Capability::AtomicWriteOptions)?;
        let request = batch_checked_with(&checks, ops, options)?;
        let results: wire::BatchResponse = self
            .call(
                "AtomicBatchCheckedWith",
                &request,
                Some(Capability::AtomicWriteOptions),
            )
            .await?;
        via(&results)
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let request = wire::SearchRequest {
            collection: collection.to_owned(),
            text: text.to_owned(),
            limit,
        };
        let hits: Vec<wire::SearchHit> = self
            .call("Search", &request, Some(Capability::FullText))
            .await?;
        via(&hits)
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        let request = wire::CollectionRequest {
            collection: collection.to_owned(),
        };
        self.call("DropCollection", &request, None).await
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let request = wire::CollectionsRequest {
            prefix: prefix.to_owned(),
        };
        self.call(
            "Collections",
            &request,
            Some(Capability::CollectionEnumeration),
        )
        .await
    }
}
