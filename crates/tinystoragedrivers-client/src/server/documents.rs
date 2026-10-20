//! `Documents` selectors.
use serde_json::Value;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::{MAX_BATCH_OPS, MAX_INLINE_BYTES, MAX_PAGE_ITEMS};
use tinystoragedrivers_core::{Precondition, ReadCheck, WriteOp};

use super::{Reply, StorageServer, decode, encode, invalid, native};
use crate::convert::{capability_names, op_from_wire, versioned_to_wire, via};

fn into_native<T: serde::Serialize, U: serde::de::DeserializeOwned>(
    value: &T,
) -> Result<U, wire::StorageFailure> {
    native(via(value))
}

impl StorageServer {
    pub(super) async fn documents(&self, selector: &str, request: Value) -> Reply {
        let docs = self.storage.documents();
        match selector {
            "Get" => {
                let r: wire::GetDocumentRequest = decode(request)?;
                let found = native(docs.get(&r.collection, &r.id).await)?;
                encode(&found.map(versioned_to_wire))
            }
            "Put" => self.put(decode(request)?).await,
            "Delete" => {
                let r: wire::DeleteDocumentRequest = decode(request)?;
                let precondition: Precondition = into_native(&r.precondition)?;
                encode(&native(
                    docs.delete(&r.collection, &r.id, precondition).await,
                )?)
            }
            "Query" => self.query(decode(request)?).await,
            "Count" => {
                let r: wire::CountRequest = decode(request)?;
                let filter = into_native(&r.filter)?;
                encode(&native(docs.count(&r.collection, &filter).await)?)
            }
            "DeleteWhere" => {
                let r: wire::DeleteWhereRequest = decode(request)?;
                let filter = into_native(&r.filter)?;
                encode(&native(docs.delete_where(&r.collection, &filter).await)?)
            }
            "Claim" => {
                let r: wire::ClaimRequest = decode(request)?;
                let filter = into_native(&r.filter)?;
                let sort: Vec<tinystoragedrivers_core::Sort> = into_native(&r.sort)?;
                let claimed = native(docs.claim(&r.collection, &filter, &sort, &r.patch).await)?;
                encode(&claimed.map(versioned_to_wire))
            }
            "AtomicBatch" => {
                let r: wire::BatchRequest = decode(request)?;
                if !r.checks.is_empty() {
                    return Err(invalid());
                }
                let ops = batch_ops(r.ops)?;
                let results = native(docs.atomic_batch(ops).await)?;
                encode(&native(via::<_, wire::BatchResponse>(&results))?)
            }
            "AtomicBatchChecked" => {
                let r: wire::BatchRequest = decode(request)?;
                if r.checks.len() > MAX_BATCH_OPS {
                    return Err(invalid());
                }
                let checks: Vec<ReadCheck> = into_native(&r.checks)?;
                let ops = batch_ops(r.ops)?;
                let results = native(docs.atomic_batch_checked(checks, ops).await)?;
                encode(&native(via::<_, wire::BatchResponse>(&results))?)
            }
            "AtomicBatchWith" => self.atomic_batch_with(request).await,
            "AtomicBatchCheckedWith" => self.atomic_batch_checked_with(request).await,
            "Search" => {
                let r: wire::SearchRequest = decode(request)?;
                let limit = r.limit.min(MAX_PAGE_ITEMS);
                let hits = native(docs.search(&r.collection, &r.text, limit).await)?;
                encode(&native(via::<_, Vec<wire::SearchHit>>(&hits))?)
            }
            "EnsureCollection" => {
                let r: wire::EnsureCollectionRequest = decode(request)?;
                native(docs.ensure_collection(&into_native(&r.spec)?).await)?;
                encode(&())
            }
            "ValidateCollectionSpec" => {
                let r: wire::ValidateCollectionSpecRequest = decode(request)?;
                native(docs.validate_collection_spec(&into_native(&r.spec)?).await)?;
                encode(&())
            }
            "DropCollection" => {
                let r: wire::CollectionRequest = decode(request)?;
                native(docs.drop_collection(&r.collection).await)?;
                encode(&())
            }
            "Collections" => {
                let r: wire::CollectionsRequest = decode(request)?;
                let names = native(docs.collections(&r.prefix).await)?;
                bounded_names(&names)
            }
            "WatchOpen" => self.watch_open(decode(request)?).await,
            "WatchClose" => self.watch_close(&decode::<wire::WatchCloseRequest>(request)?),
            "Capabilities" => {
                if !request.is_null() {
                    return Err(invalid());
                }
                encode(&wire::CapabilitiesReply {
                    capabilities: capability_names(docs.capabilities()),
                })
            }
            _ => Err(invalid()),
        }
    }

    /// `Put`: default options are exactly `put`, so a plain write needs no
    /// `record_metadata`. `created_ms` has no wire field.
    async fn put(&self, request: wire::PutDocumentRequest) -> Reply {
        let precondition: Precondition = into_native(&request.precondition)?;
        let options = tinystoragedrivers_core::WriteOptions {
            schema: request.schema,
            expires_ms: request.expires_ms,
            created_ms: None,
        };
        let version = native(
            self.storage
                .documents()
                .put_with(
                    &request.collection,
                    &request.id,
                    request.doc,
                    precondition,
                    options,
                )
                .await,
        )?;
        encode(&wire::Version(version.0))
    }

    async fn atomic_batch_with(&self, request: Value) -> Reply {
        if request.to_string().len() > MAX_INLINE_BYTES {
            return Err(invalid());
        }
        let request: wire::BatchWriteRequest = decode(request)?;
        let ops = batch_ops(request.ops)?;
        let options = native_write_options(request.options);
        let results = native(
            self.storage
                .documents()
                .atomic_batch_with(ops, options)
                .await,
        )?;
        encode(&native(via::<_, wire::BatchResponse>(&results))?)
    }

    async fn atomic_batch_checked_with(&self, request: Value) -> Reply {
        if request.to_string().len() > MAX_INLINE_BYTES {
            return Err(invalid());
        }
        let request: wire::BatchCheckedWriteRequest = decode(request)?;
        if request.checks.len() > MAX_BATCH_OPS {
            return Err(invalid());
        }
        let checks: Vec<ReadCheck> = into_native(&request.checks)?;
        let ops = batch_ops(request.ops)?;
        let options = native_write_options(request.options);
        let results = native(
            self.storage
                .documents()
                .atomic_batch_checked_with(checks, ops, options)
                .await,
        )?;
        encode(&native(via::<_, wire::BatchResponse>(&results))?)
    }

    /// A page that exceeds the item or byte cap is re-read with a smaller
    /// limit, so the returned cursor always continues exactly after the page.
    async fn query(&self, request: wire::QueryRequest) -> Reply {
        let mut query: tinystoragedrivers_core::Query = into_native(&request.query)?;
        if query.limit.is_some_and(|limit| limit > MAX_PAGE_ITEMS) {
            query.limit = Some(MAX_PAGE_ITEMS);
        }
        loop {
            let page = native(
                self.storage
                    .documents()
                    .query(&request.collection, &query)
                    .await,
            )?;
            let count = page.items.len();
            let response = wire::QueryResponse {
                items: page.items.into_iter().map(versioned_to_wire).collect(),
                next: page.next.map(|cursor| cursor.0),
            };
            let value = encode(&response)?;
            let size = value.to_string().len();
            if count <= MAX_PAGE_ITEMS && size <= MAX_INLINE_BYTES {
                return Ok(value);
            }
            if count <= 1 {
                return Err(invalid());
            }
            query.limit = Some((count.min(MAX_PAGE_ITEMS) / 2).max(1));
        }
    }
}

fn native_write_options(options: wire::BatchWriteOptions) -> tinystoragedrivers_core::WriteOptions {
    tinystoragedrivers_core::WriteOptions {
        schema: options.schema,
        expires_ms: options.expires_ms,
        created_ms: None,
    }
}

fn batch_ops(ops: Vec<wire::WriteOp>) -> Result<Vec<WriteOp>, wire::StorageFailure> {
    if ops.len() > MAX_BATCH_OPS {
        return Err(invalid());
    }
    ops.into_iter().map(|op| native(op_from_wire(op))).collect()
}

fn bounded_names(names: &[String]) -> Reply {
    if names.len() > MAX_PAGE_ITEMS {
        return Err(invalid());
    }
    encode(&names)
}

pub(super) fn bounded_list(names: &[String]) -> Reply {
    bounded_names(names)
}
