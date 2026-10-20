//! The server fails closed and never reads authority from a request.
use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_bus::{MAX_INLINE_BLOB_BYTES, StorageKind};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobSource, BlobStore, MemoryStorage, Result as StorageResult, Scope,
    ScopedStorage, StorageBackend,
};

use super::*;

fn server() -> StorageServer {
    let backend = MemoryStorage::new();
    StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
}

#[derive(Debug)]
struct OversizedBlobHead {
    expiry_reads: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl BlobStore for OversizedBlobHead {
    async fn put(
        &self,
        _key: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> StorageResult<BlobMeta> {
        unreachable!("the read-budget test never writes")
    }

    async fn get(&self, _key: &str) -> StorageResult<Option<Blob>> {
        unreachable!("the read-budget test must reject from its head")
    }

    async fn get_range(
        &self,
        _key: &str,
        _range: std::ops::Range<u64>,
    ) -> StorageResult<Option<Vec<u8>>> {
        unreachable!("the read-budget test never reads ranges")
    }

    async fn delete(&self, _key: &str) -> StorageResult<bool> {
        unreachable!("the read-budget test never deletes")
    }

    async fn list(&self, _prefix: &str) -> StorageResult<Vec<BlobMeta>> {
        unreachable!("the read-budget test never lists")
    }

    async fn get_with_expiry(&self, key: &str) -> StorageResult<Option<BlobSource>> {
        self.expiry_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(BlobSource {
            blob: Blob {
                meta: BlobMeta {
                    key: key.to_owned(),
                    len: (MAX_INLINE_BLOB_BYTES + 1) as u64,
                    content_type: None,
                },
                bytes: vec![0; MAX_INLINE_BLOB_BYTES + 1],
            },
            expires_at_ms: None,
        }))
    }

    async fn head(&self, key: &str) -> StorageResult<Option<BlobMeta>> {
        Ok(Some(BlobMeta {
            key: key.to_owned(),
            len: (MAX_INLINE_BLOB_BYTES + 1) as u64,
            content_type: None,
        }))
    }
}

async fn kind(server: &StorageServer, object: &str, selector: &str, request: Value) -> StorageKind {
    server
        .handle(Admission::Confidential, object, selector, request)
        .await
        .unwrap_err()
        .kind
}

#[tokio::test]
async fn unknown_objects_and_selectors_are_refused() {
    let server = server();
    assert_eq!(
        kind(&server, "Nope", "Get", json!({})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(&server, "Documents", "Arbitrary", json!({})).await,
        StorageKind::InvalidInput
    );
    // Streamed transfers are not request/reply selectors.
    assert_eq!(
        kind(&server, "Blobs", "PutStream", json!({"key":"a","len":0})).await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn a_forged_scope_or_grant_field_is_a_decode_failure() {
    let server = server();
    for forged in ["scope", "grant", "generation"] {
        let mut request = json!({"collection": "notes", "document_id": "n1"});
        request[forged] = json!("bob");
        assert_eq!(
            kind(&server, "Documents", "Get", request).await,
            StorageKind::InvalidInput,
            "{forged}"
        );
    }
}

#[tokio::test]
async fn expiry_blob_selectors_reject_authority_and_legacy_ttl_fields() {
    let server = server();
    assert_eq!(
        kind(
            &server,
            "Blobs",
            "GetWithExpiry",
            json!({"key":"k","scope":"other"})
        )
        .await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Blobs",
            "PutUntil",
            json!({"key":"k","bytes":[],"expires_at_ms":6000,"ttl_ms":10})
        )
        .await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn oversized_expiry_reads_are_rejected_before_materializing_blob_bytes() {
    let backend = MemoryStorage::new();
    let base = backend.for_scope(&Scope::local()).unwrap();
    let expiry_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let storage = ScopedStorage::new(
        Scope::local(),
        "memory",
        Arc::clone(base.documents()),
        Arc::clone(base.streams()),
        Arc::new(OversizedBlobHead {
            expiry_reads: Arc::clone(&expiry_reads),
        }),
    );
    let server = StorageServer::new(storage);

    assert_eq!(
        kind(
            &server,
            "Blobs",
            "GetWithExpiry",
            json!({"key":"too-large"}),
        )
        .await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        expiry_reads.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the size limit must be checked before the full-body expiry read"
    );
}

#[tokio::test]
async fn secrets_without_a_store_or_confidential_admission_are_unsupported() {
    let server = server();
    let denied = server
        .handle(Admission::Private, "Secrets", "List", json!({"prefix": ""}))
        .await
        .unwrap_err();
    assert_eq!(denied.kind, StorageKind::Unsupported);
    assert_eq!(
        kind(&server, "Secrets", "List", json!({"prefix": ""})).await,
        StorageKind::Unsupported
    );
}

#[tokio::test]
async fn batches_and_inline_blobs_over_their_limits_are_refused() {
    let server = server();
    let ops: Vec<Value> = (0..=MAX_BATCH_OPS_PLUS)
        .map(|i| json!({"op":"put","collection":"c","document_id":format!("d{i}"),"document_json":"{}","precondition":{"kind":"none"}}))
        .collect();
    assert_eq!(
        kind(&server, "Documents", "AtomicBatch", json!({"ops": ops})).await,
        StorageKind::InvalidInput
    );
    let blob = vec![0u8; tinystoragedrivers_bus::MAX_INLINE_BLOB_BYTES + 1];
    assert_eq!(
        kind(&server, "Blobs", "Put", json!({"key":"k","bytes": blob})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Blobs",
            "PresignGet",
            json!({"key":"k","ttl_secs": 3601})
        )
        .await,
        StorageKind::InvalidInput
    );
}

const MAX_BATCH_OPS_PLUS: usize = tinystoragedrivers_bus::MAX_BATCH_OPS;

#[tokio::test]
async fn capabilities_never_list_host_only_entries() {
    let server = server();
    let reply = server
        .handle(Admission::Private, "Documents", "Capabilities", Value::Null)
        .await
        .unwrap();
    let names: Vec<String> = serde_json::from_value(reply["capabilities"].clone()).unwrap();
    for host_only in [
        "fencing",
        "scope_purge",
        "atomic_scope_purge",
        "existing_schema",
        "maintenance",
        "scope_enumeration",
    ] {
        assert!(!names.iter().any(|n| n == host_only), "{host_only}");
    }
    let _ = Arc::new(());
}

#[tokio::test]
async fn closing_an_unknown_watch_is_not_found() {
    let server = server();
    assert_eq!(
        kind(
            &server,
            "Documents",
            "WatchClose",
            json!({"watch_id":"w999"})
        )
        .await,
        StorageKind::NotFound
    );
}

async fn call(server: &StorageServer, object: &str, selector: &str, request: Value) -> Value {
    server
        .handle(Admission::Private, object, selector, request)
        .await
        .unwrap()
}

#[tokio::test]
async fn put_options_land_as_metadata_that_every_read_returns() {
    let server = server();
    let far = 4_102_444_800_000_i64;
    call(
        &server,
        "Documents",
        "Put",
        json!({"collection":"c","document_id":"a","document_json":"{\"n\":1}",
               "precondition":{"kind":"none"},"schema":3,"expires_ms":far}),
    )
    .await;
    let got = call(
        &server,
        "Documents",
        "Get",
        json!({"collection":"c","document_id":"a"}),
    )
    .await;
    assert_eq!(got["meta"]["schema"], 3);
    assert_eq!(got["meta"]["expires_ms"], far);
    assert!(got["meta"]["created_ms"].as_i64().unwrap() > 0);
    let page = call(
        &server,
        "Documents",
        "Query",
        json!({"collection":"c","query":{}}),
    )
    .await;
    assert_eq!(page["items"][0]["meta"]["schema"], 3);
    let claimed = call(
        &server,
        "Documents",
        "Claim",
        json!({"collection":"c","filter":{"op":"all"},"patch":"{\"n\":2}"}),
    )
    .await;
    assert_eq!(claimed["meta"]["schema"], 3);
}

#[tokio::test]
async fn a_put_without_options_is_the_pre_v4_request_and_a_forged_created_ms_is_refused() {
    let server = server();
    call(
        &server,
        "Documents",
        "Put",
        json!({"collection":"c","document_id":"a","document_json":"{}",
               "precondition":{"kind":"none"}}),
    )
    .await;
    assert_eq!(
        kind(
            &server,
            "Documents",
            "Put",
            json!({"collection":"c","document_id":"b","document_json":"{}",
                   "precondition":{"kind":"none"},"created_ms":1}),
        )
        .await,
        StorageKind::InvalidInput
    );
    // Nothing was written by the refused request.
    let got = call(
        &server,
        "Documents",
        "Get",
        json!({"collection":"c","document_id":"b"}),
    )
    .await;
    assert!(got.is_null());
}

#[tokio::test]
async fn seal_closes_a_stream_and_maintenance_selectors_do_not_exist() {
    let server = server();
    call(
        &server,
        "Streams",
        "Append",
        json!({"stream":"s","record_json":"1"}),
    )
    .await;
    let sealed = call(&server, "Streams", "Seal", json!({"stream":"s"})).await;
    assert_eq!(
        sealed,
        json!({"base":0,"len":1,"blob_bytes":sealed["blob_bytes"]})
    );
    assert_eq!(
        kind(
            &server,
            "Streams",
            "Append",
            json!({"stream":"s","record_json":"2"})
        )
        .await,
        StorageKind::Conflict
    );
    assert_eq!(
        kind(&server, "Streams", "Seal", json!({"stream":"never"})).await,
        StorageKind::NotFound
    );
    for selector in ["Maintain", "Checkpoint", "Scopes"] {
        for object in ["Documents", "Streams", "Blobs"] {
            assert_eq!(
                kind(&server, object, selector, json!({})).await,
                StorageKind::InvalidInput
            );
        }
    }
}

#[tokio::test]
async fn info_reports_extent_and_times_of_a_stream_and_null_for_a_missing_one() {
    let server = server();
    assert!(
        call(&server, "Streams", "Info", json!({"stream":"s"}))
            .await
            .is_null()
    );
    call(
        &server,
        "Streams",
        "Append",
        json!({"stream":"s","record_json":"1"}),
    )
    .await;
    let open = call(&server, "Streams", "Info", json!({"stream":"s"})).await;
    assert_eq!(
        (open["base"].clone(), open["len"].clone()),
        (json!(0), json!(1))
    );
    assert!(open["sealed_ms"].is_null());
    assert!(open["last_append_ms"].is_i64());

    call(&server, "Streams", "Seal", json!({"stream":"s"})).await;
    let sealed = call(&server, "Streams", "Info", json!({"stream":"s"})).await;
    assert!(sealed["sealed_ms"].is_i64());
    assert_eq!(sealed["last_append_ms"], open["last_append_ms"]);
}
