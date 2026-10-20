//! Native schema policy remains sticky and performs no lazy DDL.

use super::*;
use futures_util::TryStreamExt;
use mongodb::event::{EventHandler, command::CommandEvent};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use tinystoragedrivers_core::{
    BackendBlobs, Capability, CollectionSpec, CompositeStorage, ErrorKind, Filter, MemoryStorage,
    Precondition, Scope, StorageBackend,
};

fn fixture_prefix() -> String {
    format!(
        "existing_schema_{}",
        mongodb::bson::oid::ObjectId::new().to_hex()
    )
}

async fn configured_fixture(url: &str) -> (MongoStorage, String) {
    let options = ClientOptions::parse(url).await.unwrap();
    let database = options
        .default_database
        .unwrap_or_else(|| "tsd_test".into());
    let root = MongoStorage::connect(url, &database).await.unwrap();
    assert_eq!(root.shared.db.name(), database);
    let prefix = fixture_prefix();
    (root.fixture_named(&prefix).unwrap(), prefix)
}

#[tokio::test]
async fn live_atomic_write_option_capability_tracks_transaction_support() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, _) = configured_fixture(&url).await;
    let documents = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let backend_capabilities = storage.capabilities();
    let document_capabilities = documents.capabilities();
    assert!(backend_capabilities.contains(Capability::Transactions));
    for capability in [
        Capability::Transactions,
        Capability::ValidatedBatch,
        Capability::AtomicWriteOptions,
    ] {
        assert_eq!(
            backend_capabilities.contains(capability),
            document_capabilities.contains(capability),
            "MongoStorage and its scoped DocumentStore must agree about {capability}"
        );
    }
    assert!(backend_capabilities.contains(Capability::AtomicWriteOptions));

    let composite = CompositeStorage::new(
        Arc::new(storage.clone()),
        Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new()))),
    );
    assert!(
        composite
            .capabilities()
            .contains(Capability::AtomicWriteOptions)
    );

    let without_transactions = storage.without_transactions();
    let no_transaction_documents = without_transactions
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    for capabilities in [
        without_transactions.capabilities(),
        no_transaction_documents.capabilities(),
    ] {
        assert!(!capabilities.contains(Capability::Transactions));
        assert!(!capabilities.contains(Capability::ValidatedBatch));
        assert!(!capabilities.contains(Capability::AtomicWriteOptions));
    }
    let without_transactions_composite = CompositeStorage::new(
        Arc::new(without_transactions),
        Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new()))),
    );
    assert!(
        !without_transactions_composite
            .capabilities()
            .contains(Capability::AtomicWriteOptions)
    );
}

fn default_fixture(fixture: &MongoStorage) -> MongoStorage {
    MongoStorage {
        shared: Arc::new(Shared::new(
            fixture.shared.client.clone(),
            fixture.shared.client.database(&format!(
                "tsd_schema_default_{}",
                mongodb::bson::oid::ObjectId::new().to_hex()
            )),
            Arc::new(|| 0),
            fixture.shared.transactions,
            fixture.shared.physical_transactions,
        )),
    }
}

async fn cleanup_fixture(storage: &MongoStorage, prefix: &str) {
    if storage.shared.db.name().starts_with("tsd_schema_default_") {
        storage.shared.db.drop().await.unwrap();
        return;
    }
    for name in storage.shared.db.list_collection_names().await.unwrap() {
        if name.starts_with(&format!("{prefix}:")) || name.starts_with(&format!("{prefix}_")) {
            storage
                .shared
                .db
                .collection::<mongodb::bson::Document>(&name)
                .drop()
                .await
                .unwrap();
        }
    }
}

async fn empty_window_refuses_damaged_schema(target: &str, expected: ErrorKind) {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    assert_eq!(
        actor.streams().read_window("s", 0, 0).await.unwrap(),
        Vec::<tinystoragedrivers_core::StreamEntry>::new()
    );
    match target {
        "index" => storage
            .shared
            .raw(STREAMS)
            .drop_index(crate::naming::STREAM_SEGMENT_INDEX)
            .await
            .unwrap(),
        collection => storage.shared.raw(collection).drop().await.unwrap(),
    }
    let outcome = actor.streams().read_window("s", 0, 0).await;
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(outcome.unwrap_err().kind(), expected);
}

#[tokio::test]
async fn live_empty_stream_window_requires_heads_schema() {
    empty_window_refuses_damaged_schema(STREAM_HEADS, ErrorKind::NotFound).await;
}

#[tokio::test]
async fn live_empty_stream_window_requires_segments_schema() {
    empty_window_refuses_damaged_schema(STREAMS, ErrorKind::NotFound).await;
}

#[tokio::test]
async fn live_empty_stream_window_requires_segment_index() {
    empty_window_refuses_damaged_schema("index", ErrorKind::InvalidInput).await;
}

#[tokio::test]
async fn live_standalone_existing_schema_drop_requires_persisted_declaration() {
    let Ok(url) = std::env::var("TSD_MONGO_STANDALONE_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    assert!(!storage.shared.physical_transactions);
    storage.prepare_schema().await.unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    scoped
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    scoped
        .documents()
        .put("notes", "n", json!({"body": "keep"}), Precondition::None)
        .await
        .unwrap();
    storage
        .shared
        .raw("_tsd_meta")
        .delete_one(doc! {"_id": "notes"})
        .await
        .unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    let outcome = actor.documents().drop_collection("notes").await;
    let retained = storage
        .shared
        .raw("notes")
        .find_one(doc! {"_key": "n"})
        .await
        .unwrap()
        .unwrap();
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(outcome.unwrap_err().kind(), ErrorKind::NotFound);
    assert_ne!(retained.get_bool(crate::naming::DELETED), Ok(true));
    assert_eq!(
        retained.get_document(crate::naming::BODY).unwrap(),
        &doc! {"body": "keep"}
    );
}

#[tokio::test]
async fn live_trusted_preparation_creates_unique_gridfs_chunk_ordinals() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let indexes: Vec<_> = storage
        .shared
        .raw(&format!("{BLOBS}.chunks"))
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let index = indexes
        .iter()
        .find(|index| index.keys == doc! {"files_id": 1, "n": 1})
        .unwrap();
    let unique = index.options.as_ref().and_then(|options| options.unique);
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(unique, Some(true));
}

#[tokio::test]
async fn live_existing_schema_blob_read_refuses_nonunique_chunk_index() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let chunks = storage.shared.raw(&format!("{BLOBS}.chunks"));
    let indexes: Vec<_> = chunks
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let index = indexes
        .iter()
        .find(|index| index.keys == doc! {"files_id": 1, "n": 1})
        .unwrap();
    chunks
        .drop_index(index.options.as_ref().unwrap().name.as_ref().unwrap())
        .await
        .unwrap();
    chunks
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! {"files_id": 1, "n": 1})
                .build(),
        )
        .await
        .unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    let outcome = actor.blobs().get("missing").await;
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(outcome.unwrap_err().kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn existing_schema_refuses_declaration_before_backend_network() {
    let client = Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=10")
        .await
        .unwrap();
    let storage = MongoStorage {
        shared: Arc::new(Shared::new(
            client.clone(),
            client.database("no_network"),
            Arc::new(|| 0),
            true,
            true,
        )),
    };
    let restricted = storage.existing_schema().unwrap();
    assert!(restricted.prepare_schema().await.is_err());
    let fence = Fence::new(Scope::local(), "leases", "l", Filter::All);
    let docs = restricted
        .for_scope_fenced(&Scope::local(), &fence)
        .unwrap()
        .documents()
        .clone();
    assert_eq!(
        docs.ensure_collection(&CollectionSpec::new("notes"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ExistingSchema)
    );
}

#[tokio::test]
async fn live_existing_schema_fresh_handles_do_not_issue_ddl() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        eprintln!(
            "live_existing_schema_fresh_handles_do_not_issue_ddl: no local fixture configured"
        );
        return;
    };
    let ddl = Arc::new(AtomicU64::new(0));
    let observed = ddl.clone();
    let mut options = ClientOptions::parse(url).await.unwrap();
    options.command_event_handler = Some(EventHandler::callback(move |event| {
        if let CommandEvent::Started(event) = event
            && ["create", "createIndexes", "dropIndexes", "drop", "collMod"]
                .contains(&event.command_name.as_str())
        {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let database = options
        .default_database
        .clone()
        .unwrap_or_else(|| "tsd_test".into());
    let client = Client::with_options(options).unwrap();
    let hello = client
        .database("admin")
        .run_command(doc! {"hello": 1})
        .await
        .unwrap();
    let transactions = supports_transactions(&hello);
    let db = client.database(&database);
    assert_eq!(db.name(), database);
    let prefix = fixture_prefix();
    let storage = MongoStorage {
        shared: Arc::new(Shared::new(
            client.clone(),
            db.clone(),
            Arc::new(|| 0),
            transactions,
            transactions,
        )),
    };
    let named = &storage;
    named.prepare_schema().await.unwrap();
    let scope = Scope::new("alice").unwrap();
    let spec = CollectionSpec::new("notes")
        .ttl("until")
        .searchable(["body"]);
    let trusted = named.for_scope(&scope).unwrap();
    trusted.documents().ensure_collection(&spec).await.unwrap();
    trusted.streams().append("s", json!(0)).await.unwrap();
    trusted.blobs().put("b", vec![1, 2], None).await.unwrap();
    // Rebuild native caches so classic preparation cannot mask lazy DDL.
    let fresh = storage.rebuild(Arc::new(|| 0), transactions);
    let restricted = fresh.existing_schema().unwrap();
    let actor = restricted.for_scope(&scope).unwrap();
    ddl.store(0, Ordering::SeqCst);
    assert_eq!(
        actor.streams().read_window("s", 0, 0).await.unwrap(),
        Vec::<tinystoragedrivers_core::StreamEntry>::new()
    );
    actor
        .documents()
        .validate_collection_spec(&spec)
        .await
        .unwrap();
    actor
        .documents()
        .put("notes", "n", json!({"body": "hello"}), Precondition::None)
        .await
        .unwrap();
    actor.documents().drop_collection("notes").await.unwrap();
    actor.streams().append("s", json!(1)).await.unwrap();
    assert_eq!(actor.blobs().get("b").await.unwrap().unwrap().bytes, [1, 2]);
    assert_eq!(ddl.load(Ordering::SeqCst), 0);
    assert_eq!(
        actor
            .documents()
            .ensure_collection(&spec)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ExistingSchema)
    );
    cleanup_fixture(&storage, &prefix).await;
}

#[tokio::test]
async fn live_existing_schema_missing_and_corrupt_metadata_never_repairs() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    let restricted = storage.existing_schema().unwrap();
    let docs = restricted
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("absent"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    let spec = CollectionSpec::new("notes");
    storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(&spec)
        .await
        .unwrap();
    storage
        .shared
        .raw("notes")
        .drop_index(crate::naming::SCOPE_KEY_INDEX)
        .await
        .unwrap();
    assert_eq!(
        docs.validate_collection_spec(&spec)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    // Validation must leave the missing index absent.
    let indexes: Vec<_> = storage
        .shared
        .raw("notes")
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!indexes.iter().any(|index| {
        index
            .options
            .as_ref()
            .and_then(|options| options.name.as_deref())
            == Some(crate::naming::SCOPE_KEY_INDEX)
    }));
    storage
        .shared
        .raw(crate::naming::META)
        .update_one(doc! {"_id": "notes"}, doc! {"$set": {"spec": "not json"}})
        .await
        .unwrap();
    let error = docs.validate_collection_spec(&spec).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(
        restricted
            .for_scope(&Scope::local())
            .unwrap()
            .streams()
            .append("s", json!(0))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    cleanup_fixture(&storage, &prefix).await;
}

#[tokio::test]
async fn live_existing_schema_keeps_captured_native_grant() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (fixture, prefix) = configured_fixture(&url).await;
    let storage = default_fixture(&fixture);
    let lease_collection = format!("{prefix}_leases");
    let cluster = Scope::new(format!("cluster_{prefix}")).unwrap();
    let scope = Scope::new("alice").unwrap();
    let leases = storage.for_scope(&cluster).unwrap().documents().clone();
    leases
        .ensure_collection(&CollectionSpec::new(&lease_collection))
        .await
        .unwrap();
    leases
        .put(
            &lease_collection,
            "alice",
            json!({"epoch": 1}),
            Precondition::None,
        )
        .await
        .unwrap();
    let named = &storage;
    named.prepare_schema().await.unwrap();
    named
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let fence = Fence::new(cluster, &lease_collection, "alice", Filter::eq("epoch", 1));
    let restricted = storage.existing_schema().unwrap();
    let actor = restricted.for_scope_fenced(&scope, &fence).unwrap();
    actor
        .documents()
        .put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    leases
        .put(
            &lease_collection,
            "alice",
            json!({"epoch": 2}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        actor
            .documents()
            .drop_collection("notes")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .documents()
            .atomic_batch(vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .streams()
            .append_batch_if_offset("s", 0, vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .blobs()
            .put("b", vec![1], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    cleanup_fixture(&storage, &prefix).await;
}

#[tokio::test]
async fn live_existing_schema_missing_validation_registry_is_not_created() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    if !storage.shared.transactions {
        return;
    }
    storage
        .shared
        .raw("_tsd_validated_batch")
        .drop()
        .await
        .unwrap();
    let docs = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let result = docs.atomic_batch_checked(Vec::new(), Vec::new()).await;
    let absent = !storage
        .shared
        .collection_exists("_tsd_validated_batch")
        .await
        .unwrap();
    cleanup_fixture(&storage, &prefix).await;
    assert!(
        absent,
        "restricted mutation implicitly created internal schema"
    );
    assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn live_existing_schema_refuses_missing_or_incompatible_standard_gridfs_files_index() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let mut outcomes = Vec::new();
    for incompatible in [false, true] {
        let (storage, prefix) = configured_fixture(&url).await;
        storage.prepare_schema().await.unwrap();
        let files = storage.shared.raw(&format!("{BLOBS}.files"));
        let indexes: Vec<_> = files
            .list_indexes()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let index = indexes
            .iter()
            .find(|index| index.keys == doc! {"filename": 1, "uploadDate": 1})
            .unwrap();
        files
            .drop_index(index.options.as_ref().unwrap().name.as_ref().unwrap())
            .await
            .unwrap();
        if incompatible {
            files
                .create_index(
                    mongodb::IndexModel::builder()
                        .keys(doc! {"filename": 1, "uploadDate": 1})
                        .options(
                            mongodb::options::IndexOptions::builder()
                                .unique(true)
                                .build(),
                        )
                        .build(),
                )
                .await
                .unwrap();
        }
        let actor = storage
            .existing_schema()
            .unwrap()
            .for_scope(&Scope::local())
            .unwrap();
        outcomes.push(actor.blobs().get("missing").await.err().map(|e| e.kind()));
        cleanup_fixture(&storage, &prefix).await;
    }
    assert_eq!(outcomes, [Some(ErrorKind::InvalidInput); 2]);
}

#[tokio::test]
async fn live_existing_schema_validation_refuses_retired_generation_after_purge() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let scope = Scope::local();
    let raw = storage.for_scope(&scope).unwrap();
    let spec = CollectionSpec::new("notes");
    raw.documents().ensure_collection(&spec).await.unwrap();
    let restricted = storage.existing_schema().unwrap();
    let stale = restricted.for_scope(&scope).unwrap();
    stale
        .documents()
        .validate_collection_spec(&spec)
        .await
        .unwrap();
    let receipt = raw
        .purge(&tinystoragedrivers_core::PurgeRequest {
            operation_id: "schema_validation_reset".into(),
            mode: tinystoragedrivers_core::PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    let outcome = stale.documents().validate_collection_spec(&spec).await;
    restricted
        .for_scope_generation(&scope, receipt.generation)
        .unwrap()
        .documents()
        .validate_collection_spec(&spec)
        .await
        .unwrap();
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(
        outcome.err().map(|e| e.kind()),
        Some(ErrorKind::ScopeGenerationRejected)
    );
}

#[tokio::test]
async fn live_existing_schema_missing_purge_metadata_refuses_before_data_erasure() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let scope = Scope::local();
    let raw = storage.for_scope(&scope).unwrap();
    raw.documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    raw.documents()
        .put("notes", "n", json!({"keep":true}), Precondition::None)
        .await
        .unwrap();
    storage
        .shared
        .raw(crate::purge::MANIFEST)
        .drop()
        .await
        .unwrap();
    let mut before = storage.shared.db.list_collection_names().await.unwrap();
    before.sort();
    let mut before_metadata = Vec::new();
    for name in [
        crate::purge::ADMISSIONS,
        crate::purge::RECEIPTS,
        crate::naming::META,
    ] {
        let rows: Vec<Document> = storage
            .shared
            .raw(name)
            .find(doc! {})
            .sort(doc! {"_id": 1})
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        before_metadata.push(rows);
    }
    let restricted = storage.existing_schema().unwrap();
    let advertised = restricted.capabilities().contains(Capability::ScopePurge);
    let actor = restricted.for_scope(&scope).unwrap();
    let outcome = actor
        .purge(&tinystoragedrivers_core::PurgeRequest {
            operation_id: "restricted_reset".into(),
            mode: tinystoragedrivers_core::PurgeMode::Quiescent,
        })
        .await;
    let mut after = storage.shared.db.list_collection_names().await.unwrap();
    after.sort();
    let mut after_metadata = Vec::new();
    for name in [
        crate::purge::ADMISSIONS,
        crate::purge::RECEIPTS,
        crate::naming::META,
    ] {
        let rows: Vec<Document> = storage
            .shared
            .raw(name)
            .find(doc! {})
            .sort(doc! {"_id": 1})
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        after_metadata.push(rows);
    }
    let retained = raw.documents().get("notes", "n").await;
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(outcome.err().map(|e| e.kind()), Some(ErrorKind::NotFound));
    assert!(advertised);
    assert_eq!(before, after);
    assert_eq!(before_metadata, after_metadata);
    assert!(retained.unwrap().is_some());
}

async fn blob_read_refuses_damaged_files_index(replace: bool) {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    assert!(actor.blobs().get("missing").await.unwrap().is_none());
    let files = storage.shared.raw(&format!("{BLOBS}.files"));
    let indexes: Vec<_> = files
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let index = indexes
        .iter()
        .find(|index| index.keys == doc! {"filename": 1, "uploadDate": 1})
        .unwrap();
    files
        .drop_index(index.options.as_ref().unwrap().name.as_ref().unwrap())
        .await
        .unwrap();
    if replace {
        files
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(doc! {"filename": 1, "uploadDate": 1})
                    .options(
                        mongodb::options::IndexOptions::builder()
                            .unique(true)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();
    }
    let outcome = actor.blobs().get("missing").await;
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(outcome.unwrap_err().kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn live_existing_schema_blob_read_refuses_missing_files_index() {
    blob_read_refuses_damaged_files_index(false).await;
}

#[tokio::test]
async fn live_existing_schema_blob_read_refuses_incompatible_files_index() {
    blob_read_refuses_damaged_files_index(true).await;
}

#[tokio::test]
async fn live_trusted_preparation_creates_purge_collections() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let present = [
        storage
            .shared
            .collection_exists("_tsd_purge_receipts")
            .await
            .unwrap(),
        storage
            .shared
            .collection_exists("_tsd_purge_blobs")
            .await
            .unwrap(),
    ];
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(present, [true, true]);
}

#[tokio::test]
async fn live_existing_schema_purge_does_not_create_missing_internal_collections() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    for missing in ["_tsd_purge_receipts", "_tsd_purge_blobs"] {
        let (storage, prefix) = configured_fixture(&url).await;
        storage.prepare_schema().await.unwrap();
        for name in ["_tsd_purge_receipts", "_tsd_purge_blobs"] {
            if !storage.shared.collection_exists(name).await.unwrap() {
                storage
                    .shared
                    .db
                    .create_collection(name.to_string())
                    .await
                    .unwrap();
            }
        }
        storage.shared.raw(missing).drop().await.unwrap();
        let actor = storage
            .existing_schema()
            .unwrap()
            .for_scope(&Scope::local())
            .unwrap();
        let outcome = actor
            .purge(&tinystoragedrivers_core::PurgeRequest {
                operation_id: "reset".into(),
                mode: tinystoragedrivers_core::PurgeMode::Quiescent,
            })
            .await;
        let recreated = storage.shared.collection_exists(missing).await.unwrap();
        cleanup_fixture(&storage, &prefix).await;
        assert_eq!(outcome.unwrap_err().kind(), ErrorKind::NotFound);
        assert!(!recreated);
    }
}

#[tokio::test]
async fn live_existing_schema_generation_default_guard_requires_declaration() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (fixture, prefix) = configured_fixture(&url).await;
    let storage = default_fixture(&fixture);
    storage.prepare_schema().await.unwrap();
    let lease_collection = format!("{prefix}_leases");
    let cluster = Scope::new(format!("cluster_{prefix}")).unwrap();
    let scope = Scope::new("alice").unwrap();
    let leases = storage.for_scope(&cluster).unwrap().documents().clone();
    leases
        .ensure_collection(&CollectionSpec::new(&lease_collection).ttl("until"))
        .await
        .unwrap();
    leases
        .put(
            &lease_collection,
            "alice",
            json!({"epoch": 1, "until": 0}),
            Precondition::None,
        )
        .await
        .unwrap();
    let named = &storage;
    named.prepare_schema().await.unwrap();
    named
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let fence = Fence::new(cluster, &lease_collection, "alice", Filter::eq("epoch", 1));
    let restricted = storage.existing_schema().unwrap();
    let actor = restricted
        .for_scope_fenced_generation(
            &scope,
            tinystoragedrivers_core::ScopeGeneration::default(),
            &fence,
        )
        .unwrap();
    storage
        .shared
        .raw(crate::naming::META)
        .delete_one(doc! {"_id": &lease_collection})
        .await
        .unwrap();
    let outcome = actor
        .documents()
        .put("notes", "n", json!({}), Precondition::None)
        .await;
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(outcome.unwrap_err().kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn live_existing_schema_purge_uses_prepared_internal_collections() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    let outcome = actor
        .purge(&tinystoragedrivers_core::PurgeRequest {
            operation_id: "reset".into(),
            mode: tinystoragedrivers_core::PurgeMode::Quiescent,
        })
        .await;
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(
        outcome.unwrap().status,
        tinystoragedrivers_core::PurgeStatus::Complete
    );
}

#[tokio::test]
async fn live_existing_schema_watch_requires_persisted_collection_declaration() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let (storage, prefix) = configured_fixture(&url).await;
    storage.prepare_schema().await.unwrap();
    let scope = Scope::local();
    storage
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&scope)
        .unwrap();
    let options = tinystoragedrivers_core::WatchOptions::default();
    let prepared = actor
        .documents()
        .watch("notes", &Filter::All, &options)
        .await;
    assert!(
        prepared.is_ok(),
        "prepared declarations permit restricted watches"
    );
    drop(prepared);
    storage
        .shared
        .raw(crate::naming::META)
        .delete_one(doc! {"_id": "notes"})
        .await
        .unwrap();
    let before = storage.shared.db.list_collection_names().await.unwrap();
    let mut outcomes = Vec::new();
    for collection in ["notes", "never_declared"] {
        let opened = actor
            .documents()
            .watch(collection, &Filter::All, &options)
            .await;
        outcomes.push(
            opened
                .as_ref()
                .err()
                .map(tinystoragedrivers_core::StorageError::kind),
        );
        drop(opened);
    }
    let mut before: Vec<_> = before
        .into_iter()
        .filter(|name| name.starts_with(&prefix))
        .collect();
    let mut after: Vec<_> = storage
        .shared
        .db
        .list_collection_names()
        .await
        .unwrap()
        .into_iter()
        .filter(|name| name.starts_with(&prefix))
        .collect();
    before.sort();
    after.sort();
    let metadata = storage
        .shared
        .raw(crate::naming::META)
        .find_one(doc! {"_id": "notes"})
        .await
        .unwrap();
    cleanup_fixture(&storage, &prefix).await;
    assert_eq!(outcomes, vec![Some(ErrorKind::NotFound); 2]);
    assert_eq!(
        after, before,
        "watch admission must not create native collections"
    );
    assert!(
        metadata.is_none(),
        "watch admission must not redeclare a collection"
    );
}
