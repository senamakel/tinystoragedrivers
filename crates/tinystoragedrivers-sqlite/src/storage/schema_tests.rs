//! Restricted native construction does not initialize schema.

use super::*;
use serde_json::json;
use tinystoragedrivers_core::{
    CollectionSpec, ErrorKind, ExpectedDocument, Precondition, ReadCheck, Version, WriteOp,
    WriteOptions,
};

#[tokio::test]
async fn existing_schema_keeps_declarations_and_scoped_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    storage.prepare_schema().await.unwrap();
    let named = &storage;
    let scope = Scope::new("alice").unwrap();
    let other = Scope::new("bob").unwrap();
    let spec = CollectionSpec::new("notes").ttl("until");
    named
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&spec)
        .await
        .unwrap();
    named
        .for_scope(&other)
        .unwrap()
        .documents()
        .put("notes", "other", json!({}), Precondition::None)
        .await
        .unwrap();
    let restricted = storage.existing_schema().unwrap();
    let docs = restricted.for_scope(&scope).unwrap().documents().clone();
    docs.validate_collection_spec(&spec).await.unwrap();
    assert!(docs.ensure_collection(&spec).await.is_err());
    docs.put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    docs.drop_collection("notes").await.unwrap();
    docs.validate_collection_spec(&spec).await.unwrap();
    assert!(
        named
            .for_scope(&other)
            .unwrap()
            .documents()
            .get("notes", "other")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn existing_schema_requires_persisted_specs_and_refuses_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let restricted = storage.existing_schema().unwrap();
    assert!(restricted.prepare_schema().await.is_err());
    let docs = restricted
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("missing"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        docs.put("missing", "n", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    let spec = CollectionSpec::new("notes")
        .ttl("until")
        .searchable(["body", "title"]);
    storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(&spec)
        .await
        .unwrap();
    docs.validate_collection_spec(&CollectionSpec::new("notes").searchable(["title"]))
        .await
        .unwrap();
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("notes").ttl("changed"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("notes").searchable(["missing"]))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn atomic_batch_with_persists_options_and_rolls_them_back_with_the_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("records"))
        .await
        .unwrap();
    docs.atomic_batch_with(
        vec![WriteOp::Put {
            collection: "records".into(),
            id: "a".into(),
            doc: json!({"value":1}),
            precondition: Precondition::Absent,
        }],
        WriteOptions::new().schema(2).expires_ms(9_000_000_000_000),
    )
    .await
    .unwrap();
    let before = docs.get("records", "a").await.unwrap().unwrap();
    assert_eq!(before.meta.schema, 2);
    assert_eq!(before.meta.expires_ms, Some(9_000_000_000_000));

    let error = docs
        .atomic_batch_checked_with(
            vec![ReadCheck {
                collection: "records".into(),
                id: "a".into(),
                expected: ExpectedDocument::Version(before.version),
            }],
            vec![
                WriteOp::Put {
                    collection: "records".into(),
                    id: "a".into(),
                    doc: json!({"value":2}),
                    precondition: Precondition::Version(before.version),
                },
                WriteOp::Put {
                    collection: "records".into(),
                    id: "b".into(),
                    doc: json!({"value":3}),
                    precondition: Precondition::Version(Version(99)),
                },
            ],
            WriteOptions::new().schema(7).expires_ms(9_000_000_000_001),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(docs.get("records", "a").await.unwrap().unwrap(), before);
    assert!(docs.get("records", "b").await.unwrap().is_none());
    drop(docs);
    drop(storage);
    let reopened = SqliteStorage::open(dir.path()).unwrap();
    let persisted = reopened
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .get("records", "a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted, before);
}

#[tokio::test]
async fn existing_schema_preserves_cross_database_guard_for_every_port() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let cluster = Scope::new("cluster").unwrap();
    let scope = Scope::new("alice").unwrap();
    let leases = storage.for_scope(&cluster).unwrap().documents().clone();
    leases
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    leases
        .put("leases", "alice", json!({"epoch": 1}), Precondition::None)
        .await
        .unwrap();
    storage
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let fence = tinystoragedrivers_core::Fence::new(
        cluster,
        "leases",
        "alice",
        tinystoragedrivers_core::Filter::eq("epoch", 1),
    );
    let backend = storage.existing_schema().unwrap();
    let actor = backend.for_scope_fenced(&scope, &fence).unwrap();
    actor
        .documents()
        .put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    leases
        .put("leases", "alice", json!({"epoch": 2}), Precondition::None)
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
        ErrorKind::Fenced
    );
}

#[tokio::test]
async fn existing_schema_does_not_repair_incompatible_tables() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    storage
        .db
        .run_now(|conn| {
            conn.execute_batch("DROP TABLE blobs; CREATE TABLE blobs (wrong TEXT)")
                .map_err(crate::error::during("replace test schema"))
        })
        .unwrap();
    assert_eq!(
        storage.existing_schema().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    storage
        .db
        .run_now(|conn| {
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE name = 'blobs'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(sql, "CREATE TABLE blobs (wrong TEXT)");
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn existing_schema_reopens_files_without_initializing_tables() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    {
        let named = &storage;
        named
            .for_scope(&Scope::local())
            .unwrap()
            .documents()
            .ensure_collection(&CollectionSpec::new("notes"))
            .await
            .unwrap();
    }
    let actor = storage.existing_schema().unwrap();
    let docs = actor
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.validate_collection_spec(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    docs.put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    // The actor refuses bootstrap even after reopening an existing database.
    assert!(actor.prepare_schema().await.is_err());
}

#[tokio::test]
async fn existing_schema_reports_corrupt_declaration_with_source() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    storage
        .db
        .run_now(|conn| {
            conn.execute(
                "INSERT INTO _tsd_collections (coll, spec) VALUES ('notes', 'not json')",
                [],
            )
            .map_err(crate::error::during("seed corrupt metadata"))?;
            Ok(())
        })
        .unwrap();
    let error = actor
        .documents()
        .validate_collection_spec(&CollectionSpec::new("notes"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(std::error::Error::source(&error).is_some());
}

#[tokio::test]
async fn existing_schema_checked_dependencies_require_declarations() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let docs = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let check = tinystoragedrivers_core::ReadCheck {
        collection: "missing".into(),
        id: "n".into(),
        expected: tinystoragedrivers_core::ExpectedDocument::Absent,
    };
    assert_eq!(
        docs.atomic_batch_checked(vec![check], Vec::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn existing_schema_validates_inputs_before_missing_declarations() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path())
        .unwrap()
        .existing_schema()
        .unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    let docs = scoped.documents();
    let invalid_filter = tinystoragedrivers_core::Filter::eq("", 1);
    let invalid_query = tinystoragedrivers_core::Query {
        filter: invalid_filter.clone(),
        ..tinystoragedrivers_core::Query::default()
    };
    let invalid_cursor = tinystoragedrivers_core::Query {
        cursor: Some(tinystoragedrivers_core::Cursor("invalid".into())),
        ..tinystoragedrivers_core::Query::default()
    };
    let errors = [
        docs.get("missing", "").await.unwrap_err(),
        docs.put("missing", "n", json!(null), Precondition::None)
            .await
            .unwrap_err(),
        docs.delete("missing", "", Precondition::None)
            .await
            .unwrap_err(),
        docs.query("missing", &invalid_query).await.unwrap_err(),
        docs.query("missing", &invalid_cursor).await.unwrap_err(),
        docs.count("missing", &invalid_filter).await.unwrap_err(),
        docs.delete_where("missing", &invalid_filter)
            .await
            .unwrap_err(),
        docs.claim("missing", &invalid_filter, &[], &json!({}))
            .await
            .unwrap_err(),
        docs.claim(
            "missing",
            &tinystoragedrivers_core::Filter::All,
            &[],
            &json!(null),
        )
        .await
        .unwrap_err(),
        docs.atomic_batch(vec![tinystoragedrivers_core::WriteOp::Put {
            collection: "missing".into(),
            id: "n".into(),
            doc: json!(null),
            precondition: Precondition::None,
        }])
        .await
        .unwrap_err(),
        docs.atomic_batch_checked(
            vec![tinystoragedrivers_core::ReadCheck {
                collection: "missing".into(),
                id: String::new(),
                expected: tinystoragedrivers_core::ExpectedDocument::Absent,
            }],
            vec![],
        )
        .await
        .unwrap_err(),
    ];
    for error in errors {
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}
