use super::*;
use crate::{ErrorKind, MemoryStorage, Scope, StorageBackend};
use serde_json::json;

fn declaration() -> SchemaDeclaration {
    serde_json::from_value(json!({
        "owner":"shop", "items": serde_json::to_value(Schema::new(Owner::new("shop").unwrap())
            .collection("shop.orders", 2, ScopeClass::Tenant).items).unwrap(),
        "steps":[{"id":"upgrade", "item":"shop.orders", "from":1,"to":2,"source":null,
            "transform":{"operation":"upgrade","version":1,"kind":{"kind":"documents"}}}]
    }))
    .unwrap()
}

#[tokio::test]
async fn missing_executor_keeps_external_steps_pending() {
    for index in 0..3 {
        let mut declaration = all_external();
        declaration.steps = vec![declaration.steps.remove(index)];
        if index == 0 {
            declaration.steps[0].source = Some("old_orders".into());
        } else {
            declaration
                .items
                .retain(|item| !matches!(item, Item::Collection { .. }));
        }
        let registry = Registry::compose_with_declarations([], [declaration]).unwrap();
        let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
        registry.prepare(&backend).await.unwrap();
        let storage = backend.for_scope(&Scope::local()).unwrap();
        seed(&storage).await;
        if index == 0 {
            storage
                .documents()
                .put(
                    "old_orders",
                    "legacy",
                    json!({"legacy":true}),
                    crate::Precondition::Absent,
                )
                .await
                .unwrap();
        }
        let before = snapshot(&storage).await;
        assert_eq!(
            registry
                .migrate(&storage, &MigrateOptions::new())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unavailable
        );
        assert!(
            registry
                .check_ready_in(storage.scope(), &registry.schemas()[0].steps[0].item)
                .is_err()
        );
        assert_eq!(snapshot(&storage).await, before);
    }
}

#[tokio::test]
async fn empty_external_sources_complete_without_executor() {
    let registry = Registry::compose_with_declarations([], [all_external()]).unwrap();
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let report = registry
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap();
    assert_eq!(report.ran.len(), 3);
    assert!(report.ran.iter().all(|step| step.written == 0));
    for item in ["shop.orders", "shop.events", "shop/files/"] {
        registry.check_ready_in(storage.scope(), item).unwrap();
    }
    // Durable completion also needs no function after reopening the registry.
    let reopened = Registry::compose_with_declarations([], [all_external()]).unwrap();
    assert_eq!(
        reopened
            .migrate(&storage, &MigrateOptions::new())
            .await
            .unwrap()
            .already_done,
        3
    );
}

#[tokio::test]
async fn existing_empty_stream_and_blob_still_require_executor() {
    for index in [1, 2] {
        let mut declaration = all_external();
        declaration.steps = vec![declaration.steps.remove(index)];
        declaration
            .items
            .retain(|item| !matches!(item, Item::Collection { .. }));
        let registry = Registry::compose_with_declarations([], [declaration]).unwrap();
        let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
        registry.prepare(&backend).await.unwrap();
        let storage = backend.for_scope(&Scope::local()).unwrap();
        // Truncating preserves the stream's name and offset metadata.
        storage
            .streams()
            .append("legacy/a", json!(1))
            .await
            .unwrap();
        storage
            .streams()
            .truncate_before("legacy/a", 1)
            .await
            .unwrap();
        storage
            .blobs()
            .put("old/a", vec![], Some("text/plain"))
            .await
            .unwrap();
        assert_eq!(
            storage
                .streams()
                .read_window("legacy/a", 0, 10)
                .await
                .unwrap()
                .len(),
            0
        );
        let before = snapshot(&storage).await;
        assert_eq!(
            registry
                .migrate(&storage, &MigrateOptions::new())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unavailable
        );
        assert_eq!(snapshot(&storage).await, before);
        assert!(
            registry
                .check_ready_in(storage.scope(), &registry.schemas()[0].steps[0].item)
                .is_err()
        );
    }
}

struct Gated {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl TransformExecutor for Gated {
    fn documents<'a>(
        &'a self,
        _: &'a Owner,
        request: DocumentsTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Vec<TargetRecord>>> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(request
                .records
                .into_iter()
                .map(|r| TargetRecord::new("shop.orders", r.id, r.body))
                .collect())
        })
    }
    fn stream<'a>(
        &'a self,
        _: &'a Owner,
        _: StreamTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<StreamTransformOutput>>> {
        Box::pin(async { unreachable!() })
    }
    fn blob<'a>(
        &'a self,
        _: &'a Owner,
        _: BlobTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<BlobTransformOutput>>> {
        Box::pin(async { unreachable!() })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pending_transform_yields_without_cleanup() {
    let registry = Registry::compose_with_declarations([], [declaration()]).unwrap();
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    storage
        .documents()
        .put_with(
            "shop.orders",
            "one",
            json!({"x":1}),
            crate::Precondition::Absent,
            crate::WriteOptions::new().schema(1),
        )
        .await
        .unwrap();
    let executor = Gated {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    };
    let options = MigrateOptions::new();
    let migration = registry.migrate_with_executor(&storage, &options, &executor);
    tokio::pin!(migration);
    tokio::select! {
        result = &mut migration => panic!("unexpected completion: {result:?}"),
        () = executor.entered.notified() => {}
    }
    assert!(registry.check_ready("shop.orders").is_err());
    assert_eq!(
        storage
            .documents()
            .get("shop.orders", "one")
            .await
            .unwrap()
            .unwrap()
            .meta
            .schema,
        1
    );
    executor.release.notify_one();
    assert_eq!(migration.await.unwrap().ran[0].written, 1);
    registry.check_ready("shop.orders").unwrap();
}

struct Echo;
fn mapped(records: Vec<SourceRecord>) -> Vec<TargetRecord> {
    records
        .into_iter()
        .map(|r| {
            let target = TargetRecord::new("shop.orders", r.id.clone(), r.body);
            match r.id.as_str() {
                "two" => target.expires_ms(None).created_ms(321),
                "three" => target.expires_ms(Some(65_000)),
                _ => target.created_ms(r.meta.created_ms),
            }
        })
        .collect()
}
impl TransformExecutor for Echo {
    fn documents<'a>(
        &'a self,
        owner: &'a Owner,
        request: DocumentsTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Vec<TargetRecord>>> {
        Box::pin(async move {
            assert_eq!(owner.as_str(), "shop");
            assert_eq!(request.version, 1);
            tokio::task::yield_now().await;
            if request.operation != "upgrade" {
                return Err(crate::StorageError::invalid_input(
                    "unsupported transform operation",
                ));
            }
            Ok(mapped(request.records))
        })
    }
    fn stream<'a>(
        &'a self,
        owner: &'a Owner,
        request: StreamTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<StreamTransformOutput>>> {
        Box::pin(async move {
            assert_eq!(owner.as_str(), "shop");
            assert_eq!(request.version, 1);
            tokio::task::yield_now().await;
            assert_eq!(request.operation, "move");
            Ok(Some(StreamTransformOutput {
                target: request.source.replace("legacy", "shop.events"),
                entries: request
                    .entries
                    .into_iter()
                    .flat_map(|v| [v.clone(), v])
                    .collect(),
            }))
        })
    }
    fn blob<'a>(
        &'a self,
        owner: &'a Owner,
        request: BlobTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<BlobTransformOutput>>> {
        Box::pin(async move {
            assert_eq!(owner.as_str(), "shop");
            assert_eq!(request.version, 1);
            tokio::task::yield_now().await;
            assert_eq!(request.operation, "move");
            Ok(Some(BlobTransformOutput {
                target: request.source.replace("old/", "shop/files/"),
                content_type: request.content_type,
                bytes: request.bytes,
            }))
        })
    }
}
fn all_local() -> Schema {
    Schema::new(Owner::new("shop").unwrap())
        .collection("shop.orders", 2, ScopeClass::Tenant)
        .stream("shop.events", ScopeClass::Tenant)
        .blobs("shop/files/", ScopeClass::Tenant)
        .step(Step::new("upgrade", "shop.orders", 1, 2, |r| Ok(mapped(r))))
        .step(
            Step::stream("move", "shop.events", 1, 2, |source, entries| {
                Ok(Some((
                    source.replace("legacy", "shop.events"),
                    entries.into_iter().flat_map(|v| [v.clone(), v]).collect(),
                )))
            })
            .source_prefix("legacy"),
        )
        .step(
            Step::blobs("move", "shop/files/", 1, 2, |source, ty, bytes| {
                Ok(Some((source.replace("old/", "shop/files/"), ty, bytes)))
            })
            .source_prefix("old/"),
        )
}
fn all_external() -> SchemaDeclaration {
    let local = all_local();
    SchemaDeclaration {
        owner: local.owner,
        items: local.items,
        steps: local
            .steps
            .into_iter()
            .map(|s| {
                let kind = match s.run {
                    StepBody::Documents(_) => TransformKind::Documents,
                    StepBody::Streams(_) => TransformKind::Stream,
                    StepBody::Blobs(_) => TransformKind::Blob,
                };
                StepDeclaration {
                    id: s.id.into(),
                    item: s.item.into(),
                    from: s.from,
                    to: s.to,
                    source: s.source.map(str::to_owned),
                    transform: TransformDescriptor {
                        operation: s.id.into(),
                        version: 1,
                        kind,
                    },
                }
            })
            .collect(),
    }
}
async fn seed(storage: &crate::ScopedStorage) {
    for (id, expires) in [
        ("one", Some(90_000)),
        ("two", Some(70_000)),
        ("three", Some(80_000)),
    ] {
        let mut options = crate::WriteOptions::new().schema(1).created_ms(123);
        if let Some(expires) = expires {
            options = options.expires_ms(expires);
        }
        storage
            .documents()
            .put_with(
                "shop.orders",
                id,
                json!({"id":id}),
                crate::Precondition::Absent,
                options,
            )
            .await
            .unwrap();
    }
    storage
        .streams()
        .append_batch("legacy/a", vec![json!(1), json!(2), json!(3)])
        .await
        .unwrap();
    storage.streams().seal("legacy/a").await.unwrap();
    storage
        .blobs()
        .put("old/a", vec![0, 255, 42], Some("application/octet-stream"))
        .await
        .unwrap();
}
async fn snapshot(storage: &crate::ScopedStorage) -> serde_json::Value {
    json!({
        "docs":storage.documents().query("shop.orders", &crate::Query::default().sort(crate::Sort::asc("_id"))).await.unwrap(),
        "progress":storage.documents().query(MIGRATIONS_COLLECTION, &crate::Query::default().sort(crate::Sort::asc("_id"))).await.unwrap(),
        "stream":storage.streams().read_window("shop.events/a", 0, 50).await.unwrap(),
        "stream_info":storage.streams().info("shop.events/a").await.unwrap(),
        "blob":storage.blobs().get("shop/files/a").await.unwrap().map(|b| (b.bytes, b.meta)),
        "old_docs":storage.documents().query("old_orders", &crate::Query::default()).await.unwrap(),
        "old_stream":storage.streams().read_window("legacy/a", 0, 50).await.unwrap(),
        "old_stream_info":storage.streams().info("legacy/a").await.unwrap(),
        "old_blob":storage.blobs().get("old/a").await.unwrap().map(|b| (b.bytes, b.meta)),
        "old_streams":storage.streams().streams("legacy").await.unwrap(),
        "old_blobs":storage.blobs().list("old/").await.unwrap()
    })
}

#[tokio::test]
async fn external_and_local_share_outputs_metadata_reports_and_progress() {
    use std::sync::Arc;
    let local = Registry::compose([all_local()]).unwrap();
    let external = Registry::compose_with_declarations([], [all_external()]).unwrap();
    let a = MemoryStorage::with_clock(Arc::new(|| 5_000));
    let b = MemoryStorage::with_clock(Arc::new(|| 5_000));
    local.prepare(&a).await.unwrap();
    external.prepare(&b).await.unwrap();
    let a = a.for_scope(&Scope::local()).unwrap();
    let b = b.for_scope(&Scope::local()).unwrap();
    seed(&a).await;
    seed(&b).await;
    let options = MigrateOptions::new().batch_size(2);
    assert_eq!(
        local.migrate(&a, &options).await.unwrap(),
        external
            .migrate_with_executor(&b, &options, &Echo)
            .await
            .unwrap()
    );
    assert_eq!(snapshot(&a).await, snapshot(&b).await);
    for storage in [&a, &b] {
        for (id, expiry, created) in [
            ("one", Some(90_000), 123),
            ("two", None, 321),
            ("three", Some(65_000), 123),
        ] {
            let stored = storage
                .documents()
                .get("shop.orders", id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.meta.expires_ms, expiry, "{id}");
            assert_eq!(stored.meta.created_ms, created, "{id}");
            assert_eq!(stored.meta.schema, 2, "{id}");
        }
    }
    assert_eq!(
        external.migrate(&b, &options).await.unwrap().already_done,
        3
    );
    external.check_ready_in(b.scope(), "shop.events").unwrap();
}

#[test]
fn mixed_declarations_keep_all_owned_metadata_and_validate_every_owner() {
    let local = Schema::new(Owner::new("local").unwrap())
        .collection("local.records", 2, ScopeClass::Tenant)
        .step(Step::new("local_upgrade", "local.records", 1, 2, |_| {
            Ok(vec![])
        }));
    let external = all_external();
    let registry = Registry::compose_with_declarations([local], [external.clone()]).unwrap();
    assert_eq!(registry.owners().collect::<Vec<_>>(), ["local", "shop"]);
    assert_eq!(
        registry.schemas()[0].steps[0].execution,
        StepExecution::Local {
            kind: TransformKind::Documents
        }
    );
    let metadata: Vec<SchemaMetadata> =
        serde_json::from_value(serde_json::to_value(registry.schemas()).unwrap()).unwrap();
    assert_eq!(metadata, registry.schemas());
    assert_eq!(registry.schemas()[1].owner, external.owner);
    assert_eq!(registry.schemas()[1].items, external.items);
    for (metadata, step) in registry.schemas()[1].steps.iter().zip(&external.steps) {
        assert_eq!(metadata.id, step.id);
        assert_eq!(metadata.item, step.item);
        assert_eq!(metadata.source, step.source);
        assert_eq!((metadata.from, metadata.to), (step.from, step.to));
        assert_eq!(
            metadata.execution,
            StepExecution::External(step.transform.clone())
        );
    }
    assert_eq!(registry.schemas()[1].steps.len(), 3);
    assert!(Registry::compose_with_declarations([all_local()], [all_external()]).is_err());
    let mut bad = all_external();
    bad.items[0] = Schema::new(Owner::new("other").unwrap())
        .collection("other.rows", 2, ScopeClass::Tenant)
        .items
        .remove(0);
    assert!(Registry::compose_with_declarations([], [bad]).is_err());
    for operation in ["", "bad selector", "../bad", "bad\0"] {
        let mut bad = declaration();
        bad.steps[0].transform.operation = operation.into();
        assert!(Registry::compose_with_declarations([], [bad]).is_err());
    }
    let mut bad = declaration();
    bad.steps[0].transform.version = 2;
    assert!(Registry::compose_with_declarations([], [bad]).is_err());
    let mut bad = declaration();
    bad.steps[0].transform.kind = TransformKind::Blob;
    assert!(Registry::compose_with_declarations([], [bad]).is_err());
    let mut bad = declaration();
    bad.steps.push(bad.steps[0].clone());
    assert!(Registry::compose_with_declarations([], [bad]).is_err());
    let mut bad = declaration();
    bad.steps[0].from = 0;
    assert!(Registry::compose_with_declarations([], [bad]).is_err());
    assert!(serde_json::from_value::<TransformKind>(json!({"kind":"unknown"})).is_err());
}

struct NoCalls;
impl TransformExecutor for NoCalls {
    fn documents<'a>(
        &'a self,
        _: &'a Owner,
        _: DocumentsTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Vec<TargetRecord>>> {
        panic!("unexpected documents call")
    }
    fn stream<'a>(
        &'a self,
        _: &'a Owner,
        _: StreamTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<StreamTransformOutput>>> {
        panic!("unexpected stream call")
    }
    fn blob<'a>(
        &'a self,
        _: &'a Owner,
        _: BlobTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<BlobTransformOutput>>> {
        panic!("unexpected blob call")
    }
}
#[tokio::test]
async fn empty_and_completed_steps_do_not_invoke_executor() {
    let registry = Registry::compose_with_declarations([], [all_external()]).unwrap();
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let report = registry
        .migrate_with_executor(&storage, &MigrateOptions::new(), &NoCalls)
        .await
        .unwrap();
    assert_eq!(report.ran.len(), 3);
    assert!(report.ran.iter().all(|s| s.written == 0));
    assert_eq!(
        registry
            .migrate_with_executor(&storage, &MigrateOptions::new(), &NoCalls)
            .await
            .unwrap()
            .already_done,
        3
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_pending_transform_reopens_without_lost_records() {
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let registry = Registry::compose_with_declarations([], [declaration()]).unwrap();
    registry.prepare(&backend).await.unwrap();
    storage
        .documents()
        .put_with(
            "shop.orders",
            "one",
            json!({"x":1}),
            crate::Precondition::Absent,
            crate::WriteOptions::new().schema(1),
        )
        .await
        .unwrap();
    let executor = Gated {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    };
    {
        let options = MigrateOptions::new();
        let migration = registry.migrate_with_executor(&storage, &options, &executor);
        tokio::pin!(migration);
        tokio::select! {
            r = &mut migration => panic!("unexpected completion {r:?}"),
            () = executor.entered.notified() => {}
        }
    }
    drop(registry);
    let registry = Registry::compose_with_declarations([], [declaration()]).unwrap();
    assert!(registry.check_ready("shop.orders").is_err());
    assert_eq!(
        registry
            .migrate_with_executor(&storage, &MigrateOptions::new(), &Echo)
            .await
            .unwrap()
            .ran[0]
            .written,
        1
    );
    assert_eq!(
        storage
            .documents()
            .get("shop.orders", "one")
            .await
            .unwrap()
            .unwrap()
            .meta
            .schema,
        2
    );
}

struct FailSecond(std::sync::atomic::AtomicUsize);
impl TransformExecutor for FailSecond {
    fn documents<'a>(
        &'a self,
        _: &'a Owner,
        request: DocumentsTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Vec<TargetRecord>>> {
        Box::pin(async move {
            if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                return Err(crate::StorageError::unavailable(
                    "injected page interruption",
                ));
            }
            Ok(mapped(request.records))
        })
    }
    fn stream<'a>(
        &'a self,
        _: &'a Owner,
        _: StreamTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<StreamTransformOutput>>> {
        unreachable!()
    }
    fn blob<'a>(
        &'a self,
        _: &'a Owner,
        _: BlobTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<BlobTransformOutput>>> {
        unreachable!()
    }
}
#[tokio::test]
async fn external_page_failure_keeps_cursor_and_resumes_legacy_cleanup() {
    let mut schema = declaration();
    schema.steps[0].source = Some("old_orders".into());
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let registry = Registry::compose_with_declarations([], [schema.clone()]).unwrap();
    registry.prepare(&backend).await.unwrap();
    for id in ["a", "b", "c"] {
        storage
            .documents()
            .put(
                "old_orders",
                id,
                json!({"id":id}),
                crate::Precondition::Absent,
            )
            .await
            .unwrap();
    }
    let options = MigrateOptions::new().batch_size(1);
    let fail = FailSecond(std::sync::atomic::AtomicUsize::new(0));
    assert!(
        registry
            .migrate_with_executor(&storage, &options, &fail)
            .await
            .is_err()
    );
    assert!(registry.check_ready("shop.orders").is_err());
    assert_eq!(
        storage
            .documents()
            .query("old_orders", &crate::Query::default())
            .await
            .unwrap()
            .items
            .len(),
        3
    );
    assert_eq!(
        storage
            .documents()
            .query("shop.orders", &crate::Query::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    drop(registry);
    let registry = Registry::compose_with_declarations([], [schema]).unwrap();
    assert_eq!(
        registry
            .migrate_with_executor(&storage, &options, &Echo)
            .await
            .unwrap()
            .ran[0]
            .written,
        3
    );
    assert_eq!(
        storage
            .documents()
            .query("old_orders", &crate::Query::default())
            .await
            .unwrap()
            .items
            .len(),
        0
    );
    assert_eq!(
        storage
            .documents()
            .query("shop.orders", &crate::Query::default())
            .await
            .unwrap()
            .items
            .len(),
        3
    );
}

struct BadOutput;
impl TransformExecutor for BadOutput {
    fn documents<'a>(
        &'a self,
        _: &'a Owner,
        _: DocumentsTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Vec<TargetRecord>>> {
        Box::pin(async {
            Ok(vec![
                TargetRecord::new("shop.orders", "valid", json!({"x":1})),
                TargetRecord::new("other.orders", "bad", json!({"x":2})),
            ])
        })
    }
    fn stream<'a>(
        &'a self,
        _: &'a Owner,
        _: StreamTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<StreamTransformOutput>>> {
        Box::pin(async {
            Ok(Some(StreamTransformOutput {
                target: "other.events".into(),
                entries: vec![json!(1)],
            }))
        })
    }
    fn blob<'a>(
        &'a self,
        _: &'a Owner,
        _: BlobTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<BlobTransformOutput>>> {
        Box::pin(async {
            Ok(Some(BlobTransformOutput {
                target: "other/files/a".into(),
                content_type: None,
                bytes: vec![1],
            }))
        })
    }
}
#[tokio::test]
async fn invalid_external_outputs_keep_sources_and_write_no_partial_batch() {
    for index in 0..3 {
        let mut declaration = all_external();
        declaration.steps = vec![declaration.steps.remove(index)];
        if index != 0 {
            declaration
                .items
                .retain(|i| !matches!(i, Item::Collection { .. }));
        }
        let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
        let registry = Registry::compose_with_declarations([], [declaration]).unwrap();
        registry.prepare(&backend).await.unwrap();
        let storage = backend.for_scope(&Scope::local()).unwrap();
        seed(&storage).await;
        assert_eq!(
            registry
                .migrate_with_executor(&storage, &MigrateOptions::new(), &BadOutput)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert!(
            storage
                .documents()
                .get("shop.orders", "valid")
                .await
                .unwrap()
                .is_none()
        );
        assert!(storage.streams().len("legacy/a").await.unwrap() > 0);
        assert!(storage.blobs().get("old/a").await.unwrap().is_some());
    }
}

struct DropOutput;
impl TransformExecutor for DropOutput {
    fn documents<'a>(
        &'a self,
        _: &'a Owner,
        _: DocumentsTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Vec<TargetRecord>>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn stream<'a>(
        &'a self,
        _: &'a Owner,
        _: StreamTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<StreamTransformOutput>>> {
        Box::pin(async { Ok(None) })
    }
    fn blob<'a>(
        &'a self,
        _: &'a Owner,
        _: BlobTransformRequest,
    ) -> BoxFuture<'a, crate::Result<Option<BlobTransformOutput>>> {
        Box::pin(async { Ok(None) })
    }
}
#[tokio::test]
async fn explicit_empty_external_outputs_follow_existing_drop_policy() {
    let registry = Registry::compose_with_declarations([], [all_external()]).unwrap();
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    seed(&storage).await;
    let report = registry
        .migrate_with_executor(&storage, &MigrateOptions::new(), &DropOutput)
        .await
        .unwrap();
    assert!(report.ran.iter().all(|r| r.written == 0));
    assert_eq!(
        storage
            .documents()
            .query("shop.orders", &crate::Query::default())
            .await
            .unwrap()
            .items
            .len(),
        0
    );
    assert_eq!(storage.streams().streams("legacy").await.unwrap().len(), 0);
    assert_eq!(storage.blobs().list("old/").await.unwrap().len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn source_changed_during_external_await_conflicts_without_marking_ready() {
    let registry = Registry::compose_with_declarations([], [declaration()]).unwrap();
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    storage
        .documents()
        .put_with(
            "shop.orders",
            "one",
            json!({"x":1}),
            crate::Precondition::Absent,
            crate::WriteOptions::new().schema(1),
        )
        .await
        .unwrap();
    let executor = Gated {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    };
    let options = MigrateOptions::new();
    let migration = registry.migrate_with_executor(&storage, &options, &executor);
    tokio::pin!(migration);
    tokio::select! {
        r = &mut migration => panic!("unexpected completion {r:?}"),
        () = executor.entered.notified() => {}
    }
    storage
        .documents()
        .put_with(
            "shop.orders",
            "one",
            json!({"x":2}),
            crate::Precondition::None,
            crate::WriteOptions::new().schema(1),
        )
        .await
        .unwrap();
    executor.release.notify_one();
    assert_eq!(migration.await.unwrap_err().kind(), ErrorKind::Conflict);
    assert!(registry.check_ready("shop.orders").is_err());
    assert_eq!(
        storage
            .documents()
            .get("shop.orders", "one")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({"x":2})
    );
}

#[tokio::test]
async fn existing_stream_and_blob_targets_are_not_overwritten() {
    for index in [1, 2] {
        let mut declaration = all_external();
        declaration.steps = vec![declaration.steps.remove(index)];
        declaration
            .items
            .retain(|i| !matches!(i, Item::Collection { .. }));
        let registry = Registry::compose_with_declarations([], [declaration]).unwrap();
        let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
        registry.prepare(&backend).await.unwrap();
        let storage = backend.for_scope(&Scope::local()).unwrap();
        seed(&storage).await;
        storage
            .streams()
            .append_batch("shop.events/a", vec![json!(99)])
            .await
            .unwrap();
        storage
            .blobs()
            .put("shop/files/a", vec![99], None)
            .await
            .unwrap();
        assert_eq!(
            registry
                .migrate_with_executor(&storage, &MigrateOptions::new(), &Echo)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Conflict
        );
        assert_eq!(
            storage
                .streams()
                .read_window("shop.events/a", 0, 20)
                .await
                .unwrap()[0]
                .value,
            json!(99)
        );
        assert_eq!(
            storage
                .blobs()
                .get("shop/files/a")
                .await
                .unwrap()
                .unwrap()
                .bytes,
            vec![99]
        );
        assert!(storage.streams().len("legacy/a").await.unwrap() > 0);
        assert!(storage.blobs().get("old/a").await.unwrap().is_some());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lease_fence_lost_during_external_await_prevents_writes_and_completion() {
    let registry = Registry::compose_with_declarations([], [declaration()]).unwrap();
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    storage
        .documents()
        .put(
            "lease_guard",
            "migration",
            json!({"epoch":1}),
            crate::Precondition::Absent,
        )
        .await
        .unwrap();
    storage
        .documents()
        .put_with(
            "shop.orders",
            "one",
            json!({"x":1}),
            crate::Precondition::Absent,
            crate::WriteOptions::new().schema(1),
        )
        .await
        .unwrap();
    let fence = crate::Fence::epoch(Scope::local(), "lease_guard", "migration", "epoch", 1);
    let fenced = backend.for_scope_fenced(&Scope::local(), &fence).unwrap();
    let executor = Gated {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    };
    let options = MigrateOptions::new();
    let migration = registry.migrate_with_executor(&fenced, &options, &executor);
    tokio::pin!(migration);
    tokio::select! {
        r = &mut migration => panic!("unexpected completion {r:?}"),
        () = executor.entered.notified() => {}
    }
    storage
        .documents()
        .put(
            "lease_guard",
            "migration",
            json!({"epoch":2}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
    executor.release.notify_one();
    assert_eq!(migration.await.unwrap_err().kind(), ErrorKind::Fenced);
    assert!(registry.check_ready("shop.orders").is_err());
    assert_eq!(
        storage
            .documents()
            .get("shop.orders", "one")
            .await
            .unwrap()
            .unwrap()
            .meta
            .schema,
        1
    );
    let progress = storage
        .documents()
        .query(MIGRATIONS_COLLECTION, &crate::Query::default())
        .await
        .unwrap();
    assert!(progress.items.iter().all(|p| p.doc["done"] != true));
}

#[tokio::test]
async fn unknown_external_operation_failure_is_not_hidden_or_marked_done() {
    let mut declaration = declaration();
    declaration.steps[0].transform.operation = "unknown".into();
    let registry = Registry::compose_with_declarations([], [declaration]).unwrap();
    let backend = MemoryStorage::with_clock(std::sync::Arc::new(|| 5_000));
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    storage
        .documents()
        .put_with(
            "shop.orders",
            "one",
            json!({"x":1}),
            crate::Precondition::Absent,
            crate::WriteOptions::new().schema(1),
        )
        .await
        .unwrap();
    assert_eq!(
        registry
            .migrate_with_executor(&storage, &MigrateOptions::new(), &Echo)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(registry.check_ready("shop.orders").is_err());
    assert_eq!(
        storage
            .documents()
            .get("shop.orders", "one")
            .await
            .unwrap()
            .unwrap()
            .meta
            .schema,
        1
    );
}
