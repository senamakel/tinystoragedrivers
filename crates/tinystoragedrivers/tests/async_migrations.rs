//! The async transform port through durable drivers, including process-like
//! registry/backend reconstruction after an interrupted document page.
#![cfg(any(feature = "file", feature = "sqlite"))]

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use tinystoragedrivers::schema::*;
use tinystoragedrivers::{Precondition, Query, Result, Scope, StorageConfig, StorageError};

struct Executor {
    calls: AtomicUsize,
    interrupt: bool,
}
impl TransformExecutor for Executor {
    fn documents<'a>(
        &'a self,
        _: &'a Owner,
        request: DocumentsTransformRequest,
    ) -> BoxFuture<'a, Result<Vec<TargetRecord>>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            if self.interrupt && self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(StorageError::unavailable("injected interruption"));
            }
            Ok(request
                .records
                .into_iter()
                .map(|r| {
                    TargetRecord::new("shop.orders", r.id, r.body).created_ms(r.meta.created_ms)
                })
                .collect())
        })
    }
    fn stream<'a>(
        &'a self,
        _: &'a Owner,
        request: StreamTransformRequest,
    ) -> BoxFuture<'a, Result<Option<StreamTransformOutput>>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            Ok(Some(StreamTransformOutput {
                target: request.source.replace("legacy/", "shop.events/"),
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
        _: &'a Owner,
        request: BlobTransformRequest,
    ) -> BoxFuture<'a, Result<Option<BlobTransformOutput>>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            Ok(Some(BlobTransformOutput {
                target: request.source.replace("old/", "shop/files/"),
                content_type: request.content_type,
                bytes: request.bytes,
            }))
        })
    }
}
fn registry() -> Result<ComposedRegistry> {
    let schema = Schema::new(Owner::new("shop")?)
        .collection("shop.orders", 2, ScopeClass::Tenant)
        .stream("shop.events", ScopeClass::Tenant)
        .blobs("shop/files/", ScopeClass::Tenant);
    Registry::compose_with_declarations(
        [],
        [SchemaDeclaration {
            owner: schema.owner,
            items: schema.items,
            steps: [
                (
                    "orders",
                    "shop.orders",
                    "old_orders",
                    TransformKind::Documents,
                ),
                ("events", "shop.events", "legacy/", TransformKind::Stream),
                ("files", "shop/files/", "old/", TransformKind::Blob),
            ]
            .into_iter()
            .map(|(id, item, source, kind)| StepDeclaration {
                id: id.into(),
                item: item.into(),
                from: 1,
                to: 2,
                source: Some(source.into()),
                transform: TransformDescriptor {
                    operation: id.into(),
                    version: 1,
                    kind,
                },
            })
            .collect(),
        }],
    )
}
async fn roundtrip(url: &str) -> Result<()> {
    let config = StorageConfig::parse(url)?;
    let options = MigrateOptions::new().batch_size(1);
    {
        let backend = tinystoragedrivers::open(&config).await?;
        let registry = registry()?;
        registry.prepare(backend.as_ref()).await?;
        let storage = backend.for_scope(&Scope::local())?;
        for id in ["a", "b", "c"] {
            storage
                .documents()
                .put("old_orders", id, json!({"id":id}), Precondition::Absent)
                .await?;
        }
        storage
            .streams()
            .append_batch("legacy/a", vec![json!(1), json!(2)])
            .await?;
        storage.streams().seal("legacy/a").await?;
        storage
            .blobs()
            .put("old/a", vec![0, 255], Some("application/octet-stream"))
            .await?;
        let executor = Executor {
            calls: AtomicUsize::new(0),
            interrupt: true,
        };
        assert!(
            registry
                .migrate_with_executor(&storage, &options, &executor)
                .await
                .is_err()
        );
        assert!(registry.check_ready("shop.orders").is_err());
        assert_eq!(
            storage
                .documents()
                .query("old_orders", &Query::default())
                .await?
                .items
                .len(),
            3
        );
        assert_eq!(
            storage
                .documents()
                .query("shop.orders", &Query::default())
                .await?
                .items
                .len(),
            1
        );
    }
    resume_and_check(&config, &options).await?;
    {
        let backend = tinystoragedrivers::open(&config).await?;
        let registry = registry()?;
        let storage = backend.for_scope(&Scope::local())?;
        assert_eq!(registry.migrate(&storage, &options).await?.already_done, 3);
        registry.check_ready("shop.events")?;
    }
    Ok(())
}
async fn resume_and_check(config: &StorageConfig, options: &MigrateOptions) -> Result<()> {
    {
        let backend = tinystoragedrivers::open(config).await?;
        let registry = registry()?;
        registry.prepare(backend.as_ref()).await?;
        let storage = backend.for_scope(&Scope::local())?;
        let executor = Executor {
            calls: AtomicUsize::new(0),
            interrupt: false,
        };
        let report = registry
            .migrate_with_executor(&storage, options, &executor)
            .await?;
        assert_eq!(
            report.ran.iter().map(|s| s.written).collect::<Vec<_>>(),
            [3, 4, 1]
        );
        registry.check_ready_in(storage.scope(), "shop.orders")?;
        let orders = storage
            .documents()
            .query("shop.orders", &Query::default())
            .await?
            .items;
        assert_eq!(orders.len(), 3);
        assert!(orders.iter().all(|r| r.meta.schema == 2));
        assert_eq!(
            storage
                .streams()
                .read_window("shop.events/a", 0, 20)
                .await?
                .into_iter()
                .map(|e| e.value)
                .collect::<Vec<_>>(),
            vec![json!(1), json!(1), json!(2), json!(2)]
        );
        assert!(
            storage
                .streams()
                .info("shop.events/a")
                .await?
                .ok_or_else(|| StorageError::not_found("migrated target absent"))?
                .sealed_ms
                .is_some()
        );
        assert_eq!(
            storage
                .blobs()
                .get("shop/files/a")
                .await?
                .ok_or_else(|| StorageError::not_found("migrated target absent"))?
                .bytes,
            [0, 255]
        );
        assert_eq!(
            storage
                .documents()
                .query("old_orders", &Query::default())
                .await?
                .items
                .len(),
            0
        );
        assert_eq!(storage.streams().streams("legacy/").await?.len(), 0);
        assert_eq!(storage.blobs().list("old/").await?.len(), 0);
    }
    Ok(())
}
#[cfg(feature = "file")]
#[tokio::test(flavor = "current_thread")]
async fn file_reopen_resumes_external_transforms_without_loss_or_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    roundtrip(&format!("file:{}", dir.path().display()))
        .await
        .unwrap();
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_reopen_resumes_external_transforms_without_loss_or_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    roundtrip(&format!("sqlite:{}", dir.path().join("state.db").display()))
        .await
        .unwrap();
}
