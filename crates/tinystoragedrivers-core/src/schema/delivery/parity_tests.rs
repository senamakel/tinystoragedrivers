//! The delivery executor composes with the unchanged durable stream runner.
use super::client_tests::Transport;
use super::*;
use crate::schema::{
    MigrateOptions, Owner, Registry, Schema, SchemaDeclaration, ScopeClass, Step, StepDeclaration,
    TransformDescriptor, TransformError, TransformErrorKind, TransformKind,
};
use crate::{MemoryStorage, Scope, StorageBackend};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
fn map(entries: Vec<Value>) -> Vec<Value> {
    entries
        .into_iter()
        .filter(|v| v.get("valid") == Some(&Value::Bool(true)))
        .collect()
}
fn declaration() -> SchemaDeclaration {
    SchemaDeclaration {
        owner: Owner::new("owner").unwrap(),
        items: Schema::new(Owner::new("owner").unwrap())
            .stream("owner.target", ScopeClass::Tenant)
            .items,
        steps: vec![StepDeclaration {
            id: "move".into(),
            item: "owner.target".into(),
            from: 1,
            to: 2,
            source: Some("legacy/".into()),
            transform: TransformDescriptor {
                operation: "map".into(),
                version: 1,
                kind: TransformKind::Stream,
            },
        }],
    }
}
#[tokio::test]
async fn late_delivery_failure_writes_no_target_and_original_source_retries_like_local_runner() {
    let failed = Arc::new(AtomicBool::new(true));
    let transport = mapping_transport(failed.clone());
    let client = MigrationDeliveryClient::new(transport);
    let backend = MemoryStorage::with_clock(Arc::new(|| 5000));
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let registry = Registry::compose_with_declarations([], [declaration()]).unwrap();
    registry.prepare(&backend).await.unwrap();
    let entries: Vec<_> = (0..520)
        .map(|i| json!({"index":i,"valid":![255,256,511,512].contains(&i)}))
        .collect();
    storage
        .streams()
        .append_batch("legacy/a", entries.clone())
        .await
        .unwrap();
    storage.streams().seal("legacy/a").await.unwrap();
    let options = MigrateOptions::new().batch_size(37);
    assert!(
        registry
            .migrate_with_executor(&storage, &options, &client)
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .streams()
            .streams("owner.target/")
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(storage.streams().len("legacy/a").await.unwrap(), 520);
    assert!(
        registry
            .check_ready_in(storage.scope(), "owner.target")
            .is_err()
    );
    // Reopen, retry from the original input, and let the existing runner own
    // offsets, sealing, checkpoints, source cleanup and readiness.
    failed.store(false, Ordering::SeqCst);
    drop(registry);
    let reopened = Registry::compose_with_declarations([], [declaration()]).unwrap();
    assert_eq!(
        reopened
            .migrate_with_executor(&storage, &options, &client)
            .await
            .unwrap()
            .ran[0]
            .written,
        516
    );
    reopened
        .check_ready_in(storage.scope(), "owner.target")
        .unwrap();
    assert_eq!(storage.streams().streams("legacy/").await.unwrap().len(), 0);
    assert!(
        storage
            .streams()
            .info("owner.target/a")
            .await
            .unwrap()
            .unwrap()
            .sealed_ms
            .is_some()
    );
    let external = storage
        .streams()
        .read_window("owner.target/a", 0, 1000)
        .await
        .unwrap();
    let old = local_output(entries, &options).await;
    assert_eq!(external, old);
    assert_eq!(
        reopened
            .migrate_with_executor(&storage, &options, &client)
            .await
            .unwrap()
            .already_done,
        1
    );
}

async fn local_output(entries: Vec<Value>, options: &MigrateOptions) -> Vec<crate::StreamEntry> {
    let local_backend = MemoryStorage::with_clock(Arc::new(|| 5000));
    let local_storage = local_backend.for_scope(&Scope::local()).unwrap();
    let local = Registry::compose([Schema::new(Owner::new("owner").unwrap())
        .stream("owner.target", ScopeClass::Tenant)
        .step(
            Step::stream("move", "owner.target", 1, 2, |_, entries| {
                Ok(Some(("owner.target/a".into(), map(entries))))
            })
            .source_prefix("legacy/"),
        )])
    .unwrap();
    local.prepare(&local_backend).await.unwrap();
    local_storage
        .streams()
        .append_batch("legacy/a", entries)
        .await
        .unwrap();
    local_storage.streams().seal("legacy/a").await.unwrap();
    local.migrate(&local_storage, options).await.unwrap();
    local_storage
        .streams()
        .read_window("owner.target/a", 0, 1000)
        .await
        .unwrap()
}

fn mapping_transport(failed: Arc<AtomicBool>) -> Arc<Transport> {
    let fail = failed;
    let groups = AtomicUsize::new(0);
    Transport::new(move |payload| match payload {
        MigrationPayload::StreamPlan(_) => {
            groups.store(0, Ordering::SeqCst);
            MigrationPayloadReply::StreamPlan(Ok(Some(StreamPlan {
                target: "owner.target/a".into(),
                mode: StreamMapMode::IndependentEntries,
            })))
        }
        MigrationPayload::StreamEntries(request) => {
            if groups.fetch_add(1, Ordering::SeqCst) == 1 && fail.load(Ordering::SeqCst) {
                MigrationPayloadReply::StreamEntries(Err(TransformError {
                    kind: TransformErrorKind::InvalidInput,
                    message: "PRIVATE invalid entry".into(),
                }))
            } else {
                MigrationPayloadReply::StreamEntries(Ok(map(request.entries)))
            }
        }
        _ => panic!("unexpected payload"),
    })
}
