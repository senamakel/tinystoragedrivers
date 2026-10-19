//! The stream step runner over the memory driver: renaming, entry rewriting,
//! sealed state, resume after a mid-stream failure and idempotence.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::schema::{MigrateOptions, Owner, Registry, Schema, ScopeClass, Step};
use crate::{
    ErrorKind, MemoryStorage, Result, Scope, ScopedStorage, SealOutcome, StorageBackend,
    StorageError, StreamEntry, StreamInfo, StreamStore,
};

const LEGACY: &str = "16:session_journal/";
const LONG: u64 = 1000;
const BATCH: usize = 100;

/// A stream store that fails an append once its allowance runs out.
#[derive(Debug)]
struct Flaky {
    inner: Arc<dyn StreamStore>,
    appends_left: AtomicUsize,
}

impl Flaky {
    fn spend(&self) -> Result<()> {
        let left = self.appends_left.load(Ordering::SeqCst);
        if left == 0 {
            return Err(StorageError::backend("injected append failure"));
        }
        self.appends_left.store(left - 1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl StreamStore for Flaky {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.spend()?;
        self.inner.append(stream, value).await
    }
    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.spend()?;
        self.inner.append_batch(stream, values).await
    }
    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        self.spend()?;
        self.inner
            .append_batch_if_offset(stream, expected_next_offset, values)
            .await
    }
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        self.inner.read_window(stream, from, limit).await
    }
    async fn len(&self, stream: &str) -> Result<u64> {
        self.inner.len(stream).await
    }
    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        self.inner.info(stream).await
    }
    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        self.inner.truncate_before(stream, offset).await
    }
    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        self.inner.delete_stream(stream).await
    }
    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        self.inner.seal(stream).await
    }
    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.streams(prefix).await
    }
}

fn flaky_storage() -> (ScopedStorage, Arc<Flaky>) {
    let real = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let flaky = Arc::new(Flaky {
        inner: Arc::clone(real.streams()),
        appends_left: AtomicUsize::new(usize::MAX),
    });
    let storage = ScopedStorage::new(
        Scope::local(),
        "flaky",
        Arc::clone(real.documents()),
        flaky.clone(),
        Arc::clone(real.blobs()),
    );
    (storage, flaky)
}

fn journal_step() -> Step {
    Step::stream("journal_v2", "harness.journal", 1, 2, |name, entries| {
        let Some(id) = name.strip_prefix(LEGACY) else {
            return Err(StorageError::invalid_input("unexpected source"));
        };
        Ok(Some((
            format!("harness.journal/{id}"),
            entries
                .into_iter()
                .map(|mut entry| {
                    for field in ["run_id", "root_run_id", "event_id"] {
                        entry.as_object_mut().unwrap().remove(field);
                    }
                    entry["v"] = json!(2);
                    entry
                })
                .collect(),
        )))
    })
    .source_prefix(LEGACY)
}

fn registry(step: Step) -> Registry {
    Registry::compose([Schema::new(Owner::new("harness").unwrap())
        .stream("harness.journal", ScopeClass::Tenant)
        .step(step)])
    .unwrap()
}

fn legacy_entry(run: &str, n: u64) -> Value {
    json!({"run_id": run, "root_run_id": run, "event_id": n, "n": n})
}

fn migrated_entry(n: u64) -> Value {
    json!({"n": n, "v": 2})
}

async fn seed(storage: &ScopedStorage, run: &str, count: u64) {
    let values = (0..count).map(|n| legacy_entry(run, n)).collect();
    storage
        .streams()
        .append_batch(&format!("{LEGACY}{run}"), values)
        .await
        .unwrap();
}

async fn read_all(storage: &ScopedStorage, stream: &str) -> Vec<Value> {
    let len = storage.streams().len(stream).await.unwrap();
    let entries = storage
        .streams()
        .read_window(stream, 0, usize::try_from(len).unwrap() + 1)
        .await
        .unwrap();
    for (expected, entry) in (0..).zip(&entries) {
        assert_eq!(entry.offset, expected, "{stream} has gaps or repeats");
    }
    entries.into_iter().map(|entry| entry.value).collect()
}

/// Sealed state as the stream backend reports it, not as a blob handle does.
async fn is_sealed(storage: &ScopedStorage, stream: &str) -> bool {
    storage
        .streams()
        .info(stream)
        .await
        .unwrap()
        .is_some_and(|info| info.sealed_ms.is_some())
}

#[tokio::test]
async fn resumes_a_stream_migration_after_a_mid_stream_failure() {
    let (storage, flaky) = flaky_storage();
    seed(&storage, "a", 5).await;
    seed(&storage, "b", LONG).await;
    seed(&storage, "c", 3).await;
    storage.streams().seal(&format!("{LEGACY}a")).await.unwrap();
    let registry = registry(journal_step());
    let options = MigrateOptions::new().batch_size(BATCH);

    // `a` takes one append; `b` fails on its fourth batch.
    flaky.appends_left.store(4, Ordering::SeqCst);
    let error = registry.migrate(&storage, &options).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(
        registry.check_ready("harness.journal").unwrap_err().kind(),
        ErrorKind::Unavailable
    );
    assert_eq!(
        storage.streams().len("harness.journal/b").await.unwrap(),
        300,
        "three batches of b landed before the failure"
    );
    assert_eq!(
        storage.streams().streams(LEGACY).await.unwrap().len(),
        2,
        "a is already gone"
    );

    flaky.appends_left.store(usize::MAX, Ordering::SeqCst);
    let report = registry.migrate(&storage, &options).await.unwrap();
    assert_eq!(report.ran.len(), 1);
    registry.check_ready("harness.journal").unwrap();

    assert_eq!(storage.streams().streams(LEGACY).await.unwrap().len(), 0);
    assert_eq!(
        storage.streams().streams("harness.journal").await.unwrap(),
        [
            "harness.journal/a",
            "harness.journal/b",
            "harness.journal/c"
        ]
    );
    assert_eq!(
        read_all(&storage, "harness.journal/a").await,
        (0..5).map(migrated_entry).collect::<Vec<_>>()
    );
    assert_eq!(
        read_all(&storage, "harness.journal/b").await,
        (0..LONG).map(migrated_entry).collect::<Vec<_>>(),
        "no entry is duplicated or lost across the failure"
    );
    assert_eq!(read_all(&storage, "harness.journal/c").await.len(), 3);
    assert!(is_sealed(&storage, "harness.journal/a").await);
    assert!(!is_sealed(&storage, "harness.journal/b").await);
    assert!(!is_sealed(&storage, "harness.journal/c").await);
    assert!(
        !is_sealed(&storage, &format!("{LEGACY}a")).await,
        "the source's blob went with it"
    );

    let again = registry.migrate(&storage, &options).await.unwrap();
    assert_eq!(again.ran.len(), 0);
    assert_eq!(again.already_done, 1);
    assert_eq!(read_all(&storage, "harness.journal/b").await.len(), 1000);
}

#[tokio::test]
async fn an_interrupted_run_equals_an_uninterrupted_one() {
    let (interrupted, flaky) = flaky_storage();
    let (clean, _) = flaky_storage();
    for storage in [&interrupted, &clean] {
        seed(storage, "x", 250).await;
        seed(storage, "y", 7).await;
    }
    let registry = registry(journal_step());
    let options = MigrateOptions::new().batch_size(40);
    flaky.appends_left.store(3, Ordering::SeqCst);
    registry.migrate(&interrupted, &options).await.unwrap_err();
    flaky.appends_left.store(usize::MAX, Ordering::SeqCst);
    registry.migrate(&interrupted, &options).await.unwrap();
    registry.migrate(&clean, &options).await.unwrap();
    for stream in ["harness.journal/x", "harness.journal/y"] {
        assert_eq!(
            read_all(&interrupted, stream).await,
            read_all(&clean, stream).await
        );
    }
}

#[tokio::test]
async fn drops_streams_the_step_maps_to_none() {
    let (storage, _) = flaky_storage();
    seed(&storage, "gone", 3).await;
    seed(&storage, "kept", 3).await;
    let step = Step::stream("journal_v2", "harness.journal", 1, 2, |name, entries| {
        Ok(name
            .ends_with("kept")
            .then(|| ("harness.journal/kept".to_owned(), entries)))
    })
    .source_prefix(LEGACY);
    registry(step)
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap();
    assert_eq!(storage.streams().streams(LEGACY).await.unwrap().len(), 0);
    assert_eq!(
        storage.streams().streams("harness.journal").await.unwrap(),
        ["harness.journal/kept"]
    );
}

#[tokio::test]
async fn renumbers_a_truncated_source_from_zero() {
    let (storage, _) = flaky_storage();
    seed(&storage, "t", 10).await;
    storage
        .streams()
        .truncate_before(&format!("{LEGACY}t"), 4)
        .await
        .unwrap();
    registry(journal_step())
        .migrate(&storage, &MigrateOptions::new().batch_size(3))
        .await
        .unwrap();
    assert_eq!(
        read_all(&storage, "harness.journal/t").await,
        (4..10).map(migrated_entry).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn refuses_a_target_outside_the_item_and_keeps_the_source() {
    let (storage, _) = flaky_storage();
    seed(&storage, "r", 2).await;
    let step = Step::stream("journal_v2", "harness.journal", 1, 2, |_, entries| {
        Ok(Some(("other.journal/r".to_owned(), entries)))
    })
    .source_prefix(LEGACY);
    let error = registry(step)
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        error.message().contains("other.journal/r"),
        "{}",
        error.message()
    );
    assert_eq!(
        storage.streams().len(&format!("{LEGACY}r")).await.unwrap(),
        2
    );
}

#[tokio::test]
async fn refuses_to_append_to_a_target_it_did_not_start() {
    let (storage, _) = flaky_storage();
    seed(&storage, "r", 2).await;
    storage
        .streams()
        .append("harness.journal/r", json!({"stray": true}))
        .await
        .unwrap();
    let error = registry(journal_step())
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(
        storage.streams().len(&format!("{LEGACY}r")).await.unwrap(),
        2
    );
    assert_eq!(storage.streams().len("harness.journal/r").await.unwrap(), 1);
}

/// A stream store that plays a competing migrator: the first time the runner
/// reads the length of a migrated stream, the rival appends `rival` to it.
#[derive(Debug)]
struct Rival {
    inner: Arc<dyn StreamStore>,
    rival: std::sync::Mutex<Option<Vec<Value>>>,
}

#[async_trait]
impl StreamStore for Rival {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.inner.append(stream, value).await
    }
    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.inner.append_batch(stream, values).await
    }
    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        self.inner
            .append_batch_if_offset(stream, expected_next_offset, values)
            .await
    }
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        self.inner.read_window(stream, from, limit).await
    }
    async fn len(&self, stream: &str) -> Result<u64> {
        let seen = self.inner.len(stream).await?;
        let values = if stream.starts_with("harness.journal/") {
            self.rival.lock().unwrap().take()
        } else {
            None
        };
        if let Some(values) = values {
            self.inner.append_batch(stream, values).await?;
        }
        Ok(seen)
    }
    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        self.inner.info(stream).await
    }
    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        self.inner.truncate_before(stream, offset).await
    }
    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        self.inner.delete_stream(stream).await
    }
    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        self.inner.seal(stream).await
    }
    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.streams(prefix).await
    }
}

#[tokio::test]
async fn a_competing_migrator_is_refused_instead_of_duplicating_entries() {
    let real = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let rival = Arc::new(Rival {
        inner: Arc::clone(real.streams()),
        rival: std::sync::Mutex::new(Some((0..5).map(migrated_entry).collect())),
    });
    let storage = ScopedStorage::new(
        Scope::local(),
        "rival",
        Arc::clone(real.documents()),
        rival,
        Arc::clone(real.blobs()),
    );
    seed(&storage, "a", 5).await;

    let error = registry(journal_step())
        .migrate(&storage, &MigrateOptions::new().batch_size(BATCH))
        .await
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(
        read_all(&storage, "harness.journal/a").await,
        (0..5).map(migrated_entry).collect::<Vec<_>>(),
        "only the rival's copy is in the target"
    );
    assert_eq!(
        storage.streams().len(&format!("{LEGACY}a")).await.unwrap(),
        5,
        "the source stays until a run owns the target"
    );
}

#[tokio::test]
async fn sealed_state_comes_from_the_stream_backend_not_the_blob_handle() {
    // A composite keeps sealed-stream blobs with the records, so the blob
    // handle it exposes never sees them.
    let real = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let elsewhere = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let storage = ScopedStorage::new(
        Scope::local(),
        "composite",
        Arc::clone(real.documents()),
        Arc::clone(real.streams()),
        Arc::clone(elsewhere.blobs()),
    );
    seed(&storage, "a", 5).await;
    storage.streams().seal(&format!("{LEGACY}a")).await.unwrap();

    registry(journal_step())
        .migrate(&storage, &MigrateOptions::new().batch_size(BATCH))
        .await
        .unwrap();

    assert!(is_sealed(&storage, "harness.journal/a").await);
    assert_eq!(read_all(&storage, "harness.journal/a").await.len(), 5);
}

/// Uses the existing failing append fixture to exercise the external async
/// mapping site with output counts different from source counts.
#[tokio::test]
async fn external_expanding_stream_resumes_by_emitted_offset() {
    use crate::schema::{
        BlobTransformOutput, BlobTransformRequest, BoxFuture, DocumentsTransformRequest,
        SchemaDeclaration, StepDeclaration, StreamTransformOutput, StreamTransformRequest,
        TargetRecord, TransformDescriptor, TransformExecutor, TransformKind,
    };
    struct Expand;
    impl TransformExecutor for Expand {
        fn documents<'a>(
            &'a self,
            _: &'a Owner,
            _: DocumentsTransformRequest,
        ) -> BoxFuture<'a, Result<Vec<TargetRecord>>> {
            unreachable!()
        }
        fn blob<'a>(
            &'a self,
            _: &'a Owner,
            _: BlobTransformRequest,
        ) -> BoxFuture<'a, Result<Option<BlobTransformOutput>>> {
            unreachable!()
        }
        fn stream<'a>(
            &'a self,
            _: &'a Owner,
            request: StreamTransformRequest,
        ) -> BoxFuture<'a, Result<Option<StreamTransformOutput>>> {
            Box::pin(async move {
                tokio::task::yield_now().await;
                Ok(Some(StreamTransformOutput {
                    target: request.source.replace(LEGACY, "harness.journal/"),
                    entries: request
                        .entries
                        .into_iter()
                        .flat_map(|v| [v.clone(), v])
                        .collect(),
                }))
            })
        }
    }
    fn external() -> crate::schema::ComposedRegistry {
        Registry::compose_with_declarations(
            [],
            [SchemaDeclaration {
                owner: Owner::new("harness").unwrap(),
                items: Schema::new(Owner::new("harness").unwrap())
                    .stream("harness.journal", ScopeClass::Tenant)
                    .items,
                steps: vec![StepDeclaration {
                    id: "expand".into(),
                    item: "harness.journal".into(),
                    from: 1,
                    to: 2,
                    source: Some(LEGACY.into()),
                    transform: TransformDescriptor {
                        operation: "expand".into(),
                        version: 1,
                        kind: TransformKind::Stream,
                    },
                }],
            }],
        )
        .unwrap()
    }
    let (storage, flaky) = flaky_storage();
    seed(&storage, "a", 5).await;
    storage.streams().seal(&format!("{LEGACY}a")).await.unwrap();
    let options = MigrateOptions::new().batch_size(2);
    flaky.appends_left.store(3, Ordering::SeqCst);
    let registry = external();
    assert!(
        registry
            .migrate_with_executor(&storage, &options, &Expand)
            .await
            .is_err()
    );
    assert_eq!(storage.streams().len("harness.journal/a").await.unwrap(), 6);
    assert_eq!(
        storage.streams().len(&format!("{LEGACY}a")).await.unwrap(),
        5
    );
    assert!(registry.check_ready("harness.journal").is_err());
    drop(registry);
    flaky.appends_left.store(usize::MAX, Ordering::SeqCst);
    let registry = external();
    assert_eq!(
        registry
            .migrate_with_executor(&storage, &options, &Expand)
            .await
            .unwrap()
            .ran[0]
            .written,
        10
    );
    assert_eq!(read_all(&storage, "harness.journal/a").await.len(), 10);
    assert!(is_sealed(&storage, "harness.journal/a").await);
    assert_eq!(storage.streams().streams(LEGACY).await.unwrap().len(), 0);
}
