//! Document migration checks: a two-step chain that runs across several
//! batches, a crash part way through, a resume that writes nothing twice, and
//! a re-run that does nothing.
//!
//! The runner is driver-independent ([`Registry::migrate`]); what the suite
//! pins down is what it asks of a driver: keyset paging, absent-only creates,
//! version-guarded in-place rewrites, schema versions on records, and the
//! progress document in `tsd.migrations` surviving the crash.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};

use super::{empty, fails, ok, unique};
use crate::backend::{ScopedStorage, StorageBackend};
use crate::capabilities::Capability;
use crate::document::{DocumentStoreExt, Precondition, Query, WriteOptions};
use crate::error::{ErrorKind, StorageError};
use crate::key::Key;
use crate::schema::{
    MIGRATIONS_COLLECTION, MigrateOptions, Owner, Registry, Schema, ScopeClass, Step, TargetRecord,
};
use crate::scope::Scope;

/// Legacy records to migrate.
const TOTAL: usize = 25;

/// [`TOTAL`] as the runner counts it.
const TOTAL_U64: u64 = 25;

/// Records per batch: three batches, the last one short.
const BATCH: usize = 10;

/// The call of the first step that fails, once.
const FAIL_AT: usize = 2;

/// The names one run migrates, leaked so the steps can name them `'static`.
/// A run leaks a few short strings; the suite runs a handful of times per
/// process.
struct Names {
    owner: &'static str,
    legacy: &'static str,
    orders: &'static str,
}

fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

impl Names {
    fn new() -> Self {
        // An owner is `[a-z][a-z0-9_]{1,23}`, so derive a short one from the
        // run-unique name rather than using it whole.
        let tail: String = unique("m")
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .rev()
            .take(14)
            .collect::<String>()
            .to_ascii_lowercase();
        let owner = leak(format!("m{tail}"));
        Self {
            owner,
            legacy: leak(unique("mig_legacy")),
            orders: leak(format!("{owner}.orders")),
        }
    }
}

/// Calls of each step, so the checks can tell resumed work from repeated work.
#[derive(Default)]
struct Calls {
    import: AtomicUsize,
    flatten: AtomicUsize,
    failed: AtomicUsize,
}

/// The chain: `legacy` rows become `n-<id>` orders at version 2, then every
/// order is rewritten in place at version 3. The import fails once on call
/// [`FAIL_AT`] when `crash` is set.
fn registry(names: &Names, calls: &Arc<Calls>, crash: bool) -> Registry {
    let import_calls = Arc::clone(calls);
    let flatten_calls = Arc::clone(calls);
    let orders = names.orders;
    let import = Step::new("import", orders, 1, 2, move |batch| {
        let call = import_calls.import.fetch_add(1, Ordering::SeqCst) + 1;
        if crash && call == FAIL_AT && import_calls.failed.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(StorageError::backend("injected crash"));
        }
        Ok(batch
            .into_iter()
            .map(|record| {
                let mut body = record.body;
                body["step"] = json!(2);
                TargetRecord::new(orders, format!("n-{}", record.id), body)
            })
            .collect())
    })
    .source(names.legacy);
    let flatten = Step::new("flatten", orders, 2, 3, move |batch| {
        flatten_calls.flatten.fetch_add(1, Ordering::SeqCst);
        Ok(batch
            .into_iter()
            .map(|record| {
                let mut body = record.body;
                body["step"] = json!(3);
                TargetRecord::new(orders, record.id, body)
            })
            .collect())
    });
    let schema = Schema::new(Owner::new(names.owner).expect("a valid owner"))
        .collection(orders, 3, ScopeClass::Tenant)
        .step(import)
        .step(flatten);
    Registry::compose([schema]).expect("a valid registry")
}

fn options() -> MigrateOptions {
    MigrateOptions::new().batch_size(BATCH)
}

fn legacy_id(n: usize) -> String {
    format!("o{n:03}")
}

/// Every record of `collection`: id, body, schema version and document version.
async fn snapshot(storage: &ScopedStorage, collection: &str) -> Vec<(String, Value, u32, u64)> {
    ok(
        storage
            .documents()
            .query_all(collection, &Query::all())
            .await,
        "migration snapshot",
    )
    .into_iter()
    .map(|found| (found.id, found.doc, found.meta.schema, found.version.0))
    .collect()
}

/// The `{written, done}` of the first step's progress document.
async fn progress(storage: &ScopedStorage, names: &Names, step: &str) -> Option<Value> {
    let key = ok(Key::of([names.owner, names.orders, step]), "progress key");
    ok(
        storage
            .documents()
            .get(MIGRATIONS_COLLECTION, key.as_str())
            .await,
        "progress read",
    )
    .map(|found| found.doc)
}

pub(super) async fn run(backend: &dyn StorageBackend) {
    // The runner reads and writes schema versions, which a driver without
    // record metadata cannot hold.
    if !backend.capabilities().contains(Capability::RecordMetadata) {
        return;
    }
    let storage = ok(backend.for_scope(&Scope::local()), "migration scope");
    let names = Names::new();
    let calls = Arc::new(Calls::default());
    seed(&storage, &names).await;
    crash(backend, &storage, &names, &calls).await;
    resume(&storage, &names, &calls).await;
    rerun(&storage, &names, &calls).await;
}

/// The second batch of the first step fails: the first batch's progress
/// survives, the source is untouched and the collection stays closed.
async fn crash(
    backend: &dyn StorageBackend,
    storage: &ScopedStorage,
    names: &Names,
    calls: &Arc<Calls>,
) {
    let flaky = registry(names, calls, true);
    ok(flaky.prepare(backend).await, "migration prepare");
    let error = match flaky.migrate(storage, &options()).await {
        Ok(report) => panic!("migration: the injected crash did not stop it: {report:?}"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Backend, "migration crash: {error}");
    let recorded = progress(storage, names, "import")
        .await
        .expect("migration: the first batch's progress survives the crash");
    assert_eq!(
        recorded["written"],
        json!(BATCH),
        "migration: crash progress"
    );
    assert_eq!(recorded["done"], json!(false), "migration: crash progress");
    assert_eq!(
        snapshot(storage, names.orders).await.len(),
        BATCH,
        "migration: only the finished batch landed"
    );
    assert_eq!(
        ok(
            storage
                .documents()
                .count(names.legacy, &crate::Filter::All)
                .await,
            "migration legacy count"
        ),
        TOTAL_U64,
        "migration: the source is untouched until the step is done"
    );
    assert!(
        flaky.check_ready_in(storage.scope(), names.orders).is_err(),
        "migration: a half-migrated collection is not ready"
    );
}

/// A fresh registry finishes the chain from the recorded progress, accepting
/// an output stranded between a batch's writes and its progress record, and
/// writing no record twice.
async fn resume(storage: &ScopedStorage, names: &Names, calls: &Arc<Calls>) {
    // The window between a batch's writes and its progress record: the second
    // batch's first output exists already, exactly as the step produces it.
    ok(
        storage
            .documents()
            .put_with(
                names.orders,
                &format!("n-{}", legacy_id(BATCH)),
                json!({"n": BATCH, "step": 2}),
                Precondition::Absent,
                WriteOptions::new().schema(2),
            )
            .await,
        "migration stranded output",
    );
    let before = calls.import.load(Ordering::SeqCst);
    let resumed = registry(names, calls, true);
    let report = ok(
        resumed.migrate(storage, &options()).await,
        "migration resume",
    );
    assert_eq!(
        calls.import.load(Ordering::SeqCst) - before,
        TOTAL.div_ceil(BATCH) - 1,
        "migration: the resume starts after the recorded batch"
    );
    let ran: Vec<_> = report
        .ran
        .iter()
        .map(|step| (step.step.as_str(), step.written))
        .collect();
    assert_eq!(
        ran,
        [("import", TOTAL_U64), ("flatten", TOTAL_U64)],
        "migration: the resume reports every record written once"
    );
    migrated(storage, names).await;
    resumed
        .check_ready_in(storage.scope(), names.orders)
        .unwrap_or_else(|error| panic!("migration: not ready after the resume: {error}"));
    empty(
        &snapshot(storage, names.legacy).await,
        "migration legacy rows after the step",
    );
}

/// A finished migration is a no-op for any process that runs it again.
async fn rerun(storage: &ScopedStorage, names: &Names, calls: &Arc<Calls>) {
    let after = snapshot(storage, names.orders).await;
    let flattened = calls.flatten.load(Ordering::SeqCst);
    let imported = calls.import.load(Ordering::SeqCst);
    let again = registry(names, calls, false);
    let report = ok(again.migrate(storage, &options()).await, "migration re-run");
    assert!(report.ran.is_empty(), "migration: a re-run ran {report:?}");
    assert_eq!(
        report.already_done, 2,
        "migration: a re-run skips both steps"
    );
    assert_eq!(calls.import.load(Ordering::SeqCst), imported);
    assert_eq!(calls.flatten.load(Ordering::SeqCst), flattened);
    assert_eq!(
        snapshot(storage, names.orders).await,
        after,
        "migration: a re-run changed records"
    );
    assert_eq!(
        progress(storage, names, "flatten").await.expect("progress")["done"],
        json!(true)
    );
    fails(
        again.check_ready_in(
            &Scope::new("conformance-unmigrated").expect("scope"),
            names.orders,
        ),
        ErrorKind::Unavailable,
        "migration: readiness is per scope",
    );
}

/// Legacy rows at schema version 1.
async fn seed(storage: &ScopedStorage, names: &Names) {
    for n in 0..TOTAL {
        ok(
            storage
                .documents()
                .put_with(
                    names.legacy,
                    &legacy_id(n),
                    json!({"n": n}),
                    Precondition::Absent,
                    WriteOptions::new().schema(1),
                )
                .await,
            "migration seed",
        );
    }
}

/// Every legacy row arrived once, through both steps: the right ids and
/// bodies, schema version 3, and version 2 (the create, then the in-place
/// rewrite), whether the row was written before or after the crash.
async fn migrated(storage: &ScopedStorage, names: &Names) {
    let found = snapshot(storage, names.orders).await;
    assert_eq!(
        found.len(),
        TOTAL,
        "migration: wrong record count (duplicates or losses)"
    );
    for (n, (id, body, schema, version)) in found.iter().enumerate() {
        assert_eq!(id, &format!("n-{}", legacy_id(n)), "migration: ids");
        assert_eq!(body, &json!({"n": n, "step": 3}), "migration: body of {id}");
        assert_eq!(*schema, 3, "migration: schema of {id}");
        assert_eq!(
            *version, 2,
            "migration: {id} was written more or fewer times than create plus rewrite"
        );
    }
}
