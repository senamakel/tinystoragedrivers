//! Layouts, persistence, the clock, and handle construction.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, Precondition};

use super::*;

#[test]
fn picks_the_layout_from_the_extension() {
    assert!(is_file_path(Path::new("/x/state.db")));
    assert!(is_file_path(Path::new("/x/state.SQLITE")));
    assert!(!is_file_path(Path::new("/x/state")));
    assert!(!is_file_path(Path::new("/x/state.json")));
}

#[tokio::test]
async fn a_directory_path_maps_to_the_storage_file_inside_it() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    assert!(storage.path().ends_with(ROOT_FILE));
    assert!(dir.path().join(ROOT_FILE).exists());
    let missing = dir.path().join("not").join("yet");
    let created = SqliteStorage::open(&missing).unwrap();
    assert!(created.path().ends_with(ROOT_FILE));
    assert!(missing.join(ROOT_FILE).exists());
    assert_eq!(storage.driver(), "sqlite");
    assert_eq!(
        storage.capabilities(),
        Capabilities::all()
            .without(tinystoragedrivers_core::Capability::Watch)
            .without(tinystoragedrivers_core::Capability::PresignedUrls)
            .without(tinystoragedrivers_core::Capability::StreamingBlobs)
    );
    assert!(format!("{storage:?}").contains("storage.db"));
}

#[tokio::test]
async fn a_file_path_is_the_database_and_creates_no_storage_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("one.sqlite");
    let storage = SqliteStorage::open(&file).unwrap();
    storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .put("runs", "r1", json!({}), Precondition::None)
        .await
        .unwrap();
    let tables: i64 = storage
        .native()
        .with_connection(|conn| {
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'docs'",
                [],
                |row| row.get(0),
            )
        })
        .await
        .unwrap();
    assert_eq!(tables, 1);
    assert!(file.exists());
    assert!(!dir.path().join(ROOT_FILE).exists());
}

#[tokio::test]
async fn data_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    {
        let storage = SqliteStorage::open(dir.path()).unwrap();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .put("c", "a", json!({"n": 1}), Precondition::None)
            .await
            .unwrap();
        scoped
            .streams()
            .append("log", json!("first"))
            .await
            .unwrap();
        scoped.blobs().put("k", vec![1, 2, 3], None).await.unwrap();
    }
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    let doc = scoped.documents().get("c", "a").await.unwrap().unwrap();
    assert_eq!(doc.doc, json!({"n": 1}));
    assert_eq!(scoped.streams().len("log").await.unwrap(), 1);
    assert_eq!(
        scoped.blobs().get("k").await.unwrap().unwrap().bytes,
        [1, 2, 3]
    );
}

#[tokio::test]
async fn expiry_follows_the_injected_clock() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let reading = Arc::clone(&now);
    let storage = SqliteStorage::open(dir.path())
        .unwrap()
        .with_clock(Arc::new(move || reading.load(Ordering::SeqCst)));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("leases").ttl("until").searchable(["t"]))
        .await
        .unwrap();
    docs.put(
        "leases",
        "l",
        json!({"until": 2_000, "t": "lease"}),
        Precondition::None,
    )
    .await
    .unwrap();
    assert_eq!(docs.search("leases", "lease", 5).await.unwrap().len(), 1);
    now.store(2_000, Ordering::SeqCst);
    assert!(docs.get("leases", "l").await.unwrap().is_none());
    assert_eq!(docs.search("leases", "lease", 5).await.unwrap().len(), 0);
    assert!(system_clock() > 0);
}

#[tokio::test]
async fn reopened_target_observes_guard_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let root = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("alice").unwrap();
    let guard_scope = Scope::new("cluster").unwrap();
    let guard = root.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put(
            "leases",
            "l",
            serde_json::json!({"epoch": 1}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    let target = SqliteStorage::open(dir.path()).unwrap();
    let fence = Fence::epoch(guard_scope.clone(), "leases", "l", "epoch", 1);
    let held = target.for_scope_fenced(&scope, &fence).unwrap();
    held.documents()
        .put(
            "docs",
            "d",
            serde_json::json!({"value": 1}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    drop(guard);
    let takeover = SqliteStorage::open(dir.path())
        .unwrap()
        .for_scope(&guard_scope)
        .unwrap();
    takeover
        .documents()
        .put(
            "leases",
            "l",
            serde_json::json!({"epoch": 2}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        held.documents()
            .put(
                "docs",
                "d",
                serde_json::json!({"value": 2}),
                tinystoragedrivers_core::Precondition::None
            )
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Fenced
    );
    let plain = target.for_scope(&scope).unwrap();
    assert_eq!(
        plain
            .documents()
            .get("docs", "d")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"value": 1})
    );
    assert!(
        target
            .for_scope(&Scope::new("bob").unwrap())
            .unwrap()
            .documents()
            .get("docs", "d")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn relative_paths_keep_the_opened_root_after_working_directory_changes() {
    const CHILD: &str = "TSD_RELATIVE_PATH_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let dir = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "storage::tests::relative_paths_keep_the_opened_root_after_working_directory_changes", "--nocapture"])
            .env(CHILD, "1")
            .current_dir(dir.path())
            .output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let root = std::env::current_dir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for path in ["", "relative_directory", "relative_file.db"] {
            std::env::set_current_dir(&root).unwrap();
            let backend = SqliteStorage::open(path).unwrap();
            let scope = Scope::local();
            let fence = Fence::epoch(scope.clone(), "leases", "holder", "epoch", 7);
            backend
                .for_scope(&scope)
                .unwrap()
                .documents()
                .put("leases", "holder", json!({"epoch": 7}), Precondition::None)
                .await
                .unwrap();
            let moved = root.join(format!("moved-{path}"));
            std::fs::create_dir(&moved).unwrap();
            std::env::set_current_dir(&moved).unwrap();
            let bound = backend.for_scope_fenced(&scope, &fence).unwrap();
            bound
                .documents()
                .put("records", "one", json!({"value": 1}), Precondition::None)
                .await
                .unwrap();
            assert!(
                !moved
                    .join(if path.is_empty() { ROOT_FILE } else { path })
                    .exists(),
                "binding must not recreate the relative layout under a new cwd"
            );
        }
    });
}

#[tokio::test]
async fn scope_purge_persists_revocation_across_reopen() {
    use tinystoragedrivers_core::{
        ErrorKind, PurgeMode, PurgeRequest, PurgeStatus, ScopeGeneration,
    };
    for filename in ["directory", "single.db"] {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().join(filename);
        let storage = SqliteStorage::open(&location).unwrap();
        let scope = Scope::new("reset").unwrap();
        let other = Scope::new("keep").unwrap();
        {
            let backend = Arc::new(storage.clone()) as Arc<dyn StorageBackend>;
            let target = backend.for_scope(&scope).unwrap();
            let keep = backend.for_scope(&other).unwrap();
            for ports in [&target, &keep] {
                ports
                    .documents()
                    .put("records", "one", json!({}), Precondition::None)
                    .await
                    .unwrap();
                ports.streams().append("events", json!({})).await.unwrap();
                ports.blobs().put("blob", vec![1], None).await.unwrap();
            }
            let request = PurgeRequest {
                operation_id: "reset_one".into(),
                mode: PurgeMode::Atomic,
            };
            let receipt = target.purge(&request).await.unwrap();
            assert_eq!(receipt.status, PurgeStatus::Complete);
            assert_eq!(receipt.generation, ScopeGeneration(1));
            assert_eq!(target.purge(&request).await.unwrap(), receipt);
            let verified = backend
                .for_scope_generation(&scope, receipt.generation)
                .unwrap();
            assert!(
                verified
                    .documents()
                    .get("records", "one")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(verified.streams().len("events").await.unwrap(), 0);
            assert!(verified.blobs().get("blob").await.unwrap().is_none());
            assert!(
                keep.documents()
                    .get("records", "one")
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(keep.streams().len("events").await.unwrap(), 1);
            assert!(keep.blobs().get("blob").await.unwrap().is_some());
            assert_eq!(
                target
                    .documents()
                    .put("records", "late", json!({}), Precondition::None)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::ScopeGenerationRejected
            );
            assert_eq!(
                target
                    .streams()
                    .append("events", json!({}))
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::ScopeGenerationRejected
            );
            assert_eq!(
                target
                    .blobs()
                    .put("late", vec![], None)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::ScopeGenerationRejected
            );
        }
        drop(storage);
        verify_reopened_generation(&location, &scope).await;
    }
}

async fn verify_reopened_generation(location: &Path, scope: &Scope) {
    use tinystoragedrivers_core::{ErrorKind, ScopeGeneration};
    let reopened = SqliteStorage::open(location).unwrap();
    let old = reopened.for_scope(scope).unwrap();
    assert_eq!(
        old.streams()
            .append("events", json!({}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    let fresh = reopened
        .for_scope_generation(scope, ScopeGeneration(1))
        .unwrap();
    assert_eq!(
        fresh.streams().append("events", json!({})).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn failed_scope_purge_rolls_back_data_and_generation() {
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest, ScopeGeneration};
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("reset").unwrap();
    let target = storage.for_scope(&scope).unwrap();
    target
        .documents()
        .put("records", "one", json!({}), Precondition::None)
        .await
        .unwrap();
    target.streams().append("events", json!({})).await.unwrap();
    target.blobs().put("blob", vec![1], None).await.unwrap();
    let trigger = format!(
        "CREATE TRIGGER fail_purge BEFORE DELETE ON {} BEGIN SELECT RAISE(ABORT, 'blocked'); END",
        crate::sql::ident(&storage.tables.blobs)
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute_batch(&trigger).unwrap();
            Ok(())
        })
        .unwrap();
    let request = PurgeRequest {
        operation_id: "failed_reset".into(),
        mode: PurgeMode::Atomic,
    };
    assert!(target.purge(&request).await.is_err());
    assert!(
        target
            .purge_status(&request.operation_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        target
            .documents()
            .get("records", "one")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(target.streams().len("events").await.unwrap(), 1);
    assert!(target.blobs().get("blob").await.unwrap().is_some());
    assert_eq!(
        target.streams().append("events", json!({})).await.unwrap(),
        1
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute_batch("DROP TRIGGER fail_purge").unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(
        target.purge(&request).await.unwrap().generation,
        ScopeGeneration(1)
    );
}

#[tokio::test]
async fn historical_reset_receipts_survive_reopen_in_each_path_layout() {
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest, ScopeGeneration};
    for filename in ["directory", "single.db"] {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().join(filename);
        let scope = Scope::new("reset_history").unwrap();
        let first = PurgeRequest {
            operation_id: "first".into(),
            mode: PurgeMode::Atomic,
        };
        let second = PurgeRequest {
            operation_id: "second".into(),
            mode: PurgeMode::Atomic,
        };
        let original_receipts = {
            let storage = SqliteStorage::open(&location).unwrap();
            let mut receipts = Vec::new();
            {
                let backend = Arc::new(storage.clone()) as Arc<dyn StorageBackend>;
                let original = backend.for_scope(&scope).unwrap();
                let receipt = original.purge(&first).await.unwrap();
                let next = backend
                    .for_scope_generation(&scope, receipt.generation)
                    .unwrap();
                let current_receipt = next.purge(&second).await.unwrap();
                let current = backend
                    .for_scope_generation(&scope, current_receipt.generation)
                    .unwrap();
                current
                    .documents()
                    .put(
                        "records",
                        "replacement",
                        json!({"keep":true}),
                        Precondition::Absent,
                    )
                    .await
                    .unwrap();
                current
                    .streams()
                    .append("events", json!({"keep":true}))
                    .await
                    .unwrap();
                current
                    .blobs()
                    .put("replacement", vec![7], None)
                    .await
                    .unwrap();
                receipts.push(receipt);
            }
            receipts
        };
        let reopened = SqliteStorage::open(&location).unwrap();
        for (backend, receipt) in [Arc::new(reopened) as Arc<dyn StorageBackend>]
            .into_iter()
            .zip(original_receipts)
        {
            let current = backend
                .for_scope_generation(&scope, ScopeGeneration(2))
                .unwrap();
            assert_eq!(
                current.purge_status(&first.operation_id).await.unwrap(),
                Some(receipt.clone())
            );
            assert_eq!(current.purge(&first).await.unwrap(), receipt);
            assert!(
                current
                    .documents()
                    .get("records", "replacement")
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(current.streams().len("events").await.unwrap(), 1);
            assert_eq!(
                current
                    .blobs()
                    .get("replacement")
                    .await
                    .unwrap()
                    .unwrap()
                    .bytes,
                vec![7]
            );
        }
    }
}

#[tokio::test]
async fn purge_refuses_a_fence_guard_inside_the_scope_it_would_erase() {
    use tinystoragedrivers_core::{ErrorKind, Fence, PurgeMode, PurgeRequest};

    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("self-guarded-purge").unwrap();
    let plain = storage.for_scope(&scope).unwrap();
    plain
        .documents()
        .put(
            "records",
            "keep",
            json!({"alive": true}),
            Precondition::None,
        )
        .await
        .unwrap();
    let fence = Fence::epoch(scope.clone(), "leases", "holder", "epoch", 1);
    let fenced = storage.for_scope_fenced(&scope, &fence).unwrap();

    assert_eq!(
        fenced
            .purge(&PurgeRequest {
                operation_id: "self-guarded-reset".into(),
                mode: PurgeMode::Atomic,
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(
        plain
            .documents()
            .get("records", "keep")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn purge_refuses_generation_overflow_without_erasing_current_data() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest, ScopeGeneration};

    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("overflow-reset").unwrap();
    let base = storage.for_scope(&scope).unwrap();
    storage
        .db
        .run_now(|conn| {
            conn.execute(
                &format!(
                    "INSERT INTO {} (scope, generation, operation) VALUES (?1, ?2, ?3)",
                    crate::sql::ident(&storage.tables.admissions)
                ),
                rusqlite::params![scope.as_str(), i64::MAX, "near_overflow"],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
    let current = storage
        .for_scope_generation(&scope, ScopeGeneration(i64::MAX as u64))
        .unwrap();
    current
        .documents()
        .put(
            "records",
            "keep",
            json!({"alive": true}),
            Precondition::None,
        )
        .await
        .unwrap();

    assert_eq!(
        current
            .purge(&PurgeRequest {
                operation_id: "overflow-reset".into(),
                mode: PurgeMode::Atomic,
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert!(
        current
            .documents()
            .get("records", "keep")
            .await
            .unwrap()
            .is_some()
    );
    drop(base);
}

#[tokio::test]
async fn upgrading_receipt_schema_preserves_the_last_completed_reset() {
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest};
    let dir = tempfile::tempdir().unwrap();
    let location = dir.path().join("state.db");
    let scope = Scope::new("migrated_reset").unwrap();
    let request = PurgeRequest {
        operation_id: "existing_reset".into(),
        mode: PurgeMode::Atomic,
    };
    let receipt = {
        let storage = SqliteStorage::open(&location).unwrap();
        let receipt = storage
            .for_scope(&scope)
            .unwrap()
            .purge(&request)
            .await
            .unwrap();
        let drop_receipts = format!(
            "DROP TABLE {}",
            crate::sql::ident(&storage.tables.purge_receipts)
        );
        storage
            .db
            .run_now(|conn| {
                conn.execute_batch(&drop_receipts).unwrap();
                Ok(())
            })
            .unwrap();
        receipt
    };
    let reopened = SqliteStorage::open(&location).unwrap();
    let current = reopened
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current
        .blobs()
        .put("replacement", vec![9], None)
        .await
        .unwrap();
    assert_eq!(
        current.purge_status(&request.operation_id).await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(current.purge(&request).await.unwrap(), receipt);
    assert_eq!(
        current
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![9]
    );
}

#[tokio::test]
async fn corrupt_historical_receipt_refuses_retry_without_erasing_replacements() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest};
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("corrupt_history").unwrap();
    let request = PurgeRequest {
        operation_id: "completed_reset".into(),
        mode: PurgeMode::Atomic,
    };
    let receipt = storage
        .for_scope(&scope)
        .unwrap()
        .purge(&request)
        .await
        .unwrap();
    let current = storage
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current
        .blobs()
        .put("replacement", vec![3], None)
        .await
        .unwrap();
    let corrupt = format!(
        "UPDATE {} SET generation = -1 WHERE scope = ?1",
        crate::sql::ident(&storage.tables.purge_receipts)
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute(&corrupt, [scope.as_str()]).unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(
        current
            .purge_status(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        current.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        current
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![3]
    );
}

async fn stale_operations_with_missing_admission(
    old: &tinystoragedrivers_core::ScopedStorage,
    operation_id: &str,
) -> [Option<tinystoragedrivers_core::ErrorKind>; 5] {
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest};
    [
        old.documents()
            .get("records", "replacement")
            .await
            .err()
            .map(|e| e.kind()),
        old.streams()
            .read_window("records", 0, 10)
            .await
            .err()
            .map(|e| e.kind()),
        old.blobs().get("replacement").await.err().map(|e| e.kind()),
        old.documents()
            .put("records", "stale", json!({"old": true}), Precondition::None)
            .await
            .err()
            .map(|e| e.kind()),
        old.purge(&PurgeRequest {
            operation_id: operation_id.into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .err()
        .map(|e| e.kind()),
    ]
}

async fn assert_replacements_survive_authority_loss(
    current: &tinystoragedrivers_core::ScopedStorage,
) {
    assert!(
        current
            .documents()
            .get("records", "replacement")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        current
            .documents()
            .get("records", "stale")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        current
            .streams()
            .read_window("records", 0, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        current
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![1, 2]
    );
}

#[tokio::test]
async fn missing_admission_with_receipt_history_never_revives_legacy_ports() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest};
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("missing_admission").unwrap();
    let old = storage.for_scope(&scope).unwrap();
    let receipt = old
        .purge(&PurgeRequest {
            operation_id: "missing_admission_first".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    let current = storage
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current
        .documents()
        .put(
            "records",
            "replacement",
            json!({"new": true}),
            Precondition::None,
        )
        .await
        .unwrap();
    current
        .streams()
        .append("records", json!({"new": true}))
        .await
        .unwrap();
    current
        .blobs()
        .put("replacement", vec![1, 2], None)
        .await
        .unwrap();
    let delete = format!(
        "DELETE FROM {} WHERE scope = ?1",
        crate::sql::ident(&storage.tables.admissions)
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute(&delete, [scope.as_str()]).unwrap();
            Ok(())
        })
        .unwrap();
    let errors = stale_operations_with_missing_admission(&old, "missing_admission_second").await;
    assert_eq!(errors, [Some(ErrorKind::Serialization); 5]);
    let restore = format!(
        "INSERT INTO {} (scope, generation, operation) VALUES (?1, ?2, ?3)",
        crate::sql::ident(&storage.tables.admissions)
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute(
                &restore,
                rusqlite::params![
                    scope.as_str(),
                    i64::try_from(receipt.generation.0).unwrap(),
                    receipt.operation_id
                ],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
    assert_replacements_survive_authority_loss(&current).await;
}

#[tokio::test]
async fn corrupt_or_exhausted_scope_generation_never_erases_data() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest, ScopeGeneration};
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("exhausted").unwrap();
    let target = storage.for_scope(&scope).unwrap();
    target
        .documents()
        .put("records", "one", json!({}), Precondition::None)
        .await
        .unwrap();
    let table = crate::sql::ident(&storage.tables.admissions);
    storage
        .db
        .run_now(|conn| {
            conn.execute(
                &format!("INSERT INTO {table} VALUES (?1,-1,'old')"),
                [scope.as_str()],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(
        target
            .streams()
            .append("events", json!({}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute(
                &format!("UPDATE {table} SET generation=?1 WHERE scope=?2"),
                rusqlite::params![i64::MAX, scope.as_str()],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
    let admitted = storage
        .for_scope_generation(&scope, ScopeGeneration(u64::try_from(i64::MAX).unwrap()))
        .unwrap();
    assert_eq!(
        admitted
            .purge(&PurgeRequest {
                operation_id: "overflow_reset".into(),
                mode: PurgeMode::Atomic
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert!(
        admitted
            .documents()
            .get("records", "one")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn sqlite_purge_validators_track_current_and_revoked_generations() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest};

    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("explicit-purge-validation").unwrap();
    let old = storage.for_scope(&scope).unwrap();
    old.validate_purge_generation().await.unwrap();
    old.validate_purge_binding().await.unwrap();
    let receipt = old
        .purge(&PurgeRequest {
            operation_id: "validate-reset".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    assert_eq!(
        old.validate_purge_generation().await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        old.validate_purge_binding().await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    let current = storage
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current.validate_purge_generation().await.unwrap();
    current.validate_purge_binding().await.unwrap();
}

#[tokio::test]
async fn retired_scope_handles_cannot_read_replacement_data() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest};
    let dir = tempfile::tempdir().unwrap();
    let backend = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("retired_reader").unwrap();
    let old = backend.for_scope(&scope).unwrap();
    let receipt = old
        .purge(&PurgeRequest {
            operation_id: "retire_reader".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    let fresh = backend
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    fresh
        .documents()
        .put("records", "one", json!({"new":true}), Precondition::None)
        .await
        .unwrap();
    fresh
        .streams()
        .append("events", json!({"new":true}))
        .await
        .unwrap();
    fresh.blobs().put("blob", vec![2], None).await.unwrap();
    assert_eq!(
        old.documents()
            .get("records", "one")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        old.streams()
            .read_window("events", 0, 10)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        old.blobs().get("blob").await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
}
