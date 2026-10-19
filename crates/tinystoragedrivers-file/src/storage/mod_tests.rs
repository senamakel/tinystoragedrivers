//! Opening and the per-directory lock.

use super::*;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn opening_a_file_as_the_root_fails() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    assert_eq!(
        FileStorage::open(&file).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn handles_on_one_directory_share_a_lock() {
    let dir = tempfile::tempdir().unwrap();
    let a = FileStorage::open(dir.path()).unwrap();
    let b = FileStorage::open(dir.path().join(".")).unwrap();
    assert!(Arc::ptr_eq(&a.db.lock, &b.db.lock));
    assert!(format!("{:?}", a.db).starts_with("Db"));
    assert!(system_clock() > 0);
}

#[tokio::test]
async fn a_poisoned_lock_is_taken_over() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let db = Arc::clone(&storage.db);
    let _ = std::thread::spawn(move || {
        let _guard = db.lock.lock().unwrap();
        panic!("poison the lock");
    })
    .join();
    assert_eq!(storage.db.run(|_| Ok(5)).await.unwrap(), 5);
}

#[tokio::test]
async fn a_panicking_task_is_a_backend_error() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let error = storage
        .db
        .run(|_| -> Result<()> { panic!("task panics") })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[tokio::test]
async fn reopened_target_observes_guard_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
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
    let target = FileStorage::open(dir.path()).unwrap();
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
    let takeover = FileStorage::open(dir.path())
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
fn unrelated_roots_have_independent_native_locks() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = FileStorage::open(left_dir.path()).unwrap();
    let right = FileStorage::open(right_dir.path()).unwrap();
    let held = left.db.lock();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _other = right.db.lock();
        done_tx.send(()).unwrap();
    });
    started_rx.recv().unwrap();
    let independent = done_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .is_ok();
    drop(held);
    worker.join().unwrap();
    assert!(
        independent,
        "an unrelated directory must not wait for this root"
    );
}

#[tokio::test]
async fn a_fenced_handle_that_lost_its_lease_cannot_sweep_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
    let scope = Scope::new("alice").unwrap();
    let guard_scope = Scope::new("cluster").unwrap();
    let lease = |epoch: u64| {
        let root = &root;
        let guard_scope = guard_scope.clone();
        async move {
            root.for_scope(&guard_scope)
                .unwrap()
                .documents()
                .put(
                    "leases",
                    "l",
                    serde_json::json!({"epoch": epoch}),
                    tinystoragedrivers_core::Precondition::None,
                )
                .await
                .unwrap();
        }
    };
    lease(1).await;
    let fence = Fence::epoch(guard_scope.clone(), "leases", "l", "epoch", 1);
    let held = root.for_scope_fenced(&scope, &fence).unwrap();
    held.blobs()
        .put_expiring("old", vec![1], None, std::time::Duration::from_millis(1))
        .await
        .unwrap();
    lease(2).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(
        held.blobs().sweep_expired().await.unwrap_err().kind(),
        ErrorKind::Fenced
    );
    // The refused sweep removed nothing: an unfenced sweep still finds it.
    assert_eq!(root.sweep_expired_blobs(&scope).await.unwrap(), 1);
}

#[tokio::test]
async fn stale_fenced_purge_and_status_refuse_without_erasing_data() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
    let scope = Scope::new("fenced-purge-target").unwrap();
    let guard_scope = Scope::new("fenced-purge-lease").unwrap();
    let lease_scope = root.for_scope(&guard_scope).unwrap();
    lease_scope
        .documents()
        .put(
            "leases",
            "l",
            serde_json::json!({"epoch": 1}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    let fence = Fence::epoch(guard_scope.clone(), "leases", "l", "epoch", 1);
    let target = root.for_scope(&scope).unwrap();
    target
        .documents()
        .put(
            "docs",
            "keep",
            serde_json::json!({"alive": true}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    target.blobs().put("keep", vec![7], None).await.unwrap();
    let fenced = root.for_scope_fenced(&scope, &fence).unwrap();
    lease_scope
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
        fenced.purge_status("stale-purge").await.unwrap_err().kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        fenced
            .purge(&PurgeRequest {
                operation_id: "stale-purge".into(),
                mode: PurgeMode::Quiescent
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    let current_fence = Fence::epoch(guard_scope, "leases", "l", "epoch", 2);
    let current_generation = root
        .for_scope_fenced_generation(&scope, ScopeGeneration(0), &current_fence)
        .unwrap();
    assert_eq!(
        current_generation
            .documents()
            .get("docs", "keep")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"alive": true})
    );
    assert_eq!(
        target
            .documents()
            .get("docs", "keep")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"alive": true})
    );
    assert_eq!(
        target.blobs().get("keep").await.unwrap().unwrap().bytes,
        [7]
    );
}

#[tokio::test]
async fn root_sweeper_reclaims_expired_blobs_from_the_current_generation() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest, ScopeGeneration};

    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(10_000));
    let clock_now = Arc::clone(&now);
    let storage = FileStorage::open_with_clock(
        dir.path(),
        Arc::new(move || clock_now.load(Ordering::SeqCst)),
    )
    .unwrap();
    let scope = Scope::new("sweeper-after-purge").unwrap();
    let first = storage.for_scope(&scope).unwrap();
    let receipt = first
        .purge(&PurgeRequest {
            operation_id: "sweeper-generation-reset".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(receipt.generation, ScopeGeneration(1));

    let current = storage
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current
        .blobs()
        .put_expiring(
            "expired-in-current-generation",
            vec![9],
            None,
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap();
    now.fetch_add(2, Ordering::SeqCst);

    assert_eq!(storage.sweep_expired_blobs(&scope).await.unwrap(), 1);
    assert!(
        current
            .blobs()
            .get("expired-in-current-generation")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn maximum_scope_has_bounded_durable_purge_metadata_path() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
    let scope = Scope::new("a".repeat(tinystoragedrivers_core::MAX_SCOPE_LEN)).unwrap();
    let receipt = root
        .for_scope(&scope)
        .unwrap()
        .purge(&PurgeRequest {
            operation_id: "maximum-scope".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(
        root.for_scope(&scope)
            .unwrap()
            .purge_status("maximum-scope")
            .await
            .unwrap(),
        Some(receipt)
    );
}

#[tokio::test]
async fn corrupt_purge_receipt_fails_closed_without_erasing_scope_data() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
    let scope = Scope::new("corrupt-purge-state").unwrap();
    let target = root.for_scope(&scope).unwrap();
    target
        .documents()
        .put(
            "docs",
            "keep",
            serde_json::json!({"alive": true}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    let data_path = root
        .db
        .scope_dir(&scope)
        .join("docs")
        .join(crate::encode::dir_components("docs"))
        .join(format!("{}.json", crate::encode::file_stem("keep")));
    let data_before = std::fs::read(&data_path).unwrap();
    let state_path = root.db.purge_state_path(&scope);
    std::fs::create_dir_all(state_path.parent().unwrap()).unwrap();
    std::fs::write(&state_path, b"{invalid json").unwrap();

    assert_eq!(
        target
            .purge(&PurgeRequest {
                operation_id: "must-not-erase".into(),
                mode: PurgeMode::Quiescent,
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
    assert!(
        root.db.scope_dir(&scope).exists(),
        "corrupt receipt metadata must not remove the tenant scope directory"
    );
    assert_eq!(
        std::fs::read(&data_path).unwrap(),
        data_before,
        "corrupt receipt metadata must preserve the persisted tenant document"
    );
}

#[tokio::test]
async fn blob_sweep_refuses_a_scope_while_its_purge_is_pending() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
    let scope = Scope::new("pending-sweep-scope").unwrap();
    let target = root.for_scope(&scope).unwrap();
    target.blobs().put("keep", vec![7], None).await.unwrap();
    root.db
        .save_purge_state(
            &scope,
            &FilePurgeState {
                generation: 0,
                pending: Some(PurgeReceipt {
                    operation_id: "pending-sweep".into(),
                    generation: ScopeGeneration(1),
                    status: PurgeStatus::Pending,
                    stage: PurgeStage::Records,
                }),
                receipts: HashMap::new(),
            },
        )
        .unwrap();

    assert_eq!(
        root.sweep_expired_blobs(&scope).await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        target.blobs().get("keep").await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    let data_path = root
        .db
        .scope_dir(&scope)
        .join("blobs")
        .join(crate::encode::file_stem("keep"));
    assert_eq!(std::fs::read(data_path).unwrap(), [7]);
}

#[tokio::test]
async fn completed_purge_replay_preserves_a_later_generation() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
    let scope = Scope::new("purge-replay-after-reopen").unwrap();
    let first = root.for_scope(&scope).unwrap();
    let request = PurgeRequest {
        operation_id: "first-reset".into(),
        mode: PurgeMode::Quiescent,
    };
    first.validate_purge_generation().await.unwrap();
    first.validate_purge_binding().await.unwrap();
    let first_receipt = first.purge(&request).await.unwrap();
    assert_eq!(
        first.validate_purge_generation().await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        first.validate_purge_binding().await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    let second = root
        .for_scope_generation(&scope, first_receipt.generation)
        .unwrap();
    second.validate_purge_generation().await.unwrap();
    second.validate_purge_binding().await.unwrap();
    second
        .blobs()
        .put("replacement", vec![9], None)
        .await
        .unwrap();

    assert_eq!(first.purge(&request).await.unwrap(), first_receipt);
    assert_eq!(
        first.purge_status("first-reset").await.unwrap(),
        Some(first_receipt)
    );
    assert_eq!(
        second
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [9]
    );
}
