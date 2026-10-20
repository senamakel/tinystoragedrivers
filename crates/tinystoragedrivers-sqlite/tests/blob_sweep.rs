//! The blob sweep hook reclaims lapsed blobs through `dyn StorageBackend`,
//! and blob expiry follows the injected clock rather than real time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_sqlite::SqliteStorage;

fn manual_clock(start: u64) -> (Arc<AtomicU64>, tinystoragedrivers_sqlite::Clock) {
    let now = Arc::new(AtomicU64::new(start));
    let reader = Arc::clone(&now);
    (now, Arc::new(move || reader.load(Ordering::SeqCst)))
}

#[tokio::test]
async fn the_backend_sweep_hook_reclaims_only_lapsed_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let (now, clock) = manual_clock(1_000_000);
    let storage = SqliteStorage::open(dir.path()).unwrap().with_clock(clock);
    let blobs = storage.for_scope(&Scope::local()).unwrap().blobs().clone();
    blobs
        .put_expiring("old", vec![1], None, Duration::from_millis(20))
        .await
        .unwrap();
    blobs
        .put_expiring("young", vec![2], None, Duration::from_secs(3600))
        .await
        .unwrap();
    blobs.put("forever", vec![3], None).await.unwrap();
    now.fetch_add(150, Ordering::SeqCst);

    let backend: Arc<dyn StorageBackend> = Arc::new(storage);
    assert_eq!(
        backend.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        1
    );
    assert_eq!(
        backend.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        0
    );
    let kept: Vec<String> = blobs
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(kept, ["forever", "young"]);
}

#[tokio::test]
async fn blob_reads_follow_the_injected_clock_not_real_time() {
    let dir = tempfile::tempdir().unwrap();
    let (now, clock) = manual_clock(5_000);
    let storage = SqliteStorage::open(dir.path()).unwrap().with_clock(clock);
    let blobs = storage.for_scope(&Scope::local()).unwrap().blobs().clone();
    blobs
        .put_expiring("k", vec![7], None, Duration::from_secs(10))
        .await
        .unwrap();

    now.store(14_999, Ordering::SeqCst);
    assert!(blobs.get("k").await.unwrap().is_some());
    assert!(blobs.head("k").await.unwrap().is_some());
    assert_eq!(blobs.list("").await.unwrap().len(), 1);

    now.store(15_000, Ordering::SeqCst);
    assert!(blobs.get("k").await.unwrap().is_none());
    assert!(blobs.head("k").await.unwrap().is_none());
    assert_eq!(blobs.list("").await.unwrap().len(), 0);
}

#[tokio::test]
async fn absolute_blob_deadlines_roundtrip_without_restarting_the_lifetime() {
    let dir = tempfile::tempdir().unwrap();
    let (now, clock) = manual_clock(1_000);
    let storage = SqliteStorage::open(dir.path()).unwrap().with_clock(clock);
    let blobs = storage.for_scope(&Scope::local()).unwrap().blobs().clone();
    blobs
        .put_until("absolute", vec![1, 2], Some("a/b"), Some(6_000))
        .await
        .unwrap();
    now.store(4_000, Ordering::SeqCst);
    let source = blobs.get_with_expiry("absolute").await.unwrap().unwrap();
    assert_eq!(source.blob.bytes, [1, 2]);
    assert_eq!(source.expires_at_ms, Some(6_000));
    now.store(5_999, Ordering::SeqCst);
    assert!(blobs.get("absolute").await.unwrap().is_some());
    now.store(6_000, Ordering::SeqCst);
    assert!(blobs.get_with_expiry("absolute").await.unwrap().is_none());

    blobs
        .put_until("already-expired", vec![3], None, Some(6_000))
        .await
        .unwrap();
    assert!(blobs.get("already-expired").await.unwrap().is_none());
    assert_eq!(
        blobs
            .put_until("unsupported-range", vec![4], None, Some(u64::MAX))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::InvalidInput
    );
}
