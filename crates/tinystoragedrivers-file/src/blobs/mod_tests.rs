//! Blob edge cases: sidecars without bytes, collisions, unreadable data.

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, StorageBackend};

use super::*;
use crate::FileStorage;

fn open() -> (tempfile::TempDir, FileStorage, Arc<dyn BlobStore>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let blobs = Arc::clone(storage.for_scope(&Scope::local()).unwrap().blobs());
    (dir, storage, blobs)
}

fn files(storage: &FileStorage, key: &str) -> (PathBuf, PathBuf) {
    let dir = storage.dir().join("scopes/local/blobs");
    let stem = file_stem(key);
    (dir.join(&stem), dir.join(format!("{stem}.meta.json")))
}

#[tokio::test]
async fn stores_the_bytes_verbatim_with_a_sidecar() {
    let (_dir, storage, blobs) = open();
    blobs
        .put("docs/Report.PDF", b"%PDF".to_vec(), Some("application/pdf"))
        .await
        .unwrap();
    let (data, meta) = files(&storage, "docs/Report.PDF");
    assert_eq!(std::fs::read(&data).unwrap(), b"%PDF");
    let sidecar: serde_json::Value = serde_json::from_slice(&std::fs::read(meta).unwrap()).unwrap();
    assert_eq!(
        sidecar,
        json!({"key": "docs/Report.PDF", "content_type": "application/pdf"})
    );
}

#[tokio::test]
async fn a_sidecar_without_bytes_reads_as_absent() {
    let (_dir, storage, blobs) = open();
    blobs.put("a", vec![1], None).await.unwrap();
    blobs.put("b", vec![2], None).await.unwrap();
    let (data, _) = files(&storage, "a");
    std::fs::remove_file(&data).unwrap();
    assert!(blobs.head("a").await.unwrap().is_none());
    assert!(blobs.get("a").await.unwrap().is_none());
    assert!(blobs.get_range("a", 0..1).await.unwrap().is_none());
    assert!(!blobs.delete("a").await.unwrap());
    let keys: Vec<_> = blobs
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(keys, ["b"]);
}

#[tokio::test]
async fn a_sidecar_for_another_key_is_a_collision() {
    let (_dir, storage, blobs) = open();
    let key = "z".repeat(400);
    let (_, meta) = files(&storage, &key);
    std::fs::create_dir_all(meta.parent().unwrap()).unwrap();
    std::fs::write(&meta, br#"{"key":"other"}"#).unwrap();
    assert_eq!(
        blobs.head(&key).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        blobs.get_with_expiry(&key).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        blobs.put(&key, vec![], None).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn unreadable_bytes_are_a_backend_error() {
    let (_dir, storage, blobs) = open();
    blobs.put("a", vec![1, 2, 3], None).await.unwrap();
    let (data, _) = files(&storage, "a");
    std::fs::remove_file(&data).unwrap();
    // A directory where the bytes should be: its size reads, its content
    // does not.
    std::fs::create_dir(&data).unwrap();
    assert_eq!(blobs.get("a").await.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(
        blobs.get_range("a", 0..1).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert!(format!("{blobs:?}").contains("local"));
}

#[test]
fn a_missing_data_file_has_no_meta() {
    let dir = tempfile::tempdir().unwrap();
    let sidecar = Sidecar {
        key: "k".into(),
        content_type: None,
        expires_at_ms: None,
    };
    assert!(
        meta(&dir.path().join("missing"), sidecar.clone())
            .unwrap()
            .is_none()
    );
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    assert_eq!(
        meta(&file.join("below"), sidecar).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_payload_is_refused_for_whole_and_ranged_reads() {
    let (dir, storage, blobs) = open();
    blobs.put("a", b"hello".to_vec(), None).await.unwrap();
    let (data, _) = files(&storage, "a");
    let victim = dir.path().join("victim");
    std::fs::rename(&data, &victim).unwrap();
    std::os::unix::fs::symlink(&victim, &data).unwrap();
    assert_eq!(blobs.get("a").await.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(
        blobs.head("a").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        blobs.get_range("a", 0..2).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}

fn open_driven() -> (
    tempfile::TempDir,
    FileStorage,
    Arc<dyn BlobStore>,
    Arc<std::sync::atomic::AtomicU64>,
) {
    use std::sync::atomic::{AtomicU64, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(10_000));
    let reading = Arc::clone(&now);
    let storage =
        FileStorage::open_with_clock(dir.path(), Arc::new(move || reading.load(Ordering::SeqCst)))
            .unwrap();
    let blobs = Arc::clone(storage.for_scope(&Scope::local()).unwrap().blobs());
    (dir, storage, blobs, now)
}

#[tokio::test]
async fn an_expiring_blob_records_its_deadline_and_vanishes_at_it() {
    use std::sync::atomic::Ordering;
    let (_dir, storage, blobs, now) = open_driven();
    blobs
        .put_expiring("k", vec![1, 2, 3], Some("a/b"), Duration::from_millis(250))
        .await
        .unwrap();
    let (data, meta) = files(&storage, "k");
    let sidecar: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta).unwrap()).unwrap();
    assert_eq!(sidecar["expires_at_ms"], 10_250);

    now.store(10_249, Ordering::SeqCst);
    assert_eq!(blobs.get("k").await.unwrap().unwrap().bytes, [1, 2, 3]);
    assert_eq!(blobs.list("").await.unwrap().len(), 1);

    now.store(10_250, Ordering::SeqCst);
    assert!(blobs.get("k").await.unwrap().is_none());
    assert!(blobs.head("k").await.unwrap().is_none());
    assert!(blobs.get_range("k", 0..1).await.unwrap().is_none());
    assert_eq!(blobs.list("").await.unwrap().len(), 0);
    assert!(
        !data.exists() && !meta.exists(),
        "a read that finds the blob lapsed reclaims its files"
    );
    assert!(
        !blobs.delete("k").await.unwrap(),
        "a lapsed blob is not deleted"
    );
}

#[tokio::test]
async fn a_list_reclaims_the_lapsed_blobs_it_passes() {
    use std::sync::atomic::Ordering;
    let (_dir, storage, blobs, now) = open_driven();
    blobs
        .put_expiring("gone", vec![1], None, Duration::from_millis(5))
        .await
        .unwrap();
    blobs.put("kept", vec![2], None).await.unwrap();
    now.store(10_005, Ordering::SeqCst);
    let keys: Vec<_> = blobs
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(keys, ["kept"]);
    let (data, meta) = files(&storage, "gone");
    assert!(!data.exists() && !meta.exists());
}

#[tokio::test]
async fn the_sweeper_reclaims_only_lapsed_blobs_of_its_scope() {
    use std::sync::atomic::Ordering;
    let (_dir, storage, blobs, now) = open_driven();
    blobs
        .put_expiring("old", vec![1], None, Duration::from_millis(5))
        .await
        .unwrap();
    blobs
        .put_expiring("young", vec![2], None, Duration::from_millis(500))
        .await
        .unwrap();
    blobs.put("forever", vec![3], None).await.unwrap();
    now.store(10_100, Ordering::SeqCst);
    assert_eq!(
        storage.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        1
    );
    let (data, meta) = files(&storage, "old");
    assert!(!data.exists() && !meta.exists());
    assert!(files(&storage, "young").0.exists());
    assert!(files(&storage, "forever").0.exists());
    assert_eq!(
        storage.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn a_plain_put_clears_the_deadline() {
    use std::sync::atomic::Ordering;
    let (_dir, storage, blobs, now) = open_driven();
    blobs
        .put_expiring("k", vec![1], None, Duration::from_millis(10))
        .await
        .unwrap();
    blobs.put("k", vec![2], None).await.unwrap();
    let (_, meta) = files(&storage, "k");
    let sidecar: serde_json::Value = serde_json::from_slice(&std::fs::read(meta).unwrap()).unwrap();
    assert!(sidecar.get("expires_at_ms").is_none());
    now.store(u64::MAX / 2, Ordering::SeqCst);
    assert_eq!(blobs.get("k").await.unwrap().unwrap().bytes, [2]);
}

#[tokio::test]
async fn absolute_deadlines_are_reported_and_expired_on_arrival_stays_invisible() {
    use std::sync::atomic::Ordering;

    let (_dir, _storage, blobs, now) = open_driven();
    blobs
        .put_until("absolute", vec![1, 2], Some("a/b"), Some(10_250))
        .await
        .unwrap();
    let source = blobs.get_with_expiry("absolute").await.unwrap().unwrap();
    assert_eq!(source.blob.bytes, [1, 2]);
    assert_eq!(source.expires_at_ms, Some(10_250));
    now.store(10_250, Ordering::SeqCst);
    assert!(blobs.get_with_expiry("absolute").await.unwrap().is_none());

    blobs
        .put_until("already-expired", vec![3], None, Some(10_250))
        .await
        .unwrap();
    assert!(
        blobs
            .get_with_expiry("already-expired")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn failed_sidecar_publication_keeps_the_previous_bytes_and_expiry_pair() {
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths {
        data: dir.path().join("blob"),
        meta: dir.path().join("blob.meta.json"),
        now: 1_000,
    };
    store_in(
        &paths,
        "key",
        b"old bytes",
        Some("text/plain".to_owned()),
        Some(6_000),
    )
    .unwrap();

    let error = store_in_with_sidecar_publisher(
        &paths,
        "key",
        b"replacement bytes",
        Some("application/octet-stream".to_owned()),
        Some(9_000),
        |_staged, _published| Err(StorageError::backend("injected sidecar rename failure")),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);

    let visible = source(&paths, "key").unwrap().unwrap();
    assert_eq!(visible.blob.bytes, b"old bytes");
    assert_eq!(
        visible.blob.meta.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(visible.expires_at_ms, Some(6_000));
}

#[tokio::test]
async fn reopened_storage_rolls_back_a_blob_pair_interrupted_after_data_publish() {
    let (dir, storage, blobs, _now) = open_driven();
    blobs
        .put_until(
            "key",
            b"old bytes".to_vec(),
            Some("text/plain"),
            Some(16_000),
        )
        .await
        .unwrap();
    let (data, meta) = files(&storage, "key");
    let paths = Paths {
        data: data.clone(),
        meta: meta.clone(),
        now: 10_000,
    };
    let transaction = transaction_paths(&paths, "key", "999-1").unwrap();
    write_atomic(&transaction.staged_data, b"replacement bytes").unwrap();
    write_json(
        &transaction.staged_meta,
        &Sidecar {
            key: "key".into(),
            content_type: Some("application/octet-stream".into()),
            expires_at_ms: Some(19_000),
        },
    )
    .unwrap();
    copy_atomic(&data, &transaction.backup_data).unwrap();
    copy_atomic(&meta, &transaction.backup_meta).unwrap();
    write_json(
        &transaction.marker,
        &BlobJournal {
            key: "key".into(),
            token: "999-1".into(),
            staged_len: b"replacement bytes".len() as u64,
            staged_sidecar: Sidecar {
                key: "key".into(),
                content_type: Some("application/octet-stream".into()),
                expires_at_ms: Some(19_000),
            },
            had_data: true,
            had_meta: true,
            committed: false,
        },
    )
    .unwrap();
    copy_atomic(&transaction.staged_data, &data).unwrap();
    copy_atomic(&transaction.staged_meta, &meta).unwrap();
    // A prior recovery restored only the payload before crashing; the marker
    // and backups remain so the reopened handle must finish rollback.
    copy_atomic(&transaction.backup_data, &data).unwrap();

    drop(blobs);
    drop(storage);
    let reopened = FileStorage::open_with_clock(dir.path(), Arc::new(|| 10_000)).unwrap();
    let recovered = reopened
        .for_scope(&Scope::local())
        .unwrap()
        .blobs()
        .get_with_expiry("key")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.blob.bytes, b"old bytes");
    assert_eq!(recovered.expires_at_ms, Some(16_000));
    assert!(!transaction.marker.exists());
}

#[tokio::test]
async fn reopened_storage_removes_an_uncommitted_new_blob_pair() {
    let (dir, storage, blobs, _now) = open_driven();
    let (data, meta) = files(&storage, "new-key");
    let paths = Paths {
        data: data.clone(),
        meta: meta.clone(),
        now: 1_000,
    };
    let transaction = transaction_paths(&paths, "new-key", "999-2").unwrap();
    write_atomic(&transaction.staged_data, b"uncommitted").unwrap();
    write_json(
        &transaction.staged_meta,
        &Sidecar {
            key: "new-key".into(),
            content_type: None,
            expires_at_ms: None,
        },
    )
    .unwrap();
    write_json(
        &transaction.marker,
        &BlobJournal {
            key: "new-key".into(),
            token: "999-2".into(),
            staged_len: b"uncommitted".len() as u64,
            staged_sidecar: Sidecar {
                key: "new-key".into(),
                content_type: None,
                expires_at_ms: None,
            },
            had_data: false,
            had_meta: false,
            committed: false,
        },
    )
    .unwrap();
    copy_atomic(&transaction.staged_data, &data).unwrap();

    drop(blobs);
    drop(storage);
    let reopened = FileStorage::open_with_clock(dir.path(), Arc::new(|| 10_000)).unwrap();
    let recovered_storage = reopened.for_scope(&Scope::local()).unwrap();
    assert_eq!(
        recovered_storage.blobs().list("").await.unwrap(),
        [] as [tinystoragedrivers_core::BlobMeta; 0]
    );
    assert!(
        recovered_storage
            .blobs()
            .get("new-key")
            .await
            .unwrap()
            .is_none()
    );
    assert!(!meta.exists());
    assert!(!data.exists());
    assert!(!transaction.marker.exists());
}

#[tokio::test]
async fn reopened_storage_finishes_a_committed_blob_pair_before_cleanup() {
    let (dir, storage, blobs, _now) = open_driven();
    blobs
        .put_until(
            "key",
            b"old bytes".to_vec(),
            Some("text/plain"),
            Some(16_000),
        )
        .await
        .unwrap();
    let (data, meta) = files(&storage, "key");
    let paths = Paths {
        data: data.clone(),
        meta: meta.clone(),
        now: 10_000,
    };
    let transaction = transaction_paths(&paths, "key", "999-3").unwrap();
    write_atomic(&transaction.staged_data, b"committed bytes").unwrap();
    write_json(
        &transaction.staged_meta,
        &Sidecar {
            key: "key".into(),
            content_type: Some("application/octet-stream".into()),
            expires_at_ms: Some(19_000),
        },
    )
    .unwrap();
    copy_atomic(&data, &transaction.backup_data).unwrap();
    copy_atomic(&meta, &transaction.backup_meta).unwrap();
    write_json(
        &transaction.marker,
        &BlobJournal {
            key: "key".into(),
            token: "999-3".into(),
            staged_len: b"committed bytes".len() as u64,
            staged_sidecar: Sidecar {
                key: "key".into(),
                content_type: Some("application/octet-stream".into()),
                expires_at_ms: Some(19_000),
            },
            had_data: true,
            had_meta: true,
            committed: true,
        },
    )
    .unwrap();
    copy_atomic(&transaction.staged_data, &data).unwrap();
    copy_atomic(&transaction.staged_meta, &meta).unwrap();

    drop(blobs);
    drop(storage);
    let reopened = FileStorage::open_with_clock(dir.path(), Arc::new(|| 10_000)).unwrap();
    let recovered = reopened
        .for_scope(&Scope::local())
        .unwrap()
        .blobs()
        .get_with_expiry("key")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.blob.bytes, b"committed bytes");
    assert_eq!(recovered.expires_at_ms, Some(19_000));
    assert!(!transaction.marker.exists());
}

#[tokio::test]
async fn scope_recovery_removes_only_orphaned_blob_transaction_artifacts() {
    let (_dir, storage, blobs, _now) = open_driven();
    blobs
        .put("ordinary", b"preserve me".to_vec(), Some("text/plain"))
        .await
        .unwrap();
    let (data, meta) = files(&storage, "orphan");
    let paths = Paths {
        data,
        meta,
        now: 1_000,
    };
    let transaction = transaction_paths(&paths, "orphan", "999-1").unwrap();
    write_atomic(&transaction.staged_data, b"orphan staged data").unwrap();
    write_json(
        &transaction.staged_meta,
        &Sidecar {
            key: "orphan".into(),
            content_type: None,
            expires_at_ms: None,
        },
    )
    .unwrap();
    write_atomic(&transaction.backup_data, b"orphan old data").unwrap();
    write_json(
        &transaction.backup_meta,
        &Sidecar {
            key: "orphan".into(),
            content_type: None,
            expires_at_ms: None,
        },
    )
    .unwrap();

    let listed: Vec<_> = blobs
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(listed, ["ordinary"]);
    assert!(!transaction.staged_data.exists());
    assert!(!transaction.staged_meta.exists());
    assert!(!transaction.backup_data.exists());
    assert!(!transaction.backup_meta.exists());
    assert_eq!(
        blobs.get("ordinary").await.unwrap().unwrap().bytes,
        b"preserve me"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn scope_recovery_refuses_symlinked_transaction_artifacts_and_ignores_invalid_names() {
    let (_dir, storage, blobs, _now) = open_driven();
    blobs.put("ordinary", b"keep".to_vec(), None).await.unwrap();
    let (data, _) = files(&storage, "transaction-owner");
    let dir = data.parent().unwrap();
    let stem = file_stem("transaction-owner");
    let invalid = dir.join(format!("{stem}.txn-not-a-token.stage-data"));
    std::fs::write(&invalid, b"unrecognized artifact").unwrap();
    assert_eq!(
        blobs.list("").await.unwrap()[0].key,
        "ordinary",
        "an invalid artifact name does not disrupt listing"
    );
    assert_eq!(std::fs::read(&invalid).unwrap(), b"unrecognized artifact");
    let victim = dir.join("victim");
    std::fs::write(&victim, b"outside transaction namespace").unwrap();
    let symlink = dir.join(format!("{stem}.txn-999-91.stage-data"));
    std::os::unix::fs::symlink(&victim, &symlink).unwrap();

    assert_eq!(blobs.list("").await.unwrap_err().kind(), ErrorKind::Backend);
    assert!(symlink.symlink_metadata().unwrap().file_type().is_symlink());
    assert_eq!(
        std::fs::read(victim).unwrap(),
        b"outside transaction namespace"
    );
    assert_eq!(std::fs::read(invalid).unwrap(), b"unrecognized artifact");
}

#[tokio::test]
async fn failed_scope_recovery_keeps_artifacts_referenced_by_its_journal() {
    let (_dir, storage, blobs, _now) = open_driven();
    blobs
        .put_until("keep", b"old pair".to_vec(), None, Some(6_000))
        .await
        .unwrap();
    let (data, meta) = files(&storage, "keep");
    let paths = Paths {
        data: data.clone(),
        meta: meta.clone(),
        now: 1_000,
    };
    let transaction = transaction_paths(&paths, "keep", "999-2").unwrap();
    write_atomic(&transaction.staged_data, b"replacement").unwrap();
    let staged_sidecar = Sidecar {
        key: "keep".into(),
        content_type: Some("application/octet-stream".into()),
        expires_at_ms: Some(9_000),
    };
    write_json(&transaction.staged_meta, &staged_sidecar).unwrap();
    write_json(
        &transaction.marker,
        &BlobJournal {
            key: "keep".into(),
            token: "999-2".into(),
            staged_len: b"replacement".len() as u64,
            staged_sidecar,
            had_data: true,
            had_meta: true,
            committed: false,
        },
    )
    .unwrap();

    assert_eq!(blobs.list("").await.unwrap_err().kind(), ErrorKind::Backend);
    assert!(transaction.marker.exists());
    assert!(transaction.staged_data.exists());
    assert!(transaction.staged_meta.exists());
    assert_eq!(std::fs::read(data).unwrap(), b"old pair");
}
