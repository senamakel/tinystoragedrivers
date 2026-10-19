//! Blob edge cases: sidecars without bytes, collisions, unreadable data.

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
    assert_eq!(sidecar["key"], "docs/Report.PDF");
    assert_eq!(sidecar["content_type"], "application/pdf");
    assert!(sidecar["written_at_ms"].as_u64().is_some());
}

#[tokio::test]
async fn legacy_sidecars_without_write_time_are_retained_by_orphan_cleanup() {
    let (_dir, storage, blobs) = open();
    blobs.put("legacy/blob", vec![1], None).await.unwrap();
    let (_, sidecar_path) = files(&storage, "legacy/blob");
    let mut sidecar: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&sidecar_path).unwrap()).unwrap();
    sidecar.as_object_mut().unwrap().remove("written_at_ms");
    std::fs::write(&sidecar_path, serde_json::to_vec(&sidecar).unwrap()).unwrap();

    let removed = tinystoragedrivers_core::sweep_unreferenced(
        blobs.as_ref(),
        "legacy/",
        u64::MAX,
        |_| async { Ok(false) },
    )
    .await
    .unwrap();

    assert_eq!(removed, 0);
    assert!(blobs.head("legacy/blob").await.unwrap().is_some());
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
        written_at_ms: None,
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
    assert_eq!(sidecar["written_at_ms"], 10_000);

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
