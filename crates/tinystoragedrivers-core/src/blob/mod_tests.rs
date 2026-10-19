//! Blob key validation and range clamping.

use super::*;

#[test]
fn keys_validate() {
    assert!(validate_blob_key("uploads/2026/a.png").is_ok());
    for bad in ["", "/a", "a/", "a//b", "./a", "a/../b", "nul\0"] {
        assert!(validate_blob_key(bad).is_err(), "{bad:?}");
    }
    assert!(validate_blob_key(&"a".repeat(MAX_BLOB_KEY_LEN + 1)).is_err());
}

#[test]
fn ranges_clamp_to_the_blob() {
    assert_eq!(clamp_range(&(2..5), 10).unwrap(), 2..5);
    assert_eq!(clamp_range(&(2..50), 10).unwrap(), 2..10);
    assert_eq!(clamp_range(&(20..50), 10).unwrap(), 10..10);
    assert_eq!(clamp_range(&(0..u64::MAX), 3).unwrap(), 0..3);
    #[allow(
        clippy::reversed_empty_ranges,
        reason = "the inverted range is the input under test"
    )]
    let inverted = 5..1;
    assert!(clamp_range(&inverted, 10).is_err());
}

#[test]
fn metadata_omits_a_missing_content_type() {
    let meta = BlobMeta {
        key: "k".into(),
        len: 1,
        content_type: None,
    };
    assert_eq!(
        serde_json::to_string(&meta).unwrap(),
        r#"{"key":"k","len":1}"#
    );
}

#[tokio::test]
async fn default_streaming_checks_length_before_publishing() {
    use crate::{MemoryStorage, Scope, StorageBackend};
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let blobs = storage.blobs();
    blobs.put("old", vec![9], None).await.unwrap();
    for len in [1, 3] {
        let result = blobs
            .put_stream("old", Box::pin(std::io::Cursor::new(vec![1, 2])), len, None)
            .await;
        assert!(result.is_err());
        assert_eq!(blobs.get("old").await.unwrap().unwrap().bytes, [9]);
    }
    assert!(!blobs.capabilities().contains(Capability::StreamingBlobs));
    assert_eq!(
        blobs
            .presign_put("old", Duration::from_secs(60))
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    let mut stream = blobs.get_stream("old").await.unwrap().unwrap();
    let mut bytes = Vec::new();
    stream.reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, [9]);
}

#[tokio::test]
async fn default_streaming_publishes_exact_bytes_and_reads_missing_as_absent() {
    use crate::{MemoryStorage, Scope, StorageBackend};
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let blobs = storage.blobs();
    let meta = blobs
        .put_stream(
            "new",
            Box::pin(std::io::Cursor::new(vec![1, 2])),
            2,
            Some("text/plain"),
        )
        .await
        .unwrap();
    assert_eq!(meta.len, 2);
    assert!(blobs.get_stream("missing").await.unwrap().is_none());
    let stream = blobs.get_stream("new").await.unwrap().unwrap();
    assert!(format!("{stream:?}").contains("BlobStream"));
    assert_eq!(
        blobs
            .presign_get("new", Duration::from_secs(60))
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::PresignedUrls)
    );
}

#[tokio::test]
async fn unreferenced_sweep_observes_cutoff_reference_and_prefix() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::{MemoryStorage, Scope, StorageBackend, sweep_unreferenced};

    let now = Arc::new(AtomicU64::new(100));
    let clock = Arc::clone(&now);
    let storage = MemoryStorage::with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let blobs = storage.for_scope(&Scope::local()).unwrap().blobs().clone();

    blobs.put("orphans/old", vec![1], None).await.unwrap();
    now.store(200, Ordering::SeqCst);
    blobs
        .put("orphans/referenced", vec![2], None)
        .await
        .unwrap();
    blobs.put("elsewhere/old", vec![3], None).await.unwrap();
    now.store(300, Ordering::SeqCst);
    blobs.put("orphans/fresh", vec![4], None).await.unwrap();

    let removed = sweep_unreferenced(&*blobs, "orphans/", 250, |key| async move {
        Ok(key == "orphans/referenced")
    })
    .await
    .unwrap();

    assert_eq!(removed, 1);
    assert!(blobs.head("orphans/old").await.unwrap().is_none());
    assert!(blobs.head("orphans/referenced").await.unwrap().is_some());
    assert!(blobs.head("orphans/fresh").await.unwrap().is_some());
    assert!(blobs.head("elsewhere/old").await.unwrap().is_some());
}

#[tokio::test]
async fn unreferenced_sweep_stops_on_reference_check_error() {
    use crate::{MemoryStorage, Scope, StorageBackend, sweep_unreferenced};

    let blobs = MemoryStorage::new()
        .for_scope(&Scope::local())
        .unwrap()
        .blobs()
        .clone();
    blobs.put("orphans/old", vec![1], None).await.unwrap();

    let error = sweep_unreferenced(&*blobs, "orphans/", u64::MAX, |_| async {
        Err(crate::StorageError::backend("reference lookup failed"))
    })
    .await
    .unwrap_err();

    assert_eq!(error.kind(), crate::ErrorKind::Backend);
    assert!(blobs.head("orphans/old").await.unwrap().is_some());
}

#[tokio::test]
async fn cleanup_pages_include_expired_physical_blobs() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::{MemoryStorage, Scope, StorageBackend};

    let now = Arc::new(AtomicU64::new(10));
    let clock = Arc::clone(&now);
    let storage = MemoryStorage::with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let blobs = storage.for_scope(&Scope::local()).unwrap().blobs().clone();
    blobs
        .put_expiring("expired/physical", vec![1], None, Duration::from_millis(1))
        .await
        .unwrap();
    now.store(12, Ordering::SeqCst);

    assert!(blobs.get("expired/physical").await.unwrap().is_none());
    let page = blobs.list_sweep_page("expired/", None, 8).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].meta.key, "expired/physical");
    assert_eq!(page.items[0].written_at_ms, Some(10));
}

#[tokio::test]
async fn deletion_error_does_not_report_a_completed_sweep() {
    use serde_json::json;

    use crate::{CollectionSpec, Fence, MemoryStorage, Precondition, Scope, StorageBackend};

    let storage = MemoryStorage::new();
    let scope = Scope::local();
    let docs = storage.for_scope(&scope).unwrap().documents().clone();
    docs.ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    docs.put("leases", "sweeper", json!({"epoch": 1}), Precondition::None)
        .await
        .unwrap();
    let fence = Fence::epoch(scope.clone(), "leases", "sweeper", "epoch", 1);
    let fenced = storage.for_scope_fenced(&scope, &fence).unwrap();
    fenced
        .blobs()
        .put("guarded/orphan", vec![1], None)
        .await
        .unwrap();

    let result = sweep_unreferenced(fenced.blobs().as_ref(), "guarded/", u64::MAX, move |_| {
        let docs = docs.clone();
        async move {
            docs.put("leases", "sweeper", json!({"epoch": 2}), Precondition::None)
                .await?;
            Ok(false)
        }
    })
    .await;

    assert_eq!(result.unwrap_err().kind(), crate::ErrorKind::Fenced);
    assert!(
        storage
            .for_scope(&scope)
            .unwrap()
            .blobs()
            .head("guarded/orphan")
            .await
            .unwrap()
            .is_some()
    );
}
