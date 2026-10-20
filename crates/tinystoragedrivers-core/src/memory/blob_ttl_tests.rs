//! Blob lifetimes against a driven clock.
use super::*;
use crate::{BlobStore, ErrorKind, Scope};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn driven() -> (MemoryStorage, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(1_000));
    let reading = Arc::clone(&now);
    (
        MemoryStorage::with_clock(Arc::new(move || reading.load(Ordering::SeqCst))),
        now,
    )
}

#[tokio::test]
async fn a_blob_expires_exactly_when_its_lifetime_has_elapsed() {
    let (storage, now) = driven();
    let blobs = storage.for_scope(&Scope::local()).unwrap();
    let blobs = blobs.blobs();
    blobs
        .put_expiring("k", vec![1, 2], Some("a/b"), Duration::from_millis(500))
        .await
        .unwrap();
    now.store(1_499, Ordering::SeqCst);
    assert_eq!(blobs.get("k").await.unwrap().unwrap().bytes, [1, 2]);
    assert_eq!(blobs.list("").await.unwrap().len(), 1);
    now.store(1_500, Ordering::SeqCst);
    assert!(blobs.get("k").await.unwrap().is_none());
    assert!(blobs.head("k").await.unwrap().is_none());
    assert!(blobs.get_range("k", 0..1).await.unwrap().is_none());
    assert!(blobs.get_stream("k").await.unwrap().is_none());
    assert_eq!(blobs.list("").await.unwrap().len(), 0);
    assert!(!blobs.delete("k").await.unwrap());
}

#[tokio::test]
async fn absolute_blob_deadlines_are_not_restarted_or_visible_on_arrival() {
    let (storage, now) = driven();
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
}

#[tokio::test]
async fn a_plain_put_clears_a_lifetime_and_lapsed_blobs_are_swept_by_lifetime_writes() {
    let (storage, now) = driven();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    let blobs = scoped.blobs();
    blobs
        .put_expiring("k", vec![1], None, Duration::from_millis(10))
        .await
        .unwrap();
    blobs.put("k", vec![2], None).await.unwrap();
    now.store(1_000_000, Ordering::SeqCst);
    assert_eq!(blobs.get("k").await.unwrap().unwrap().bytes, [2]);

    blobs
        .put_expiring("gone", vec![1], None, Duration::from_millis(10))
        .await
        .unwrap();
    now.store(2_000_000, Ordering::SeqCst);
    blobs
        .put_expiring("other", vec![1], None, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        !storage
            .db
            .lock()
            .unwrap()
            .blobs
            .contains_key(&("local".to_owned(), "gone".to_owned())),
        "the lapsed blob was dropped by the next lifetime write"
    );
}

#[tokio::test]
async fn an_unsupported_default_driver_refuses_lifetimes() {
    #[derive(Debug)]
    struct Plain;
    #[async_trait::async_trait]
    impl BlobStore for Plain {
        async fn put(
            &self,
            key: &str,
            _: Vec<u8>,
            _: Option<&str>,
        ) -> crate::Result<crate::BlobMeta> {
            Ok(crate::BlobMeta {
                key: key.into(),
                len: 0,
                content_type: None,
            })
        }
        async fn get(&self, _: &str) -> crate::Result<Option<crate::Blob>> {
            Ok(None)
        }
        async fn get_range(
            &self,
            _: &str,
            _: std::ops::Range<u64>,
        ) -> crate::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn head(&self, _: &str) -> crate::Result<Option<crate::BlobMeta>> {
            Ok(None)
        }
        async fn delete(&self, _: &str) -> crate::Result<bool> {
            Ok(false)
        }
        async fn list(&self, _: &str) -> crate::Result<Vec<crate::BlobMeta>> {
            Ok(vec![])
        }
    }
    let error = Plain
        .put_expiring("k", vec![], None, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(crate::Capability::BlobTtl)
    );
    let error = Plain.get_with_expiry("k").await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(crate::Capability::BlobTtl)
    );
    let error = Plain.get_stream_with_expiry("k").await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(crate::Capability::BlobTtl)
    );
    let error = Plain.put_until("k", vec![], None, None).await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(crate::Capability::BlobTtl)
    );
    let error = Plain
        .put_stream_expiring(
            "k",
            Box::pin(std::io::Cursor::new(vec![])),
            0,
            None,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(crate::Capability::BlobTtl)
    );
}
