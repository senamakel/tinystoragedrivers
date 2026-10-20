//! Expiry-aware blob operations fail closed on legacy transports.

use super::*;
use crate::client::Wire;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinystoragedrivers_bus::BlobCapabilities;
use tokio::sync::broadcast;

#[derive(Debug)]
struct LegacyTransport(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl crate::StorageTransport for LegacyTransport {
    async fn call(
        &self,
        _: crate::Object,
        _: &str,
        _: serde_json::Value,
    ) -> std::result::Result<serde_json::Value, crate::WireError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(crate::WireError::Transport("legacy call".to_owned()))
    }

    async fn put_blob_stream(
        &self,
        _: tinystoragedrivers_bus::PutBlobStreamRequest,
        _: crate::BlobBody,
    ) -> std::result::Result<serde_json::Value, crate::WireError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(crate::WireError::Transport("legacy stream put".to_owned()))
    }

    async fn get_blob_stream(
        &self,
        _: tinystoragedrivers_bus::BlobKeyRequest,
    ) -> std::result::Result<
        Option<(tinystoragedrivers_bus::BlobStreamHead, crate::BlobBody)>,
        crate::WireError,
    > {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(crate::WireError::Transport("legacy stream get".to_owned()))
    }

    fn signals(&self) -> broadcast::Receiver<tinystoragedrivers_bus::WatchChanged> {
        let (_sender, receiver) = broadcast::channel(1);
        receiver
    }
}

#[tokio::test]
async fn expiry_methods_refuse_legacy_transports_without_plain_blob_fallback() {
    let calls = Arc::new(AtomicUsize::new(0));
    let blobs = BusBlobs::new(
        Wire::new(Arc::new(LegacyTransport(Arc::clone(&calls)))),
        BlobCapabilities {
            presigned_urls: false,
            ttl: false,
            max_key_len: 1024,
        },
    );

    for error in [
        blobs.get_with_expiry("key").await.unwrap_err(),
        blobs.get_stream_with_expiry("key").await.unwrap_err(),
        blobs
            .put_until("key", vec![1], None, Some(5_000))
            .await
            .unwrap_err(),
        blobs
            .put_stream_until(
                "key",
                Box::pin(std::io::Cursor::new(vec![1])),
                1,
                None,
                Some(5_000),
            )
            .await
            .unwrap_err(),
    ] {
        assert_eq!(
            error.kind(),
            tinystoragedrivers_core::ErrorKind::Unsupported(Capability::BlobTtl)
        );
    }
    let large = vec![2; tinystoragedrivers_bus::MAX_INLINE_BLOB_BYTES + 1];
    assert_eq!(
        blobs
            .put_until("large", large, None, Some(5_000))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Unsupported(Capability::BlobTtl)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
