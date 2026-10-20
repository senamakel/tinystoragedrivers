//! In-process transport: a client wired straight to a [`StorageServer`].
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_bus::{
    BLOB_STREAM_CHUNK_BYTES, BlobKeyRequest, BlobStreamHead, BlobStreamHeadWithExpiry,
    BlobWithExpiry, MAX_FRAME_BYTES, PutBlobStreamRequest, PutBlobStreamUntilRequest,
    PutBlobUntilRequest, WatchChanged,
};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::broadcast;

use crate::server::StorageServer;
use crate::transport::{Admission, BlobBody, Object, StorageTransport, WireError};

/// A [`StorageTransport`] that calls a server in the same process.
///
/// Every request and reply is serialized to JSON text, checked against
/// `MAX_FRAME_BYTES` and parsed again, so what crosses is exactly what a bus
/// would carry. Blob bodies move in `BLOB_STREAM_CHUNK_BYTES` reads, as a bus
/// stream does.
#[derive(Debug)]
pub struct Loopback {
    server: Arc<StorageServer>,
    admission: Admission,
    largest_frame: AtomicUsize,
    streamed_bytes: Arc<AtomicU64>,
}

impl Loopback {
    /// Connect to `server` as a caller admitted with `admission`.
    #[must_use]
    pub fn new(server: Arc<StorageServer>, admission: Admission) -> Self {
        Self {
            server,
            admission,
            largest_frame: AtomicUsize::new(0),
            streamed_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The largest request or reply frame seen, in bytes.
    #[must_use]
    pub fn largest_frame(&self) -> usize {
        self.largest_frame.load(Ordering::Relaxed)
    }

    /// Total blob bytes that travelled over streams in either direction.
    #[must_use]
    pub fn streamed_bytes(&self) -> u64 {
        self.streamed_bytes.load(Ordering::Relaxed)
    }

    /// Serialize as a frame, enforce the cap and parse it back.
    fn frame(&self, value: &Value) -> Result<Value, WireError> {
        let text = serde_json::to_string(value)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        self.largest_frame.fetch_max(text.len(), Ordering::Relaxed);
        if text.len() > MAX_FRAME_BYTES {
            return Err(WireError::Transport("frame exceeds the 16 MiB cap".into()));
        }
        serde_json::from_str(&text).map_err(|_| WireError::Transport("frame decode failed".into()))
    }

    fn chunked(&self, body: BlobBody) -> BlobBody {
        Box::pin(Chunked {
            inner: body,
            moved: Arc::clone(&self.streamed_bytes),
        })
    }
}

/// Reads at most one stream chunk at a time and counts the bytes moved.
struct Chunked {
    inner: BlobBody,
    moved: Arc<AtomicU64>,
}

impl AsyncRead for Chunked {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let want = buf.remaining().min(BLOB_STREAM_CHUNK_BYTES);
        let mut chunk = vec![0u8; want];
        let mut window = ReadBuf::new(&mut chunk);
        match Pin::new(&mut self.inner).poll_read(cx, &mut window) {
            Poll::Ready(Ok(())) => {
                let filled = window.filled();
                self.moved.fetch_add(filled.len() as u64, Ordering::Relaxed);
                buf.put_slice(filled);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

fn wire_failure(failure: tinystoragedrivers_bus::StorageFailure) -> WireError {
    WireError::Storage(failure)
}

#[async_trait]
impl StorageTransport for Loopback {
    async fn call(
        &self,
        object: Object,
        selector: &str,
        request: Value,
    ) -> Result<Value, WireError> {
        let request = self.frame(&request)?;
        let reply = self
            .server
            .handle(self.admission, object.as_str(), selector, request)
            .await
            .map_err(wire_failure)?;
        self.frame(&reply)
    }

    async fn put_blob_stream(
        &self,
        head: PutBlobStreamRequest,
        body: BlobBody,
    ) -> Result<Value, WireError> {
        let head = serde_json::to_value(&head)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        let head = serde_json::from_value(self.frame(&head)?)
            .map_err(|_| WireError::Transport("frame decode failed".into()))?;
        let info = self
            .server
            .put_blob_stream(head, self.chunked(body))
            .await
            .map_err(wire_failure)?;
        let info = serde_json::to_value(&info)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        self.frame(&info)
    }

    async fn get_blob_stream(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobBody)>, WireError> {
        let found = self
            .server
            .get_blob_stream(request)
            .await
            .map_err(wire_failure)?;
        Ok(found.map(|(head, body)| (head, self.chunked(body))))
    }

    async fn put_blob_until(&self, request: PutBlobUntilRequest) -> Result<Value, WireError> {
        let request = self.frame(
            &serde_json::to_value(request)
                .map_err(|_| WireError::Transport("frame encode failed".into()))?,
        )?;
        let reply = self
            .server
            .handle(
                Admission::Private,
                Object::Blobs.as_str(),
                "PutUntil",
                request,
            )
            .await
            .map_err(wire_failure)?;
        self.frame(&reply)
    }

    async fn put_blob_stream_until(
        &self,
        head: PutBlobStreamUntilRequest,
        body: BlobBody,
    ) -> Result<Value, WireError> {
        let head = serde_json::to_value(&head)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        let head = serde_json::from_value(self.frame(&head)?)
            .map_err(|_| WireError::Transport("frame decode failed".into()))?;
        let info = self
            .server
            .put_blob_stream_until(head, self.chunked(body))
            .await
            .map_err(wire_failure)?;
        let info = serde_json::to_value(&info)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        self.frame(&info)
    }

    async fn get_blob_with_expiry(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<BlobWithExpiry>, WireError> {
        let request = self.frame(
            &serde_json::to_value(request)
                .map_err(|_| WireError::Transport("frame encode failed".into()))?,
        )?;
        let reply = self
            .server
            .handle(
                Admission::Private,
                Object::Blobs.as_str(),
                "GetWithExpiry",
                request,
            )
            .await
            .map_err(wire_failure)?;
        serde_json::from_value(self.frame(&reply)?)
            .map_err(|_| WireError::Transport("frame decode failed".into()))
    }

    async fn get_blob_stream_with_expiry(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHeadWithExpiry, BlobBody)>, WireError> {
        let found = self
            .server
            .get_blob_stream_with_expiry(request)
            .await
            .map_err(wire_failure)?;
        Ok(found.map(|(head, body)| (head, self.chunked(body))))
    }

    fn signals(&self) -> broadcast::Receiver<WatchChanged> {
        self.server.signals()
    }
}

#[cfg(test)]
#[path = "loopback_tests.rs"]
mod tests;
