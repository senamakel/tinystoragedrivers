//! `Blobs` selectors, including the two streamed transfers.
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use serde_json::Value;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::{MAX_INLINE_BLOB_BYTES, MAX_PAGE_ITEMS, MAX_PRESIGN_TTL_SECS};
use tinystoragedrivers_core::{BlobMeta, Capability};
use tokio::io::{AsyncRead, ReadBuf};

use super::{Reply, StorageServer, decode, encode, invalid, native};
use crate::transport::BlobBody;

pub(crate) fn info(meta: BlobMeta) -> wire::BlobInfo {
    wire::BlobInfo {
        key: meta.key,
        len: meta.len,
        content_type: meta.content_type,
    }
}

/// Fails a read that would take more than the declared number of bytes, so a
/// body longer than `len` is an error rather than a silent truncation.
struct Exact {
    inner: BlobBody,
    remaining: u64,
}

impl AsyncRead for Exact {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let read = (buf.filled().len() - before) as u64;
            if read > self.remaining {
                return Poll::Ready(Err(std::io::Error::other(
                    "stream body exceeds its declared length",
                )));
            }
            self.remaining -= read;
        }
        result
    }
}

impl StorageServer {
    pub(super) async fn blobs(&self, selector: &str, request: Value) -> Reply {
        let blobs = self.storage.blobs();
        match selector {
            "Put" => {
                let r: wire::PutBlobRequest = decode(request)?;
                if r.bytes.len() > MAX_INLINE_BLOB_BYTES {
                    return Err(invalid());
                }
                let content_type = r.content_type.as_deref();
                let meta = match r.ttl_ms {
                    None => native(blobs.put(&r.key, r.bytes, content_type).await)?,
                    Some(ttl_ms) => native(
                        blobs
                            .put_expiring(
                                &r.key,
                                r.bytes,
                                content_type,
                                Duration::from_millis(ttl_ms),
                            )
                            .await,
                    )?,
                };
                encode(&info(meta))
            }
            "PutUntil" => self.put_until(request).await,
            "Get" => {
                let r: wire::BlobKeyRequest = decode(request)?;
                let Some(meta) = native(blobs.head(&r.key).await)? else {
                    return encode(&None::<wire::BlobBody>);
                };
                if meta.len > MAX_INLINE_BLOB_BYTES as u64 {
                    return Err(invalid());
                }
                let found = native(blobs.get(&r.key).await)?;
                encode(&found.map(|blob| wire::BlobBody {
                    meta: info(blob.meta),
                    bytes: blob.bytes,
                }))
            }
            "GetWithExpiry" => self.get_with_expiry(request).await,
            "GetRange" => {
                let r: wire::GetRangeRequest = decode(request)?;
                if r.end < r.start || r.end - r.start > MAX_INLINE_BLOB_BYTES as u64 {
                    return Err(invalid());
                }
                encode(&native(blobs.get_range(&r.key, r.start..r.end).await)?)
            }
            "Head" => {
                let r: wire::BlobKeyRequest = decode(request)?;
                encode(&native(blobs.head(&r.key).await)?.map(info))
            }
            "Delete" => {
                let r: wire::BlobKeyRequest = decode(request)?;
                encode(&native(blobs.delete(&r.key).await)?)
            }
            "List" => {
                let r: wire::ListBlobsRequest = decode(request)?;
                let listed = native(blobs.list(&r.prefix).await)?;
                if listed.len() > MAX_PAGE_ITEMS {
                    return Err(invalid());
                }
                encode(&listed.into_iter().map(info).collect::<Vec<_>>())
            }
            "PresignGet" | "PresignPut" => {
                let r: wire::PresignRequest = decode(request)?;
                if r.ttl_secs == 0 || r.ttl_secs > MAX_PRESIGN_TTL_SECS {
                    return Err(invalid());
                }
                let ttl = Duration::from_secs(r.ttl_secs);
                let url = if selector == "PresignGet" {
                    native(blobs.presign_get(&r.key, ttl).await)?
                } else {
                    native(blobs.presign_put(&r.key, ttl).await)?
                };
                encode(&wire::PresignedUrl { url })
            }
            "Capabilities" => {
                if !request.is_null() {
                    return Err(invalid());
                }
                encode(&wire::BlobCapabilities {
                    presigned_urls: blobs.capabilities().contains(Capability::PresignedUrls),
                    ttl: blobs.capabilities().contains(Capability::BlobTtl),
                    max_key_len: blobs.max_key_len(),
                })
            }
            // Streamed transfers carry a body, so they are not request/reply.
            _ => Err(invalid()),
        }
    }

    async fn put_until(&self, request: Value) -> Reply {
        let r: wire::PutBlobUntilRequest = decode(request)?;
        if r.bytes.len() > MAX_INLINE_BLOB_BYTES {
            return Err(invalid());
        }
        let meta = native(
            self.storage
                .blobs()
                .put_until(&r.key, r.bytes, r.content_type.as_deref(), r.expires_at_ms)
                .await,
        )?;
        encode(&info(meta))
    }

    async fn get_with_expiry(&self, request: Value) -> Reply {
        let r: wire::BlobKeyRequest = decode(request)?;
        let blobs = self.storage.blobs();
        let Some(head) = native(blobs.head(&r.key).await)? else {
            return encode(&None::<wire::BlobWithExpiry>);
        };
        if head.len > MAX_INLINE_BLOB_BYTES as u64 {
            return Err(invalid());
        }
        let found = native(blobs.get_with_expiry(&r.key).await)?;
        if found
            .as_ref()
            .is_some_and(|source| source.blob.bytes.len() > MAX_INLINE_BLOB_BYTES)
        {
            return Err(invalid());
        }
        encode(&found.map(|source| wire::BlobWithExpiry {
            meta: info(source.blob.meta),
            expires_at_ms: source.expires_at_ms,
            bytes: source.blob.bytes,
        }))
    }

    pub(super) async fn put_stream(
        &self,
        head: wire::PutBlobStreamRequest,
        body: BlobBody,
    ) -> Result<wire::BlobInfo, wire::StorageFailure> {
        let reader: BlobBody = Box::pin(Exact {
            inner: body,
            remaining: head.len,
        });
        let blobs = self.storage.blobs();
        let content_type = head.content_type.as_deref();
        let meta = match head.ttl_ms {
            None => native(
                blobs
                    .put_stream(&head.key, reader, head.len, content_type)
                    .await,
            )?,
            Some(ttl_ms) => native(
                blobs
                    .put_stream_expiring(
                        &head.key,
                        reader,
                        head.len,
                        content_type,
                        Duration::from_millis(ttl_ms),
                    )
                    .await,
            )?,
        };
        Ok(info(meta))
    }

    pub(super) async fn put_stream_until(
        &self,
        head: wire::PutBlobStreamUntilRequest,
        body: BlobBody,
    ) -> Result<wire::BlobInfo, wire::StorageFailure> {
        let reader: BlobBody = Box::pin(Exact {
            inner: body,
            remaining: head.len,
        });
        let blobs = self.storage.blobs();
        let meta = native(
            blobs
                .put_stream_until(
                    &head.key,
                    reader,
                    head.len,
                    head.content_type.as_deref(),
                    head.expires_at_ms,
                )
                .await,
        )?;
        Ok(info(meta))
    }

    pub(super) async fn get_stream(
        &self,
        request: wire::BlobKeyRequest,
    ) -> Result<Option<(wire::BlobStreamHead, BlobBody)>, wire::StorageFailure> {
        let found = native(self.storage.blobs().get_stream(&request.key).await)?;
        Ok(found.map(|stream| {
            (
                wire::BlobStreamHead {
                    meta: info(stream.meta),
                },
                stream.reader,
            )
        }))
    }

    pub(super) async fn get_stream_with_expiry(
        &self,
        request: wire::BlobKeyRequest,
    ) -> Result<Option<(wire::BlobStreamHeadWithExpiry, BlobBody)>, wire::StorageFailure> {
        let found = native(
            self.storage
                .blobs()
                .get_stream_with_expiry(&request.key)
                .await,
        )?;
        Ok(found.map(|source| {
            (
                wire::BlobStreamHeadWithExpiry {
                    meta: info(source.meta),
                    expires_at_ms: source.expires_at_ms,
                },
                source.reader,
            )
        }))
    }
}
