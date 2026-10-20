//! `BlobStore` over the `Blobs` object; large bodies ride a stream.
use std::ops::Range;
use std::time::Duration;

use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::MAX_INLINE_BLOB_BYTES;
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobReader, BlobStore, BlobStream, Capabilities, Capability, Result,
    StorageError, async_trait, clamp_range,
};
use tokio::io::AsyncReadExt;

use super::{Wire, wire_error};
use crate::transport::Object;

/// The blob port over a bus.
#[derive(Clone, Debug)]
pub struct BusBlobs {
    wire: Wire,
    server: wire::BlobCapabilities,
}

impl BusBlobs {
    pub(super) fn new(wire: Wire, server: wire::BlobCapabilities) -> Self {
        Self { wire, server }
    }

    async fn call<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        selector: &str,
        request: &Req,
        unsupported_as: Option<Capability>,
    ) -> Result<Resp> {
        self.wire
            .call(Object::Blobs, selector, request, unsupported_as)
            .await
    }

    /// An inline write, or a streamed one when the body is over the inline cap.
    /// A write with a lifetime on a server without `ttl` fails before any I/O.
    async fn write(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        self.require_ttl(ttl_ms)?;
        if bytes.len() as u64 > inline_limit() {
            let len = bytes.len() as u64;
            return self
                .write_stream(
                    key,
                    Box::pin(std::io::Cursor::new(bytes)),
                    len,
                    content_type,
                    ttl_ms,
                )
                .await;
        }
        let request = wire::PutBlobRequest {
            key: key.to_owned(),
            bytes,
            content_type: content_type.map(str::to_owned),
            ttl_ms,
        };
        Ok(meta(
            self.call("Put", &request, ttl_ms.map(|_| Capability::BlobTtl))
                .await?,
        ))
    }

    async fn write_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        self.require_ttl(ttl_ms)?;
        let head = wire::PutBlobStreamRequest {
            key: key.to_owned(),
            len,
            content_type: content_type.map(str::to_owned),
            ttl_ms,
        };
        let reply = self
            .wire
            .transport()
            .put_blob_stream(head, reader)
            .await
            .map_err(|error| wire_error(error, ttl_ms.map(|_| Capability::BlobTtl)))?;
        Ok(meta(serde_json::from_value(reply)?))
    }

    async fn write_until(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        if bytes.len() as u64 > inline_limit() {
            let len = bytes.len() as u64;
            return self
                .write_stream_until(
                    key,
                    Box::pin(std::io::Cursor::new(bytes)),
                    len,
                    content_type,
                    expires_at_ms,
                )
                .await;
        }
        let request = wire::PutBlobUntilRequest {
            key: key.to_owned(),
            bytes,
            content_type: content_type.map(str::to_owned),
            expires_at_ms,
        };
        let reply = self
            .wire
            .transport()
            .put_blob_until(request)
            .await
            .map_err(|error| wire_error(error, Some(Capability::BlobTtl)))?;
        Ok(meta(serde_json::from_value(reply)?))
    }

    async fn write_stream_until(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        let head = wire::PutBlobStreamUntilRequest {
            key: key.to_owned(),
            len,
            content_type: content_type.map(str::to_owned),
            expires_at_ms,
        };
        let reply = self
            .wire
            .transport()
            .put_blob_stream_until(head, reader)
            .await
            .map_err(|error| wire_error(error, Some(Capability::BlobTtl)))?;
        Ok(meta(serde_json::from_value(reply)?))
    }

    fn require_ttl(&self, ttl_ms: Option<u64>) -> Result<()> {
        if ttl_ms.is_some() && !self.server.ttl {
            return Err(StorageError::unsupported(
                Capability::BlobTtl,
                "the storage host's blob handle cannot expire blobs",
            ));
        }
        Ok(())
    }
}

fn meta(info: wire::BlobInfo) -> BlobMeta {
    BlobMeta {
        key: info.key,
        len: info.len,
        content_type: info.content_type,
    }
}

fn key(key: &str) -> wire::BlobKeyRequest {
    wire::BlobKeyRequest {
        key: key.to_owned(),
    }
}

/// A lifetime on the wire: whole milliseconds, saturating.
fn ttl_ms(ttl: Duration) -> u64 {
    u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
}

fn inline_limit() -> u64 {
    MAX_INLINE_BLOB_BYTES as u64
}

#[async_trait]
impl BlobStore for BusBlobs {
    fn max_key_len(&self) -> usize {
        self.server.max_key_len
    }

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::none().with(Capability::StreamingBlobs);
        if self.server.presigned_urls {
            capabilities = capabilities.with(Capability::PresignedUrls);
        }
        if self.server.ttl {
            capabilities = capabilities.with(Capability::BlobTtl);
        }
        capabilities
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.write(key, bytes, content_type, None).await
    }

    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.write(key, bytes, content_type, Some(ttl_ms(ttl)))
            .await
    }

    async fn put_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.write_stream(key, reader, len, content_type, None)
            .await
    }

    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.write_stream(key, reader, len, content_type, Some(ttl_ms(ttl)))
            .await
    }

    async fn put_until(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        self.write_until(key, bytes, content_type, expires_at_ms)
            .await
    }

    async fn put_stream_until(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        self.write_stream_until(key, reader, len, content_type, expires_at_ms)
            .await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        let found = self
            .wire
            .transport()
            .get_blob_stream(self::key(key))
            .await
            .map_err(|error| wire_error(error, None))?;
        Ok(found.map(|(head, reader)| BlobStream {
            meta: meta(head.meta),
            reader,
        }))
    }

    async fn get_stream_with_expiry(
        &self,
        key: &str,
    ) -> Result<Option<tinystoragedrivers_core::BlobSourceStream>> {
        let found = self
            .wire
            .transport()
            .get_blob_stream_with_expiry(self::key(key))
            .await
            .map_err(|error| wire_error(error, Some(Capability::BlobTtl)))?;
        Ok(
            found.map(|(head, reader)| tinystoragedrivers_core::BlobSourceStream {
                meta: meta(head.meta),
                expires_at_ms: head.expires_at_ms,
                reader,
            }),
        )
    }

    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        let request = wire::PresignRequest {
            key: key.to_owned(),
            ttl_secs: ttl.as_secs(),
        };
        let url: wire::PresignedUrl = self
            .call("PresignGet", &request, Some(Capability::PresignedUrls))
            .await?;
        Ok(url.url)
    }

    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        let request = wire::PresignRequest {
            key: key.to_owned(),
            ttl_secs: ttl.as_secs(),
        };
        let url: wire::PresignedUrl = self
            .call("PresignPut", &request, Some(Capability::PresignedUrls))
            .await?;
        Ok(url.url)
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        let Some(head) = self.head(key).await? else {
            return Ok(None);
        };
        if head.len <= inline_limit() {
            let body: Option<wire::BlobBody> = self.call("Get", &self::key(key), None).await?;
            return Ok(body.map(|body| Blob {
                meta: meta(body.meta),
                bytes: body.bytes,
            }));
        }
        let Some(mut stream) = self.get_stream(key).await? else {
            return Ok(None);
        };
        let mut bytes = Vec::with_capacity(usize::try_from(head.len).unwrap_or(0));
        stream
            .reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| StorageError::unavailable("blob stream failed"))?;
        Ok(Some(Blob {
            meta: stream.meta,
            bytes,
        }))
    }

    async fn get_with_expiry(
        &self,
        key: &str,
    ) -> Result<Option<tinystoragedrivers_core::BlobSource>> {
        let Some(mut source) = self.get_stream_with_expiry(key).await? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        source
            .reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| StorageError::unavailable("blob stream failed"))?;
        Ok(Some(tinystoragedrivers_core::BlobSource {
            blob: Blob {
                meta: source.meta,
                bytes,
            },
            expires_at_ms: source.expires_at_ms,
        }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        let Some(head) = self.head(key).await? else {
            return Ok(None);
        };
        let len = usize::try_from(head.len).unwrap_or(usize::MAX);
        let span = clamp_range(&range, len)?;
        let (mut start, end) = (span.start as u64, span.end as u64);
        let mut out = Vec::with_capacity(span.len());
        while start < end {
            let to = end.min(start + inline_limit());
            let request = wire::GetRangeRequest {
                key: key.to_owned(),
                start,
                end: to,
            };
            let chunk: Option<Vec<u8>> = self.call("GetRange", &request, None).await?;
            let Some(chunk) = chunk else {
                return Ok(None);
            };
            out.extend_from_slice(&chunk);
            start = to;
        }
        Ok(Some(out))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        let info: Option<wire::BlobInfo> = self.call("Head", &self::key(key), None).await?;
        Ok(info.map(meta))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        self.call("Delete", &self::key(key), None).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        let request = wire::ListBlobsRequest {
            prefix: prefix.to_owned(),
        };
        let listed: Vec<wire::BlobInfo> = self.call("List", &request, None).await?;
        Ok(listed.into_iter().map(meta).collect())
    }
}

#[cfg(test)]
#[path = "blobs_tests.rs"]
mod tests;
