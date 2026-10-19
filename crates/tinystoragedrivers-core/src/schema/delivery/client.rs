//! Fragmentation and local blob/stream policies, without storage side effects.
use super::{
    BlobTargetRequest, ExchangeErrorKind, MIGRATION_CHUNK_BYTES, MigrationExchangeReply,
    MigrationExchangeRequest, MigrationPayload, MigrationPayloadReply, MigrationTransport,
    StreamEntriesRequest, StreamMapMode, StreamPlanRequest, types,
};
use crate::schema::{
    BlobTransformOutput, BlobTransformRequest, BoxFuture, DocumentsTransformRequest, Owner,
    StreamTransformOutput, StreamTransformRequest, TargetRecord, TransformExecutor,
};
use crate::{Result, StorageError};
use std::{sync::Arc, time::Duration};

/// Transport-independent external transform executor. Whole results are returned
/// before the durable runner writes or advances progress.
pub struct MigrationDeliveryClient {
    transport: Arc<dyn MigrationTransport>,
}
struct Abandon {
    transport: Arc<dyn MigrationTransport>,
    owner: String,
    transfer: Option<String>,
    runtime: tokio::runtime::Handle,
}
impl Drop for Abandon {
    fn drop(&mut self) {
        let Some(transfer) = self.transfer.take() else {
            return;
        };
        let transport = Arc::clone(&self.transport);
        let owner = self.owner.clone();
        self.runtime.spawn(async move {
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                transport.exchange(&owner, MigrationExchangeRequest::Cancel { transfer }),
            )
            .await;
        });
    }
}
fn unavailable() -> StorageError {
    StorageError::unavailable("migration delivery unavailable")
}

impl MigrationDeliveryClient {
    /// Build using an adapter contractually confidential for every message.
    #[must_use]
    pub fn new(transport: Arc<dyn MigrationTransport>) -> Self {
        Self { transport }
    }
    async fn exchange(
        &self,
        owner: &str,
        request: MigrationExchangeRequest,
    ) -> Result<MigrationExchangeReply> {
        self.transport
            .exchange(owner, request)
            .await
            .map_err(|_| unavailable())
    }
    async fn upload(&self, owner: &str, transfer: &str, bytes: &[u8]) -> Result<()> {
        let mut offset = 0u64;
        for fragment in bytes.chunks(MIGRATION_CHUNK_BYTES) {
            let mut chunk = Vec::new();
            chunk
                .try_reserve(fragment.len())
                .map_err(|_| unavailable())?;
            chunk.extend_from_slice(fragment);
            let expected = offset
                .checked_add(fragment.len() as u64)
                .ok_or_else(unavailable)?;
            if self
                .exchange(
                    owner,
                    MigrationExchangeRequest::Upload {
                        transfer: transfer.to_owned(),
                        offset,
                        bytes: chunk,
                    },
                )
                .await?
                != (MigrationExchangeReply::Uploaded {
                    next_offset: expected,
                })
            {
                return Err(unavailable());
            }
            offset = expected;
        }
        Ok(())
    }
    async fn download(&self, owner: &str, transfer: &str, reply_len: u64) -> Result<Vec<u8>> {
        let mut reply = Vec::new();
        while (reply.len() as u64) < reply_len {
            let offset = reply.len() as u64;
            let MigrationExchangeReply::Chunk {
                offset: received,
                bytes,
            } = self
                .exchange(
                    owner,
                    MigrationExchangeRequest::Download {
                        transfer: transfer.to_owned(),
                        offset,
                    },
                )
                .await?
            else {
                return Err(unavailable());
            };
            let next = offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(unavailable)?;
            if received != offset
                || bytes.is_empty()
                || bytes.len() > MIGRATION_CHUNK_BYTES
                || next > reply_len
            {
                return Err(unavailable());
            }
            reply.try_reserve(bytes.len()).map_err(|_| unavailable())?;
            reply.extend_from_slice(&bytes);
        }
        Ok(reply)
    }
    /// Deliver one complete payload and return its complete typed reply.
    ///
    /// # Errors
    /// Returns a static `InvalidInput` failure when the owner conversion fails,
    /// or `Unavailable` on transport, allocation, invalid reply or protocol
    /// ambiguity. The durable runner remains pending.
    pub async fn transform(
        &self,
        owner: &str,
        payload: MigrationPayload,
    ) -> Result<MigrationPayloadReply> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| unavailable())?;
        let bytes = types::encode(&payload).map_err(|()| unavailable())?;
        let request_len = u64::try_from(bytes.len()).map_err(|_| unavailable())?;
        let MigrationExchangeReply::Begun { transfer } = self
            .exchange(owner, MigrationExchangeRequest::Begin { request_len })
            .await?
        else {
            return Err(unavailable());
        };
        if !types::valid_id(&transfer) {
            return Err(unavailable());
        }
        let mut abandon = Abandon {
            transport: Arc::clone(&self.transport),
            owner: owner.into(),
            transfer: Some(transfer.clone()),
            runtime,
        };
        self.upload(owner, &transfer, &bytes).await?;
        drop(bytes);
        let reply_len = match self
            .exchange(
                owner,
                MigrationExchangeRequest::Execute {
                    transfer: transfer.clone(),
                },
            )
            .await?
        {
            MigrationExchangeReply::Ready { reply_len } => reply_len,
            MigrationExchangeReply::Error {
                kind: ExchangeErrorKind::TransformFailed,
            } => {
                return Err(StorageError::invalid_input("migration transform failed"));
            }
            _ => return Err(unavailable()),
        };
        let reply = self.download(owner, &transfer, reply_len).await?;
        let result: MigrationPayloadReply =
            serde_json::from_slice(&reply).map_err(|_| unavailable())?;
        let valid = match (&payload, &result) {
            (MigrationPayload::Documents(_), MigrationPayloadReply::Documents(r)) => r.is_ok(),
            (MigrationPayload::BlobTarget(_), MigrationPayloadReply::BlobTarget(r)) => r.is_ok(),
            (MigrationPayload::StreamPlan(_), MigrationPayloadReply::StreamPlan(r)) => r.is_ok(),
            (MigrationPayload::StreamEntries(_), MigrationPayloadReply::StreamEntries(r)) => {
                r.is_ok()
            }
            _ => false,
        };
        if !valid {
            return Err(unavailable());
        }
        if self
            .exchange(owner, MigrationExchangeRequest::Finish { transfer })
            .await?
            != MigrationExchangeReply::Finished
        {
            return Err(unavailable());
        }
        abandon.transfer = None;
        Ok(result)
    }
}
impl TransformExecutor for MigrationDeliveryClient {
    fn documents<'a>(
        &'a self,
        owner: &'a Owner,
        request: DocumentsTransformRequest,
    ) -> BoxFuture<'a, Result<Vec<TargetRecord>>> {
        Box::pin(async move {
            match self
                .transform(owner.as_str(), MigrationPayload::Documents(request))
                .await?
            {
                MigrationPayloadReply::Documents(Ok(records)) => Ok(records),
                _ => Err(unavailable()),
            }
        })
    }
    fn blob<'a>(
        &'a self,
        owner: &'a Owner,
        request: BlobTransformRequest,
    ) -> BoxFuture<'a, Result<Option<BlobTransformOutput>>> {
        Box::pin(async move {
            let payload = BlobTargetRequest {
                operation: request.operation,
                version: request.version,
                source: request.source,
            };
            match self
                .transform(owner.as_str(), MigrationPayload::BlobTarget(payload))
                .await?
            {
                MigrationPayloadReply::BlobTarget(Ok(target)) => {
                    Ok(target.map(|target| BlobTransformOutput {
                        target,
                        content_type: request.content_type,
                        bytes: request.bytes,
                    }))
                }
                _ => Err(unavailable()),
            }
        })
    }
    fn stream<'a>(
        &'a self,
        owner: &'a Owner,
        request: StreamTransformRequest,
    ) -> BoxFuture<'a, Result<Option<StreamTransformOutput>>> {
        Box::pin(async move {
            let payload = StreamPlanRequest {
                operation: request.operation.clone(),
                version: request.version,
                source: request.source,
            };
            let MigrationPayloadReply::StreamPlan(Ok(plan)) = self
                .transform(owner.as_str(), MigrationPayload::StreamPlan(payload))
                .await?
            else {
                return Err(unavailable());
            };
            let Some(plan) = plan else {
                return Ok(None);
            };
            let entries = match plan.mode {
                StreamMapMode::Identity => request.entries,
                StreamMapMode::IndependentEntries => {
                    let mut input = request.entries.into_iter();
                    let mut output = Vec::new();
                    loop {
                        let entries: Vec<_> = input.by_ref().take(256).collect();
                        if entries.is_empty() {
                            break;
                        }
                        let payload = StreamEntriesRequest {
                            operation: request.operation.clone(),
                            version: request.version,
                            entries,
                        };
                        let MigrationPayloadReply::StreamEntries(Ok(mut mapped)) = self
                            .transform(owner.as_str(), MigrationPayload::StreamEntries(payload))
                            .await?
                        else {
                            return Err(unavailable());
                        };
                        output
                            .try_reserve(mapped.len())
                            .map_err(|_| unavailable())?;
                        output.append(&mut mapped);
                    }
                    output
                }
            };
            Ok(Some(StreamTransformOutput {
                target: plan.target,
                entries,
            }))
        })
    }
}

impl std::fmt::Debug for MigrationDeliveryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationDeliveryClient")
            .finish_non_exhaustive()
    }
}
