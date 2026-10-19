//! Fragmentation, local payload preservation and complete-stream regressions.
use super::*;
use crate::Meta;
use crate::schema::{
    BlobTransformRequest, DocumentsTransformRequest, Owner, SourceRecord, StreamTransformRequest,
    TargetRecord, TransformExecutor,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

const FRAME_CAP: usize = 16 * 1024 * 1024;
struct Clock;
impl ExchangeClock for Clock {
    fn now_ms(&self) -> u64 {
        0
    }
}
struct Ids(AtomicU64);
impl TransferIds for Ids {
    fn next_id(&self) -> String {
        format!("token{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}
struct Handler<F>(F);
impl<F: Fn(MigrationPayload) -> MigrationPayloadReply + Send + Sync> MigrationPayloadHandler
    for Handler<F>
{
    fn transform(&self, payload: MigrationPayload) -> MigrationPayloadReply {
        (self.0)(payload)
    }
}
pub(super) struct Transport {
    server: MigrationExchangeServer,
    max_frame: AtomicUsize,
    upload_bytes: AtomicUsize,
    calls: Mutex<Vec<&'static str>>,
}
impl Transport {
    pub(super) fn new(
        handler: impl Fn(MigrationPayload) -> MigrationPayloadReply + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            server: MigrationExchangeServer::new(
                Arc::new(Handler(handler)),
                Arc::new(Clock),
                Arc::new(Ids(AtomicU64::new(0))),
                16,
            ),
            max_frame: AtomicUsize::new(0),
            upload_bytes: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
        })
    }
    fn frame(&self, value: &impl serde::Serialize) {
        let length = serde_json::to_vec(value).unwrap().len();
        assert!(
            length <= FRAME_CAP,
            "real serialized frame exceeds TinyBus limit"
        );
        self.max_frame.fetch_max(length, Ordering::SeqCst);
    }
}
impl MigrationTransport for Transport {
    fn exchange<'a>(
        &'a self,
        _owner: &'a str,
        request: MigrationExchangeRequest,
    ) -> TransformFuture<'a, MigrationExchangeReply> {
        Box::pin(async move {
            // This fake implements the confidential transport contract. Actual
            // framework exclusion is an owning-module integration test.
            self.frame(&request);
            let kind = match &request {
                MigrationExchangeRequest::Begin { .. } => "begin",
                MigrationExchangeRequest::Upload { bytes, .. } => {
                    assert!(bytes.len() <= MIGRATION_CHUNK_BYTES);
                    self.upload_bytes.fetch_add(bytes.len(), Ordering::SeqCst);
                    "upload"
                }
                MigrationExchangeRequest::Execute { .. } => "execute",
                MigrationExchangeRequest::Download { .. } => "download",
                MigrationExchangeRequest::Finish { .. } => "finish",
                MigrationExchangeRequest::Cancel { .. } => "cancel",
            };
            self.calls.lock().unwrap().push(kind);
            let reply = self
                .server
                .handle(&PeerIdentity::new("trusted-peer"), request)
                .await;
            self.frame(&reply);
            Ok(reply)
        })
    }
}
fn documents(text: &str) -> DocumentsTransformRequest {
    DocumentsTransformRequest {
        operation: "convert".into(),
        version: 1,
        records: vec![SourceRecord {
            id: "id".into(),
            body: json!({"text":text}),
            meta: Meta::default(),
        }],
    }
}
fn doc_handler(payload: MigrationPayload) -> MigrationPayloadReply {
    let MigrationPayload::Documents(request) = payload else {
        panic!("unexpected payload")
    };
    MigrationPayloadReply::Documents(Ok(request
        .records
        .into_iter()
        .map(|r| TargetRecord::new("owner.target", r.id, r.body))
        .collect()))
}
#[tokio::test]
async fn oversized_request_and_reply_and_single_document_use_bounded_frames() {
    let request = documents(&"x".repeat(FRAME_CAP + 1));
    let payload = MigrationPayload::Documents(request.clone());
    assert!(
        serde_json::to_vec(&payload).unwrap().len() > FRAME_CAP,
        "old direct call must fail real frame cap"
    );
    let direct_reply = doc_handler(payload);
    assert!(
        serde_json::to_vec(&direct_reply).unwrap().len() > FRAME_CAP,
        "old direct reply must fail real frame cap"
    );
    let transport = Transport::new(doc_handler);
    let client = MigrationDeliveryClient::new(transport.clone());
    let output = client
        .documents(&Owner::new("owner").unwrap(), request)
        .await
        .unwrap();
    assert_eq!(
        output[0].body["text"].as_str().unwrap().len(),
        FRAME_CAP + 1
    );
    assert!(transport.max_frame.load(Ordering::SeqCst) < FRAME_CAP);
    assert!(
        transport
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|&&s| s == "upload")
            .count()
            > 128
    );
    assert_eq!(transport.calls.lock().unwrap().last(), Some(&"finish"));
}
#[tokio::test]
async fn small_input_expanding_to_oversized_reply_uses_bounded_frames() {
    let transport = Transport::new(|_| {
        MigrationPayloadReply::Documents(Ok(vec![TargetRecord::new(
            "owner.target",
            "id",
            json!({"text":"z".repeat(FRAME_CAP+1)}),
        )]))
    });
    let client = MigrationDeliveryClient::new(transport.clone());
    let output = client
        .documents(&Owner::new("owner").unwrap(), documents("tiny"))
        .await
        .unwrap();
    assert_eq!(
        output[0].body["text"].as_str().unwrap().len(),
        FRAME_CAP + 1
    );
    assert!(transport.upload_bytes.load(Ordering::SeqCst) < 1024);
}
#[tokio::test]
async fn blob_over_public_stream_cap_stays_local_byte_identical_and_keeps_allocation() {
    let transport = Transport::new(|p| {
        let MigrationPayload::BlobTarget(r) = p else {
            panic!("blob bytes crossed transport")
        };
        assert_eq!(r.source, "source");
        MigrationPayloadReply::BlobTarget(Ok(Some("destination".into())))
    });
    let client = MigrationDeliveryClient::new(transport.clone());
    let bytes = vec![0xad; 257 * 1024 * 1024];
    let ptr = bytes.as_ptr();
    let output = client
        .blob(
            &Owner::new("owner").unwrap(),
            BlobTransformRequest {
                operation: "move".into(),
                version: 1,
                source: "source".into(),
                content_type: Some("application/octet-stream".into()),
                bytes,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.bytes.as_ptr(), ptr);
    assert_eq!(
        output.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(output.bytes.len(), 257 * 1024 * 1024);
    assert!(output.bytes.iter().all(|&byte| byte == 0xad));
    assert!(transport.upload_bytes.load(Ordering::SeqCst) < 1024);
}
#[tokio::test]
async fn identity_aggregate_stream_sends_zero_entries_and_keeps_allocation() {
    let transport = Transport::new(|p| {
        assert!(matches!(p, MigrationPayload::StreamPlan(_)));
        MigrationPayloadReply::StreamPlan(Ok(Some(StreamPlan {
            target: "target".into(),
            mode: StreamMapMode::Identity,
        })))
    });
    let client = MigrationDeliveryClient::new(transport.clone());
    let entries =
        vec![json!({"private_entry":"entry sentinel","large":"x".repeat(257 * 1024 * 1024)})];
    let ptr = entries.as_ptr();
    let output = client
        .stream(
            &Owner::new("owner").unwrap(),
            StreamTransformRequest {
                operation: "identity".into(),
                version: 1,
                source: "source".into(),
                entries,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.entries.as_ptr(), ptr);
    assert_eq!(
        output.entries[0]["large"].as_str().unwrap().len(),
        257 * 1024 * 1024
    );
    assert!(transport.upload_bytes.load(Ordering::SeqCst) < 1024);
}
#[tokio::test]
async fn independent_groups_skip_invalid_entries_and_fragment_a_huge_single_entry() {
    let sizes = Arc::new(Mutex::new(Vec::new()));
    let seen = sizes.clone();
    let transport = Transport::new(move |p| match p {
        MigrationPayload::StreamPlan(_) => {
            MigrationPayloadReply::StreamPlan(Ok(Some(StreamPlan {
                target: "target".into(),
                mode: StreamMapMode::IndependentEntries,
            })))
        }
        MigrationPayload::StreamEntries(r) => {
            seen.lock().unwrap().push(r.entries.len());
            MigrationPayloadReply::StreamEntries(Ok(r
                .entries
                .into_iter()
                .filter(|v| v.get("valid") == Some(&Value::Bool(true)))
                .collect()))
        }
        _ => panic!("unexpected payload"),
    });
    let mut entries: Vec<_> = (0..520)
        .map(|i| {
            if [255, 256, 511, 512].contains(&i) {
                json!({"invalid":true})
            } else {
                json!({"valid":true,"index":i})
            }
        })
        .collect();
    entries[260] = json!({"valid":true,"index":260,"huge":"h".repeat(FRAME_CAP+1)});
    assert!(serde_json::to_vec(&entries[260]).unwrap().len() > FRAME_CAP);
    let expected: Vec<_> = entries
        .iter()
        .filter(|v| v.get("valid") == Some(&Value::Bool(true)))
        .cloned()
        .collect();
    let client = MigrationDeliveryClient::new(transport);
    let output = client
        .stream(
            &Owner::new("owner").unwrap(),
            StreamTransformRequest {
                operation: "map".into(),
                version: 1,
                source: "source".into(),
                entries,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.entries, expected);
    assert_eq!(*sizes.lock().unwrap(), vec![256, 256, 8]);
}
#[tokio::test]
async fn late_group_failure_returns_no_partial_stream_and_sanitizes_owner_error() {
    let group = AtomicUsize::new(0);
    let transport = Transport::new(move |p| match p {
        MigrationPayload::StreamPlan(_) => {
            MigrationPayloadReply::StreamPlan(Ok(Some(StreamPlan {
                target: "target".into(),
                mode: StreamMapMode::IndependentEntries,
            })))
        }
        MigrationPayload::StreamEntries(r) => {
            if group.fetch_add(1, Ordering::SeqCst) == 0 {
                MigrationPayloadReply::StreamEntries(Ok(r.entries))
            } else {
                MigrationPayloadReply::StreamEntries(Err(crate::schema::TransformError {
                    kind: crate::schema::TransformErrorKind::InvalidInput,
                    message: "PRIVATE source credentials serde backend".into(),
                }))
            }
        }
        _ => panic!("unexpected payload"),
    });
    let client = MigrationDeliveryClient::new(transport);
    let error = client
        .stream(
            &Owner::new("owner").unwrap(),
            StreamTransformRequest {
                operation: "map".into(),
                version: 1,
                source: "source".into(),
                entries: vec![json!(1); 257],
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "invalid input: migration transform failed"
    );
}

#[derive(Clone, Copy)]
enum Fault {
    UploadOffset,
    DownloadOffset,
    HugeChunk,
    EmptyChunk,
    Overrun,
    InvalidJson,
    WrongFamily,
    Finish,
    Transport,
}
struct FaultTransport {
    inner: Arc<Transport>,
    fault: Fault,
}
impl MigrationTransport for FaultTransport {
    fn exchange<'a>(
        &'a self,
        owner: &'a str,
        request: MigrationExchangeRequest,
    ) -> TransformFuture<'a, MigrationExchangeReply> {
        Box::pin(async move {
            if matches!(self.fault, Fault::Transport) {
                return Err(crate::StorageError::unavailable(
                    "PRIVATE backend source credentials",
                ));
            }
            let reply = self.inner.exchange(owner, request).await?;
            Ok(match (self.fault, reply) {
                (Fault::UploadOffset, MigrationExchangeReply::Uploaded { next_offset }) => {
                    MigrationExchangeReply::Uploaded {
                        next_offset: next_offset + 1,
                    }
                }
                (Fault::DownloadOffset, MigrationExchangeReply::Chunk { offset, bytes }) => {
                    MigrationExchangeReply::Chunk {
                        offset: offset + 1,
                        bytes,
                    }
                }
                (Fault::HugeChunk, MigrationExchangeReply::Chunk { offset, .. }) => {
                    MigrationExchangeReply::Chunk {
                        offset,
                        bytes: vec![0; MIGRATION_CHUNK_BYTES + 1],
                    }
                }
                (Fault::EmptyChunk, MigrationExchangeReply::Chunk { offset, .. }) => {
                    MigrationExchangeReply::Chunk {
                        offset,
                        bytes: vec![],
                    }
                }
                (Fault::Overrun, MigrationExchangeReply::Ready { .. }) => {
                    MigrationExchangeReply::Ready { reply_len: 1 }
                }
                (Fault::InvalidJson, MigrationExchangeReply::Chunk { offset, mut bytes }) => {
                    bytes[0] = b'!';
                    MigrationExchangeReply::Chunk { offset, bytes }
                }
                (Fault::Finish, MigrationExchangeReply::Finished) => {
                    MigrationExchangeReply::Cancelled
                }
                (_, reply) => reply,
            })
        })
    }
}
#[tokio::test]
async fn malformed_replies_and_transport_errors_are_content_free_and_fail_closed() {
    for fault in [
        Fault::UploadOffset,
        Fault::DownloadOffset,
        Fault::HugeChunk,
        Fault::EmptyChunk,
        Fault::Overrun,
        Fault::InvalidJson,
        Fault::WrongFamily,
        Fault::Finish,
        Fault::Transport,
    ] {
        let inner = Transport::new(move |_| {
            if matches!(fault, Fault::WrongFamily) {
                MigrationPayloadReply::StreamEntries(Ok(vec![]))
            } else {
                MigrationPayloadReply::BlobTarget(Ok(Some("target".into())))
            }
        });
        let transport = Arc::new(FaultTransport { inner, fault });
        let client = MigrationDeliveryClient::new(transport);
        let error = client
            .transform(
                "owner",
                MigrationPayload::BlobTarget(BlobTargetRequest {
                    operation: "private operation".into(),
                    version: 1,
                    source: "private source".into(),
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unavailable: migration delivery unavailable"
        );
        assert!(std::error::Error::source(&error).is_none());
    }
}
struct DropTransport {
    inner: Arc<Transport>,
    entered: tokio::sync::Notify,
    cancelled: tokio::sync::Notify,
}
impl MigrationTransport for DropTransport {
    fn exchange<'a>(
        &'a self,
        owner: &'a str,
        request: MigrationExchangeRequest,
    ) -> TransformFuture<'a, MigrationExchangeReply> {
        Box::pin(async move {
            if matches!(request, MigrationExchangeRequest::Upload { .. }) {
                self.entered.notify_one();
                return std::future::pending().await;
            }
            let cancel = matches!(request, MigrationExchangeRequest::Cancel { .. });
            let reply = self.inner.exchange(owner, request).await?;
            if cancel {
                self.cancelled.notify_one();
            }
            Ok(reply)
        })
    }
}
#[tokio::test]
async fn dropped_client_future_sends_confidential_best_effort_cancel() {
    let inner = Transport::new(doc_handler);
    let transport = Arc::new(DropTransport {
        inner: inner.clone(),
        entered: tokio::sync::Notify::new(),
        cancelled: tokio::sync::Notify::new(),
    });
    let client = MigrationDeliveryClient::new(transport.clone());
    let task = tokio::spawn(async move {
        client
            .transform("owner", MigrationPayload::Documents(documents("private")))
            .await
    });
    transport.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    transport.cancelled.notified().await;
    assert_eq!(inner.calls.lock().unwrap().as_slice(), ["begin", "cancel"]);
}
struct CancelLifetime(Arc<AtomicUsize>);
impl Drop for CancelLifetime {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct HangingCancel {
    inner: Arc<Transport>,
    started: tokio::sync::Notify,
    dropped: Arc<AtomicUsize>,
}
impl MigrationTransport for HangingCancel {
    fn exchange<'a>(
        &'a self,
        owner: &'a str,
        request: MigrationExchangeRequest,
    ) -> TransformFuture<'a, MigrationExchangeReply> {
        Box::pin(async move {
            if matches!(request, MigrationExchangeRequest::Cancel { .. }) {
                let _lifetime = CancelLifetime(self.dropped.clone());
                self.started.notify_one();
                return std::future::pending().await;
            }
            let reply = self.inner.exchange(owner, request).await?;
            if matches!(reply, MigrationExchangeReply::Uploaded { .. }) {
                Ok(MigrationExchangeReply::Uploaded {
                    next_offset: u64::MAX,
                })
            } else {
                Ok(reply)
            }
        })
    }
}
#[tokio::test(start_paused = true)]
async fn abandoned_cancel_task_is_bounded_by_runtime_timeout() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let transport = Arc::new(HangingCancel {
        inner: Transport::new(doc_handler),
        started: tokio::sync::Notify::new(),
        dropped: dropped.clone(),
    });
    let client = MigrationDeliveryClient::new(transport.clone());
    assert!(
        client
            .transform("owner", MigrationPayload::Documents(documents("private")))
            .await
            .is_err()
    );
    transport.started.notified().await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
