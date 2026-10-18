//! Dynamically loaded storage consumer used by the owning-repository proof.

#![allow(
    missing_docs,
    reason = "The cdylib exports are a private test fixture."
)]

use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tinybus::{BusName, Connection, Result, StreamDescriptor, StreamWriter};
use tinystoragedrivers_bus::{
    AppendRequest, BlobInfo, BlobKeyRequest, Filter, MAX_FRAME_BYTES, PutBlobStreamRequest,
    PutDocumentRequest, ReadWindowRequest, WatchChanged,
};
use tinystoragedrivers_busproxy::{BusTransport, StorageDescriptor};
use tinystoragedrivers_client::{Object, StorageTransport};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, broadcast};

struct Consumer {
    connection: Connection,
    transport: Mutex<Option<std::sync::Arc<BusTransport>>>,
    signals: Mutex<Option<broadcast::Receiver<WatchChanged>>>,
    descriptor: Mutex<Option<StorageDescriptor>>,
    staged_stream: Mutex<Option<StreamWriter>>,
    watch_generation: AtomicUsize,
}

#[tinybus::interface(name = "ai.tinyhumans.test.GovernedTestConsumer")]
impl Consumer {
    async fn configure(&self, descriptor: StorageDescriptor) -> Result<()> {
        let transport = std::sync::Arc::new(
            BusTransport::connect_bound(self.connection.clone(), descriptor.clone())
                .await
                .map_err(|_| tinybus::Error::failed("bound storage setup failed"))?,
        );
        let signals = transport.signals();
        *self.descriptor.lock().await = Some(descriptor);
        *self.transport.lock().await = Some(transport);
        *self.signals.lock().await = Some(signals);
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "The loaded consumer proof exercises the complete admitted storage surface."
    )]
    async fn exercise(&self) -> Result<Value> {
        let transport = self.transport().await?;
        transport
            .call(
                Object::Documents,
                "Put",
                serde_json::to_value(PutDocumentRequest {
                    collection: "shared.records".to_owned(),
                    id: "loaded-cdylib".to_owned(),
                    doc: json!({"consumer":"loaded-cdylib"}),
                    precondition: tinystoragedrivers_bus::Precondition::Absent,
                    schema: 0,
                    expires_ms: None,
                })
                .map_err(|_| tinybus::Error::failed("document encoding failed"))?,
            )
            .await
            .map_err(|_| tinybus::Error::failed("document write failed"))?;
        let document = transport
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"loaded-cdylib"}),
            )
            .await
            .map_err(|_| tinybus::Error::failed("document read failed"))?;

        transport
            .call(
                Object::Streams,
                "Append",
                serde_json::to_value(AppendRequest {
                    stream: "events/loaded-cdylib".to_owned(),
                    record: json!({"consumer":"loaded-cdylib"}),
                })
                .map_err(|_| tinybus::Error::failed("stream encoding failed"))?,
            )
            .await
            .map_err(|_| tinybus::Error::failed("stream append failed"))?;
        let stream = transport
            .call(
                Object::Streams,
                "ReadWindow",
                serde_json::to_value(ReadWindowRequest {
                    stream: "events/loaded-cdylib".to_owned(),
                    from: 0,
                    limit: 4,
                })
                .map_err(|_| tinybus::Error::failed("stream read encoding failed"))?,
            )
            .await
            .map_err(|_| tinybus::Error::failed("stream read failed"))?;

        transport
            .call(
                Object::Secrets,
                "Set",
                json!({"name":"provider.key","value":[17,18,19]}),
            )
            .await
            .map_err(|_| tinybus::Error::failed("secret write failed"))?;
        let secret = transport
            .call(Object::Secrets, "Get", json!({"name":"provider.key"}))
            .await
            .map_err(|_| tinybus::Error::failed("secret read failed"))?;

        let len = MAX_FRAME_BYTES + 1024 * 1024 + 17;
        let body: Vec<u8> = (0..len)
            .map(|index| u8::try_from(index % 251).expect("byte pattern is bounded"))
            .collect();
        transport
            .put_blob_stream(
                PutBlobStreamRequest {
                    key: "canary/loaded-cdylib.bin".to_owned(),
                    len: u64::try_from(body.len()).expect("blob length fits u64"),
                    content_type: Some("application/octet-stream".to_owned()),
                    ttl_ms: None,
                },
                Box::pin(Cursor::new(body.clone())),
            )
            .await
            .map_err(|_| tinybus::Error::failed("blob upload failed"))?;
        let (head, mut reader) = transport
            .get_blob_stream(BlobKeyRequest {
                key: "canary/loaded-cdylib.bin".to_owned(),
            })
            .await
            .map_err(|_| tinybus::Error::failed("blob download failed"))?
            .ok_or_else(|| tinybus::Error::failed("blob disappeared"))?;
        let mut downloaded = Vec::new();
        reader
            .read_to_end(&mut downloaded)
            .await
            .map_err(|_| tinybus::Error::failed("blob body read failed"))?;
        if downloaded != body || usize::try_from(head.meta.len).ok() != Some(body.len()) {
            return Err(tinybus::Error::failed("blob body mismatch"));
        }
        Ok(json!({
            "document": document,
            "stream": stream,
            "secret": secret,
            "blob_len": downloaded.len(),
        }))
    }

    async fn reject_scope_argument(&self) -> Result<bool> {
        let result = self
            .transport()
            .await?
            .call(
                Object::Documents,
                "Get",
                json!({
                    "collection":"shared.records",
                    "document_id":"loaded-cdylib",
                    "scope":"forged-scope-marker"
                }),
            )
            .await;
        Ok(result.is_err())
    }

    async fn reject_secret_suffix(&self) -> Result<bool> {
        let result = self
            .transport()
            .await?
            .call(
                Object::Secrets,
                "Get",
                json!({"name":"provider.key.suffix-marker"}),
            )
            .await;
        Ok(result.is_err())
    }

    #[allow(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "The oversized-body fixture uses fixed representable test constants."
    )]
    async fn reject_oversized_upload(&self) -> Result<bool> {
        let len = u64::try_from(MAX_FRAME_BYTES + 1024 * 1024 + 18)
            .expect("test upload ceiling fits u64");
        let result = self
            .transport()
            .await?
            .put_blob_stream(
                PutBlobStreamRequest {
                    key: "canary/over-ceiling".to_owned(),
                    len,
                    content_type: None,
                    ttl_ms: None,
                },
                Box::pin(Cursor::new(vec![0; usize::try_from(len).unwrap()])),
            )
            .await;
        Ok(result.is_err())
    }

    async fn reject_mismatched_upload_lengths(&self) -> Result<bool> {
        let descriptor = self.descriptor().await?;
        let peer = BusName::new(descriptor.service_peer().to_owned())?;
        let mut writer = self
            .connection
            .open_stream(&peer, StreamDescriptor::with_len(4))
            .await?;
        let stream_ref = writer.stream_ref();
        let object_path = format!("{}/Blobs", descriptor.object_root());
        let proxy = self.connection.proxy(
            descriptor.service_peer(),
            object_path,
            tinystoragedrivers_bus::STORAGE_INTERFACE,
        )?;
        let call = proxy.call::<Value>(
            "PutStream",
            (
                PutBlobStreamRequest {
                    key: "canary/mismatched-length".to_owned(),
                    len: 3,
                    content_type: None,
                    ttl_ms: None,
                },
                stream_ref,
            ),
        );
        let send = async {
            let _ = writer.write(&[1, 2, 3, 4]).await;
            let _ = writer.finish().await;
        };
        let (reply, ()) = tokio::join!(call, send);
        Ok(reply.is_err())
    }

    async fn open_staged_stream(&self) -> Result<Value> {
        let descriptor = self.descriptor().await?;
        let peer = BusName::new(descriptor.service_peer().to_owned())?;
        let writer = self
            .connection
            .open_stream(&peer, StreamDescriptor::with_len(4))
            .await?;
        let stream_ref = serde_json::to_value(writer.stream_ref())
            .map_err(|_| tinybus::Error::failed("stream reference encoding failed"))?;
        *self.staged_stream.lock().await = Some(writer);
        Ok(stream_ref)
    }

    async fn abort_staged_stream(&self) -> Result<()> {
        if let Some(writer) = self.staged_stream.lock().await.take() {
            writer.abort().await?;
        }
        Ok(())
    }

    #[allow(
        clippy::expect_used,
        reason = "The route fixture uses a fixed representable byte pattern."
    )]
    async fn verify_routes(&self) -> Result<Value> {
        let transport = self.transport().await?;
        let document = transport
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"loaded-cdylib"}),
            )
            .await
            .map_err(|_| tinybus::Error::failed("bound document route changed"))?;
        let stream = transport
            .call(
                Object::Streams,
                "ReadWindow",
                serde_json::to_value(ReadWindowRequest {
                    stream: "events/loaded-cdylib".to_owned(),
                    from: 0,
                    limit: 4,
                })
                .map_err(|_| tinybus::Error::failed("stream read encoding failed"))?,
            )
            .await
            .map_err(|_| tinybus::Error::failed("bound stream route changed"))?;
        let secret = transport
            .call(Object::Secrets, "Get", json!({"name":"provider.key"}))
            .await
            .map_err(|_| tinybus::Error::failed("bound secret route changed"))?;
        let (head, mut reader) = transport
            .get_blob_stream(BlobKeyRequest {
                key: "canary/loaded-cdylib.bin".to_owned(),
            })
            .await
            .map_err(|_| tinybus::Error::failed("bound download route changed"))?
            .ok_or_else(|| tinybus::Error::failed("bound blob disappeared"))?;
        let mut downloaded = Vec::new();
        reader
            .read_to_end(&mut downloaded)
            .await
            .map_err(|_| tinybus::Error::failed("bound blob body read failed"))?;
        let expected_len = MAX_FRAME_BYTES + 1024 * 1024 + 17;
        if usize::try_from(head.meta.len).ok() != Some(expected_len)
            || downloaded.len() != expected_len
            || downloaded
                .iter()
                .enumerate()
                .any(|(index, byte)| usize::from(*byte) != index % 251)
        {
            return Err(tinybus::Error::failed("bound blob content changed"));
        }
        let alias_bytes: Vec<u8> = (0..4097)
            .map(|index| u8::try_from(index % 251).expect("byte pattern is bounded"))
            .collect();
        let upload = transport
            .put_blob_stream(
                PutBlobStreamRequest {
                    key: "canary/after-alias".to_owned(),
                    len: u64::try_from(alias_bytes.len()).expect("blob length fits u64"),
                    content_type: Some("application/octet-stream".to_owned()),
                    ttl_ms: None,
                },
                Box::pin(Cursor::new(alias_bytes.clone())),
            )
            .await
            .map_err(|_| tinybus::Error::failed("bound upload route changed"))?;
        let upload: BlobInfo = serde_json::from_value(upload)
            .map_err(|_| tinybus::Error::failed("bound upload reply changed"))?;
        let (alias_head, mut alias_reader) = transport
            .get_blob_stream(BlobKeyRequest {
                key: "canary/after-alias".to_owned(),
            })
            .await
            .map_err(|_| tinybus::Error::failed("bound upload readback changed"))?
            .ok_or_else(|| tinybus::Error::failed("bound upload disappeared"))?;
        let mut alias_download = Vec::new();
        alias_reader
            .read_to_end(&mut alias_download)
            .await
            .map_err(|_| tinybus::Error::failed("bound upload readback failed"))?;
        if upload.key != "canary/after-alias"
            || alias_head.meta.len != u64::try_from(alias_bytes.len()).unwrap_or(u64::MAX)
            || alias_download != alias_bytes
        {
            return Err(tinybus::Error::failed(
                "bound upload reached a different host",
            ));
        }
        Ok(json!({
            "document":document,
            "stream":stream,
            "secret":secret,
            "upload_len":alias_download.len(),
        }))
    }

    async fn poke_watch(&self) -> Result<()> {
        let generation = self.watch_generation.fetch_add(1, Ordering::AcqRel);
        self.transport()
            .await?
            .call(
                Object::Documents,
                "Put",
                serde_json::to_value(PutDocumentRequest {
                    collection: "shared.records".to_owned(),
                    id: format!("loaded-cdylib-watch-canary-{generation}"),
                    doc: json!({"invalidated":true}),
                    precondition: tinystoragedrivers_bus::Precondition::Absent,
                    schema: 0,
                    expires_ms: None,
                })
                .map_err(|_| tinybus::Error::failed("watch write encoding failed"))?,
            )
            .await
            .map_err(|_| tinybus::Error::failed("bound watch write failed"))?;
        Ok(())
    }

    async fn open_watch(&self) -> Result<String> {
        let result = self
            .transport()
            .await?
            .call(
                Object::Documents,
                "WatchOpen",
                json!({
                    "collection":"shared.records",
                    "filter":serde_json::to_value(Filter::All)
                        .map_err(|_| tinybus::Error::failed("watch filter encoding failed"))?,
                    "interval_ms":1000
                }),
            )
            .await
            .map_err(|_| tinybus::Error::failed("watch open failed"))?;
        result
            .get("watch_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| tinybus::Error::failed("watch handle missing"))
    }

    async fn close_watch(&self, watch_id: String) -> Result<()> {
        self.transport()
            .await?
            .call(
                Object::Documents,
                "WatchClose",
                json!({"watch_id":watch_id}),
            )
            .await
            .map_err(|_| tinybus::Error::failed("watch close failed"))?;
        Ok(())
    }

    async fn wait_watch(&self, expected_watch_id: String) -> Result<Value> {
        let mut guard = self.signals.lock().await;
        let signals = guard
            .as_mut()
            .ok_or_else(|| tinybus::Error::failed("watch receiver unavailable"))?;
        let signal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let signal = signals
                    .recv()
                    .await
                    .map_err(|_| tinybus::Error::failed("watch signal unavailable"))?;
                if signal.watch_id == expected_watch_id {
                    return Ok::<_, tinybus::Error>(signal);
                }
            }
        })
        .await
        .map_err(|_| tinybus::Error::failed("watch signal timed out"))??;
        serde_json::to_value(signal)
            .map_err(|_| tinybus::Error::failed("watch signal encoding failed"))
    }

    async fn bound_routes_fail(&self) -> Result<Value> {
        let transport = self.transport().await?;
        let document = transport
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"loaded-cdylib"}),
            )
            .await
            .is_err();
        let stream = transport
            .call(
                Object::Streams,
                "ReadWindow",
                serde_json::to_value(ReadWindowRequest {
                    stream: "events/loaded-cdylib".to_owned(),
                    from: 0,
                    limit: 4,
                })
                .map_err(|_| tinybus::Error::failed("stream read encoding failed"))?,
            )
            .await
            .is_err();
        let upload = transport
            .put_blob_stream(
                PutBlobStreamRequest {
                    key: "canary/after-disconnect".to_owned(),
                    len: 0,
                    content_type: None,
                    ttl_ms: None,
                },
                Box::pin(Cursor::new(Vec::<u8>::new())),
            )
            .await
            .is_err();
        let download = transport
            .get_blob_stream(BlobKeyRequest {
                key: "canary/loaded-cdylib.bin".to_owned(),
            })
            .await
            .is_err();
        let secret = transport
            .call(Object::Secrets, "Get", json!({"name":"provider.key"}))
            .await
            .is_err();
        let watch = transport
            .call(
                Object::Documents,
                "WatchOpen",
                json!({
                    "collection":"shared.records",
                    "filter":serde_json::to_value(Filter::All)
                        .map_err(|_| tinybus::Error::failed("watch filter encoding failed"))?,
                    "interval_ms":1000
                }),
            )
            .await
            .is_err();
        Ok(json!({
            "document": document,
            "stream": stream,
            "upload": upload,
            "download": download,
            "secret": secret,
            "watch": watch,
        }))
    }

    async fn legacy_marker(&self) -> Result<Value> {
        let legacy = BusTransport::connect(self.connection.clone())
            .await
            .map_err(|_| tinybus::Error::failed("legacy storage connect failed"))?;
        legacy
            .call(Object::Documents, "Capabilities", Value::Null)
            .await
            .map_err(|_| tinybus::Error::failed("legacy storage call failed"))
    }
}

impl Consumer {
    async fn transport(&self) -> Result<std::sync::Arc<BusTransport>> {
        self.transport
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| tinybus::Error::failed("storage descriptor not configured"))
    }

    async fn descriptor(&self) -> Result<StorageDescriptor> {
        self.descriptor
            .lock()
            .await
            .clone()
            .ok_or_else(|| tinybus::Error::failed("storage descriptor not configured"))
    }
}

async fn setup(connection: Connection, _: Value) -> Result<()> {
    connection
        .serve_at(
            "/ai/tinyhumans/test/GovernedTestConsumer".try_into()?,
            Consumer {
                connection: connection.clone(),
                transport: Mutex::new(None),
                signals: Mutex::new(None),
                descriptor: Mutex::new(None),
                staged_stream: Mutex::new(None),
                watch_generation: AtomicUsize::new(0),
            },
        )
        .await?;
    connection
        .request_name("ai.tinyhumans.test.GovernedTestConsumer")
        .await?;
    Ok(())
}

tinybus_module::module_export! {
    setup = setup,
    config = Value,
    worker_threads = 1,
    provides = ["ai.tinyhumans.test.GovernedTestConsumer"],
    methods = [
        "Configure",
        "Exercise",
        "RejectScopeArgument",
        "RejectSecretSuffix",
        "RejectOversizedUpload",
        "RejectMismatchedUploadLengths",
        "OpenStagedStream",
        "AbortStagedStream",
        "VerifyRoutes",
        "PokeWatch",
        "OpenWatch",
        "CloseWatch",
        "WaitWatch",
        "BoundRoutesFail",
        "LegacyMarker",
    ],
    signals = [],
    requires = [],
    optional = [],
    lazy = false,
}
