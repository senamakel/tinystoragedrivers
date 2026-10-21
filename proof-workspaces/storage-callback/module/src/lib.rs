//! Real SDK module that exercises every host-bound storage callback port.
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use serde::Deserialize;
use serde_json::{Value, json};
use tinybus::{Connection, Error, Interface, InterfaceName, MemberName, Result};
use tinystoragedrivers_api::{DocumentWatch, Storage};
use tinystoragedrivers_busproxy::{StorageAddress, connect_storage_at, connect_storage_at_address};
use tinystoragedrivers_types::{
    CollectionSpec, Filter, NewMessage, Precondition, QueueSpec, SecretPrecondition, WatchNotice,
    WatchOptions,
};

#[derive(Clone, Deserialize)]
struct Config {
    host: tinybus::BusName,
    #[serde(default)]
    object_root: Option<String>,
}

struct StorageModule {
    connection: Connection,
    config: Config,
    storage: tokio::sync::OnceCell<Storage>,
    watch: tokio::sync::Mutex<Option<Box<dyn DocumentWatch>>>,
}

#[async_trait]
impl Interface for StorageModule {
    fn name(&self) -> InterfaceName {
        "org.example.StorageCallbackProof".parse().unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        [
            "Ready",
            "Exercise",
            "OpenWatch",
            "ReadWatch",
            "Addressed",
            "AddressedSecretsDenied",
            "AddressedRead",
        ]
        .into_iter()
        .map(|member| member.parse().unwrap())
        .collect()
    }

    async fn call(&self, member: &MemberName, args: Value) -> Result<Value> {
        if args != json!([]) {
            return Err(Error::failed("callback proof takes no arguments"));
        }
        // This readiness probe deliberately does not touch storage. The host
        // uses it to wait for module setup before publishing the invocation
        // endpoint that the module will connect to on its first storage call.
        if member.as_str() == "Ready" {
            return Ok(json!(true));
        }
        let storage = self.storage().await?;
        match member.as_str() {
            "Exercise" => self.exercise(storage).await,
            "Addressed" => {
                storage
                    .documents()
                    .ensure_collection(&CollectionSpec::new("addressed"))
                    .await
                    .map_err(|_| Error::failed("addressed collection ensure failed"))?;
                storage
                    .documents()
                    .put(
                        "addressed",
                        "module",
                        json!({"value": 63}),
                        Precondition::Absent,
                    )
                    .await
                    .map_err(|_| Error::failed("addressed document put failed"))?;
                let value = storage
                    .documents()
                    .get("addressed", "module")
                    .await
                    .map_err(|_| Error::failed("addressed document get failed"))?
                    .ok_or_else(|| Error::failed("addressed document readback missing"))?;
                Ok(json!(value.doc["value"] == 63))
            }
            "AddressedSecretsDenied" => {
                let result = storage.secrets().get_versioned("addressed/secret").await;
                Ok(json!(result.is_err()))
            }
            "AddressedRead" => storage
                .documents()
                .get("addressed", "module")
                .await
                .map(|value| json!(value.is_some()))
                .map_err(|_| Error::failed("addressed document read failed")),
            "OpenWatch" => {
                let watch = storage
                    .documents()
                    .watch(
                        "callback-proof",
                        &Filter::default(),
                        &WatchOptions::default(),
                    )
                    .await
                    .map_err(|_| Error::failed("document watch open failed"))?;
                *self.watch.lock().await = Some(watch);
                Ok(Value::Null)
            }
            "ReadWatch" => {
                let mut watch = self
                    .watch
                    .lock()
                    .await
                    .take()
                    .ok_or_else(|| Error::failed("document watch was not opened"))?;
                let notice = watch
                    .next()
                    .await
                    .map_err(|_| Error::failed("document watch read failed"))?;
                Ok(json!(matches!(
                    notice,
                    Some(WatchNotice::Invalidated { .. })
                )))
            }
            _ => Err(Error::failed("unknown callback proof method")),
        }
    }
}

impl StorageModule {
    async fn storage(&self) -> Result<&Storage> {
        self.storage
            .get_or_try_init(|| async {
                let storage = if let Some(object_root) = &self.config.object_root {
                    let address =
                        StorageAddress::try_new(self.config.host.clone(), object_root.parse()?)?;
                    connect_storage_at_address(self.connection.clone(), address).await
                } else {
                    connect_storage_at(self.connection.clone(), self.config.host.clone()).await
                };
                storage.map_err(|error| {
                    Error::failed(format!("host callback storage connection failed: {error}"))
                })
            })
            .await
    }

    async fn exercise(&self, storage: &Storage) -> Result<Value> {
        let docs = storage.documents();
        docs.ensure_collection(&CollectionSpec::new("callback-proof"))
            .await
            .map_err(|_| Error::failed("documents ensure failed"))?;
        docs.put(
            "callback-proof",
            "module-document",
            json!({"origin":"module"}),
            Precondition::Absent,
        )
        .await
        .map_err(|_| Error::failed("documents put failed"))?;
        let document = docs
            .get("callback-proof", "module-document")
            .await
            .map_err(|_| Error::failed("documents get failed"))?
            .ok_or_else(|| Error::failed("documents readback missing"))?;

        storage
            .kv()
            .put("callback-proof/module", json!({"value": 7}))
            .await
            .map_err(|_| Error::failed("key/value put failed"))?;
        let kv = storage
            .kv()
            .get("callback-proof/module")
            .await
            .map_err(|_| Error::failed("key/value get failed"))?
            .ok_or_else(|| Error::failed("key/value readback missing"))?;
        let kv_names = storage
            .kv()
            .list("callback-proof/", None, 10)
            .await
            .map_err(|_| Error::failed("key/value list failed"))?;

        let offset = storage
            .streams()
            .append("callback-proof", json!({"value":"stream"}))
            .await
            .map_err(|_| Error::failed("stream append failed"))?;
        let stream_rows = storage
            .streams()
            .read_window("callback-proof", offset, 1)
            .await
            .map_err(|_| Error::failed("stream read failed"))?;

        storage
            .queues()
            .ensure_queue("callback-proof", &QueueSpec::default())
            .await
            .map_err(|_| Error::failed("queue ensure failed"))?;
        storage
            .queues()
            .enqueue("callback-proof", &[NewMessage::new(json!({"queued":true}))])
            .await
            .map_err(|_| Error::failed("queue enqueue failed"))?;
        let deliveries = storage
            .queues()
            .receive("callback-proof", 1, 1_000, "module")
            .await
            .map_err(|_| Error::failed("queue receive failed"))?;

        let upload = b"ordinary module stream".to_vec();
        let upload_reader = Box::pin(stream::iter(vec![Ok(Bytes::from(upload.clone()))]));
        storage
            .blobs()
            .put_stream("module-upload", upload_reader, upload.len() as u64, None)
            .await
            .map_err(|_| Error::failed("blob upload stream failed"))?;
        let mut download = storage
            .blobs()
            .get_stream("host-download")
            .await
            .map_err(|_| Error::failed("blob download stream failed"))?
            .ok_or_else(|| Error::failed("host blob was not found"))?;
        let mut downloaded = Vec::new();
        while let Some(chunk) = download.reader.next().await {
            downloaded.extend_from_slice(&chunk.map_err(|_| Error::failed("blob read failed"))?);
        }

        let token = storage
            .secrets()
            .set_if(
                "callback-proof/token",
                b"value-one",
                SecretPrecondition::Absent,
            )
            .await
            .map_err(|_| Error::failed("secret conditional create failed"))?;
        let (secret, version) = storage
            .secrets()
            .get_versioned("callback-proof/token")
            .await
            .map_err(|_| Error::failed("secret versioned read failed"))?
            .ok_or_else(|| Error::failed("secret readback missing"))?;
        if version != token || secret.as_slice() != b"value-one" {
            return Err(Error::failed("secret versioned readback differed"));
        }
        let updated = storage
            .secrets()
            .set_if(
                "callback-proof/token",
                b"value-two",
                SecretPrecondition::Version(version),
            )
            .await
            .map_err(|_| Error::failed("secret conditional update failed"))?;
        let stale = storage
            .secrets()
            .set_if(
                "callback-proof/token",
                b"stale",
                SecretPrecondition::Version(token),
            )
            .await;
        if !matches!(stale, Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict)
        {
            return Err(Error::failed(
                "stale secret conditional update was not rejected",
            ));
        }
        let secret_names = storage
            .secrets()
            .list("callback-proof/")
            .await
            .map_err(|_| Error::failed("secret list failed"))?;

        Ok(json!({
            "document": document.doc,
            "kv": kv.doc,
            "kv_names": kv_names.items.len(),
            "stream_rows": stream_rows.len(),
            "queue_deliveries": deliveries.len(),
            "upload": String::from_utf8_lossy(&upload),
            "download": String::from_utf8_lossy(&downloaded),
            "secret_updated": updated != version,
            "secret_names": secret_names,
        }))
    }
}

async fn setup(connection: Connection, config: Config) -> Result<()> {
    let module = StorageModule {
        connection: connection.clone(),
        config: config.clone(),
        storage: tokio::sync::OnceCell::new(),
        watch: tokio::sync::Mutex::new(None),
    };
    // Ordinary callback fixtures keep their established eager setup path.
    // The invocation-address fixture defers its first storage call until after
    // ModuleHost has admitted this instance and served its peer-bound endpoint.
    if config.object_root.is_none() {
        module.storage().await?;
    }
    connection
        .serve_at("/storage_proof".parse()?, module)
        .await?;
    connection
        .request_name("org.example.StorageCallbackProof")
        .await?;
    Ok(())
}

tinybus_module::module_export! {
    setup = setup, config = Config,
    worker_threads = 1,
    provides = ["org.example.StorageCallbackProof"],
        methods = [
            "Ready",
            "Exercise",
            "OpenWatch",
            "ReadWatch",
            "Addressed",
            "AddressedSecretsDenied",
            "AddressedRead",
        ],
    signals = ["WatchChanged"], requires = [], optional = [], lazy = false,
}
