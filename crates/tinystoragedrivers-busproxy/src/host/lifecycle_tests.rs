use super::*;
use async_trait::async_trait;
use std::time::Duration;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_bus::{BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest};
use tinystoragedrivers_core::BlobReader;
use tokio::sync::broadcast;

struct ClosingWatchEndpoint {
    sender: Mutex<Option<broadcast::Sender<WatchChanged>>>,
}

#[async_trait]
impl StorageEndpoint for ClosingWatchEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<Value, HostFailure> {
        Ok(Value::Null)
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: PutBlobStreamRequest,
        _body: BlobReader,
    ) -> Result<Value, HostFailure> {
        Ok(Value::Null)
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure> {
        Ok(None)
    }

    fn watch_signals(&self, _sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>> {
        self.sender
            .lock()
            .ok()?
            .as_ref()
            .map(broadcast::Sender::subscribe)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_the_watch_producer_terminates_its_forwarder() {
    let bus = MemoryBus::new();
    let _broker_task = Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let (watch_sender, _) = broadcast::channel(8);
    let endpoint = Arc::new(ClosingWatchEndpoint {
        sender: Mutex::new(Some(watch_sender)),
    });
    let shared = Shared {
        connection: connection.downgrade(),
        endpoint: endpoint.clone(),
        watch_root: OBJECT_ROOT.to_owned(),
        forwarders: Mutex::new(HashMap::new()),
    };
    let caller = BusName::new(":1.7").unwrap();

    shared.forward_watch_signals(&caller);
    let task = shared
        .forwarders
        .lock()
        .unwrap()
        .remove(&caller)
        .expect("the subscribed watch forwarder is registered")
        .1;
    drop(endpoint.sender.lock().unwrap().take());

    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("the closed broadcast receiver must terminate its forwarder")
        .expect("the forwarding task must exit normally on channel closure");
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_an_armed_served_lease_withdraws_its_bus_paths() {
    let bus = MemoryBus::new();
    let _broker_task = Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service_peer = connection.unique_name().unwrap();
    let path = tinybus::ObjectPath::new(format!("{OBJECT_ROOT}/Leases/91/Documents")).unwrap();
    let proxy = tinybus::Proxy::new(client, service_peer, path.clone(), interface()).unwrap();
    let lease = serve_lease(
        &connection,
        &format!("{OBJECT_ROOT}/Leases/91"),
        Arc::new(ClosingWatchEndpoint {
            sender: Mutex::new(None),
        }),
    )
    .await
    .unwrap();

    proxy
        .call::<Value>("Capabilities", (Value::Null,))
        .await
        .expect("the served path must be reachable before lease drop");
    drop(lease);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match proxy.call::<Value>("Capabilities", (Value::Null,)).await {
                Err(error)
                    if error.wire_name() == "ai.tinyhumans.tinybus.Error.UnknownObject"
                        && error.wire_message() == format!("no object at `{path}`") =>
                {
                    break;
                }
                Ok(_) => tokio::task::yield_now().await,
                Err(error) => panic!("lease withdrawal returned an unexpected bus error: {error}"),
            }
        }
    })
    .await
    .expect("dropping the armed lease must eventually withdraw its served path");
}
