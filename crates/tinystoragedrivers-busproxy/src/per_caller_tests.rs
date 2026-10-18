use super::*;
use serde_json::json;
use std::time::Duration;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_client::{Admission, Object};
use tinystoragedrivers_core::{CollectionSpec, MemoryStorage, Scope, StorageBackend};
use tokio::sync::{Semaphore, mpsc};

fn endpoint() -> PerCallerEndpoint<impl Fn(&BusName) -> Option<StorageServer>> {
    let backend = MemoryStorage::new();
    PerCallerEndpoint::new(move |sender| {
        (sender.as_str() == ":1.1")
            .then(|| StorageServer::new(backend.for_scope(&Scope::local()).unwrap()))
    })
}

fn call(sender: Option<&BusName>) -> StorageCall<'_> {
    StorageCall {
        object: Object::Documents,
        selector: "Capabilities",
        request: serde_json::Value::Null,
        sender,
        admission: Admission::Private,
    }
}

#[tokio::test]
async fn an_unauthenticated_or_unknown_caller_is_denied_on_every_path() {
    let endpoint = endpoint();
    let stranger = BusName::new(":1.2").unwrap();
    for sender in [None, Some(&stranger)] {
        let expected = if sender.is_none() {
            "unauthenticated-caller"
        } else {
            "unknown-caller"
        };
        assert_eq!(
            endpoint.call(call(sender)).await.unwrap_err(),
            HostFailure::Denied(expected)
        );
        let head = PutBlobStreamRequest {
            key: "k".into(),
            len: 0,
            content_type: None,
            ttl_ms: None,
        };
        let body: BlobReader = Box::pin(std::io::Cursor::new(Vec::new()));
        assert_eq!(
            endpoint
                .put_blob_stream(sender, head, body)
                .await
                .unwrap_err(),
            HostFailure::Denied(expected)
        );
        let get = BlobKeyRequest { key: "k".into() };
        assert_eq!(
            endpoint.get_blob_stream(sender, get).await.err(),
            Some(HostFailure::Denied(expected))
        );
    }
    if let Some(sender) = Some(&stranger) {
        assert!(endpoint.watch_signals(sender).is_none());
    }
    assert!(format!("{endpoint:?}").contains("PerCallerEndpoint"));
}

#[tokio::test]
async fn a_known_caller_is_served_from_one_stable_binding() {
    let endpoint = endpoint();
    let known = BusName::new(":1.1").unwrap();
    let reply = endpoint.call(call(Some(&known))).await.unwrap();
    assert!(reply.get("capabilities").is_some());
    // Secrets without a configured store are refused as unsupported.
    let secrets = StorageCall {
        object: Object::Secrets,
        selector: "Get",
        request: json!({"name": "x"}),
        sender: Some(&known),
        admission: Admission::Private,
    };
    assert_eq!(
        endpoint.call(secrets).await.unwrap_err(),
        HostFailure::Storage(tinystoragedrivers_bus::StorageFailure {
            kind: StorageKind::Unsupported
        })
    );
    assert!(endpoint.watch_signals(&known).is_some());
    let head = PutBlobStreamRequest {
        key: "k".into(),
        len: 2,
        content_type: None,
        ttl_ms: None,
    };
    let body: BlobReader = Box::pin(std::io::Cursor::new(vec![1, 2]));
    let stored = endpoint
        .put_blob_stream(Some(&known), head, body)
        .await
        .unwrap();
    assert_eq!(stored["len"], 2);
    let found = endpoint
        .get_blob_stream(Some(&known), BlobKeyRequest { key: "k".into() })
        .await
        .unwrap();
    assert_eq!(found.unwrap().0.meta.len, 2);
}

#[tokio::test]
async fn a_released_caller_is_rebound_on_its_next_call() {
    let bound = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&bound);
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_: &BusName| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    });
    let caller = BusName::new(":1.1").unwrap();
    endpoint.call(call(Some(&caller))).await.unwrap();
    endpoint.call(call(Some(&caller))).await.unwrap();
    assert_eq!(bound.load(std::sync::atomic::Ordering::SeqCst), 1);
    let first = endpoint.watch_epoch(&caller);
    assert_eq!(first, endpoint.watch_epoch(&caller), "stable while bound");

    assert!(endpoint.release(&caller));
    assert!(!endpoint.release(&caller), "nothing left to release");
    endpoint.call(call(Some(&caller))).await.unwrap();
    assert_eq!(bound.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_ne!(
        first,
        endpoint.watch_epoch(&caller),
        "a rebind changes the epoch so the host replaces its signal forwarder"
    );
}

#[tokio::test]
async fn async_bindings_run_without_holding_the_shared_cache_lock() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let first_gate = Arc::new(Semaphore::new(0));
    let gate_for_binder = Arc::clone(&first_gate);
    let backend = Arc::new(MemoryStorage::new());
    let endpoint = PerCallerEndpoint::new_async(move |sender: BusName| {
        let started = started_tx.clone();
        let gate = Arc::clone(&gate_for_binder);
        let backend = Arc::clone(&backend);
        async move {
            started.send(sender.clone()).unwrap();
            if sender.as_str() == ":1.1" {
                gate.acquire().await.unwrap().forget();
            }
            Some(StorageServer::new(
                backend.for_scope(&Scope::local()).unwrap(),
            ))
        }
    })
    .with_max_concurrent_bindings(2);
    let first = BusName::new(":1.1").unwrap();
    let second = BusName::new(":1.2").unwrap();

    let first_endpoint = Arc::new(endpoint);
    let first_for_task = Arc::clone(&first_endpoint);
    let first_task = tokio::spawn(async move { first_for_task.call(call(Some(&first))).await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .as_str(),
        ":1.1"
    );

    let second_for_task = Arc::clone(&first_endpoint);
    let second_task = tokio::spawn(async move { second_for_task.call(call(Some(&second))).await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .expect("second binder was blocked behind the first")
            .unwrap()
            .as_str(),
        ":1.2"
    );
    first_gate.add_permits(1);
    assert!(first_task.await.unwrap().is_ok());
    assert!(second_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn caller_cache_evicts_an_idle_binding_at_its_configured_bound() {
    let bound = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = Arc::clone(&bound);
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_: &BusName| {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    })
    .with_max_callers(1);
    let first = BusName::new(":1.1").unwrap();
    let second = BusName::new(":1.2").unwrap();
    assert!(endpoint.call(call(Some(&first))).await.is_ok());
    assert!(endpoint.call(call(Some(&second))).await.is_ok());
    assert_eq!(bound.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        endpoint.watch_epoch(&first),
        0,
        "the idle first entry was evicted"
    );
    assert!(endpoint.call(call(Some(&second))).await.is_ok());
    assert_eq!(bound.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(endpoint.call(call(Some(&first))).await.is_ok());
    assert_eq!(bound.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[tokio::test]
async fn caller_churn_stays_within_the_configured_retained_binding_bound() {
    let binds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = Arc::clone(&binds);
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_: &BusName| {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    })
    .with_max_callers(64);
    let mut newest = None;

    for index in 1..=300 {
        let caller = BusName::new(format!(":1.{index}")).unwrap();
        endpoint
            .call(call(Some(&caller)))
            .await
            .expect("idle bindings must be evicted as callers churn");
        newest = Some(caller);
        assert!(endpoint.servers.lock().unwrap().len() <= 64);
    }

    assert_eq!(binds.load(std::sync::atomic::Ordering::SeqCst), 300);
    assert_eq!(endpoint.servers.lock().unwrap().len(), 64);
    assert!(endpoint.pending_bindings.lock().unwrap().is_empty());
    let newest = newest.unwrap();
    let epoch_before = endpoint.watch_epoch(&newest);
    assert!(epoch_before > 0);
    assert!(endpoint.call(call(Some(&newest))).await.is_ok());
    assert_eq!(endpoint.watch_epoch(&newest), epoch_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn caller_cache_does_not_evict_a_binding_during_an_ordinary_call() {
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let gate_for_clock = Arc::clone(&gate);
    let armed_for_clock = Arc::clone(&armed);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let backend = MemoryStorage::with_clock(Arc::new(move || {
        if armed_for_clock.swap(false, std::sync::atomic::Ordering::AcqRel) {
            started_tx.send(()).unwrap();
            let (lock, changed) = &*gate_for_clock;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
        }
        0
    }));
    let endpoint = Arc::new(
        PerCallerEndpoint::new(move |_: &BusName| {
            Some(StorageServer::new(
                backend.for_scope(&Scope::local()).unwrap(),
            ))
        })
        .with_max_callers(1),
    );
    let active = BusName::new(":1.1").unwrap();
    let newcomer = BusName::new(":1.2").unwrap();
    let endpoint_for_call = Arc::clone(&endpoint);
    let active_call = tokio::spawn(async move {
        endpoint_for_call
            .call(StorageCall {
                object: Object::Documents,
                selector: "Get",
                request: json!({"collection":"active.records","document_id":"in-flight"}),
                sender: Some(&active),
                admission: Admission::Private,
            })
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || started_rx.recv().unwrap()),
    )
    .await
    .expect("ordinary call did not reach its gated backend read")
    .unwrap();

    assert_eq!(
        endpoint.call(call(Some(&newcomer))).await.unwrap_err(),
        HostFailure::Denied("caller-capacity"),
        "an in-flight ordinary operation keeps its bound server from eviction"
    );
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let _outcome = active_call.await.unwrap();
    assert!(endpoint.call(call(Some(&newcomer))).await.is_ok());
}

#[tokio::test]
async fn caller_cache_keeps_a_live_download_stream_binding() {
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_: &BusName| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    })
    .with_max_callers(1);
    let first = BusName::new(":1.1").unwrap();
    let second = BusName::new(":1.2").unwrap();
    endpoint
        .put_blob_stream(
            Some(&first),
            PutBlobStreamRequest {
                key: "cache/live".to_owned(),
                len: 1,
                content_type: None,
                ttl_ms: None,
            },
            Box::pin(std::io::Cursor::new(vec![7])),
        )
        .await
        .unwrap();
    let (_head, download) = endpoint
        .get_blob_stream(
            Some(&first),
            BlobKeyRequest {
                key: "cache/live".to_owned(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        endpoint.call(call(Some(&second))).await.unwrap_err(),
        HostFailure::Denied("caller-capacity")
    );
    drop(download);
    assert!(endpoint.call(call(Some(&second))).await.is_ok());
}

#[tokio::test]
async fn caller_cache_keeps_a_binding_with_an_open_watch() {
    let backend = Arc::new(MemoryStorage::with_native_watch());
    backend
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("cache.records"))
        .await
        .unwrap();
    let backend_for_binder = Arc::clone(&backend);
    let endpoint = PerCallerEndpoint::new(move |_: &BusName| {
        Some(StorageServer::new(
            backend_for_binder.for_scope(&Scope::local()).unwrap(),
        ))
    })
    .with_max_callers(1);
    let first = BusName::new(":1.1").unwrap();
    let second = BusName::new(":1.2").unwrap();
    let opened = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"cache.records",
                "filter":serde_json::to_value(tinystoragedrivers_bus::Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&first),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let watch_id = opened["watch_id"].as_str().unwrap().to_owned();
    assert_eq!(
        endpoint.call(call(Some(&second))).await.unwrap_err(),
        HostFailure::Denied("caller-capacity")
    );
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchClose",
            request: json!({"watch_id":watch_id}),
            sender: Some(&first),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    assert!(endpoint.call(call(Some(&second))).await.is_ok());
}

#[tokio::test]
async fn release_fences_a_pending_async_binding_before_it_can_be_cached() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    let gate_for_binder = Arc::clone(&gate);
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_for_binder = Arc::clone(&attempts);
    let backend = Arc::new(MemoryStorage::new());
    let endpoint = PerCallerEndpoint::new_async(move |sender: BusName| {
        let started = started_tx.clone();
        let gate = Arc::clone(&gate_for_binder);
        let attempts = Arc::clone(&attempts_for_binder);
        let backend = Arc::clone(&backend);
        async move {
            let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                started.send(sender.clone()).unwrap();
                gate.acquire().await.unwrap().forget();
            }
            Some(StorageServer::new(
                backend.for_scope(&Scope::local()).unwrap(),
            ))
        }
    });
    let caller = BusName::new(":1.9").unwrap();
    let caller_for_task = caller.clone();
    let endpoint = Arc::new(endpoint);
    let endpoint_for_task = Arc::clone(&endpoint);
    let pending =
        tokio::spawn(async move { endpoint_for_task.call(call(Some(&caller_for_task))).await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        caller
    );
    assert!(!endpoint.release(&caller));
    gate.add_permits(1);
    assert_eq!(
        pending.await.unwrap().unwrap_err(),
        HostFailure::Denied("stale-caller-binding")
    );
    assert!(endpoint.call(call(Some(&caller))).await.is_ok());
}
