//! The driver against a real MongoDB, named `live_*`.
//!
//! Set `TSD_MONGO_URL` (for example
//! `mongodb://localhost:27017/tsd_test?directConnection=true`, a single-node
//! replica set) to run them; without it each test prints a note and passes.

mod support;

use support::{connect, skip};
use tinystoragedrivers_core::conformance;

#[tokio::test]
async fn live_conformance_on_a_replica_set() {
    let Some(backend) = connect().await else {
        return skip("live_conformance_on_a_replica_set");
    };
    conformance::run(&backend, false).await;
}

#[tokio::test]
async fn live_independent_connections_share_one_database() {
    let Some(url) = support::url() else {
        return skip("live_independent_connections_share_one_database");
    };
    let database = support::url_database(&url);
    conformance::run_shared(|| {
        let (url, database) = (url.clone(), database.clone());
        async move {
            tinystoragedrivers_mongodb::MongoStorage::connect(&url, &database)
                .await
                .expect("TSD_MONGO_URL is set but the server cannot be reached")
        }
    })
    .await;
}

#[tokio::test]
async fn live_conformance_without_transactions() {
    let Some(backend) = connect().await else {
        return skip("live_conformance_without_transactions");
    };
    conformance::run(&backend.without_transactions(), false).await;
}

#[tokio::test]
async fn live_true_standalone_keeps_single_document_writes_and_refuses_checked_batches() {
    use tinystoragedrivers_core::{Capability, ErrorKind, Precondition, Scope, StorageBackend};
    let Ok(url) = std::env::var("TSD_MONGO_STANDALONE_URL") else {
        return;
    };
    let backend =
        tinystoragedrivers_mongodb::MongoStorage::connect(&url, &support::url_database(&url))
            .await
            .unwrap();
    for capability in [
        Capability::Transactions,
        Capability::ValidatedBatch,
        Capability::Fencing,
        Capability::ScopePurge,
        Capability::AtomicScopePurge,
    ] {
        assert!(!backend.capabilities().contains(capability));
    }
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    scoped
        .documents()
        .put(
            "plain",
            "d",
            serde_json::json!({"works":true}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(
        scoped
            .documents()
            .get("plain", "d")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        scoped
            .documents()
            .atomic_batch_checked(vec![], vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ValidatedBatch)
    );
    let request = tinystoragedrivers_core::PurgeRequest {
        operation_id: "standalone_reset".into(),
        mode: tinystoragedrivers_core::PurgeMode::Quiescent,
    };
    assert_eq!(
        scoped.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        backend
            .for_scope_generation(&Scope::local(), tinystoragedrivers_core::ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    conformance::run(&backend, false).await;
}

/// A synchronous caller on the only worker of the runtime that built the
/// client reaches the server through the blocking bridge. The client's
/// connection pool runs on that runtime, so the bridge must not starve it.
#[test]
fn live_a_bridge_call_completes_on_a_one_worker_runtime() {
    use std::sync::mpsc;
    use tinystoragedrivers_core::{Blocking, Precondition, Scope, StorageBackend};
    if support::url().is_none() {
        return skip("live_a_bridge_call_completes_on_a_one_worker_runtime");
    }
    let (finished, outcome) = mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let written = runtime.block_on(async {
            let backend = connect().await.unwrap();
            let docs = backend
                .for_scope(&Scope::local())
                .unwrap()
                .documents()
                .clone();
            tokio::spawn(async move {
                Blocking::new()
                    .unwrap()
                    .run(async move {
                        docs.put(
                            "bridge",
                            "one-worker",
                            serde_json::json!({"ok": true}),
                            Precondition::None,
                        )
                        .await
                    })
                    .unwrap()
                    .is_ok()
            })
            .await
            .unwrap()
        });
        let _ = finished.send(written);
    });
    assert_eq!(
        outcome.recv_timeout(std::time::Duration::from_secs(30)),
        Ok(true),
        "the bridge call deadlocked the client's runtime"
    );
}
