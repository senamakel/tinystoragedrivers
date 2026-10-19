//! The Queues object enforces its limits before native I/O, sizes replies to
//! the frame, and serves only a manifest's queues when it has one.
use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_core::queue::{NewMessage, QueueManifest, Retry};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend, StorageError};

use super::*;
use crate::Admission;

fn server() -> StorageServer {
    let backend = MemoryStorage::new();
    StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
}

async fn call(server: &StorageServer, selector: &str, request: Value) -> Reply {
    server
        .handle(Admission::Private, "Queues", selector, request)
        .await
}

async fn kind(server: &StorageServer, selector: &str, request: Value) -> StorageKind {
    call(server, selector, request).await.unwrap_err().kind
}

fn big(n: usize) -> Vec<NewMessage> {
    let filler = "x".repeat(200 * 1024);
    (0..n)
        .map(|i| NewMessage::new(json!({ "i": i, "fill": filler })))
        .collect()
}

#[tokio::test]
async fn unknown_selectors_and_oversized_enqueues_are_refused() {
    let server = server();
    assert_eq!(
        kind(&server, "Drop", json!({"queue": "q"})).await,
        StorageKind::InvalidInput
    );
    let huge = "x".repeat(MAX_QUEUE_MESSAGE_BYTES);
    assert_eq!(
        kind(
            &server,
            "Enqueue",
            json!({"queue": "q", "messages": [{"payload_json": serde_json::to_string(&huge).unwrap()}]})
        )
        .await,
        StorageKind::InvalidInput
    );
    let many: Vec<_> = (0..=MAX_BATCH_OPS)
        .map(|_| json!({"payload_json": "1"}))
        .collect();
    assert_eq!(
        kind(&server, "Enqueue", json!({"queue": "q", "messages": many})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Receive",
            json!({"queue": "q", "max": 0, "visibility_ms": 10, "consumer": "w"})
        )
        .await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Receive",
            json!({"queue": "q", "max": 1, "visibility_ms": 0, "consumer": "w"})
        )
        .await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn receive_leases_only_what_one_reply_can_carry() {
    let server = server();
    let queues = server.storage.queues();
    queues.enqueue("big", &big(45)).await.unwrap();
    let reply = call(
        &server,
        "Receive",
        json!({"queue": "big", "max": 45, "visibility_ms": 60_000, "consumer": "w"}),
    )
    .await
    .unwrap();
    let first: Vec<wire::QueueDelivery> = serde_json::from_value(reply.clone()).unwrap();
    assert!(!first.is_empty() && first.len() < 45, "{}", first.len());
    assert!(reply.to_string().len() <= MAX_INLINE_BYTES);
    let stats = queues.stats("big").await.unwrap();
    assert_eq!(
        stats.leased,
        first.len() as u64,
        "no lease beyond the reply"
    );
    let reply = call(
        &server,
        "Receive",
        json!({"queue": "big", "max": 45, "visibility_ms": 60_000, "consumer": "w"}),
    )
    .await
    .unwrap();
    let rest: Vec<wire::QueueDelivery> = serde_json::from_value(reply).unwrap();
    assert_eq!(first.len() + rest.len(), 45);
}

#[tokio::test]
async fn dead_letters_are_cut_at_the_inline_cap() {
    let server = server();
    let queues = server.storage.queues();
    queues.enqueue("dlq", &big(45)).await.unwrap();
    let leased = queues.receive("dlq", 45, 60_000, "w").await.unwrap();
    for delivery in &leased {
        assert!(
            queues
                .nack("dlq", &delivery.receipt, Retry::DeadLetter("x".into()))
                .await
                .unwrap()
        );
    }
    let reply = call(&server, "DeadLetters", json!({"queue": "dlq", "limit": 45}))
        .await
        .unwrap();
    let page: Vec<wire::QueueDeadLetter> = serde_json::from_value(reply).unwrap();
    assert!(!page.is_empty() && page.len() < 45, "{}", page.len());
}

fn manifest() -> Arc<QueueManifest> {
    Arc::new(
        QueueManifest::from_json(
            r#"{"version": 1, "queues": [{"name": "inbound", "max_attempts": 2}]}"#,
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn a_manifest_bounds_the_queues_a_caller_reaches() {
    let server = server().with_queue_manifest(manifest());
    assert!(format!("{server:?}").contains("queue_manifest: true"));
    // Features is not about one queue.
    assert!(call(&server, "Features", json!(null)).await.is_ok());
    assert_eq!(
        kind(&server, "Stats", json!({"queue": "other"})).await,
        StorageKind::NotFound
    );
    assert_eq!(
        kind(
            &server,
            "Enqueue",
            json!({"queue": "other", "messages": [{"payload_json": "1"}]})
        )
        .await,
        StorageKind::NotFound
    );
    assert_eq!(
        kind(
            &server,
            "EnsureQueue",
            json!({"queue": "other", "spec": {"max_attempts": 2}})
        )
        .await,
        StorageKind::NotFound
    );
    assert_eq!(
        kind(
            &server,
            "EnsureQueue",
            json!({"queue": "inbound", "spec": {"max_attempts": 9}})
        )
        .await,
        StorageKind::Conflict
    );
    assert!(
        call(
            &server,
            "EnsureQueue",
            json!({"queue": "inbound", "spec": {"max_attempts": 2}})
        )
        .await
        .is_ok()
    );

    // The host applied the declared spec: two attempts, then dead.
    call(
        &server,
        "Enqueue",
        json!({"queue": "inbound", "messages": [{"payload_json": "1"}]}),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        let reply = call(
            &server,
            "Receive",
            json!({"queue": "inbound", "max": 1, "visibility_ms": 60_000, "consumer": "w"}),
        )
        .await
        .unwrap();
        let batch: Vec<wire::QueueDelivery> = serde_json::from_value(reply).unwrap();
        let nacked = call(
            &server,
            "Nack",
            json!({"queue": "inbound", "receipt": batch[0].receipt, "retry": {"kind": "after", "delay_ms": 0}}),
        )
        .await
        .unwrap();
        assert_eq!(nacked, json!(true));
    }
    let stats: wire::QueueStatsReply = serde_json::from_value(
        call(&server, "Stats", json!({"queue": "inbound"}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stats.dead, 1);
}

#[tokio::test]
async fn a_manifest_that_cannot_be_applied_refuses_the_call() {
    // The host's queue is down when the manifest is first applied.
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let failing = storage.clone().with_queues(Arc::new(FailingQueue));
    let server = StorageServer::new(failing).with_queue_manifest(manifest());
    assert_eq!(
        kind(&server, "Stats", json!({"queue": "inbound"})).await,
        StorageKind::Unavailable
    );
}

#[derive(Debug)]
struct FailingQueue;

#[tinystoragedrivers_core::async_trait]
impl tinystoragedrivers_core::QueueStore for FailingQueue {
    fn features(&self) -> tinystoragedrivers_core::queue::QueueFeatures {
        tinystoragedrivers_core::queue::QueueFeatures::default()
    }
    async fn ensure_queue(
        &self,
        _: &str,
        _: &tinystoragedrivers_core::queue::QueueSpec,
    ) -> tinystoragedrivers_core::Result<()> {
        Err(StorageError::unavailable("down"))
    }
    async fn enqueue(
        &self,
        _: &str,
        _: &[NewMessage],
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::queue::Enqueued>> {
        Err(StorageError::unavailable("down"))
    }
    async fn receive(
        &self,
        _: &str,
        _: u32,
        _: u64,
        _: &str,
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::queue::Delivery>> {
        Err(StorageError::unavailable("down"))
    }
    async fn ack(
        &self,
        _: &str,
        _: &tinystoragedrivers_core::queue::Receipt,
    ) -> tinystoragedrivers_core::Result<bool> {
        Err(StorageError::unavailable("down"))
    }
    async fn nack(
        &self,
        _: &str,
        _: &tinystoragedrivers_core::queue::Receipt,
        _: Retry,
    ) -> tinystoragedrivers_core::Result<bool> {
        Err(StorageError::unavailable("down"))
    }
    async fn extend(
        &self,
        _: &str,
        _: &tinystoragedrivers_core::queue::Receipt,
        _: u64,
    ) -> tinystoragedrivers_core::Result<bool> {
        Err(StorageError::unavailable("down"))
    }
    async fn stats(
        &self,
        _: &str,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::queue::QueueStats> {
        Err(StorageError::unavailable("down"))
    }
    async fn dead_letters(
        &self,
        _: &str,
        _: usize,
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::queue::DeadLetter>> {
        Err(StorageError::unavailable("down"))
    }
    async fn redrive(&self, _: &str, _: &str) -> tinystoragedrivers_core::Result<bool> {
        Err(StorageError::unavailable("down"))
    }
    async fn purge(&self, _: &str) -> tinystoragedrivers_core::Result<u64> {
        Err(StorageError::unavailable("down"))
    }
}
