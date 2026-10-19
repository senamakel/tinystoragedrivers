//! `open_queues` against a real `RabbitMQ`, named `live_*`. Set
//! `TSD_RABBITMQ_URL` to run it; without it the test prints a note and passes.
#![cfg(feature = "rabbitmq")]

use tinystoragedrivers::queue::NewMessage;
use tinystoragedrivers::{MemoryStorage, QueueConfig, Scope, StorageBackend, open_queues};

#[tokio::test(flavor = "multi_thread")]
async fn live_an_amqp_queue_url_moves_a_scopes_queues_to_the_broker() {
    let Some(url) = std::env::var("TSD_RABBITMQ_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!("TSD_RABBITMQ_URL is unset, skipping");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("queues.json");
    std::fs::write(
        &manifest,
        r#"{"version": 1, "queues": [{"name": "facade.inbound", "max_attempts": 3}]}"#,
    )
    .unwrap();
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let config = QueueConfig::parse(&url).unwrap();
    let storage = open_queues(&config, storage, Some(&manifest))
        .await
        .unwrap();
    let queues = storage.queues();
    assert!(format!("{queues:?}").contains("RabbitQueues"));
    assert!(!queues.features().groups);
    queues.purge("facade.inbound").await.unwrap();
    queues
        .enqueue(
            "facade.inbound",
            &[NewMessage::new(serde_json::json!("hi"))],
        )
        .await
        .unwrap();
    let got = queues
        .receive("facade.inbound", 1, 60_000, "w")
        .await
        .unwrap();
    assert_eq!(got[0].payload, "hi");
    assert!(queues.ack("facade.inbound", &got[0].receipt).await.unwrap());
}
