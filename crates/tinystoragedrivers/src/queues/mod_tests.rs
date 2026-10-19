//! Queue URLs parse strictly, never print credentials, and a manifest is
//! applied to whichever queues are installed.
use super::*;
use crate::queue::{NewMessage, Retry};
use crate::{MemoryStorage, Scope, StorageBackend};

#[test]
fn storage_is_the_default_queue_location() {
    assert_eq!(QueueConfig::parse("").unwrap(), QueueConfig::Storage);
    assert_eq!(
        QueueConfig::parse(" storage ").unwrap(),
        QueueConfig::Storage
    );
    assert_eq!(QueueConfig::Storage.to_string(), "storage");
    assert_eq!(
        QueueConfig::parse("redis://x").unwrap_err().kind(),
        crate::ErrorKind::InvalidInput
    );
}

#[test]
fn amqp_urls_need_the_feature_and_hide_credentials() {
    let parsed = QueueConfig::parse("amqps://user:secret@broker:5671/%2f");
    if cfg!(feature = "rabbitmq") {
        let config = parsed.unwrap();
        assert_eq!(config.to_string(), "amqps://***@broker:5671/%2f");
        assert!(!format!("{config:?}").contains("secret"));
    } else {
        assert_eq!(parsed.unwrap_err().kind(), crate::ErrorKind::InvalidInput);
    }
    assert_eq!(redacted("amqp://broker/%2f"), "amqp://broker/%2f");
    assert_eq!(redacted("not a url"), "not a url");
    assert_eq!(
        redacted("amqp://u:p@host/v@x"),
        "amqp://***@host/v@x",
        "only the authority is redacted"
    );
}

#[tokio::test]
async fn a_manifest_declares_the_storage_queues() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queues.json");
    std::fs::write(
        &path,
        r#"{"version": 1, "queues": [{"name": "inbound", "max_attempts": 1}]}"#,
    )
    .unwrap();
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let storage = open_queues(&QueueConfig::Storage, storage, Some(&path))
        .await
        .unwrap();
    let queues = storage.queues();
    queues
        .enqueue("inbound", &[NewMessage::new(serde_json::json!(1))])
        .await
        .unwrap();
    let got = queues.receive("inbound", 1, 60_000, "w").await.unwrap();
    assert!(
        queues
            .nack("inbound", &got[0].receipt, Retry::After(0))
            .await
            .unwrap()
    );
    assert_eq!(
        queues.stats("inbound").await.unwrap().dead,
        1,
        "the manifest's one attempt applied"
    );

    let missing = dir.path().join("absent.json");
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    assert_eq!(
        open_queues(&QueueConfig::Storage, storage, Some(&missing))
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::NotFound
    );
}

#[cfg(not(feature = "rabbitmq"))]
#[tokio::test]
async fn a_broker_config_without_the_feature_is_refused() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let config = QueueConfig::RabbitMq {
        url: "amqp://localhost".into(),
    };
    assert_eq!(
        open_queues(&config, storage, None)
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn without_a_manifest_the_storage_queues_are_returned_as_they_are() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let before = std::sync::Arc::clone(storage.queues());
    let after = open_queues(&QueueConfig::Storage, storage, None)
        .await
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(&before, after.queues()));
}
