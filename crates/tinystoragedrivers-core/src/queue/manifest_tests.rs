use super::*;
use crate::backend::StorageBackend;
use crate::error::ErrorKind;
use crate::memory::MemoryStorage;
use crate::queue::DocumentQueue;
use crate::scope::Scope;

const EXAMPLE: &str = include_str!("../../../../docs/examples/queues.json");
const SCHEMA: &str = include_str!("../../../../schemas/queues.schema.json");

fn invalid(text: &str) -> String {
    let error = QueueManifest::from_json(text).expect_err("manifest must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidInput, "{error}");
    error.message().to_owned()
}

#[test]
fn the_example_manifest_loads() {
    let manifest = QueueManifest::from_json(EXAMPLE).expect("example is valid");
    let ingress = manifest.get("inbound.ingress").expect("ingress declared");
    assert_eq!(ingress.spec().dedupe_window_ms, 2_592_000_000);
    assert_eq!(ingress.visibility_ms, Some(30_000));
    assert!(manifest.get("missing").is_none());
}

#[test]
fn omitted_fields_take_the_spec_defaults() {
    let manifest =
        QueueManifest::from_json(r#"{"version":1,"queues":[{"name":"a"}]}"#).expect("valid");
    assert_eq!(manifest.queues[0].spec(), QueueSpec::default());
    assert_eq!(manifest.queues[0].visibility_ms, None);
}

#[test]
fn typos_and_bad_values_name_the_offending_entry() {
    assert!(
        invalid(r#"{"version":1,"queues":[{"name":"a","max_attempt":3}]}"#).contains("max_attempt")
    );
    assert!(invalid(r#"{"version":2,"queues":[]}"#).contains("version 2"));
    assert!(
        invalid(r#"{"version":1,"queues":[{"name":"ok"},{"name":"Bad"}]}"#)
            .contains("queues[1] (\"Bad\")")
    );
    assert!(
        invalid(r#"{"version":1,"queues":[{"name":"a"},{"name":"a"}]}"#).contains("more than once")
    );
    assert!(
        invalid(r#"{"version":1,"queues":[{"name":"a","max_attempts":0}]}"#).contains("queues[0]")
    );
    assert!(
        invalid(r#"{"version":1,"queues":[{"name":"a","visibility_ms":0}]}"#)
            .contains("visibility_ms")
    );
    assert!(invalid("{").contains("queue manifest"));
}

#[test]
fn the_json_schema_lists_exactly_the_declaration_fields() {
    let schema: serde_json::Value = serde_json::from_str(SCHEMA).expect("schema is JSON");
    let mut documented: Vec<&str> = schema["$defs"]["queue"]["properties"]
        .as_object()
        .expect("queue properties")
        .keys()
        .map(String::as_str)
        .collect();
    documented.sort_unstable();
    let full = QueueDeclaration {
        name: "a".into(),
        description: "d".into(),
        owner: Some("o".into()),
        max_attempts: 1,
        dedupe_window_ms: 0,
        visibility_ms: Some(1),
    };
    let wire = serde_json::to_value(full).expect("serializes");
    let mut fields: Vec<&str> = wire
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    fields.sort_unstable();
    assert_eq!(
        documented, fields,
        "schemas/queues.schema.json drifted from QueueDeclaration"
    );
    assert_eq!(
        schema["properties"]["version"]["const"],
        QUEUE_MANIFEST_VERSION
    );
}

#[test]
fn load_reads_a_file_and_names_it_in_errors() {
    let dir = std::env::temp_dir().join(format!("tsd-manifest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let good = dir.join("queues.json");
    std::fs::write(&good, EXAMPLE).expect("write");
    assert_eq!(QueueManifest::load(&good).expect("loads").queues.len(), 3);

    let bad = dir.join("bad.json");
    std::fs::write(&bad, r#"{"version":1,"queues":[{"name":"x","nope":1}]}"#).expect("write");
    let error = QueueManifest::load(&bad).expect_err("rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.message().contains("bad.json"), "{error}");

    let missing = QueueManifest::load(dir.join("absent.json")).expect_err("missing");
    assert_eq!(missing.kind(), ErrorKind::NotFound);
    let directory = QueueManifest::load(&dir).expect_err("a directory is not a file");
    assert_eq!(directory.kind(), ErrorKind::Backend);
    std::fs::remove_dir_all(&dir).expect("cleanup");
}

#[tokio::test]
async fn apply_declares_every_queue() {
    let storage = MemoryStorage::new()
        .for_scope(&Scope::local())
        .expect("local scope");
    let queue = DocumentQueue::new(storage.documents());
    let manifest = QueueManifest::from_json(EXAMPLE).expect("valid");
    manifest.apply(&queue).await.expect("applies");
    let fresh = DocumentQueue::new(storage.documents());
    assert_eq!(
        fresh
            .spec("inbound.digest")
            .await
            .expect("spec")
            .max_attempts,
        3,
        "the declared spec is stored, not just cached"
    );
}
