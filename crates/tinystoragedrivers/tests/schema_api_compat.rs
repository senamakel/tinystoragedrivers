//! Legacy constructors and executable schema APIs retain their original types.
use serde_json::json;
use tinystoragedrivers::*;

#[test]
fn pure_exports_preserve_legacy_constructor_error_and_port_signatures() {
    let owner: std::result::Result<Owner, StorageError> = Owner::new("orders");
    let spec = CollectionSpec::new("orders.rows");
    let valid: std::result::Result<(), StorageError> = spec.validate();
    let merged: std::result::Result<CollectionSpec, StorageError> = spec.merge(&spec);
    let key: std::result::Result<Key, StorageError> = Key::of(["a/b", "100%"]);
    valid.unwrap();
    let spec = merged.unwrap();
    assert_eq!(key.unwrap().as_str(), "a%2Fb/100%25");
    let schema = Schema::new(owner.unwrap())
        .collection_with(2, spec, ScopeClass::Tenant)
        .step(Step::new(
            "upgrade",
            "orders.rows",
            1,
            2,
            |batch: Vec<SourceRecord>| {
                Ok(batch
                    .into_iter()
                    .map(|r| TargetRecord::new("orders.rows", r.id, r.body))
                    .collect())
            },
        ));
    let _registry: std::result::Result<Registry, StorageError> = Registry::compose([schema]);
    let backend: &dyn StorageBackend = &MemoryStorage::new();
    let storage: std::result::Result<ScopedStorage, StorageError> =
        backend.for_scope(&Scope::local());
    let storage = storage.unwrap();
    let _: &std::sync::Arc<dyn DocumentStore> = storage.documents();
    let _: &std::sync::Arc<dyn StreamStore> = storage.streams();
    let _: &std::sync::Arc<dyn BlobStore> = storage.blobs();
    let _: declarations::TargetRecord = TargetRecord::new("orders.rows", "id", json!({}));
    let _: schema::Item = Item::Blobs {
        prefix: "orders/files/".into(),
        scope: ScopeClass::Agent,
    };
    let _: declarations::Owner = Owner::new("orders").unwrap();
    let _: SchemaDeclaration = declarations::SchemaDeclaration {
        owner: Owner::new("orders").unwrap(),
        items: vec![],
        steps: vec![],
    };
    let _: Step = Step::stream("move", "orders.audit", 0, 1, |source, entries| {
        Ok(Some((source, entries)))
    })
    .source_prefix("legacy/");
    let _: Step = Step::blobs(
        "move",
        "orders/files/",
        0,
        1,
        |source, content_type, bytes| Ok(Some((source, content_type, bytes))),
    )
    .source_prefix("legacy/");
}
