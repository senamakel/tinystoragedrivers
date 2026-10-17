use tinystoragedrivers_bus::{CollectionSpec, IndexSpec, SearchSpec, StorageNeeds};

fn main() {
    let collections = [CollectionSpec {
        name: "records".into(),
        indexes: vec![IndexSpec {
            name: "by_owner".into(),
            fields: vec!["owner.id".into()],
            unique: true,
        }],
        ttl_field: Some("expires_at".into()),
        search: Some(SearchSpec {
            fields: vec!["body.text".into()],
        }),
    }];
    let blob_prefixes = ["imports/"];
    let stream_prefixes = ["events/"];
    let secret_prefixes = ["provider/"];
    let exact_secret_names = ["mcp_cache_fingerprint_key"];
    let needs = StorageNeeds {
        collections: &collections,
        blob_prefixes: &blob_prefixes,
        stream_prefixes: &stream_prefixes,
        secret_prefixes: &secret_prefixes,
    };

    assert_eq!(needs.collections[0].name, "records");
    assert_eq!(needs.collections[0].indexes[0].fields, ["owner.id"]);
    assert_eq!(needs.collections[0].ttl_field.as_deref(), Some("expires_at"));
    assert_eq!(
        needs.collections[0]
            .search
            .as_ref()
            .map(|search| search.fields.as_slice()),
        Some(["body.text".to_owned()].as_slice())
    );
    assert_eq!(needs.blob_prefixes, ["imports/"]);
    assert_eq!(needs.stream_prefixes, ["events/"]);
    assert_eq!(needs.secret_prefixes, ["provider/"]);

    let exact_needs = needs.with_exact_secret_names(&exact_secret_names);
    assert_eq!(exact_needs.needs, needs);
    assert_eq!(exact_needs.exact_secret_names, ["mcp_cache_fingerprint_key"]);
    let debug = format!("{exact_needs:?}");
    assert!(!debug.contains("provider/"));
    assert!(!debug.contains("mcp_cache_fingerprint_key"));
}
