//! Scope and database isolation checks.

use serde_json::json;

use super::{empty, fails, has, ok, unique};
use crate::backend::{ScopedStorage, StorageBackend};
use crate::capabilities::Capability;
use crate::document::{Precondition, Query};
use crate::error::ErrorKind;
use crate::filter::Filter;
use crate::scope::Scope;

/// Two scopes on one backend never see each other's data.
pub(super) async fn run(backend: &dyn StorageBackend) {
    let a = ok(backend.for_scope(&Scope::local()), "scope a");
    let b = ok(
        backend.for_scope(&Scope::new("conformance-b").expect("valid scope")),
        "scope b",
    );
    let coll = unique("iso");
    let stream = unique("iso_stream");
    let key = format!("{}/k", unique("iso_blob"));

    ok(
        a.documents()
            .put(&coll, "same", json!({"owner": "a"}), Precondition::None)
            .await,
        "a put",
    );
    assert!(
        ok(b.documents().get(&coll, "same").await, "b get").is_none(),
        "documents leak"
    );
    ok(
        b.documents()
            .put(&coll, "same", json!({"owner": "b"}), Precondition::Absent)
            .await,
        "b may create the same id",
    );
    collection_listing(&a, &b).await;
    assert_eq!(
        ok(a.documents().count(&coll, &Filter::All).await, "a count"),
        1
    );
    assert_eq!(
        ok(
            b.documents().delete_where(&coll, &Filter::All).await,
            "b delete_where"
        ),
        1,
        "delete_where stays in its scope"
    );
    let still =
        ok(a.documents().get(&coll, "same").await, "a re-read").expect("a's document survives");
    assert_eq!(still.doc["owner"], "a");
    assert!(
        ok(
            b.documents()
                .claim(&coll, &Filter::All, &[], &json!({"x": 1}))
                .await,
            "b claim"
        )
        .is_none(),
        "claims stay in their scope"
    );
    ok(b.documents().drop_collection(&coll).await, "b drop");
    assert!(ok(a.documents().get(&coll, "same").await, "a after b drop").is_some());

    ok(a.streams().append(&stream, json!(1)).await, "a append");
    assert_eq!(
        ok(b.streams().len(&stream).await, "b len"),
        0,
        "streams leak"
    );
    if has(&a, Capability::ConditionalStreamAppend) && has(&b, Capability::ConditionalStreamAppend)
    {
        ok(
            a.streams()
                .append_batch_if_offset(&stream, 1, vec![json!(2)])
                .await,
            "scope a conditional",
        );
        ok(
            b.streams()
                .append_batch_if_offset(&stream, 0, vec![json!(3)])
                .await,
            "scope b conditional",
        );
        assert_eq!(ok(a.streams().len(&stream).await, "scope a offset"), 2);
        ok(
            b.streams().delete_stream(&stream).await,
            "cleanup scope b conditional",
        );
    }
    empty(
        &ok(b.streams().streams(&stream).await, "b list"),
        "expected nothing",
    );

    ok(a.blobs().put(&key, vec![1], None).await, "a blob");
    assert!(
        ok(b.blobs().get(&key).await, "b blob").is_none(),
        "blobs leak"
    );
    empty(
        &ok(b.blobs().list(&key).await, "b list blobs"),
        "expected nothing",
    );

    assert_eq!(b.scope().as_str(), "conformance-b");
}

async fn collection_listing(a: &ScopedStorage, b: &ScopedStorage) {
    if !has(a, Capability::CollectionEnumeration) || !has(b, Capability::CollectionEnumeration) {
        for storage in [a, b] {
            if !has(storage, Capability::CollectionEnumeration) {
                fails(
                    storage.documents().collections("").await,
                    ErrorKind::Unsupported(Capability::CollectionEnumeration),
                    "scope collections without the capability",
                );
            }
        }
        return;
    }
    let only_a = unique("iso_only_a");
    let only_b = unique("iso_only_b");
    ok(
        a.documents()
            .put(&only_a, "only", json!({}), Precondition::None)
            .await,
        "a put in an A-only collection",
    );
    ok(
        b.documents()
            .put(&only_b, "only", json!({}), Precondition::None)
            .await,
        "b put in a B-only collection",
    );
    assert_eq!(
        ok(a.documents().collections(&only_b).await, "a list B-only"),
        Vec::<String>::new(),
        "A cannot enumerate B-only collections"
    );
    assert_eq!(
        ok(b.documents().collections(&only_a).await, "b list A-only"),
        Vec::<String>::new(),
        "B cannot enumerate A-only collections"
    );
}

/// A tenant scope and the agent and module scopes beneath it never see each
/// other's data, though their names share a prefix.
///
/// The same collection and id, stream and blob key are written under the
/// tenant `S`, `S/agent:a` and `S/module:m` with a different payload each.
/// Reads, counts, listings and bulk deletes in one scope must neither reach
/// nor change the others, and scope enumeration must report each scope once.
pub(super) async fn nested(backend: &dyn StorageBackend) {
    let parent_scope = ok(Scope::tenant(unique("nest")), "nested tenant scope");
    let agent_scope = ok(parent_scope.agent("a"), "nested agent scope");
    let module_scope = ok(parent_scope.module("m"), "nested module scope");
    let scopes = [
        ok(backend.for_scope(&parent_scope), "nested parent handle"),
        ok(backend.for_scope(&agent_scope), "nested agent handle"),
        ok(backend.for_scope(&module_scope), "nested module handle"),
    ];
    let names = ["parent", "agent", "module"];
    let coll = unique("nest");
    let stream = unique("nest_stream");
    let blob = format!("{}/k", unique("nest_blob"));

    for (storage, name) in scopes.iter().zip(names) {
        ok(
            storage
                .documents()
                .put(&coll, "same", json!({"owner": name}), Precondition::Absent)
                .await,
            "nested put of the same id in each scope",
        );
        ok(
            storage.streams().append(&stream, json!(name)).await,
            "nested append",
        );
        ok(
            storage
                .blobs()
                .put(&blob, name.as_bytes().to_vec(), None)
                .await,
            "nested blob put",
        );
    }

    for (storage, name) in scopes.iter().zip(names) {
        nested_reads(storage, name, &coll, &stream, &blob).await;
    }
    nested_collection_listings(&scopes).await;
    nested_enumeration(backend, &parent_scope, &agent_scope, &module_scope).await;

    // Bulk deletes in the parent leave both children alone, and the reverse.
    assert_eq!(
        ok(
            scopes[0]
                .documents()
                .delete_where(&coll, &Filter::All)
                .await,
            "nested parent delete_where"
        ),
        1,
        "delete_where in the tenant scope reaches only its own document"
    );
    ok(
        scopes[0].documents().drop_collection(&coll).await,
        "nested parent drop",
    );
    ok(
        scopes[0].streams().delete_stream(&stream).await,
        "nested parent delete_stream",
    );
    ok(
        scopes[0].blobs().delete(&blob).await,
        "nested parent blob delete",
    );
    for (storage, name) in scopes.iter().zip(names).skip(1) {
        nested_reads(storage, name, &coll, &stream, &blob).await;
    }
    ok(
        scopes[1].documents().drop_collection(&coll).await,
        "nested agent drop",
    );
    assert_eq!(
        ok(
            scopes[2].documents().count(&coll, &Filter::All).await,
            "nested module count after the agent dropped its collection"
        ),
        1,
        "dropping a collection in a sibling scope leaves the module's document"
    );
}

/// The scope holds exactly its own document, stream entry and blob.
async fn nested_reads(storage: &ScopedStorage, name: &str, coll: &str, stream: &str, blob: &str) {
    let found = ok(storage.documents().get(coll, "same").await, "nested get")
        .unwrap_or_else(|| panic!("nested: {name} lost its document"));
    assert_eq!(
        found.doc["owner"], name,
        "nested: {name} read another scope's document"
    );
    assert_eq!(
        ok(
            storage.documents().count(coll, &Filter::All).await,
            "nested count"
        ),
        1,
        "nested: {name} counts another scope's documents"
    );
    let page = ok(
        storage.documents().query(coll, &Query::all()).await,
        "nested query",
    );
    assert_eq!(
        page.items.len(),
        1,
        "nested: {name} queries another scope's documents"
    );
    assert_eq!(
        ok(storage.streams().len(stream).await, "nested len"),
        1,
        "nested: {name} sees another scope's stream entries"
    );
    let window = ok(
        storage.streams().read_window(stream, 0, 10).await,
        "nested read_window",
    );
    assert_eq!(
        window.len(),
        1,
        "nested: {name} reads another scope's entries"
    );
    assert_eq!(
        window[0].value,
        json!(name),
        "nested: {name} stream payload"
    );
    let bytes = ok(storage.blobs().get(blob).await, "nested blob get")
        .unwrap_or_else(|| panic!("nested: {name} lost its blob"));
    assert_eq!(bytes.bytes, name.as_bytes(), "nested: {name} blob payload");
    let listed = ok(storage.blobs().list(blob).await, "nested blob list");
    assert_eq!(
        listed.len(),
        1,
        "nested: {name} lists another scope's blobs"
    );
}

/// A collection that exists only in one scope is listed by that scope alone.
async fn nested_collection_listings(scopes: &[ScopedStorage; 3]) {
    if !scopes
        .iter()
        .all(|storage| has(storage, Capability::CollectionEnumeration))
    {
        return;
    }
    let only: Vec<String> = (0..3).map(|_| unique("nest_only")).collect();
    for (storage, coll) in scopes.iter().zip(&only) {
        ok(
            storage
                .documents()
                .put(coll, "only", json!({}), Precondition::None)
                .await,
            "nested put in a scope-only collection",
        );
    }
    for (index, storage) in scopes.iter().enumerate() {
        for (other, coll) in only.iter().enumerate() {
            let listed = ok(
                storage.documents().collections(coll).await,
                "nested collections",
            );
            if index == other {
                assert_eq!(
                    &listed,
                    std::slice::from_ref(coll),
                    "nested: a scope lists its own collection"
                );
            } else {
                assert_eq!(
                    listed,
                    Vec::<String>::new(),
                    "nested: scope {index} lists scope {other}'s collection"
                );
            }
        }
    }
}

/// Scope enumeration reports the tenant and each principal once, and a scope's
/// own listings stay inside it.
async fn nested_enumeration(
    backend: &dyn StorageBackend,
    parent: &Scope,
    agent: &Scope,
    module: &Scope,
) {
    if !backend
        .capabilities()
        .contains(Capability::ScopeEnumeration)
    {
        return;
    }
    let mut expected = vec![parent.clone(), agent.clone(), module.clone()];
    expected.sort();
    assert_eq!(
        ok(
            backend.scopes_of_tenant(parent).await,
            "nested scopes_of_tenant"
        ),
        expected,
        "the tenant enumerates itself and its principals, once each"
    );
    // A look-alike tenant that shares the textual prefix is not part of the family.
    let lookalike = ok(
        Scope::new(format!("{}-2", parent.as_str())),
        "look-alike scope",
    );
    ok(
        ok(backend.for_scope(&lookalike), "look-alike handle")
            .documents()
            .put(&unique("nest_look"), "x", json!({}), Precondition::None)
            .await,
        "look-alike put",
    );
    assert_eq!(
        ok(
            backend.scopes_of_tenant(parent).await,
            "nested scopes_of_tenant after a look-alike"
        ),
        expected,
        "a look-alike tenant is not part of the family"
    );
    let literal = ok(
        backend.scopes(agent.as_str()).await,
        "nested scopes by prefix",
    );
    assert_eq!(
        literal,
        vec![agent.clone()],
        "the agent prefix lists the agent alone"
    );
}
