//! Document value types and the typed extension trait.

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::*;
use crate::{ErrorKind, ID_FIELD, MemoryStorage, Scope, StorageBackend};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Task {
    state: String,
}

fn docs() -> std::sync::Arc<dyn DocumentStore> {
    MemoryStorage::new()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone()
}

#[test]
fn versions_start_at_one_and_rise() {
    assert_eq!(Version::FIRST, Version(1));
    assert_eq!(Version(1).next(), Some(Version(2)));
    assert_eq!(Version(u64::MAX).next(), None);
}

#[test]
fn versioned_maps_and_builds_its_precondition() {
    let found = Versioned {
        id: "a".to_owned(),
        version: Version(4),
        doc: 2,
        meta: Meta::unknown(),
    };
    assert_eq!(found.unchanged(), Precondition::Version(Version(4)));
    let mapped = found.map(|n| n * 10);
    assert_eq!(
        (mapped.id.as_str(), mapped.version, mapped.doc),
        ("a", Version(4), 20)
    );
}

#[test]
fn preconditions_serialize_tagged() {
    assert_eq!(
        serde_json::to_value(Precondition::None).unwrap(),
        json!({"kind": "none"})
    );
    assert_eq!(
        serde_json::to_value(Precondition::Version(Version(3))).unwrap(),
        json!({"kind": "version", "version": 3})
    );
    assert_eq!(Precondition::default(), Precondition::None);
}

#[test]
fn collection_specs_validate() {
    let spec = CollectionSpec::new("jobs")
        .index(IndexSpec::new("by_state", ["state", "run_at"]))
        .index(IndexSpec::new("by_key", ["key"]).unique())
        .ttl("expires_at")
        .searchable(["title"]);
    assert!(spec.validate().is_ok());
    assert!(spec.indexes[1].unique);
    assert_eq!(spec.ttl_field.as_deref(), Some("expires_at"));

    let duplicate = CollectionSpec::new("jobs")
        .index(IndexSpec::new("i", ["a"]))
        .index(IndexSpec::new("i", ["b"]));
    assert_eq!(
        duplicate.validate().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(
        CollectionSpec::new("jobs")
            .index(IndexSpec::new("", ["a"]))
            .validate()
            .is_err()
    );
    assert!(
        CollectionSpec::new("jobs")
            .index(IndexSpec::new("i", ["a..b"]))
            .validate()
            .is_err()
    );
    assert!(CollectionSpec::new("jobs").ttl("").validate().is_err());
    assert!(
        CollectionSpec::new("jobs")
            .searchable([""])
            .validate()
            .is_err()
    );
    assert!(CollectionSpec::new("bad name").validate().is_err());
}

#[test]
fn names_ids_and_bodies_validate() {
    assert!(validate_collection("flows.runs-v2_x").is_ok());
    for bad in ["", "has space", "ümlaut", "_tsd_meta"] {
        assert!(validate_collection(bad).is_err(), "{bad}");
    }
    assert!(validate_collection(&"a".repeat(MAX_COLLECTION_LEN + 1)).is_err());
    assert!(validate_id("any id/with:chars").is_ok());
    assert!(validate_id("").is_err());
    assert!(validate_id("nul\0").is_err());
    assert!(validate_id(&"a".repeat(MAX_ID_LEN + 1)).is_err());
    assert!(validate_doc(&json!({})).is_ok());
    assert!(validate_doc(&json!("x")).is_err());
}

#[test]
fn queries_build_fluently() {
    let query = Query::filter(Filter::eq("a", 1))
        .sort(Sort::desc("b"))
        .limit(5)
        .after(Cursor("c".into()));
    assert_eq!(query.limit, Some(5));
    assert_eq!(query.sort, vec![Sort::desc("b")]);
    assert_eq!(query.cursor, Some(Cursor("c".into())));
    assert_eq!(Query::all(), Query::default());
    let encoded = serde_json::to_value(Query::all()).unwrap();
    assert_eq!(encoded, json!({"filter": {"op": "all"}, "sort": []}));
}

#[test]
fn keyset_positions_validate_shape_and_order_by_values_then_id() {
    let sort = [Sort::desc("rank"), Sort::asc(ID_FIELD)];
    let position = CursorPosition::for_document("b", &json!({"rank": 4}), &sort);
    assert_eq!(position.values, [json!(4), json!("b")]);
    assert_eq!(
        position.compare(
            &CursorPosition::for_document("a", &json!({"rank": 4}), &sort),
            &sort,
        ),
        std::cmp::Ordering::Greater,
        "the id breaks ties after all requested directions"
    );
    position.validate(&sort).unwrap();

    for invalid in [
        CursorPosition {
            id: String::new(),
            values: vec![json!(4), json!("b")],
        },
        CursorPosition {
            id: "b".into(),
            values: vec![json!(4)],
        },
        CursorPosition {
            id: "b".into(),
            values: vec![json!(4), json!("other")],
        },
    ] {
        assert_eq!(
            invalid.validate(&sort).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }

    assert_eq!(
        Cursor("mem:keyset1:0000000000000000:{\"id\":\"x\",\"values\":[],\"extra\":true}".into())
            .decode_keyset("mem", 0)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn write_ops_serialize_tagged() {
    let op = WriteOp::Delete {
        collection: "c".into(),
        id: "i".into(),
        precondition: Precondition::Absent,
    };
    assert_eq!(
        serde_json::to_value(&op).unwrap(),
        json!({"op": "delete", "collection": "c", "id": "i", "precondition": {"kind": "absent"}})
    );
    assert_eq!(
        serde_json::to_value(WriteResult::Put {
            version: Version(2)
        })
        .unwrap(),
        json!({"op": "put", "version": 2})
    );
}

#[tokio::test]
async fn typed_helpers_round_trip() {
    let docs = docs();
    docs.put_as(
        "tasks",
        "a",
        &Task {
            state: "open".into(),
        },
        Precondition::None,
    )
    .await
    .unwrap();
    docs.put_as(
        "tasks",
        "b",
        &Task {
            state: "done".into(),
        },
        Precondition::None,
    )
    .await
    .unwrap();
    let a = docs.get_as::<Task>("tasks", "a").await.unwrap().unwrap();
    assert_eq!(
        a.doc,
        Task {
            state: "open".into()
        }
    );
    assert!(docs.get_as::<Task>("tasks", "zz").await.unwrap().is_none());

    let page = docs
        .query_as::<Task>("tasks", &Query::all().limit(1))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(page.next.is_some());

    let all = docs
        .query_all("tasks", &Query::all().limit(1))
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn typed_reads_report_decode_failures() {
    let docs = docs();
    docs.put("tasks", "x", json!({"state": 7}), Precondition::None)
        .await
        .unwrap();
    let error = docs.get_as::<Task>("tasks", "x").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    let error = docs
        .query_as::<Task>("tasks", &Query::all())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
}

#[derive(Debug)]
struct Baseline;

#[async_trait::async_trait]
impl DocumentStore for Baseline {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    async fn ensure_collection(&self, _: &CollectionSpec) -> Result<()> {
        Ok(())
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<Versioned<Value>>> {
        Ok(None)
    }
    async fn put(&self, _: &str, _: &str, _: Value, _: Precondition) -> Result<Version> {
        Ok(Version::FIRST)
    }
    async fn delete(&self, _: &str, _: &str, _: Precondition) -> Result<bool> {
        Ok(false)
    }
    async fn query(&self, _: &str, _: &Query) -> Result<Page<Versioned<Value>>> {
        Ok(Page {
            items: Vec::new(),
            next: None,
        })
    }
    async fn count(&self, _: &str, _: &Filter) -> Result<u64> {
        Ok(0)
    }
    async fn delete_where(&self, _: &str, _: &Filter) -> Result<u64> {
        Ok(0)
    }
    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[Sort],
        _: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        Ok(None)
    }
    async fn drop_collection(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn put_with_default_options_is_a_plain_put_and_declared_options_are_unsupported() {
    let baseline = Baseline;
    assert_eq!(
        baseline
            .put_with(
                "c",
                "i",
                json!({}),
                Precondition::None,
                WriteOptions::default()
            )
            .await
            .unwrap(),
        Version::FIRST
    );
    for options in [
        WriteOptions::new().schema(1),
        WriteOptions::new().expires_ms(5),
    ] {
        let error = baseline
            .put_with("c", "i", json!({}), Precondition::None, options)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::Unsupported(Capability::RecordMetadata)
        );
    }
}

#[tokio::test]
async fn optional_operations_default_to_unsupported() {
    let baseline = Baseline;
    assert_eq!(
        baseline
            .validate_collection_spec(&CollectionSpec::new("notes"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::CollectionValidation)
    );
    let error = baseline.atomic_batch(Vec::new()).await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::Transactions)
    );
    let error = baseline
        .atomic_batch_with(Vec::new(), WriteOptions::new().schema(1))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::AtomicWriteOptions)
    );
    let error = baseline
        .atomic_batch_checked_with(Vec::new(), Vec::new(), WriteOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::AtomicWriteOptions)
    );
    let error = baseline.search("c", "x", 1).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::FullText));
    let error = baseline.collections("").await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::CollectionEnumeration)
    );
    let none = Baseline;
    assert!(none.get("c", "i").await.unwrap().is_none());
    assert_eq!(
        none.put("c", "i", json!({}), Precondition::None)
            .await
            .unwrap(),
        Version::FIRST
    );
    assert!(!none.delete("c", "i", Precondition::None).await.unwrap());
    assert_eq!(none.query("c", &Query::all()).await.unwrap().items.len(), 0);

    assert_eq!(none.count("c", &Filter::All).await.unwrap(), 0);
    assert_eq!(none.delete_where("c", &Filter::All).await.unwrap(), 0);
    assert!(
        none.claim("c", &Filter::All, &[], &json!({}))
            .await
            .unwrap()
            .is_none()
    );
    none.ensure_collection(&CollectionSpec::new("c"))
        .await
        .unwrap();
    none.drop_collection("c").await.unwrap();
}

#[test]
fn collection_declarations_merge() {
    let first = CollectionSpec::new("users")
        .index(IndexSpec::new("by_email", ["email"]).unique())
        .searchable(["name"]);
    let second = CollectionSpec::new("users")
        .index(IndexSpec::new("by_team", ["team"]))
        .ttl("expires_at")
        .searchable(["bio", "name"]);
    let merged = first.merge(&second).unwrap();
    let names: Vec<_> = merged.indexes.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, ["by_email", "by_team"], "earlier indexes survive");
    assert_eq!(merged.ttl_field.as_deref(), Some("expires_at"));
    assert_eq!(merged.search.unwrap().fields, ["name", "bio"]);

    let base = first.merge(&CollectionSpec::new("users")).unwrap();
    assert_eq!(base, first, "a bare redeclaration changes nothing");
    assert!(
        base.merge(&first).is_ok(),
        "redeclaring the same index is fine"
    );

    let clash = CollectionSpec::new("users").index(IndexSpec::new("by_email", ["mail"]));
    assert_eq!(
        first.merge(&clash).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let ttl_a = CollectionSpec::new("users").ttl("a");
    let ttl_b = CollectionSpec::new("users").ttl("b");
    assert!(ttl_a.merge(&ttl_b).is_err(), "expiry field cannot change");
    assert!(ttl_a.merge(&ttl_a).is_ok());
    assert!(first.merge(&CollectionSpec::new("other")).is_err());
}

#[test]
fn zero_page_sizes_are_rejected() {
    assert_eq!(
        Query::all().limit(0).validate().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(Query::all().limit(1).validate().is_ok());
    assert!(Query::filter(Filter::eq("", 1)).validate().is_err());
}

#[derive(Debug)]
struct Stuck;

#[async_trait::async_trait]
impl DocumentStore for Stuck {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    async fn ensure_collection(&self, _: &CollectionSpec) -> Result<()> {
        Ok(())
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<Versioned<Value>>> {
        Ok(None)
    }
    async fn put(&self, _: &str, _: &str, _: Value, _: Precondition) -> Result<Version> {
        Ok(Version::FIRST)
    }
    async fn delete(&self, _: &str, _: &str, _: Precondition) -> Result<bool> {
        Ok(false)
    }
    async fn query(&self, _: &str, _: &Query) -> Result<Page<Versioned<Value>>> {
        Ok(Page {
            items: Vec::new(),
            next: Some(Cursor("same".into())),
        })
    }
    async fn count(&self, _: &str, _: &Filter) -> Result<u64> {
        Ok(0)
    }
    async fn delete_where(&self, _: &str, _: &Filter) -> Result<u64> {
        Ok(0)
    }
    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[Sort],
        _: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        Ok(None)
    }
    async fn drop_collection(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn query_all_refuses_a_cursor_that_never_advances() {
    let stuck = Stuck;
    let error = stuck.query_all("c", &Query::all()).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(stuck.get("c", "i").await.unwrap().is_none());
    assert_eq!(
        stuck
            .put("c", "i", json!({}), Precondition::None)
            .await
            .unwrap(),
        Version::FIRST
    );
    assert!(!stuck.delete("c", "i", Precondition::None).await.unwrap());
    assert_eq!(stuck.count("c", &Filter::All).await.unwrap(), 0);
    assert_eq!(stuck.delete_where("c", &Filter::All).await.unwrap(), 0);
    assert!(
        stuck
            .claim("c", &Filter::All, &[], &json!({}))
            .await
            .unwrap()
            .is_none()
    );
    stuck
        .ensure_collection(&CollectionSpec::new("c"))
        .await
        .unwrap();
    stuck.drop_collection("c").await.unwrap();
    assert!(stuck.capabilities().iter().next().is_none());
}

#[test]
fn sort_fields_are_validated() {
    assert!(Query::all().sort(Sort::asc("_id")).validate().is_ok());
    assert!(Query::all().sort(Sort::asc("a.b")).validate().is_ok());
    for bad in ["", "a..b", ".a"] {
        let error = Query::all().sort(Sort::asc(bad)).validate().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
}
