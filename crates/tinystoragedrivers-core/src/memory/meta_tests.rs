//! Record metadata in the memory driver, driven by an injected clock so every
//! timestamp is exact.

use super::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;

use crate::{
    CollectionSpec, DocumentStore, Filter, Meta, Precondition, Query, ReadCheck, Sort, Version,
    WriteOp, WriteOptions,
};

struct Rig {
    now: Arc<AtomicU64>,
    docs: Arc<dyn DocumentStore>,
}

impl Rig {
    fn at(start: u64) -> Self {
        let now = Arc::new(AtomicU64::new(start));
        let reading = Arc::clone(&now);
        let storage = MemoryStorage::with_clock(Arc::new(move || reading.load(Ordering::SeqCst)));
        let docs = storage
            .for_scope(&Scope::local())
            .unwrap()
            .documents()
            .clone();
        Self { now, docs }
    }

    fn set(&self, ms: u64) {
        self.now.store(ms, Ordering::SeqCst);
    }

    async fn meta(&self, id: &str) -> Meta {
        self.docs.get("c", id).await.unwrap().unwrap().meta
    }

    async fn put(&self, id: &str) {
        self.docs
            .put("c", id, json!({"n": 1}), Precondition::None)
            .await
            .unwrap();
    }
}

#[test]
fn the_memory_driver_reports_record_metadata() {
    let rig = Rig::at(0);
    assert!(
        rig.docs
            .capabilities()
            .contains(crate::Capability::RecordMetadata)
    );
}

#[tokio::test]
async fn created_survives_updates_and_updated_advances() {
    let rig = Rig::at(1_000);
    rig.put("a").await;
    assert_eq!(
        rig.meta("a").await,
        Meta {
            schema: 0,
            created_ms: 1_000,
            updated_ms: 1_000,
            expires_ms: None
        }
    );
    rig.set(1_500);
    rig.put("a").await;
    let meta = rig.meta("a").await;
    assert_eq!((meta.created_ms, meta.updated_ms), (1_000, 1_500));
}

#[tokio::test]
async fn recreating_a_deleted_id_starts_a_new_creation_time() {
    let rig = Rig::at(1_000);
    rig.put("a").await;
    rig.set(2_000);
    rig.docs.delete("c", "a", Precondition::None).await.unwrap();
    rig.set(3_000);
    rig.put("a").await;
    let meta = rig.meta("a").await;
    assert_eq!((meta.created_ms, meta.updated_ms), (3_000, 3_000));
}

#[tokio::test]
async fn rewriting_an_expired_document_creates_it_anew() {
    let rig = Rig::at(1_000);
    rig.docs
        .put_with(
            "c",
            "b",
            json!({}),
            Precondition::None,
            WriteOptions::new().expires_ms(1_100),
        )
        .await
        .unwrap();
    rig.set(1_200);
    rig.put("b").await;
    let meta = rig.meta("b").await;
    assert_eq!((meta.created_ms, meta.expires_ms), (1_200, None));
}

#[tokio::test]
async fn updated_never_goes_backwards_when_the_clock_does() {
    let rig = Rig::at(5_000);
    rig.put("a").await;
    rig.set(4_000);
    rig.put("a").await;
    let meta = rig.meta("a").await;
    assert_eq!((meta.created_ms, meta.updated_ms), (5_000, 5_000));
}

#[tokio::test]
async fn schema_and_expiry_round_trip_and_a_plain_put_clears_them() {
    let rig = Rig::at(1_000);
    rig.docs
        .put_with(
            "c",
            "a",
            json!({}),
            Precondition::None,
            WriteOptions::new().schema(3).expires_ms(9_000),
        )
        .await
        .unwrap();
    let meta = rig.meta("a").await;
    assert_eq!((meta.schema, meta.expires_ms), (3, Some(9_000)));
    let page = rig.docs.query("c", &Query::all()).await.unwrap();
    assert_eq!(page.items[0].meta, meta, "query returns the same metadata");

    rig.put("a").await;
    let meta = rig.meta("a").await;
    assert_eq!((meta.schema, meta.expires_ms), (0, None));
}

#[tokio::test]
async fn an_expired_document_reads_as_absent_everywhere() {
    let rig = Rig::at(1_000);
    rig.docs
        .ensure_collection(&CollectionSpec::new("c").searchable(["title"]))
        .await
        .unwrap();
    rig.docs
        .put_with(
            "c",
            "a",
            json!({"title": "harbor", "state": "queued"}),
            Precondition::None,
            WriteOptions::new().expires_ms(2_000),
        )
        .await
        .unwrap();
    let claim = |docs: &Arc<dyn DocumentStore>| {
        let docs = Arc::clone(docs);
        async move {
            docs.claim(
                "c",
                &Filter::eq("state", "queued"),
                &[],
                &json!({"state": "taken"}),
            )
            .await
            .unwrap()
        }
    };

    rig.set(1_999);
    assert!(rig.docs.get("c", "a").await.unwrap().is_some());
    assert_eq!(rig.docs.count("c", &Filter::All).await.unwrap(), 1);
    assert_eq!(rig.docs.search("c", "harbor", 5).await.unwrap().len(), 1);

    rig.set(2_000);
    assert!(rig.docs.get("c", "a").await.unwrap().is_none());
    assert_eq!(
        rig.docs
            .query("c", &Query::all())
            .await
            .unwrap()
            .items
            .len(),
        0
    );
    assert_eq!(rig.docs.count("c", &Filter::All).await.unwrap(), 0);
    assert_eq!(rig.docs.search("c", "harbor", 5).await.unwrap().len(), 0);
    assert!(claim(&rig.docs).await.is_none());
    assert_eq!(rig.docs.delete_where("c", &Filter::All).await.unwrap(), 0);
    assert!(
        !rig.docs.delete("c", "a", Precondition::None).await.unwrap(),
        "deleting an expired document removes nothing"
    );
}

#[tokio::test]
async fn an_expired_document_does_not_hold_a_unique_index() {
    let rig = Rig::at(1_000);
    rig.docs
        .ensure_collection(
            &CollectionSpec::new("c").index(crate::IndexSpec::new("by_n", ["n"]).unique()),
        )
        .await
        .unwrap();
    rig.docs
        .put_with(
            "c",
            "a",
            json!({"n": 1}),
            Precondition::None,
            WriteOptions::new().expires_ms(1_500),
        )
        .await
        .unwrap();
    rig.set(1_500);
    rig.put("b").await;
    assert!(rig.docs.get("c", "b").await.unwrap().is_some());
}

#[tokio::test]
async fn a_claim_keeps_schema_and_expiry_and_stamps_the_update() {
    let rig = Rig::at(1_000);
    rig.docs
        .put_with(
            "c",
            "a",
            json!({"state": "queued"}),
            Precondition::None,
            WriteOptions::new().schema(2).expires_ms(9_000),
        )
        .await
        .unwrap();
    rig.set(1_400);
    let claimed = rig
        .docs
        .claim(
            "c",
            &Filter::eq("state", "queued"),
            &[],
            &json!({"state": "taken"}),
        )
        .await
        .unwrap()
        .unwrap();
    let expected = Meta {
        schema: 2,
        created_ms: 1_000,
        updated_ms: 1_400,
        expires_ms: Some(9_000),
    };
    assert_eq!(claimed.meta, expected);
    assert_eq!(rig.meta("a").await, expected);
}

#[tokio::test]
async fn batched_puts_stamp_times_and_declare_no_options() {
    let rig = Rig::at(1_000);
    rig.docs
        .put_with(
            "c",
            "a",
            json!({}),
            Precondition::None,
            WriteOptions::new().schema(1),
        )
        .await
        .unwrap();
    rig.set(1_200);
    rig.docs
        .atomic_batch(vec![
            WriteOp::Put {
                collection: "c".into(),
                id: "a".into(),
                doc: json!({}),
                precondition: Precondition::None,
            },
            WriteOp::Put {
                collection: "c".into(),
                id: "b".into(),
                doc: json!({}),
                precondition: Precondition::None,
            },
        ])
        .await
        .unwrap();
    assert_eq!(
        (
            rig.meta("a").await.created_ms,
            rig.meta("a").await.updated_ms
        ),
        (1_000, 1_200)
    );
    assert_eq!(rig.meta("a").await.schema, 0);
    assert_eq!(rig.meta("b").await.created_ms, 1_200);
}

#[tokio::test]
async fn atomic_batch_with_applies_one_options_value_to_every_put() {
    let rig = Rig::at(1_000);
    let results = rig
        .docs
        .atomic_batch_with(
            vec![
                WriteOp::Put {
                    collection: "c".into(),
                    id: "a".into(),
                    doc: json!({"n": 1}),
                    precondition: Precondition::Absent,
                },
                WriteOp::Put {
                    collection: "c".into(),
                    id: "b".into(),
                    doc: json!({"n": 2}),
                    precondition: Precondition::Absent,
                },
            ],
            WriteOptions::new()
                .schema(4)
                .expires_ms(5_000)
                .created_ms(40),
        )
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(
        results,
        [
            crate::WriteResult::Put {
                version: Version::FIRST
            },
            crate::WriteResult::Put {
                version: Version::FIRST,
            },
        ]
    );
    for id in ["a", "b"] {
        assert_eq!(
            rig.meta(id).await,
            Meta {
                schema: 4,
                created_ms: 40,
                updated_ms: 1_000,
                expires_ms: Some(5_000),
            }
        );
    }
}

#[tokio::test]
async fn rejected_read_check_leaves_every_batch_write_unapplied() {
    let rig = Rig::at(1_000);
    rig.put("a").await;
    let before = rig.docs.get("c", "a").await.unwrap().unwrap();
    let error = rig
        .docs
        .atomic_batch_checked_with(
            vec![ReadCheck {
                collection: "c".into(),
                id: "a".into(),
                expected: crate::ExpectedDocument::Version(Version(99)),
            }],
            vec![WriteOp::Put {
                collection: "c".into(),
                id: "a".into(),
                doc: json!({"n": 2}),
                precondition: Precondition::Version(before.version),
            }],
            WriteOptions::new().schema(7).expires_ms(2_000),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), crate::ErrorKind::Conflict);
    assert_eq!(rig.docs.get("c", "a").await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn rejected_checked_batch_rolls_back_bodies_versions_and_options() {
    let rig = Rig::at(1_000);
    rig.docs
        .put_with(
            "c",
            "a",
            json!({"n": 1}),
            Precondition::Absent,
            WriteOptions::new().schema(1).created_ms(10),
        )
        .await
        .unwrap();
    let before = rig.docs.get("c", "a").await.unwrap().unwrap();
    let error = rig
        .docs
        .atomic_batch_checked_with(
            vec![ReadCheck {
                collection: "c".into(),
                id: "a".into(),
                expected: crate::ExpectedDocument::Version(before.version),
            }],
            vec![
                WriteOp::Put {
                    collection: "c".into(),
                    id: "a".into(),
                    doc: json!({"n": 2}),
                    precondition: Precondition::Version(before.version),
                },
                WriteOp::Put {
                    collection: "c".into(),
                    id: "b".into(),
                    doc: json!({"n": 3}),
                    precondition: Precondition::Version(Version(99)),
                },
            ],
            WriteOptions::new()
                .schema(7)
                .expires_ms(2_000)
                .created_ms(20),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), crate::ErrorKind::Conflict);
    assert_eq!(rig.docs.get("c", "a").await.unwrap().unwrap(), before);
    assert!(rig.docs.get("c", "b").await.unwrap().is_none());
}

#[tokio::test]
async fn metadata_paths_filter_and_sort_with_stable_paging() {
    let rig = Rig::at(0);
    for (id, at) in [("a", 300), ("b", 100), ("c", 200), ("d", 200)] {
        rig.set(at);
        rig.put(id).await;
    }
    let ids = |page: crate::Page<crate::Versioned<serde_json::Value>>| {
        page.items
            .into_iter()
            .map(|item| item.id)
            .collect::<Vec<_>>()
    };

    let recent = Query::filter(Filter::gte("_created_ms", 200)).sort(Sort::asc("_created_ms"));
    assert_eq!(
        ids(rig.docs.query("c", &recent).await.unwrap()),
        ["c", "d", "a"]
    );

    let newest_first = Query::all().sort(Sort::desc("_created_ms"));
    assert_eq!(
        ids(rig.docs.query("c", &newest_first).await.unwrap()),
        ["a", "c", "d", "b"]
    );

    let mut paged = Query::all().sort(Sort::asc("_created_ms")).limit(2);
    let first = rig.docs.query("c", &paged).await.unwrap();
    paged.cursor = first.next.clone();
    assert_eq!(ids(first), ["b", "c"]);
    assert_eq!(ids(rig.docs.query("c", &paged).await.unwrap()), ["d", "a"]);

    assert_eq!(
        rig.docs
            .count("c", &Filter::lt("_updated_ms", 200))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        rig.docs
            .delete_where("c", &Filter::eq("_created_ms", 200))
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn a_supplied_created_ms_is_kept_and_the_update_time_stays_the_clock() {
    let rig = Rig::at(1_000);
    rig.docs
        .put_with(
            "c",
            "a",
            json!({}),
            Precondition::Absent,
            WriteOptions::new().schema(2).created_ms(40),
        )
        .await
        .unwrap();
    let meta = rig.meta("a").await;
    assert_eq!(meta.created_ms, 40);
    assert_eq!(meta.updated_ms, 1_000);
    assert_eq!(meta.schema, 2);
    rig.set(2_000);
    rig.put("a").await;
    assert_eq!(rig.meta("a").await.created_ms, 40);
}
