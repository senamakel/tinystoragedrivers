//! Wire compatibility and authority-selector rejection tests.
use super::*;
use serde_json::json;

#[test]
fn query_roundtrips_and_rejects_authority_selectors() {
    let query = Query {
        filter: Filter::Eq {
            field: "state".into(),
            value: json!("ready"),
        },
        sort: vec![Sort {
            field: "created_at".into(),
            direction: Direction::Desc,
        }],
        limit: Some(25),
        cursor: Some("opaque-cursor".into()),
    };
    let encoded = serde_json::to_value(&query).unwrap();
    assert_eq!(
        serde_json::from_value::<Query>(encoded.clone()).unwrap(),
        query
    );
    let mut forged = encoded;
    forged["scope"] = json!("bob");
    assert!(serde_json::from_value::<Query>(forged).is_err());
    assert_eq!(DOCUMENTS_SELECTORS.len(), 17);
    assert!(!DOCUMENTS_SELECTORS.contains(&"Arbitrary"));
    assert!(is_documents_selector("Query"));
    assert!(!is_documents_selector("Arbitrary"));
    assert_eq!(STORAGE_INTERFACE, "ai.tinyhumans.tinystorage.Storage");
    assert_eq!(DOCUMENTS_OBJECT, "Documents");
}

#[test]
fn document_requests_route_only_within_an_explicit_host_grant() {
    let get = GetDocumentRequest {
        collection: "notes".into(),
        id: "n1".into(),
    };
    let wire = serde_json::to_value(&get).unwrap();
    assert_eq!(wire["collection"], "notes");
    assert_eq!(wire["document_id"], "n1");
    assert!(
        serde_json::from_value::<GetDocumentRequest>(json!({
            "collection":"notes", "document_id":"n1", "scope":"bob"
        }))
        .is_err()
    );

    let watch = WatchNotice::Invalidated {
        cursor: Some(vec![0, 1, 255]),
    };
    let wire = serde_json::to_value(&watch).unwrap();
    assert_eq!(wire, json!({"kind":"invalidated", "cursor":[0, 1, 255]}));
    assert!(wire.get("document").is_none());
}

#[test]
fn exact_secret_needs_wrap_the_unchanged_prefix_declaration_without_leaking_names() {
    let secret_prefixes = ["mcp_client_env/"];
    let exact_secret_names = ["mcp_cache_fingerprint_key"];
    let needs: StorageNeedsWithExactSecrets<'_> = StorageNeeds {
        secret_prefixes: &secret_prefixes,
        ..StorageNeeds::default()
    }
    .with_exact_secret_names(&exact_secret_names);

    assert_eq!(needs.needs.secret_prefixes, ["mcp_client_env/"]);
    assert_eq!(needs.exact_secret_names, ["mcp_cache_fingerprint_key"]);
    let debug = format!("{needs:?}");
    assert!(!debug.contains("mcp_client_env/"));
    assert!(!debug.contains("mcp_cache_fingerprint_key"));
}

#[allow(clippy::needless_pass_by_value)]
fn roundtrip<T>(value: T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_value(&value).unwrap();
    assert_eq!(serde_json::from_value::<T>(encoded).unwrap(), value);
}

#[test]
#[allow(clippy::too_many_lines)]
fn every_documents_message_shape_roundtrips() {
    let spec = CollectionSpec {
        name: "notes".into(),
        indexes: vec![IndexSpec {
            name: "by_state".into(),
            fields: vec!["state".into()],
            unique: false,
        }],
        ttl_field: None,
        search: Some(SearchSpec {
            fields: vec!["title".into()],
        }),
    };
    let query = Query::default();
    for filter in [
        Filter::All,
        Filter::Ne {
            field: "state".into(),
            value: json!("ready"),
        },
        Filter::In {
            field: "state".into(),
            values: vec![json!("ready"), json!("done")],
        },
        Filter::Range {
            field: "n".into(),
            gt: Some(json!(1)),
            gte: None,
            lt: Some(json!(10)),
            lte: None,
        },
        Filter::Exists {
            field: "state".into(),
            exists: true,
        },
        Filter::And {
            filters: vec![Filter::All],
        },
        Filter::Or {
            filters: vec![Filter::All],
        },
        Filter::Not {
            filter: Box::new(Filter::All),
        },
    ] {
        roundtrip(filter);
    }
    roundtrip(spec.clone());
    let get = GetDocumentRequest {
        collection: "notes".into(),
        id: "n".into(),
    };
    roundtrip(get);
    let put = PutDocumentRequest {
        collection: "notes".into(),
        id: "n".into(),
        doc: json!({"nested":{"id":"business-data","array":[1,true,null]}}),
        precondition: Precondition::Absent,
        schema: 0,
        expires_ms: None,
    };
    let put_wire = serde_json::to_value(&put).unwrap();
    assert!(put_wire["document_json"].is_string());
    assert_eq!(
        serde_json::from_value::<PutDocumentRequest>(put_wire).unwrap(),
        put
    );
    roundtrip(DeleteDocumentRequest {
        collection: "notes".into(),
        id: "n".into(),
        precondition: Precondition::Version(Version(8)),
    });
    roundtrip(QueryRequest {
        collection: "notes".into(),
        query: query.clone(),
    });
    roundtrip(QueryResponse {
        items: vec![],
        next: Some("cursor".into()),
    });
    roundtrip(CountRequest {
        collection: "notes".into(),
        filter: Filter::default(),
    });
    roundtrip(DeleteWhereRequest {
        collection: "notes".into(),
        filter: Filter::Not {
            filter: Box::new(Filter::All),
        },
    });
    roundtrip(ClaimRequest {
        collection: "notes".into(),
        filter: Filter::All,
        sort: vec![],
        patch: json!({"claimed":true}),
    });
    roundtrip(ReadCheck {
        collection: "notes".into(),
        id: "n".into(),
        expected: ExpectedDocument::Absent,
    });
    roundtrip(BatchRequest {
        checks: vec![],
        ops: vec![
            WriteOp::Put {
                collection: "notes".into(),
                id: "n".into(),
                doc: json!({"ok":true}),
                precondition: Precondition::Absent,
            },
            WriteOp::Delete {
                collection: "notes".into(),
                id: "old".into(),
                precondition: Precondition::None,
            },
        ],
    });
    roundtrip(BatchResponse::from([
        WriteResult::Put {
            version: Version(2),
        },
        WriteResult::Delete { removed: true },
    ]));
    roundtrip(SearchRequest {
        collection: "notes".into(),
        text: "term".into(),
        limit: 3,
    });
    roundtrip(SearchHit {
        id: "n".into(),
        score: 1.0,
    });
    roundtrip(EnsureCollectionRequest { spec: spec.clone() });
    roundtrip(ValidateCollectionSpecRequest { spec });
    roundtrip(CollectionRequest {
        collection: "notes".into(),
    });
    roundtrip(CollectionsRequest {
        prefix: "user_".into(),
    });
    roundtrip(WatchOpenRequest {
        collection: "notes".into(),
        filter: Filter::All,
        after: Some(vec![2, 3]),
        interval_ms: 1000,
    });
    roundtrip(WatchCloseRequest {
        watch_id: "host-watch".into(),
    });
    roundtrip(WatchChanged {
        watch_id: "host-watch".into(),
        notice: WatchNotice::ResetRequired {
            reason: WatchReset::CursorExpired,
        },
    });
}

#[test]
fn nested_schema_and_sort_fields_reject_unknown_data() {
    assert!(serde_json::from_value::<CollectionSpec>(json!({
        "name":"notes", "indexes":[{"name":"i","fields":["state"],"unique":false,"scope":"forged"}]
    })).is_err());
    assert!(
        serde_json::from_value::<Query>(json!({
            "sort":[{"field":"created","direction":"asc","collection":"forged"}]
        }))
        .is_err()
    );
}

#[test]
fn checked_batch_roundtrips_and_rejects_authority_selectors() {
    let batch = BatchRequest {
        checks: vec![ReadCheck {
            collection: "items".into(),
            id: "item-1".into(),
            expected: ExpectedDocument::Version(Version(4)),
        }],
        ops: vec![WriteOp::Put {
            collection: "items".into(),
            id: "item-1".into(),
            doc: json!({"state":"done"}),
            precondition: Precondition::Absent,
        }],
    };
    let encoded = serde_json::to_value(&batch).unwrap();
    assert_eq!(
        serde_json::from_value::<BatchRequest>(encoded.clone()).unwrap(),
        batch
    );
    let mut forged = encoded;
    forged["scope"] = json!("bob");
    assert!(serde_json::from_value::<BatchRequest>(forged).is_err());
}

#[test]
fn batch_writes_reuse_document_id_and_inline_document_json_fields() {
    let value = json!({"nested":{"id":"business-data","items":[1,true,null]}});
    let batch = BatchRequest {
        checks: vec![],
        ops: vec![
            WriteOp::Put {
                collection: "items".into(),
                id: "put-id".into(),
                doc: value.clone(),
                precondition: Precondition::Absent,
            },
            WriteOp::Delete {
                collection: "items".into(),
                id: "delete-id".into(),
                precondition: Precondition::None,
            },
        ],
    };
    let wire = serde_json::to_value(batch).unwrap();
    let put = &wire["ops"][0];
    let delete = &wire["ops"][1];
    assert_eq!(put["document_id"], "put-id");
    assert!(put["document_id"].is_string());
    assert!(put["document_json"].is_string());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(put["document_json"].as_str().unwrap()).unwrap(),
        value
    );
    assert!(put.get("id").is_none());
    assert!(put.get("doc").is_none());
    assert_eq!(delete["document_id"], "delete-id");
    assert!(delete.get("id").is_none());
}

#[test]
fn watch_open_response_and_capabilities_reply_reject_unknown_fields() {
    let open = WatchOpenResponse {
        watch_id: "w1".into(),
    };
    let wire = serde_json::to_value(&open).unwrap();
    assert_eq!(wire, json!({"watch_id":"w1"}));
    assert!(
        serde_json::from_value::<WatchOpenResponse>(json!({"watch_id":"w1","scope":"bob"}))
            .is_err()
    );
    let caps = CapabilitiesReply {
        capabilities: vec!["watch".into()],
    };
    let wire = serde_json::to_value(&caps).unwrap();
    assert_eq!(wire, json!({"capabilities":["watch"]}));
    assert!(is_documents_selector("Capabilities"));
}

#[test]
fn put_options_are_optional_pinned_and_never_carry_created_ms() {
    let plain = json!({"collection":"c","document_id":"n","document_json":"{}","precondition":{"kind":"none"}});
    let put: PutDocumentRequest = serde_json::from_value(plain.clone()).unwrap();
    assert_eq!((put.schema, put.expires_ms), (0, None));
    // Defaults are omitted, so a new module's plain put is an old host's put.
    assert_eq!(serde_json::to_value(&put).unwrap(), plain);
    let declared = PutDocumentRequest {
        schema: 2,
        expires_ms: Some(99),
        ..put
    };
    let wire = serde_json::to_value(&declared).unwrap();
    assert_eq!(wire["schema"], 2);
    assert_eq!(wire["expires_ms"], 99);
    assert_eq!(
        serde_json::from_value::<PutDocumentRequest>(wire.clone()).unwrap(),
        declared
    );
    // `created_ms` is a host-only migration option; the wire refuses it.
    let mut forged = wire;
    forged["created_ms"] = json!(1);
    assert!(serde_json::from_value::<PutDocumentRequest>(forged).is_err());
}
