//! Declaration and transform wire compatibility.
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tinystoragedrivers_schema::*;

#[path = "fixtures/owner_examples.rs"]
mod owner_examples;

#[test]
fn all_seven_pre_port_owner_declarations_match_complete_frozen_wire_values() {
    let expected: Value = serde_json::from_str(include_str!("fixtures/seven_owners.json")).unwrap();
    let declarations = owner_examples::all();
    assert_eq!(serde_json::to_value(&declarations).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<Vec<SchemaDeclaration>>(expected).unwrap(),
        declarations
    );
}

fn roundtrip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
    value: T,
) -> serde_json::Result<Value> {
    let wire = serde_json::to_value(&value)?;
    assert_eq!(serde_json::from_value::<T>(wire.clone())?, value);
    drop(value);
    Ok(wire)
}

#[test]
fn owned_declarations_roundtrip_without_runtime_code() {
    let owner: Owner = serde_json::from_value(json!(String::from("orders"))).unwrap();
    let spec = CollectionSpec::new(String::from("orders.rows"))
        .index(IndexSpec::new("by_name", [String::from("name")]).unique())
        .ttl("deadline")
        .searchable(["name"]);
    let items = vec![
        Item::Collection {
            name: spec.name.clone(),
            version: 2,
            spec,
            scope: ScopeClass::Tenant,
        },
        Item::Stream {
            name: "orders.audit".into(),
            scope: ScopeClass::Agent,
            seal: SealPolicy::new(SealWhen::Idle(42)).retaining_ms(99),
        },
        Item::Blobs {
            prefix: "orders/files/".into(),
            scope: ScopeClass::Module,
        },
        Item::Secrets {
            prefix: "orders/keys/".into(),
            scope: ScopeClass::Cluster,
        },
    ];
    let declaration = SchemaDeclaration {
        owner,
        items,
        steps: vec![StepDeclaration {
            id: "upgrade".into(),
            item: "orders.rows".into(),
            from: 1,
            to: 2,
            source: Some("legacy".into()),
            transform: TransformDescriptor {
                operation: "upgrade_rows".into(),
                kind: TransformKind::Documents,
                version: 1,
            },
        }],
    };
    let wire = roundtrip(declaration).unwrap();
    assert_eq!(wire["owner"], "orders");
    assert_eq!(wire["items"][0]["kind"], "collection");
    assert_eq!(wire["items"][0]["scope"], json!({"kind":"tenant"}));
    assert_eq!(
        wire["steps"][0]["transform"]["kind"],
        json!({"kind":"documents"})
    );
    assert_eq!(wire["items"][1]["seal"]["when"], json!({"idle":42}));
}

#[test]
fn deserialization_cannot_bypass_owner_validation() {
    for invalid in [
        "",
        "a",
        "Ab",
        "a-b",
        "tsd",
        "a/b",
        "012",
        "abcdefghijklmnopqrstuvwxy",
    ] {
        assert!(
            serde_json::from_value::<Owner>(json!(invalid)).is_err(),
            "{invalid}"
        );
    }
    for valid in ["ab", "a1_b", "abcdefghijklmnopqrstuvwx"] {
        roundtrip(Owner::new(valid).unwrap()).unwrap();
    }
}

#[test]
fn document_records_keep_metadata_and_all_three_expiry_states() {
    let source = SourceRecord {
        id: "id".into(),
        body: json!({"v":1}),
        meta: Meta {
            schema: u32::MAX,
            created_ms: i64::MIN,
            updated_ms: i64::MAX,
            expires_ms: Some(-1),
        },
    };
    let wire = roundtrip(DocumentsTransformRequest {
        operation: "split".into(),
        version: 1,
        records: vec![source],
    })
    .unwrap();
    assert_eq!(wire["records"][0]["meta"]["created_ms"], i64::MIN);
    for (expires, expected) in [
        (Expiry::Carry, json!({"kind":"carry"})),
        (Expiry::Never, json!({"kind":"never"})),
        (Expiry::At(-12), json!({"kind":"at","value":-12})),
    ] {
        let record = TargetRecord {
            collection: "orders.rows".into(),
            id: "id".into(),
            body: json!({}),
            created_ms: Some(-9),
            expires,
        };
        assert_eq!(roundtrip(record).unwrap()["expires"], expected);
    }
    roundtrip::<TransformResult<Vec<TargetRecord>>>(Ok(vec![])).unwrap();
    roundtrip::<TransformResult<Vec<TargetRecord>>>(Ok(vec![
        TargetRecord::new("orders.rows", "a", json!({})),
        TargetRecord::new("orders.rows", "b", json!({}))
            .created_ms(2)
            .expires_ms(None),
    ]))
    .unwrap();
}

#[test]
fn stream_and_binary_blob_outputs_distinguish_skip_from_failure() {
    for kind in [
        TransformKind::Documents,
        TransformKind::Stream,
        TransformKind::Blob,
    ] {
        roundtrip(kind).unwrap();
    }
    roundtrip(StreamTransformRequest {
        operation: "stream".into(),
        version: 1,
        source: "old/s".into(),
        entries: vec![json!(1), json!({"x":2})],
    })
    .unwrap();
    roundtrip::<TransformResult<Option<StreamTransformOutput>>>(Ok(Some(StreamTransformOutput {
        target: "orders.audit/a".into(),
        entries: vec![],
    })))
    .unwrap();
    roundtrip(BlobTransformRequest {
        operation: "blob".into(),
        version: 1,
        source: "old/b".into(),
        content_type: None,
        bytes: vec![0, 255, 128],
    })
    .unwrap();
    roundtrip::<TransformResult<Option<BlobTransformOutput>>>(Ok(Some(BlobTransformOutput {
        target: "orders/files/a".into(),
        content_type: Some("application/octet-stream".into()),
        bytes: vec![255, 0, 128],
    })))
    .unwrap();
    assert_eq!(
        roundtrip::<TransformResult<Option<BlobTransformOutput>>>(Ok(None)).unwrap(),
        json!({"Ok":null})
    );
    for kind in [
        TransformErrorKind::InvalidInput,
        TransformErrorKind::UnsupportedOperation,
        TransformErrorKind::UnsupportedVersion,
    ] {
        let wire = roundtrip::<TransformResult<Option<BlobTransformOutput>>>(Err(TransformError {
            kind,
            message: "safe error".into(),
        }))
        .unwrap();
        assert!(wire["Err"]["kind"]["kind"].is_string());
        assert!(wire["Err"].get("source").is_none());
    }
}

#[test]
fn invalid_enum_tags_and_integer_widths_fail_decoding() {
    assert!(serde_json::from_value::<TransformKind>(json!({"kind":"streams"})).is_err());
    assert!(serde_json::from_value::<ScopeClass>(json!({"kind":"user"})).is_err());
    assert!(serde_json::from_value::<Expiry>(json!({"kind":"at","value":null})).is_err());
    assert!(
        serde_json::from_value::<StepDeclaration>(json!({
            "id":"x","item":"orders.rows","from":-1,"to":2,"source":null,
            "transform":{"operation":"upgrade","kind":{"kind":"documents"},"version":1}
        }))
        .is_err()
    );
    let bad_owner = json!({"owner":"tsd","items":[],"steps":[]});
    assert!(serde_json::from_value::<SchemaDeclaration>(bad_owner).is_err());
    let source = SourceRecord {
        id: "id".into(),
        body: json!({}),
        meta: Meta::unknown(),
    };
    let wire = roundtrip(source).unwrap();
    assert_eq!(
        wire["meta"],
        json!({"schema":0,"created_ms":0,"updated_ms":0})
    );
    assert_eq!(Meta::unknown().field("_schema"), Some(json!(0)));
}
