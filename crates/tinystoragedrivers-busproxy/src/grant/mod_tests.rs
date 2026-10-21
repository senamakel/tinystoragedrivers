//! Tests for host storage grant construction and authorization.

use super::*;
use serde_json::json;
use tinystoragedrivers_bus::{IndexSpec, SearchSpec};

fn collection() -> CollectionSpec {
    CollectionSpec {
        name: "module.records".into(),
        indexes: vec![IndexSpec {
            name: "by_state".into(),
            fields: vec!["state".into(), "created".into()],
            unique: true,
        }],
        ttl_field: Some("expires".into()),
        search: Some(SearchSpec {
            fields: vec!["title".into()],
        }),
    }
}

fn grant(writable: bool) -> StorageGrant {
    let collections = [collection()];
    let blobs = ["attachments/"];
    let streams = ["events/"];
    let secrets = ["provider/"];
    StorageGrant::from_needs(
        StorageNeeds {
            collections: &collections,
            blob_prefixes: &blobs,
            stream_prefixes: &streams,
            secret_prefixes: &secrets,
        },
        &GrantPolicy {
            writable,
            allow_secrets: true,
            reserved_collections: &[],
        },
    )
    .unwrap()
}

fn empty_grant() -> StorageGrant {
    StorageGrant::from_needs(StorageNeeds::default(), &GrantPolicy::default()).unwrap()
}

#[test]
fn queues_are_denied_without_host_manifest_opt_in() {
    for grant in [grant(true), empty_grant()] {
        for selector in tinystoragedrivers_bus::QUEUES_SELECTORS {
            let decision = grant.authorize(Object::Queues, selector, &serde_json::Value::Null);
            if selector == "Features" {
                assert_eq!(
                    decision,
                    Ok(Plan::Answer(serde_json::json!({
                        "dedupe": false,
                        "groups": false,
                        "priorities": false,
                        "delay": false
                    }))),
                    "Queues.Features must report no optional authority",
                );
            } else {
                assert_eq!(
                    decision,
                    Err(Denied::Object),
                    "Queues.{selector} must not forward without a queue grant",
                );
            }
        }
    }
}

#[test]
fn unsupported_queue_selectors_remain_malformed() {
    let grant = grant(true);
    for selector in ["Capabilities", "MadeUp"] {
        assert_eq!(
            grant.authorize(Object::Queues, selector, &serde_json::Value::Null),
            Err(Denied::Malformed),
            "unknown Queues.{selector} must not be treated as an ungranted selector",
        );
    }
    assert_eq!(
        grant.authorize(
            Object::Queues,
            "Features",
            &serde_json::json!({"unexpected":true}),
        ),
        Err(Denied::Malformed),
        "queue feature discovery must reject malformed payloads",
    );
}

fn queue_manifest(names: &[&str]) -> std::sync::Arc<tinystoragedrivers_core::queue::QueueManifest> {
    let queues = names
        .iter()
        .map(|name| serde_json::json!({"name": name}))
        .collect::<Vec<_>>();
    let json = serde_json::json!({"version": 1, "queues": queues}).to_string();
    std::sync::Arc::new(
        tinystoragedrivers_core::queue::QueueManifest::from_json(&json)
            .expect("queue manifest is valid"),
    )
}

fn queue_manifest_with_max_attempts(
    max_attempts: u32,
) -> std::sync::Arc<tinystoragedrivers_core::queue::QueueManifest> {
    let json = serde_json::json!({
        "version": 1,
        "queues": [{"name": "inbound", "max_attempts": max_attempts}]
    })
    .to_string();
    std::sync::Arc::new(
        tinystoragedrivers_core::queue::QueueManifest::from_json(&json)
            .expect("queue manifest is valid"),
    )
}

fn queue_request(selector: &str, queue: &str) -> serde_json::Value {
    match selector {
        "Features" => serde_json::Value::Null,
        "EnsureQueue" => serde_json::json!({
            "queue": queue,
            "spec": {"max_attempts": 3, "dedupe_window_ms": 0}
        }),
        "Enqueue" => serde_json::json!({
            "queue": queue,
            "messages": [{"payload_json": "{\"task\":\"work\"}"}]
        }),
        "Receive" => serde_json::json!({
            "queue": queue,
            "max": 1,
            "visibility_ms": 1000,
            "consumer": "worker"
        }),
        "Ack" => serde_json::json!({
            "queue": queue,
            "receipt": {"id": "message", "token": "lease"}
        }),
        "Nack" => serde_json::json!({
            "queue": queue,
            "receipt": {"id": "message", "token": "lease"},
            "retry": {"kind": "dead_letter", "reason": "done"}
        }),
        "Extend" => serde_json::json!({
            "queue": queue,
            "receipt": {"id": "message", "token": "lease"},
            "visibility_ms": 2000
        }),
        "Stats" | "Purge" => serde_json::json!({"queue": queue}),
        "DeadLetters" => serde_json::json!({"queue": queue, "limit": 5}),
        "Redrive" => serde_json::json!({"queue": queue, "id": "message"}),
        _ => unreachable!("the bus contract defines every queue selector"),
    }
}

fn is_queue_write(selector: &str) -> bool {
    matches!(
        selector,
        "EnsureQueue" | "Enqueue" | "Receive" | "Ack" | "Nack" | "Extend" | "Redrive" | "Purge"
    )
}

#[test]
fn queue_manifest_opt_in_forwards_only_declared_queues_with_typed_plans() {
    let grant = grant(true)
        .with_queue_manifest(queue_manifest(&["inbound"]))
        .expect("host-approved queue manifest is accepted");

    assert_eq!(
        grant.authorize(Object::Queues, "Features", &queue_request("Features", "")),
        Ok(Plan::Forward {
            target: String::new(),
            filter: ReplyFilter::None,
            write: false,
        })
    );

    for selector in tinystoragedrivers_bus::QUEUES_SELECTORS {
        if selector == "Features" {
            continue;
        }
        assert_eq!(
            grant.authorize(
                Object::Queues,
                selector,
                &queue_request(selector, "inbound")
            ),
            Ok(Plan::Forward {
                target: "inbound".to_string(),
                filter: ReplyFilter::None,
                write: is_queue_write(selector),
            }),
            "Queues.{selector} uses the manifested target and correct write policy",
        );
    }

    assert_eq!(
        grant.authorize(
            Object::Queues,
            "Stats",
            &queue_request("Stats", "not-granted"),
        ),
        Err(Denied::Target),
        "the manifest is the target allow-list before the server is reached",
    );
    assert_eq!(
        grant.authorize(Object::Queues, "Stats", &serde_json::Value::Null),
        Err(Denied::Malformed),
        "an opted-in grant still validates request shapes",
    );
}

#[test]
fn queue_manifest_opt_in_is_immutable_and_part_of_grant_equality() {
    let first = empty_grant()
        .with_queue_manifest(queue_manifest_with_max_attempts(3))
        .unwrap();
    let same = empty_grant()
        .with_queue_manifest(queue_manifest_with_max_attempts(3))
        .unwrap();
    let changed = empty_grant()
        .with_queue_manifest(queue_manifest_with_max_attempts(5))
        .unwrap();

    assert_eq!(first, same);
    assert_ne!(first, changed);
    assert_eq!(empty_grant(), empty_grant(), "default remains queue-denied");
}

#[test]
fn an_invalid_queue_manifest_cannot_become_grant_authority() {
    let invalid = tinystoragedrivers_core::queue::QueueManifest {
        schema: None,
        version: 99,
        queues: Vec::new(),
    };
    assert_eq!(
        empty_grant().with_queue_manifest(std::sync::Arc::new(invalid)),
        Err(QueueGrantError::InvalidManifest),
    );
}

#[test]
fn read_only_queue_authority_allows_reads_but_denies_every_mutator() {
    let read_only = StorageGrant::from_needs(
        StorageNeeds::default(),
        &GrantPolicy {
            writable: false,
            ..GrantPolicy::default()
        },
    )
    .unwrap()
    .with_queue_manifest(queue_manifest(&["inbound"]))
    .unwrap();

    for selector in ["Stats", "DeadLetters"] {
        assert!(matches!(
            read_only.authorize(
                Object::Queues,
                selector,
                &queue_request(selector, "inbound")
            ),
            Ok(Plan::Forward { write: false, .. })
        ));
    }
    for selector in tinystoragedrivers_bus::QUEUES_SELECTORS {
        if is_queue_write(selector) {
            assert_eq!(
                read_only.authorize(
                    Object::Queues,
                    selector,
                    &queue_request(selector, "inbound"),
                ),
                Err(Denied::ReadOnly),
                "Queues.{selector} is a mutation",
            );
        }
    }
}

#[test]
fn needs_are_copied_and_the_complete_collection_spec_is_preserved() {
    let grant = {
        let collections = [collection()];
        let streams = ["events/"];
        StorageGrant::from_needs(
            StorageNeeds {
                collections: &collections,
                blob_prefixes: &[],
                stream_prefixes: &streams,
                secret_prefixes: &[],
            },
            &GrantPolicy::default(),
        )
        .unwrap()
    };
    assert_eq!(grant.collections(), &[collection()]);
}

#[test]
fn default_policy_denies_secret_needs_and_debug_redacts_secret_names() {
    let secrets = ["private/provider-key"];
    let needs = StorageNeeds {
        secret_prefixes: &secrets,
        ..StorageNeeds::default()
    };
    assert_eq!(
        StorageGrant::from_needs(needs, &GrantPolicy::default()).unwrap_err(),
        GrantError::SecretsDenied
    );
    assert!(!format!("{needs:?}").contains("provider-key"));
    assert!(!format!("{:?}", grant(true)).contains("provider/"));
    assert!(
        !format!(
            "{:?}",
            GrantPolicy {
                writable: false,
                allow_secrets: true,
                reserved_collections: &["private_collection"],
            }
        )
        .contains("private_collection")
    );
}

#[test]
fn grant_errors_and_denials_have_stable_content_free_labels() {
    let grant_errors = [
        (GrantError::Collection, "invalid collection grant"),
        (GrantError::Prefix, "invalid storage prefix grant"),
        (
            GrantError::SecretsDenied,
            "secret storage grant denied by host policy",
        ),
    ];
    for (error, message) in grant_errors {
        assert_eq!(error.to_string(), message);
    }

    let denials = [
        (Denied::Object, "object-not-granted"),
        (Denied::Target, "target-not-granted"),
        (Denied::ReadOnly, "read-only-grant"),
        (Denied::Schema, "schema-is-host-only"),
        (Denied::Malformed, "malformed-request"),
    ];
    for (denial, label) in denials {
        assert_eq!(denial.label(), label);
        assert_eq!(denial.to_string(), label);
    }
}

#[test]
fn default_policy_is_read_only_and_product_reservations_are_host_supplied() {
    let collections = [collection()];
    let grant = StorageGrant::from_needs(
        StorageNeeds {
            collections: &collections,
            ..StorageNeeds::default()
        },
        &GrantPolicy::default(),
    )
    .unwrap();
    assert!(!grant.writable());
    let product_reserved = ["module.records"];
    assert_eq!(
        StorageGrant::from_needs(
            StorageNeeds {
                collections: &collections,
                ..StorageNeeds::default()
            },
            &GrantPolicy {
                reserved_collections: &product_reserved,
                ..GrantPolicy::default()
            },
        )
        .unwrap_err(),
        GrantError::Collection
    );
}

#[test]
fn secret_presence_reports_only_grants_that_include_secrets() {
    let empty = StorageGrant::from_needs(StorageNeeds::default(), &GrantPolicy::default()).unwrap();
    assert!(!empty.has_secrets());

    let collections = [collection()];
    let streams = ["events/"];
    let blobs = ["attachments/"];
    for needs in [
        StorageNeeds {
            collections: &collections,
            ..StorageNeeds::default()
        },
        StorageNeeds {
            stream_prefixes: &streams,
            ..StorageNeeds::default()
        },
        StorageNeeds {
            blob_prefixes: &blobs,
            ..StorageNeeds::default()
        },
    ] {
        let grant = StorageGrant::from_needs(needs, &GrantPolicy::default()).unwrap();
        assert!(!grant.has_secrets());
    }
}

#[test]
fn rejects_reserved_names_duplicate_specs_and_malformed_prefixes() {
    for name in ["_tsd_internal", "tsd.migrations"] {
        let specs = [CollectionSpec {
            name: name.into(),
            ..collection()
        }];
        assert_eq!(
            StorageGrant::from_needs(
                StorageNeeds {
                    collections: &specs,
                    ..StorageNeeds::default()
                },
                &GrantPolicy::default(),
            )
            .unwrap_err(),
            GrantError::Collection
        );
    }
    let duplicate = [collection(), collection()];
    assert_eq!(
        StorageGrant::from_needs(
            StorageNeeds {
                collections: &duplicate,
                ..StorageNeeds::default()
            },
            &GrantPolicy::default(),
        )
        .unwrap_err(),
        GrantError::Collection
    );
    for prefix in ["", "bad\0prefix"] {
        let prefixes = [prefix];
        assert_eq!(
            StorageGrant::from_needs(
                StorageNeeds {
                    blob_prefixes: &prefixes,
                    ..StorageNeeds::default()
                },
                &GrantPolicy::default(),
            )
            .unwrap_err(),
            GrantError::Prefix
        );
    }
    let duplicate_streams = ["events/", "events/"];
    assert_eq!(
        StorageGrant::from_needs(
            StorageNeeds {
                stream_prefixes: &duplicate_streams,
                ..StorageNeeds::default()
            },
            &GrantPolicy::default(),
        )
        .unwrap_err(),
        GrantError::Prefix
    );
    let too_long = "x".repeat(tinystoragedrivers_core::MAX_ID_LEN + 1);
    let too_long_refs = [too_long.as_str()];
    assert_eq!(
        StorageGrant::from_needs(
            StorageNeeds {
                stream_prefixes: &too_long_refs,
                ..StorageNeeds::default()
            },
            &GrantPolicy::default(),
        )
        .unwrap_err(),
        GrantError::Prefix
    );
}

#[test]
fn secret_prefixes_follow_the_secret_store_name_contract() {
    use tinystoragedrivers_secrets::MAX_SECRET_NAME_LEN;

    let policy = GrantPolicy {
        allow_secrets: true,
        ..GrantPolicy::default()
    };
    let too_long = "s".repeat(300);
    for prefix in [too_long.as_str(), "provider/\n", "provider/\u{0007}"] {
        let secret_prefixes = [prefix];
        assert_eq!(
            StorageGrant::from_needs(
                StorageNeeds {
                    secret_prefixes: &secret_prefixes,
                    ..StorageNeeds::default()
                },
                &policy,
            )
            .unwrap_err(),
            GrantError::Prefix
        );
    }

    let max_name = "x".repeat(MAX_SECRET_NAME_LEN);
    let secret_prefixes = [max_name.as_str(), "provider/"];
    let grant = StorageGrant::from_needs(
        StorageNeeds {
            secret_prefixes: &secret_prefixes,
            ..StorageNeeds::default()
        },
        &policy,
    )
    .unwrap();
    assert!(
        grant
            .authorize(Object::Secrets, "Get", &json!({"name":max_name}))
            .is_ok()
    );
    assert!(
        grant
            .authorize(Object::Secrets, "Get", &json!({"name":"provider/api-key"}))
            .is_ok()
    );
}

#[test]
fn rejects_any_blob_prefix_overlapping_reserved_sealed_keys() {
    for prefix in ["_tsd/sealed/", "_tsd/", "_tsd/sealed/x"] {
        let prefixes = [prefix];
        assert_eq!(
            StorageGrant::from_needs(
                StorageNeeds {
                    blob_prefixes: &prefixes,
                    ..StorageNeeds::default()
                },
                &GrantPolicy::default(),
            )
            .unwrap_err(),
            GrantError::Prefix
        );
    }
    let valid = ["_tsd-other/"];
    assert!(
        StorageGrant::from_needs(
            StorageNeeds {
                blob_prefixes: &valid,
                ..StorageNeeds::default()
            },
            &GrantPolicy::default(),
        )
        .is_ok()
    );
    for prefix in ["/absolute", "attachments/../private", "attachments//"] {
        let invalid = [prefix];
        assert_eq!(
            StorageGrant::from_needs(
                StorageNeeds {
                    blob_prefixes: &invalid,
                    ..StorageNeeds::default()
                },
                &GrantPolicy::default(),
            )
            .unwrap_err(),
            GrantError::Prefix
        );
    }
}

#[test]
fn blob_keys_are_validated_and_prefix_matching_is_literal() {
    let slash_prefix = ["foo/"];
    let slash_grant = StorageGrant::from_needs(
        StorageNeeds {
            blob_prefixes: &slash_prefix,
            ..StorageNeeds::default()
        },
        &GrantPolicy {
            writable: true,
            ..GrantPolicy::default()
        },
    )
    .unwrap();
    assert!(
        slash_grant
            .authorize(Object::Blobs, "Get", &json!({"key":"foo/bar"}))
            .is_ok()
    );
    assert_eq!(
        slash_grant.authorize(Object::Blobs, "Get", &json!({"key":"foobar/bar"})),
        Err(Denied::Target)
    );
    assert_eq!(
        slash_grant.authorize(Object::Blobs, "Get", &json!({"key":"foo/../private"})),
        Err(Denied::Malformed)
    );
    for (selector, request) in [
        (
            "PutUntil",
            json!({"key":"foo/bar","bytes":[],"expires_at_ms":6000}),
        ),
        (
            "PutStreamUntil",
            json!({"key":"foo/bar","len":0,"expires_at_ms":6000}),
        ),
        ("GetWithExpiry", json!({"key":"foo/bar"})),
        ("GetStreamWithExpiry", json!({"key":"foo/bar"})),
    ] {
        assert!(
            slash_grant
                .authorize(Object::Blobs, selector, &request)
                .is_ok()
        );
        let mut outside = request;
        outside["key"] = json!("foobar/bar");
        assert_eq!(
            slash_grant.authorize(Object::Blobs, selector, &outside),
            Err(Denied::Target),
            "{selector}"
        );
    }

    let plain_prefix = ["foo"];
    let plain_grant = StorageGrant::from_needs(
        StorageNeeds {
            blob_prefixes: &plain_prefix,
            ..StorageNeeds::default()
        },
        &GrantPolicy::default(),
    )
    .unwrap();
    assert!(
        plain_grant
            .authorize(Object::Blobs, "Get", &json!({"key":"foobar"}))
            .is_ok()
    );
}

#[test]
fn selector_write_classification_is_complete_and_seal_is_a_write() {
    let writes = [
        (Object::Documents, "Put"),
        (Object::Documents, "Delete"),
        (Object::Documents, "DeleteWhere"),
        (Object::Documents, "Claim"),
        (Object::Documents, "AtomicBatch"),
        (Object::Documents, "AtomicBatchChecked"),
        (Object::Documents, "AtomicBatchWith"),
        (Object::Documents, "AtomicBatchCheckedWith"),
        (Object::Documents, "DropCollection"),
        (Object::Streams, "Append"),
        (Object::Streams, "AppendBatch"),
        (Object::Streams, "AppendBatchIfOffset"),
        (Object::Streams, "TruncateBefore"),
        (Object::Streams, "DeleteStream"),
        (Object::Streams, "Seal"),
        (Object::Blobs, "Put"),
        (Object::Blobs, "PutStream"),
        (Object::Blobs, "PutUntil"),
        (Object::Blobs, "PutStreamUntil"),
        (Object::Blobs, "Delete"),
        (Object::Blobs, "PresignPut"),
        (Object::Secrets, "Set"),
        (Object::Secrets, "Delete"),
    ];
    for object in [
        Object::Documents,
        Object::Streams,
        Object::Blobs,
        Object::Secrets,
    ] {
        let selectors: &[&str] = match object {
            Object::Documents => &tinystoragedrivers_bus::DOCUMENTS_SELECTORS,
            Object::Streams => &tinystoragedrivers_bus::STREAMS_SELECTORS,
            Object::Blobs => &tinystoragedrivers_bus::BLOBS_SELECTORS,
            Object::Secrets => &tinystoragedrivers_bus::SECRETS_SELECTORS,
            // Queue selectors are tested separately as deny-all until needs exist.
            Object::Queues => &[],
        };
        for selector in selectors {
            let classified = is_write(object, selector).unwrap();
            assert_eq!(
                classified,
                writes.contains(&(object, *selector)),
                "{object:?}.{selector}"
            );
        }
    }
    assert_eq!(is_write(Object::Streams, "Seal"), Ok(true));
    assert_eq!(is_write(Object::Streams, "Info"), Ok(false));
    assert_eq!(is_write(Object::Streams, "future"), Err(Denied::Malformed));
}

#[test]
fn authorization_checks_schema_readonly_writes_batches_and_prefix_boundaries() {
    let read_only = grant(false);
    assert_eq!(
        read_only.authorize(
            Object::Documents,
            "Put",
            &json!({
                "collection": "module.records",
                "document_id": "x",
                "document_json": "{}",
                "precondition": {"kind":"none"}
            })
        ),
        Err(Denied::ReadOnly)
    );
    assert_eq!(
        read_only.authorize(
            Object::Documents,
            "DropCollection",
            &json!({"collection":"module.records"})
        ),
        Err(Denied::Schema)
    );
    assert_eq!(
        read_only.authorize(
            Object::Documents,
            "EnsureCollection",
            &json!({"spec":collection()})
        ),
        Ok(Plan::Answer(Value::Null))
    );
    assert_eq!(
        read_only.authorize(
            Object::Documents,
            "EnsureCollection",
            &json!({"spec":{"name":"module.records"}})
        ),
        Err(Denied::Schema)
    );
    assert_eq!(
        read_only.authorize(Object::Streams, "Info", &json!({"stream":"events/run"})),
        Ok(Plan::Forward {
            target: "events/run".into(),
            filter: ReplyFilter::None,
            write: false
        })
    );
    assert_eq!(
        read_only.authorize(Object::Streams, "Info", &json!({"stream":"eventual/run"})),
        Err(Denied::Target)
    );
    assert_eq!(
        read_only.authorize(Object::Streams, "Info", &json!({"stream":"events2/run"})),
        Err(Denied::Target)
    );
    let writable = grant(true);
    assert_eq!(
        writable.authorize(Object::Documents, "AtomicBatch", &json!({"checks":[{"collection":"other","id":"x","expected":{"kind":"absent"}}],"ops":[]})),
        Err(Denied::Target)
    );
    assert_eq!(
        writable.authorize(
            Object::Documents,
            "AtomicBatch",
            &json!({"checks":[],"ops":[],"extra":true})
        ),
        Err(Denied::Malformed)
    );
    assert_eq!(
        writable.authorize(
            Object::Documents,
            "AtomicBatch",
            &json!({
                "checks":[{"collection":"module.records","id":"x","expected":{"kind":"absent"}}],
                "ops":[]
            })
        ),
        Err(Denied::Malformed)
    );
    assert_eq!(
        writable.authorize(Object::Documents, "AtomicBatchChecked", &json!({"checks":[{"collection":"module.records","id":"x","expected":{"kind":"absent"}}],"ops":[]})),
        Ok(Plan::Forward { target: "module.records".into(), filter: ReplyFilter::None, write: true })
    );
    assert_eq!(
        writable.authorize(
            Object::Documents,
            "AtomicBatchChecked",
            &json!({
                "checks":[],
                "ops":[{"op":"put","collection":"other","document_id":"x","document_json":"{}","precondition":{"kind":"none"}}]
            })
        ),
        Err(Denied::Target)
    );
}

#[test]
fn metadata_batches_authorize_every_dependency_and_write_before_forwarding() {
    let request = json!({
        "ops":[
            {"op":"put","collection":"module.records","document_id":"a","document_json":"{}","precondition":{"kind":"absent"}},
            {"op":"put","collection":"other","document_id":"b","document_json":"{}","precondition":{"kind":"absent"}}
        ],
        "options":{"schema":2,"expires_ms":500}
    });
    assert_eq!(
        grant(true).authorize(Object::Documents, "AtomicBatchWith", &request),
        Err(Denied::Target)
    );
    assert_eq!(
        grant(false).authorize(Object::Documents, "AtomicBatchWith", &request),
        Err(Denied::ReadOnly)
    );
    let mut forged = json!({"ops":[],"options":{"schema":2,"created_ms":10}});
    assert_eq!(
        grant(true).authorize(Object::Documents, "AtomicBatchWith", &forged),
        Err(Denied::Malformed)
    );
    forged = json!({
        "checks":[{"collection":"other","id":"x","expected":{"kind":"absent"}}],
        "ops":[],"options":{"schema":2}
    });
    assert_eq!(
        grant(true).authorize(Object::Documents, "AtomicBatchCheckedWith", &forged),
        Err(Denied::Target)
    );
}

#[test]
fn capability_answers_require_null_and_collections_replies_are_filtered_or_rejected() {
    let empty_grant =
        StorageGrant::from_needs(StorageNeeds::default(), &GrantPolicy::default()).unwrap();
    assert_eq!(
        empty_grant.authorize(Object::Documents, "Capabilities", &Value::Null),
        Ok(Plan::Answer(json!({"capabilities":[]})))
    );
    assert_eq!(
        empty_grant.authorize(Object::Documents, "Capabilities", &json!({"extra":true})),
        Err(Denied::Malformed)
    );
    assert_eq!(
        empty_grant.authorize(Object::Blobs, "Capabilities", &Value::Null),
        Ok(Plan::Answer(json!({
            "presigned_urls": false,
            "ttl": false,
            "max_key_len": 0
        })))
    );
    assert_eq!(
        empty_grant.authorize(Object::Streams, "Info", &json!({"stream":"events/x"})),
        Err(Denied::Object)
    );
    let grant = grant(false);
    for request in [json!({}), json!({"prefix":""}), json!({"prefix":"outside"})] {
        assert_eq!(
            grant.authorize(Object::Documents, "Collections", &request),
            Ok(Plan::Forward {
                target: String::new(),
                filter: ReplyFilter::GrantedCollections,
                write: false,
            })
        );
    }
    assert_eq!(
        grant
            .filter_reply(
                ReplyFilter::GrantedCollections,
                json!(["module.records", "other"])
            )
            .unwrap(),
        json!(["module.records"])
    );
    assert_eq!(
        grant.filter_reply(
            ReplyFilter::GrantedCollections,
            json!(["module.records", 3])
        ),
        Err(Denied::Malformed)
    );
}

#[test]
fn typed_requests_reject_unknown_fields_and_secret_targets_stay_redacted() {
    let grant = grant(true);
    assert_eq!(
        grant.authorize(
            Object::Documents,
            "Get",
            &json!({
                "collection": "module.records",
                "document_id": "x",
                "scope": "another-tenant"
            })
        ),
        Err(Denied::Malformed)
    );
    let plan = grant
        .authorize(Object::Secrets, "Get", &json!({"name":"provider/api-key"}))
        .unwrap();
    assert!(!format!("{plan:?}").contains("provider/api-key"));
}

#[test]
fn every_current_selector_uses_its_typed_request_and_grant_rule() {
    let grant = grant(true);
    let suites = [
        (Object::Documents, document_requests()),
        (Object::Streams, stream_requests()),
        (Object::Blobs, blob_requests()),
        (Object::Secrets, secret_requests()),
    ];
    for (object, requests) in suites {
        for (selector, request) in requests {
            let result = grant.authorize(object, selector, &request);
            if object == Object::Documents && selector == "DropCollection" {
                assert_eq!(result, Err(Denied::Schema));
            } else {
                assert!(
                    result.is_ok(),
                    "{object:?}.{selector} was denied: {result:?}"
                );
            }
        }
    }
}

fn document_requests() -> Vec<(&'static str, Value)> {
    vec![
        (
            "Get",
            json!({"collection":"module.records","document_id":"x"}),
        ),
        (
            "Put",
            json!({"collection":"module.records","document_id":"x","document_json":"{}","precondition":{"kind":"none"}}),
        ),
        (
            "Delete",
            json!({"collection":"module.records","document_id":"x","precondition":{"kind":"none"}}),
        ),
        ("Query", json!({"collection":"module.records","query":{}})),
        ("Count", json!({"collection":"module.records"})),
        (
            "DeleteWhere",
            json!({"collection":"module.records","filter":{"op":"all"}}),
        ),
        (
            "Claim",
            json!({"collection":"module.records","filter":{"op":"all"},"patch":"{}"}),
        ),
        ("AtomicBatch", json!({"checks":[],"ops":[]})),
        ("AtomicBatchChecked", json!({"checks":[],"ops":[]})),
        (
            "Search",
            json!({"collection":"module.records","text":"x","limit":1}),
        ),
        ("EnsureCollection", json!({"spec":collection()})),
        ("ValidateCollectionSpec", json!({"spec":collection()})),
        ("DropCollection", json!({"collection":"module.records"})),
        ("Collections", json!({"prefix":"module.r"})),
        (
            "WatchOpen",
            json!({"collection":"module.records","filter":{"op":"all"},"interval_ms":100}),
        ),
        ("WatchClose", json!({"watch_id":"host-issued"})),
        ("Capabilities", Value::Null),
    ]
}

fn stream_requests() -> Vec<(&'static str, Value)> {
    vec![
        ("Append", json!({"stream":"events/x","record_json":"{}"})),
        ("AppendBatch", json!({"stream":"events/x","records":[]})),
        (
            "AppendBatchIfOffset",
            json!({"stream":"events/x","expected_next_offset":0,"records":[]}),
        ),
        (
            "ReadWindow",
            json!({"stream":"events/x","from":0,"limit":1}),
        ),
        ("Len", json!({"stream":"events/x"})),
        ("TruncateBefore", json!({"stream":"events/x","offset":0})),
        ("DeleteStream", json!({"stream":"events/x"})),
        ("Streams", json!({"prefix":"events/"})),
        ("Seal", json!({"stream":"events/x"})),
        ("Info", json!({"stream":"events/x"})),
    ]
}

fn blob_requests() -> Vec<(&'static str, Value)> {
    vec![
        ("Put", json!({"key":"attachments/x","bytes":[]})),
        ("PutStream", json!({"key":"attachments/x","len":0})),
        (
            "PutUntil",
            json!({"key":"attachments/x","bytes":[],"expires_at_ms":6000}),
        ),
        (
            "PutStreamUntil",
            json!({"key":"attachments/x","len":0,"expires_at_ms":6000}),
        ),
        ("Get", json!({"key":"attachments/x"})),
        ("GetStream", json!({"key":"attachments/x"})),
        ("GetWithExpiry", json!({"key":"attachments/x"})),
        ("GetStreamWithExpiry", json!({"key":"attachments/x"})),
        ("GetRange", json!({"key":"attachments/x","start":0,"end":0})),
        ("Head", json!({"key":"attachments/x"})),
        ("Delete", json!({"key":"attachments/x"})),
        ("List", json!({"prefix":"attachments/"})),
        ("PresignGet", json!({"key":"attachments/x","ttl_secs":1})),
        ("PresignPut", json!({"key":"attachments/x","ttl_secs":1})),
        ("Capabilities", Value::Null),
    ]
}

fn secret_requests() -> Vec<(&'static str, Value)> {
    vec![
        ("Get", json!({"name":"provider/key"})),
        ("Set", json!({"name":"provider/key","value":[1]})),
        ("Delete", json!({"name":"provider/key"})),
        ("List", json!({"prefix":"provider/"})),
    ]
}
