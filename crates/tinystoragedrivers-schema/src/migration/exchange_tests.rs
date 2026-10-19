//! Stable version-one wire fixtures.
use super::*;
use serde_json::json;
#[test]
fn stable_tagged_exchange_shapes() {
    assert_eq!(
        serde_json::to_value(MigrationExchangeRequest::Begin { request_len: 12 }).unwrap(),
        json!({"type":"begin","request_len":12})
    );
    assert_eq!(
        serde_json::to_value(MigrationExchangeReply::Error {
            kind: ExchangeErrorKind::Busy
        })
        .unwrap(),
        json!({"type":"error","kind":{"type":"busy"}})
    );
    let payload = MigrationPayload::BlobTarget(BlobTargetRequest {
        operation: "move".into(),
        version: 1,
        source: "old".into(),
    });
    assert_eq!(
        serde_json::to_value(&payload).unwrap(),
        json!({"type":"blob_target","payload":{"operation":"move","version":1,"source":"old"}})
    );
    assert_eq!(
        serde_json::from_value::<MigrationPayload>(serde_json::to_value(&payload).unwrap())
            .unwrap(),
        payload
    );
    assert_eq!(
        serde_json::to_value(StreamMapMode::IndependentEntries).unwrap(),
        json!({"type":"independent_entries"})
    );
}

fn fixture<T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(
    value: &T,
    expected: serde_json::Value,
) {
    assert_eq!(serde_json::to_value(value).unwrap(), expected);
    assert_eq!(&serde_json::from_value::<T>(expected).unwrap(), value);
}
#[test]
fn every_exchange_phase_has_a_fixed_wire_fixture() {
    for (value, expected) in [
        (
            MigrationExchangeRequest::Upload {
                transfer: "id".into(),
                offset: 2,
                bytes: vec![0, 255],
            },
            json!({"type":"upload","transfer":"id","offset":2,"bytes":[0,255]}),
        ),
        (
            MigrationExchangeRequest::Execute {
                transfer: "id".into(),
            },
            json!({"type":"execute","transfer":"id"}),
        ),
        (
            MigrationExchangeRequest::Download {
                transfer: "id".into(),
                offset: 2,
            },
            json!({"type":"download","transfer":"id","offset":2}),
        ),
        (
            MigrationExchangeRequest::Finish {
                transfer: "id".into(),
            },
            json!({"type":"finish","transfer":"id"}),
        ),
        (
            MigrationExchangeRequest::Cancel {
                transfer: "id".into(),
            },
            json!({"type":"cancel","transfer":"id"}),
        ),
    ] {
        fixture(&value, expected);
    }
    for (value, expected) in [
        (
            MigrationExchangeReply::Begun {
                transfer: "id".into(),
            },
            json!({"type":"begun","transfer":"id"}),
        ),
        (
            MigrationExchangeReply::Uploaded { next_offset: 2 },
            json!({"type":"uploaded","next_offset":2}),
        ),
        (
            MigrationExchangeReply::Ready { reply_len: 2 },
            json!({"type":"ready","reply_len":2}),
        ),
        (
            MigrationExchangeReply::Chunk {
                offset: 0,
                bytes: vec![0, 255],
            },
            json!({"type":"chunk","offset":0,"bytes":[0,255]}),
        ),
        (MigrationExchangeReply::Finished, json!({"type":"finished"})),
        (
            MigrationExchangeReply::Cancelled,
            json!({"type":"cancelled"}),
        ),
    ] {
        fixture(&value, expected);
    }
}
#[test]
fn payload_families_and_mapping_modes_have_fixed_wire_fixtures() {
    fixture(
        &MigrationPayload::Documents(DocumentsTransformRequest {
            operation: "op".into(),
            version: 1,
            records: vec![],
        }),
        json!({"type":"documents","payload":{"operation":"op","version":1,"records":[]}}),
    );
    fixture(
        &MigrationPayload::StreamPlan(StreamPlanRequest {
            operation: "op".into(),
            version: 1,
            source: "old".into(),
        }),
        json!({"type":"stream_plan","payload":{"operation":"op","version":1,"source":"old"}}),
    );
    fixture(
        &MigrationPayload::StreamEntries(StreamEntriesRequest {
            operation: "op".into(),
            version: 1,
            entries: vec![json!(1)],
        }),
        json!({"type":"stream_entries","payload":{"operation":"op","version":1,"entries":[1]}}),
    );
    fixture(&StreamMapMode::Identity, json!({"type":"identity"}));
    fixture(
        &MigrationPayloadReply::Documents(Ok(vec![])),
        json!({"type":"documents","payload":{"Ok":[]}}),
    );
    fixture(
        &MigrationPayloadReply::BlobTarget(Ok(Some("target".into()))),
        json!({"type":"blob_target","payload":{"Ok":"target"}}),
    );
    fixture(
        &MigrationPayloadReply::StreamPlan(Ok(Some(StreamPlan {
            target: "target".into(),
            mode: StreamMapMode::Identity,
        }))),
        json!({"type":"stream_plan","payload":{"Ok":{"target":"target","mode":{"type":"identity"}}}}),
    );
    fixture(
        &MigrationPayloadReply::StreamEntries(Ok(vec![json!(1)])),
        json!({"type":"stream_entries","payload":{"Ok":[1]}}),
    );
}
#[test]
fn static_error_vocabulary_and_unknown_fields_fail_closed() {
    for (kind, name) in [
        (ExchangeErrorKind::InvalidRequest, "invalid_request"),
        (ExchangeErrorKind::Unavailable, "unavailable"),
        (ExchangeErrorKind::UnknownTransfer, "unknown_transfer"),
        (ExchangeErrorKind::WrongSequence, "wrong_sequence"),
        (ExchangeErrorKind::Incomplete, "incomplete"),
        (ExchangeErrorKind::Busy, "busy"),
        (ExchangeErrorKind::Expired, "expired"),
        (ExchangeErrorKind::InvalidPayload, "invalid_payload"),
        (ExchangeErrorKind::TransformFailed, "transform_failed"),
    ] {
        fixture(
            &MigrationExchangeReply::Error { kind },
            json!({"type":"error","kind":{"type":name}}),
        );
    }
    assert!(
        serde_json::from_value::<MigrationExchangeRequest>(
            json!({"type":"begin","request_len":2,"peer":"forged"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<BlobTargetRequest>(
            json!({"operation":"op","version":1,"source":"old","bytes":[1]})
        )
        .is_err()
    );
}
