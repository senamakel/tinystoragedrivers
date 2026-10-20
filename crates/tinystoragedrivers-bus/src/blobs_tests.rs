use super::*;
use serde_json::json;

#[test]
fn blob_messages_roundtrip() {
    let meta = BlobInfo {
        key: "a/b".into(),
        len: 3,
        content_type: Some("text/plain".into()),
    };
    let body = BlobBody {
        meta: meta.clone(),
        bytes: vec![0, 1, 255],
    };
    let wire = serde_json::to_value(&body).unwrap();
    assert_eq!(serde_json::from_value::<BlobBody>(wire).unwrap(), body);
    let put = PutBlobStreamRequest {
        key: "k".into(),
        len: 1 << 30,
        content_type: None,
        ttl_ms: Some(60_000),
    };
    let wire = serde_json::to_value(&put).unwrap();
    assert!(wire.get("content_type").is_none());
    assert_eq!(wire["ttl_ms"], 60_000);
    assert_eq!(
        serde_json::from_value::<PutBlobStreamRequest>(wire).unwrap(),
        put
    );
    let range = GetRangeRequest {
        key: "k".into(),
        start: 1,
        end: 9,
    };
    let wire = serde_json::to_value(&range).unwrap();
    assert_eq!(
        serde_json::from_value::<GetRangeRequest>(wire).unwrap(),
        range
    );
    let caps = BlobCapabilities {
        presigned_urls: true,
        ttl: true,
        max_key_len: 1024,
    };
    let wire = serde_json::to_value(caps).unwrap();
    assert_eq!(
        serde_json::from_value::<BlobCapabilities>(wire).unwrap(),
        caps
    );
}

#[test]
fn absolute_expiry_messages_roundtrip_without_changing_legacy_blob_info() {
    let meta = BlobInfo {
        key: "a".into(),
        len: 3,
        content_type: Some("text/plain".into()),
    };
    let put = PutBlobUntilRequest {
        key: meta.key.clone(),
        bytes: vec![1, 2, 3],
        content_type: meta.content_type.clone(),
        expires_at_ms: Some(6_000),
    };
    assert_eq!(
        serde_json::from_value::<PutBlobUntilRequest>(serde_json::to_value(&put).unwrap()).unwrap(),
        put
    );
    let streamed = PutBlobStreamUntilRequest {
        key: "large".into(),
        len: 1_000_000,
        content_type: None,
        expires_at_ms: Some(6_000),
    };
    assert_eq!(
        serde_json::from_value::<PutBlobStreamUntilRequest>(
            serde_json::to_value(&streamed).unwrap()
        )
        .unwrap(),
        streamed
    );
    let body = BlobWithExpiry {
        meta: meta.clone(),
        expires_at_ms: Some(6_000),
        bytes: vec![1, 2, 3],
    };
    assert_eq!(
        serde_json::from_value::<BlobWithExpiry>(serde_json::to_value(&body).unwrap()).unwrap(),
        body
    );
    let head = BlobStreamHeadWithExpiry {
        meta,
        expires_at_ms: None,
    };
    assert_eq!(
        serde_json::from_value::<BlobStreamHeadWithExpiry>(serde_json::to_value(&head).unwrap())
            .unwrap(),
        head
    );
    assert!(
        serde_json::from_value::<BlobInfo>(json!({"key":"a","len":3,"expires_at_ms":6000}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<BlobBody>(json!({
            "meta": {"key":"a","len":3},
            "bytes": [1,2,3],
            "expires_at_ms": 6000
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<BlobStreamHead>(json!({
            "meta": {"key":"a","len":3},
            "expires_at_ms": 6000
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<PutBlobRequest>(json!({
            "key":"a",
            "bytes": [1,2,3],
            "expires_at_ms": 6000
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<PutBlobStreamRequest>(json!({
            "key":"a",
            "len": 3,
            "expires_at_ms": 6000
        }))
        .is_err()
    );
}

#[test]
fn blob_requests_reject_authority_selectors_and_hide_urls() {
    assert!(serde_json::from_value::<BlobKeyRequest>(json!({"key":"k","bucket":"b"})).is_err());
    assert!(
        serde_json::from_value::<PresignRequest>(json!({"key":"k","ttl_secs":5,"endpoint":"x"}))
            .is_err()
    );
    let url = PresignedUrl {
        url: "https://example.test/?sig=secret".into(),
    };
    assert!(!format!("{url:?}").contains("secret"));
}

#[test]
fn blob_selectors_are_fixed() {
    assert_eq!(BLOBS_OBJECT, "Blobs");
    assert_eq!(BLOBS_SELECTORS.len(), 15);
    assert!(is_blobs_selector("GetRange"));
    assert!(is_blobs_selector("GetWithExpiry"));
    assert!(is_blobs_selector("GetStreamWithExpiry"));
    assert!(is_blobs_selector("PutUntil"));
    assert!(is_blobs_selector("PutStreamUntil"));
    assert!(!is_blobs_selector("Arbitrary"));
}

#[test]
fn a_write_without_a_lifetime_omits_it_and_old_capabilities_default_it_off() {
    let put = PutBlobRequest {
        key: "k".into(),
        bytes: vec![1],
        content_type: None,
        ttl_ms: None,
    };
    assert!(serde_json::to_value(&put).unwrap().get("ttl_ms").is_none());
    let caps: BlobCapabilities =
        serde_json::from_value(json!({"presigned_urls": false, "max_key_len": 7})).unwrap();
    assert!(!caps.ttl, "a server from before lifetimes reports none");
}
