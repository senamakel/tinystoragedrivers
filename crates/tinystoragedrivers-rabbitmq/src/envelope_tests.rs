//! The envelope's wire form is pinned; foreign bodies are recognized.
use super::*;
use serde_json::json;

#[test]
fn the_envelope_pins_its_representation() {
    let envelope = Envelope {
        id: "m1".into(),
        payload: json!({"t": 1}),
        attempt: 2,
        max_attempts: 8,
        enqueued_ms: 5,
        reason: None,
    };
    let bytes = envelope.to_bytes().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap(),
        json!({"id":"m1","payload":{"t":1},"attempt":2,"max_attempts":8,"enqueued_ms":5})
    );
    let back = Envelope::from_bytes(&bytes, None, 0);
    assert_eq!(back, envelope);
    assert!(!back.is_foreign());
}

#[test]
fn a_body_the_driver_did_not_write_is_foreign() {
    let named = Envelope::from_bytes(b"plain text", Some("x1"), 9);
    assert!(named.is_foreign());
    assert_eq!(named.id, "x1");
    assert_eq!(named.payload, json!("plain text"));
    assert_eq!(named.enqueued_ms, 9);
    let unnamed = Envelope::from_bytes(b"{\"other\":1}", None, 0);
    assert!(unnamed.is_foreign());
    assert_eq!(unnamed.id.len(), 26, "a fresh ulid");
}
