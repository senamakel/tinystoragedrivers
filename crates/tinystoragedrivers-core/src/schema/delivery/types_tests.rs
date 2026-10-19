//! Buffer writes and confidential peer diagnostics.
use super::*;
use std::io::Write;

#[test]
fn buffer_appends_complete_writes_and_flush_preserves_bytes() {
    let mut buffer = Buffer(Vec::new());
    buffer.flush().unwrap();
    assert_eq!(buffer.0.len(), 0);
    assert_eq!(buffer.write(b"first").unwrap(), 5);
    assert_eq!(buffer.write(b"").unwrap(), 0);
    assert_eq!(buffer.write(b"\0second").unwrap(), 7);
    buffer.flush().unwrap();
    buffer.flush().unwrap();
    assert_eq!(buffer.0, b"first\0second");
    buffer.write_all(b"after flush").unwrap();
    assert_eq!(buffer.0, b"first\0secondafter flush");
}

#[test]
fn peer_debug_hides_identity_in_standard_and_pretty_formatting() {
    for identity in ["", "private-dispatch-peer", "peer\nwith-control-characters"] {
        let peer = PeerIdentity::new(identity);
        assert_eq!(format!("{peer:?}"), "PeerIdentity(..)");
        assert_eq!(format!("{peer:#?}"), "PeerIdentity(..)");
    }
}

#[test]
fn encoding_preserves_json_escaping_and_nested_values() {
    let value = serde_json::json!({
        "message": "line\nquoted \" text and unicode λ",
        "values": [null, true, 42, {"empty": ""}],
    });
    let encoded = encode(&value).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
        value
    );
    assert_eq!(encoded, serde_json::to_vec(&value).unwrap());
}
