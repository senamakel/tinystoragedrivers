//! Name and prefix validation.

use tinystoragedrivers_core::ErrorKind;

use super::*;

#[test]
fn accepts_namespaced_printable_names() {
    validate_name("alice:openai_api_key").unwrap();
    validate_name("with space and ünïcode").unwrap();
    validate_name(&"x".repeat(MAX_SECRET_NAME_LEN)).unwrap();
}

#[test]
fn rejects_empty_long_and_control_names() {
    for bad in [
        String::new(),
        "x".repeat(MAX_SECRET_NAME_LEN + 1),
        "nul\0byte".to_string(),
        "new\nline".to_string(),
    ] {
        let error = validate_name(&bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn error_messages_never_repeat_the_name() {
    let error = validate_name("sk-live\0secret").unwrap_err();
    assert!(!error.to_string().contains("sk-live"));
}

#[test]
fn prefixes_may_be_empty_but_not_long_or_control() {
    validate_prefix("").unwrap();
    validate_prefix("alice:").unwrap();
    assert_eq!(
        validate_prefix(&"x".repeat(MAX_SECRET_NAME_LEN + 1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        validate_prefix("a\0").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn require_utf8_rejects_binary_without_echoing_it() {
    assert_eq!(require_utf8(b"text", "file").unwrap(), "text");
    let error = require_utf8(&[0xff, 0xfe], "file").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("file"));
}

#[test]
fn secret_version_round_trips_opaque_wire_bytes() {
    let bytes = [0x5a; 32];
    let version = SecretVersion::from_bytes(bytes);
    assert_eq!(version.as_bytes(), &bytes);
    assert_eq!(version.into_bytes(), bytes);
}
