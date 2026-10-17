//! Tests for the shared secret-name contract.

use super::*;

#[test]
fn accepts_printable_names_through_the_byte_limit() {
    validate_secret_name("alice:openai_api_key").unwrap();
    validate_secret_name("with space and ünïcode").unwrap();
    validate_secret_name(&"x".repeat(MAX_SECRET_NAME_LEN)).unwrap();
}

#[test]
fn rejects_empty_overlong_and_control_character_names_without_echoing_them() {
    for name in [
        String::new(),
        "x".repeat(MAX_SECRET_NAME_LEN + 1),
        "nul\0byte".to_owned(),
        "new\nline".to_owned(),
        "bell\u{0007}".to_owned(),
    ] {
        let error = validate_secret_name(&name).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        if !name.is_empty() {
            assert!(!error.to_string().contains(&name));
        }
    }
    assert_eq!(
        validate_secret_name("").unwrap_err().message(),
        "secret name must be 1 to 256 bytes"
    );
    assert_eq!(
        validate_secret_name("a\n").unwrap_err().message(),
        "secret names must not contain control characters"
    );
}

#[test]
fn permits_empty_and_literal_prefixes_through_the_byte_limit() {
    validate_secret_prefix("").unwrap();
    validate_secret_prefix("provider/").unwrap();
    validate_secret_prefix(&"x".repeat(MAX_SECRET_NAME_LEN)).unwrap();
}

#[test]
fn rejects_overlong_and_control_character_prefixes_without_echoing_them() {
    for prefix in [
        "x".repeat(MAX_SECRET_NAME_LEN + 1),
        "provider/\n".to_owned(),
        "provider/\u{0007}".to_owned(),
    ] {
        let error = validate_secret_prefix(&prefix).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        assert!(!error.to_string().contains(&prefix));
    }
    assert_eq!(
        validate_secret_prefix(&"x".repeat(MAX_SECRET_NAME_LEN + 1))
            .unwrap_err()
            .message(),
        "secret name prefix must be at most 256 bytes"
    );
}
