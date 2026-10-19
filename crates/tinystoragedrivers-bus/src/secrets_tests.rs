use super::*;
use serde_json::json;

#[test]
fn secret_messages_roundtrip_and_reject_unknown_fields() {
    let set = SetSecretRequest {
        name: "mcp/env".into(),
        value: vec![1, 2, 3],
    };
    let wire = serde_json::to_value(&set).unwrap();
    assert_eq!(
        serde_json::from_value::<SetSecretRequest>(wire).unwrap(),
        set
    );
    assert!(
        serde_json::from_value::<SecretNameRequest>(json!({"name":"n","scope":"bob"})).is_err()
    );
    assert!(serde_json::from_value::<ListSecretsRequest>(json!({"prefix":"p","x":1})).is_err());

    let versioned = VersionedSecret {
        value: vec![4, 5],
        version: [9; 32],
    };
    let encoded = serde_json::to_value(&versioned).unwrap();
    assert_eq!(
        serde_json::from_value::<VersionedSecret>(encoded).unwrap(),
        versioned
    );
    let conditional = SetSecretIfRequest {
        name: "mcp/env".into(),
        value: vec![6, 7],
        condition: SecretPrecondition::Version(SecretRevision([9; 32])),
    };
    let encoded = serde_json::to_value(&conditional).unwrap();
    assert_eq!(
        serde_json::from_value::<SetSecretIfRequest>(encoded).unwrap(),
        conditional
    );
}

#[test]
fn secret_debug_output_is_redacted() {
    let set = SetSecretRequest {
        name: "very-secret-name".into(),
        value: b"hunter2".to_vec(),
    };
    let text = format!(
        "{set:?} {:?} {:?}",
        SecretValue {
            value: b"hunter2".to_vec()
        },
        VersionedSecret {
            value: b"hunter2".to_vec(),
            version: [7; 32],
        },
    );
    assert!(!text.contains("hunter2") && !text.contains("very-secret-name"));
    let conditional = SetSecretIfRequest {
        name: "very-secret-name".into(),
        value: b"hunter2".to_vec(),
        condition: SecretPrecondition::Version(SecretRevision([7; 32])),
    };
    let text = format!("{conditional:?} {:?}", conditional.condition);
    assert!(!text.contains("hunter2") && !text.contains("very-secret-name"));
    assert_eq!(
        format!("{:?}", conditional.condition),
        "Version(SecretRevision(..))"
    );
}

#[test]
fn secret_selectors_are_fixed() {
    assert_eq!(SECRETS_OBJECT, "Secrets");
    assert_eq!(
        SECRETS_SELECTORS,
        ["Get", "GetVersioned", "Set", "SetIf", "Delete", "List"]
    );
    assert!(!is_secrets_selector("Dump"));
}
