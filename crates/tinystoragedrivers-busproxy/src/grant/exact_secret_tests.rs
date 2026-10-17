//! Exact secret declarations and prefix-only listing authorization tests.
use super::*;
use serde_json::json;
use tinystoragedrivers_bus::StorageNeeds;
use tinystoragedrivers_secrets::{MemorySecrets, SecretStore};

const CACHE_KEY: &str = "mcp_cache_fingerprint_key";
const CACHE_KEY_SUFFIX: &str = "mcp_cache_fingerprint_key_derived";
const ENV_PREFIX: &str = "mcp_client_env/";

fn declared_grant(
    exact_secret_names: &[&str],
    secret_prefixes: &[&str],
    allow_secrets: bool,
) -> Result<StorageGrant, GrantError> {
    let needs = StorageNeeds {
        secret_prefixes,
        ..StorageNeeds::default()
    }
    .with_exact_secret_names(exact_secret_names);
    StorageGrant::from_needs_with_exact_secrets(
        needs,
        &GrantPolicy {
            writable: true,
            allow_secrets,
            reserved_collections: &[],
        },
    )
}

#[test]
fn exact_name_grants_permit_only_crud_for_the_declared_key() {
    let grant = declared_grant(&[CACHE_KEY], &[], true).unwrap();
    for (selector, request, write) in [
        ("Get", json!({"name": CACHE_KEY}), false),
        ("Set", json!({"name": CACHE_KEY, "value": [1, 2]}), true),
        ("Delete", json!({"name": CACHE_KEY}), true),
    ] {
        assert!(matches!(
            grant.authorize(Object::Secrets, selector, &request),
            Ok(Plan::Forward { target, write: actual_write, .. })
                if target == "[REDACTED]" && actual_write == write
        ));
    }

    for (selector, request) in [
        ("Get", json!({"name": "mcp_cache_fingerprint_key_suffix"})),
        (
            "Set",
            json!({"name": "mcp_cache_fingerprint_key_suffix", "value": [3]}),
        ),
        (
            "Delete",
            json!({"name": "mcp_cache_fingerprint_key_suffix"}),
        ),
    ] {
        assert_eq!(
            grant.authorize(Object::Secrets, selector, &request),
            Err(Denied::Target),
            "selector {selector} must not widen an exact declaration"
        );
    }
}

#[test]
fn exact_only_grants_do_not_authorize_secret_lists_even_at_the_exact_name() {
    let grant = declared_grant(&[CACHE_KEY], &[], true).unwrap();
    assert_eq!(
        grant.authorize(Object::Secrets, "List", &json!({"prefix": CACHE_KEY})),
        Err(Denied::Target)
    );
}

#[test]
fn mixed_exact_and_prefix_grants_keep_prefix_crud_and_list_semantics() {
    let grant = declared_grant(&[CACHE_KEY], &[ENV_PREFIX], true).unwrap();
    assert!(
        grant
            .authorize(Object::Secrets, "Get", &json!({"name": CACHE_KEY}))
            .is_ok()
    );
    assert!(
        grant
            .authorize(
                Object::Secrets,
                "Get",
                &json!({"name": "mcp_client_env/provider"})
            )
            .is_ok()
    );
    assert!(
        grant
            .authorize(Object::Secrets, "List", &json!({"prefix": ENV_PREFIX}))
            .is_ok()
    );
    assert_eq!(
        grant.authorize(Object::Secrets, "List", &json!({"prefix": CACHE_KEY})),
        Err(Denied::Target)
    );
}

#[test]
fn legacy_prefix_grants_keep_literal_suffix_and_list_behavior() {
    let secret_prefixes = [ENV_PREFIX];
    let grant = StorageGrant::from_needs(
        StorageNeeds {
            secret_prefixes: &secret_prefixes,
            ..StorageNeeds::default()
        },
        &GrantPolicy {
            writable: true,
            allow_secrets: true,
            reserved_collections: &[],
        },
    )
    .unwrap();
    assert!(
        grant
            .authorize(
                Object::Secrets,
                "Get",
                &json!({"name": "mcp_client_env/provider"})
            )
            .is_ok()
    );
    assert!(
        grant
            .authorize(Object::Secrets, "List", &json!({"prefix": ENV_PREFIX}))
            .is_ok()
    );
}

#[test]
fn exact_names_require_host_secret_permission_and_valid_unique_names() {
    assert_eq!(
        declared_grant(&[CACHE_KEY], &[], false).unwrap_err(),
        GrantError::SecretsDenied
    );
    for invalid in ["", "bad\nname", "bad\0name"] {
        let error = declared_grant(&[invalid], &[], true).unwrap_err();
        assert_eq!(error, GrantError::Prefix);
        assert_eq!(error.to_string(), "invalid storage prefix grant");
    }
    let too_long = "x".repeat(tinystoragedrivers_core::MAX_SECRET_NAME_LEN + 1);
    assert_eq!(
        declared_grant(&[&too_long], &[], true).unwrap_err(),
        GrantError::Prefix
    );
    assert_eq!(
        declared_grant(&[CACHE_KEY, CACHE_KEY], &[], true).unwrap_err(),
        GrantError::Prefix
    );

    let grant = declared_grant(&[CACHE_KEY], &[], true).unwrap();
    assert!(!format!("{grant:?}").contains(CACHE_KEY));
    assert!(!format!("{grant:?}").contains("mcp_cache"));
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_grant_crud_works_with_memory_secrets_but_does_not_authorize_suffix_listing() {
    let store = MemorySecrets::new();
    store.set(CACHE_KEY, b"before").await.unwrap();
    store.set(CACHE_KEY_SUFFIX, b"derived").await.unwrap();
    let grant = declared_grant(&[CACHE_KEY], &[], true).unwrap();

    let set_request = json!({"name": CACHE_KEY, "value": b"fingerprint".to_vec()});
    assert!(matches!(
        grant.authorize(Object::Secrets, "Set", &set_request),
        Ok(Plan::Forward { write: true, .. })
    ));
    store.set(CACHE_KEY, b"fingerprint").await.unwrap();
    assert!(
        grant
            .authorize(Object::Secrets, "Get", &json!({"name": CACHE_KEY}))
            .is_ok()
    );
    assert_eq!(
        store.get(CACHE_KEY).await.unwrap().unwrap().as_slice(),
        b"fingerprint"
    );

    let backend_names = store.list(CACHE_KEY).await.unwrap();
    assert_eq!(backend_names, [CACHE_KEY, CACHE_KEY_SUFFIX]);
    assert_eq!(
        grant.authorize(Object::Secrets, "List", &json!({"prefix": CACHE_KEY})),
        Err(Denied::Target),
        "an exact-name grant must not expose backend suffix names"
    );

    assert_eq!(
        grant.authorize(
            Object::Secrets,
            "Delete",
            &json!({"name": CACHE_KEY_SUFFIX})
        ),
        Err(Denied::Target)
    );
    assert!(
        grant
            .authorize(Object::Secrets, "Delete", &json!({"name": CACHE_KEY}))
            .is_ok()
    );
    assert!(store.delete(CACHE_KEY).await.unwrap());
    assert!(store.get(CACHE_KEY).await.unwrap().is_none());
    assert_eq!(
        store
            .get(CACHE_KEY_SUFFIX)
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"derived"
    );
}
