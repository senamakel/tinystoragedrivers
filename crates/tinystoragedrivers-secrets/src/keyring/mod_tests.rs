//! The keyring driver over an in-memory credential builder (never the real OS
//! store), the error mapping, and the opt-in `live_*` test.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Mutex;

use keyring::credential::{Credential, CredentialApi, CredentialBuilderApi};
use tinystoragedrivers_core::ErrorKind;

use super::*;
use crate::crypto::tests::FIXTURE_KEY_HEX;
use crate::testkit::secrets_conformance;

type Shared = Arc<Mutex<HashMap<(String, String), Vec<u8>>>>;

/// What a [`FakeCredential`] does instead of storing.
#[derive(Clone, Copy, Debug, Default)]
enum Fault {
    #[default]
    None,
    /// Every call fails with `NoStorageAccess`.
    Locked,
    /// Writes are accepted but dropped, so a read-back finds nothing new.
    DropWrites,
    /// Writes store something other than what was written.
    CorruptWrites,
}

/// A credential store shared by every credential the builder makes, so a set
/// through one `Entry` is visible through the next, as on a real keychain.
#[derive(Debug, Default)]
struct FakeBuilder {
    store: Shared,
    fault: Fault,
}

#[derive(Debug)]
struct FakeCredential {
    store: Shared,
    id: (String, String),
    fault: Fault,
}

impl CredentialBuilderApi for FakeBuilder {
    fn build(
        &self,
        _target: Option<&str>,
        service: &str,
        user: &str,
    ) -> keyring::Result<Box<Credential>> {
        if user.len() > 200 {
            return Err(keyring::Error::TooLong("user".to_string(), 200));
        }
        Ok(Box::new(FakeCredential {
            store: Arc::clone(&self.store),
            id: (service.to_string(), user.to_string()),
            fault: self.fault,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn locked() -> keyring::Error {
    keyring::Error::NoStorageAccess(Box::new(std::io::Error::other("locked")))
}

impl CredentialApi for FakeCredential {
    fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
        let stored = match self.fault {
            Fault::Locked => return Err(locked()),
            Fault::DropWrites => return Ok(()),
            Fault::CorruptWrites => b"something else".to_vec(),
            Fault::None => secret.to_vec(),
        };
        self.store.lock().unwrap().insert(self.id.clone(), stored);
        Ok(())
    }

    fn get_secret(&self) -> keyring::Result<Vec<u8>> {
        if let Fault::Locked = self.fault {
            return Err(locked());
        }
        self.store
            .lock()
            .unwrap()
            .get(&self.id)
            .cloned()
            .ok_or(keyring::Error::NoEntry)
    }

    fn delete_credential(&self) -> keyring::Result<()> {
        if let Fault::Locked = self.fault {
            return Err(locked());
        }
        self.store
            .lock()
            .unwrap()
            .remove(&self.id)
            .map(|_| ())
            .ok_or(keyring::Error::NoEntry)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn fake(fault: Fault) -> (KeyringSecrets, Shared) {
    let builder = FakeBuilder {
        fault,
        ..FakeBuilder::default()
    };
    let store = Arc::clone(&builder.store);
    (
        KeyringSecrets::with_credential_builder("tsd-test", Box::new(builder)),
        store,
    )
}

#[tokio::test]
async fn passes_the_secrets_conformance_suite() {
    let (secrets, _) = fake(Fault::None);
    secrets_conformance(&secrets).await;
    assert_eq!(secrets.backend_name(), "keyring");
    assert!(!secrets.enumerable());
    assert_eq!(secrets.service(), "tsd-test");
}

#[tokio::test]
async fn keyring_reports_conditional_secret_writes_as_unsupported() {
    use tinystoragedrivers_core::Capability;

    let (secrets, _) = fake(Fault::None);
    assert_eq!(
        secrets.get_versioned("token").await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ConditionalSecretWrites)
    );
    assert_eq!(
        secrets
            .set_if("token", b"value", crate::Precondition::Absent)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ConditionalSecretWrites)
    );
}

#[tokio::test]
async fn stores_one_password_per_name_under_the_service() {
    let (secrets, store) = fake(Fault::None);
    secrets.set("alice:api_key", b"sk-live-abc").await.unwrap();
    let stored = store.lock().unwrap().clone();
    assert_eq!(
        stored[&("tsd-test".to_string(), "alice:api_key".to_string())],
        b"sk-live-abc"
    );
}

#[tokio::test]
async fn reads_an_entry_written_in_openhumans_layout() {
    let (secrets, store) = fake(Fault::None);
    store.lock().unwrap().insert(
        ("tsd-test".to_string(), "alice:oauth".to_string()),
        "tok-\u{e9}".as_bytes().to_vec(),
    );
    assert_eq!(
        secrets
            .get("alice:oauth")
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        "tok-\u{e9}".as_bytes()
    );
}

#[tokio::test]
async fn binary_values_and_listing_are_refused() {
    let (secrets, _) = fake(Fault::None);
    assert_eq!(
        secrets.set("bin", &[0xff]).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        secrets.list("x").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        secrets.list("x\0").await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn a_non_utf8_stored_value_is_a_serialization_error() {
    let (secrets, store) = fake(Fault::None);
    store.lock().unwrap().insert(
        ("tsd-test".to_string(), "raw".to_string()),
        vec![0xff, 0xfe],
    );
    assert_eq!(
        secrets.get("raw").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn a_locked_store_is_unavailable_everywhere() {
    let (secrets, _) = fake(Fault::Locked);
    assert_eq!(
        secrets.get("a").await.unwrap_err().kind(),
        ErrorKind::Unavailable
    );
    assert_eq!(
        secrets.set("a", b"1").await.unwrap_err().kind(),
        ErrorKind::Unavailable
    );
    assert_eq!(
        secrets.delete("a").await.unwrap_err().kind(),
        ErrorKind::Unavailable
    );
}

#[tokio::test]
async fn a_name_the_platform_rejects_is_invalid_input() {
    let (secrets, _) = fake(Fault::None);
    let name = "n".repeat(201);
    assert_eq!(
        secrets.get(&name).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn load_or_create_key_creates_once_then_reuses() {
    let (secrets, store) = fake(Fault::None);
    let first = secrets.load_or_create_key("app:master_key").await.unwrap();
    let stored =
        store.lock().unwrap()[&("tsd-test".to_string(), "app:master_key".to_string())].clone();
    assert_eq!(String::from_utf8(stored).unwrap(), *key_to_hex(&first));
    let second = secrets.load_or_create_key("app:master_key").await.unwrap();
    assert_eq!(*first, *second);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_key_creation_agrees_on_one_key() {
    let (secrets, _) = fake(Fault::None);
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let secrets = secrets.clone();
            tokio::spawn(async move { secrets.load_or_create_key("app:race").await.unwrap() })
        })
        .collect();
    let mut keys = Vec::new();
    for task in tasks {
        keys.push(*task.await.unwrap());
    }
    assert!(keys.windows(2).all(|pair| pair[0] == pair[1]));
}

#[tokio::test]
async fn a_creation_lock_file_is_reused_and_rejects_a_non_directory_parent() {
    let root = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let dir = root.path().to_path_buf();
    let (secrets, _) = fake(Fault::None);
    let secrets = secrets.with_key_creation_lock(dir.join("keyring.lock"));
    let key = secrets.load_or_create_key("app:locked").await.unwrap();
    assert_eq!(
        *secrets.load_or_create_key("app:locked").await.unwrap(),
        *key
    );
    assert!(dir.join("keyring.lock").exists());
    assert!(format!("{secrets:?}").contains("keyring.lock"));

    // A lock that cannot be taken fails the call before anything is written.
    let not_a_dir = dir.join("not-a-dir");
    std::fs::write(&not_a_dir, b"").unwrap();
    let (unlockable, store) = fake(Fault::None);
    let unlockable = unlockable.with_key_creation_lock(not_a_dir.join("keyring.lock"));
    assert_eq!(
        unlockable
            .load_or_create_key("app:x")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert!(store.lock().unwrap().is_empty());
}

#[tokio::test]
async fn load_or_create_key_reads_openhumans_hex_master_key() {
    let (secrets, store) = fake(Fault::None);
    store.lock().unwrap().insert(
        ("tsd-test".to_string(), "app:master_key".to_string()),
        format!("{FIXTURE_KEY_HEX}\n").into_bytes(),
    );
    let key = secrets.load_or_create_key("app:master_key").await.unwrap();
    assert_eq!(*key, *crate::crypto::tests::fixture_key());
}

#[tokio::test]
async fn load_or_create_key_never_mints_over_a_locked_store() {
    let (secrets, store) = fake(Fault::Locked);
    let error = secrets
        .load_or_create_key("app:master_key")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(store.lock().unwrap().is_empty());
}

#[tokio::test]
async fn load_or_create_key_rejects_a_key_that_does_not_read_back() {
    let (dropped, _) = fake(Fault::DropWrites);
    assert_eq!(
        dropped.load_or_create_key("k").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    let (corrupted, _) = fake(Fault::CorruptWrites);
    assert_eq!(
        corrupted.load_or_create_key("k").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
    let (secrets, _) = fake(Fault::None);
    assert_eq!(
        secrets.load_or_create_key("").await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn load_or_create_key_rejects_a_malformed_stored_key() {
    let (secrets, store) = fake(Fault::None);
    store
        .lock()
        .unwrap()
        .insert(("tsd-test".to_string(), "k".to_string()), b"zz".to_vec());
    assert_eq!(
        secrets.load_or_create_key("k").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[test]
fn error_mapping_covers_every_variant_without_leaking() {
    let platform = keyring::Error::PlatformFailure(Box::new(std::io::Error::other("dbus down")));
    let cases = [
        (locked(), ErrorKind::Unavailable, true),
        (platform, ErrorKind::Backend, true),
        (keyring::Error::NoEntry, ErrorKind::NotFound, false),
        (
            keyring::Error::BadEncoding(b"sk-leak".to_vec()),
            ErrorKind::Serialization,
            false,
        ),
        (
            keyring::Error::TooLong("sk-leak".into(), 1),
            ErrorKind::InvalidInput,
            false,
        ),
        (
            keyring::Error::Invalid("user".into(), "sk-leak".into()),
            ErrorKind::InvalidInput,
            false,
        ),
        (
            keyring::Error::Ambiguous(Vec::new()),
            ErrorKind::Backend,
            false,
        ),
    ];
    for (error, kind, has_source) in cases {
        let mapped = map_keyring_error(error);
        assert_eq!(mapped.kind(), kind);
        assert_eq!(std::error::Error::source(&mapped).is_some(), has_source);
        assert!(!mapped.to_string().contains("sk-leak"));
    }
}

#[test]
fn debug_shows_service_only() {
    let (secrets, _) = fake(Fault::None);
    assert_eq!(
        format!("{secrets:?}"),
        "KeyringSecrets { service: \"tsd-test\", custom_builder: true, creation_lock: None }"
    );
    let default = KeyringSecrets::new("svc");
    assert!(format!("{default:?}").contains("custom_builder: false"));
}

/// The real OS store. Opt-in only: `TSD_LIVE_KEYRING=1 cargo test -p
/// tinystoragedrivers-secrets --features keyring live_`.
#[tokio::test]
async fn live_keyring_conformance() {
    if std::env::var("TSD_LIVE_KEYRING").as_deref() != Ok("1") {
        return;
    }
    let secrets = KeyringSecrets::new("tinystoragedrivers-live-test");
    secrets_conformance(&secrets).await;
    // A name unique to this run, so the cleanup below can only remove the
    // credential this test created.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("live:master_key:{}:{nanos}", std::process::id());
    assert!(secrets.get(&name).await.unwrap().is_none());
    let key = secrets.load_or_create_key(&name).await.unwrap();
    assert_eq!(*secrets.load_or_create_key(&name).await.unwrap(), *key);
    assert!(secrets.delete(&name).await.unwrap());
}

#[cfg(target_os = "linux")]
#[test]
fn explicit_kernel_factory_retains_its_concrete_builder_without_provider_access() {
    let store = KeyringSecrets::linux_kernel("factory-test");
    let expected = keyring::keyutils::default_credential_builder();
    let concrete = store
        .builder
        .as_ref()
        .is_some_and(|builder| builder.as_any().type_id() == expected.as_any().type_id());
    assert!(
        concrete,
        "kernel factory must retain the explicit kernel builder"
    );
    assert_eq!(store.service(), "factory-test");
    assert!(KeyringSecrets::new("factory-test").builder.is_none());
}

#[cfg(all(target_os = "linux", feature = "keyring-secret-service"))]
#[test]
fn explicit_secret_service_factory_retains_its_concrete_builder_without_provider_access() {
    let store = KeyringSecrets::secret_service("factory-test");
    let concrete = store.builder.as_ref().is_some_and(|builder| {
        builder
            .as_any()
            .is::<keyring::secret_service::SsCredentialBuilder>()
    });
    assert!(
        concrete,
        "Secret Service factory must retain its concrete builder"
    );
    let kernel = keyring::keyutils::default_credential_builder();
    let distinct = store
        .builder
        .as_ref()
        .is_some_and(|builder| builder.as_any().type_id() != kernel.as_any().type_id());
    assert!(
        distinct,
        "native factories must select different provider builders"
    );
    assert_eq!(store.service(), "factory-test");
    assert!(KeyringSecrets::new("factory-test").builder.is_none());
}

// Only the private-bus launcher opts into these real provider operations.
#[cfg(all(target_os = "linux", feature = "keyring-secret-service"))]
#[tokio::test]
async fn live_isolated_secret_service_probe() {
    let mode = std::env::var("TSD_ISOLATED_SECRET_SERVICE_MODE").ok();
    if mode.is_none() {
        return;
    }
    let store = KeyringSecrets::secret_service("tinystoragedrivers-disposable-probe");
    let concrete = store.builder.as_ref().is_some_and(|builder| {
        builder
            .as_any()
            .is::<keyring::secret_service::SsCredentialBuilder>()
    });
    assert!(
        concrete,
        "native probe must use the concrete Secret Service builder"
    );
    let passed = match mode.as_deref() {
        Some("roundtrip") => {
            let absent_before = matches!(store.get("disposable-entry").await, Ok(None));
            let set = store
                .set("disposable-entry", b"public disposable fixture value")
                .await
                .is_ok();
            let exact_readback = matches!(store.get("disposable-entry").await, Ok(Some(value)) if value.as_slice() == b"public disposable fixture value");
            let deleted = matches!(store.delete("disposable-entry").await, Ok(true));
            let absent_after = matches!(store.get("disposable-entry").await, Ok(None));
            let no_entry_delete = matches!(store.delete("disposable-entry").await, Ok(false));
            absent_before && set && exact_readback && deleted && absent_after && no_entry_delete
        }
        Some("seed") => store
            .set("disposable-entry", b"public disposable fixture value")
            .await
            .is_ok(),
        Some("denied") => {
            let get_denied = matches!(store.get("disposable-entry").await, Err(error) if matches!(error.kind(), ErrorKind::Backend | ErrorKind::Unavailable));
            let set_denied = matches!(store.set("disposable-entry", b"public disposable fixture value").await, Err(error) if matches!(error.kind(), ErrorKind::Backend | ErrorKind::Unavailable));
            let delete_denied = matches!(store.delete("disposable-entry").await, Err(error) if matches!(error.kind(), ErrorKind::Backend | ErrorKind::Unavailable));
            get_denied && set_denied && delete_denied
        }
        _ => false,
    };
    assert!(passed, "isolated native provider operation failed");
}
