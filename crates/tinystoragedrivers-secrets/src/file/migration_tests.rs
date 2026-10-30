//! Import identity, recovery and conditional cleanup behavior.

use super::*;

use crate::{KEY_FILE_NAME, SecretStore};
use std::io::Write;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

fn setup() -> (
    tempfile::TempDir,
    PathBuf,
    EncryptedFileSecrets,
    FileImportSource,
) {
    setup_with_sync(true)
}

fn setup_with_sync(
    sync_source: bool,
) -> (
    tempfile::TempDir,
    PathBuf,
    EncryptedFileSecrets,
    FileImportSource,
) {
    let root = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let directory = root.path().join("private");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
    }
    #[cfg(windows)]
    clack_private_fs::create_dir(&directory).unwrap();
    let source_path = directory.join("legacy");
    #[cfg(unix)]
    let mut file = {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&source_path)
            .unwrap()
    };
    #[cfg(windows)]
    let mut file = clack_private_fs::create_new_file(&source_path).unwrap();
    file.write_all(b"disposable legacy fixture").unwrap();
    if sync_source {
        file.sync_all().unwrap();
    }
    drop(file);
    let source = FileImportSource::open(&source_path).unwrap();
    let store = EncryptedFileSecrets::new(&directory, crate::crypto::tests::fixture_key());
    (root, source_path, store, source)
}

#[tokio::test]
async fn an_exact_import_retry_reconciles_and_another_operation_conflicts() {
    let (_root, path, store, source) = setup();
    assert_eq!(source.bytes(), b"disposable legacy fixture");
    let first = store
        .prepare_file_import("operation-one", "fixture", b"imported", &source)
        .await
        .unwrap();
    assert!(path.exists());
    let retry = store
        .prepare_file_import("operation-one", "fixture", b"imported", &source)
        .await
        .unwrap();
    assert_eq!(retry.boundary(), first.boundary());
    assert_eq!(
        store
            .prepare_file_import("operation-two", "fixture", b"imported", &source)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(
        store
            .prepare_file_import("operation-one", "fixture", b"different", &source)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(
        store.get("fixture").await.unwrap().unwrap().as_slice(),
        b"imported"
    );
    assert_eq!(
        store
            .recover_file_import_source("operation-one", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
    assert!(
        store
            .recover_file_import_source("operation-two", "fixture")
            .await
            .is_err()
    );
    assert!(!format!("{source:?} {first:?}").contains("disposable"));
}

#[tokio::test]
async fn import_refuses_an_unbound_existing_value_and_wrong_key_receipt() {
    let (_root, path, store, source) = setup();
    store.set("fixture", b"imported").await.unwrap();
    assert!(
        store
            .prepare_file_import("operation-one", "fixture", b"imported", &source)
            .await
            .is_err()
    );
    let receipt = store
        .prepare_file_import("operation-one", "other", b"imported", &source)
        .await
        .unwrap();
    let other = EncryptedFileSecrets::at_path(store.path(), crypto::generate_key().unwrap());
    assert!(other.cleanup_file_import(&receipt).await.is_err());
    assert!(path.exists());
}

#[tokio::test]
async fn a_changed_destination_never_authorizes_source_cleanup() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("operation-one", "fixture", b"imported", &source)
        .await
        .unwrap();
    store.set("fixture", b"changed").await.unwrap();
    #[cfg(unix)]
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    #[cfg(windows)]
    assert_eq!(
        store.cleanup_file_import(&receipt).await.unwrap(),
        CleanupAcknowledgment::UnsupportedBoundary
    );
    assert!(path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_rejects_a_replaced_source_and_preserves_both_objects() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("operation-one", "fixture", b"imported", &source)
        .await
        .unwrap();
    let old = path.with_file_name("old-source");
    std::fs::rename(&path, &old).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    file.write_all(b"replacement fixture").unwrap();
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement fixture");
    assert_eq!(std::fs::read(old).unwrap(), b"disposable legacy fixture");
}

#[tokio::test]
async fn cleanup_acknowledges_only_the_supported_native_boundary() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("operation-one", "fixture", b"imported", &source)
        .await
        .unwrap();
    #[cfg(unix)]
    {
        assert_eq!(receipt.boundary(), PublicationBoundary::UnixDirectorySync);
        assert_eq!(
            store.cleanup_file_import(&receipt).await.unwrap(),
            CleanupAcknowledgment::RemovedAndDirectorySynced
        );
        assert!(!path.exists());
        assert_eq!(
            store.cleanup_file_import(&receipt).await.unwrap(),
            CleanupAcknowledgment::AlreadyCompleted
        );
    }
    #[cfg(windows)]
    {
        assert_eq!(
            receipt.boundary(),
            PublicationBoundary::WindowsVisibleFileFlushed
        );
        assert_eq!(
            store.cleanup_file_import(&receipt).await.unwrap(),
            CleanupAcknowledgment::UnsupportedBoundary
        );
        assert!(path.exists());
    }
    assert_eq!(
        store
            .recover_file_import_source("operation-one", "fixture")
            .await
            .unwrap()
            .as_slice(),
        b"disposable legacy fixture"
    );
}

#[test]
fn missing_sources_are_rejected_without_key_creation() {
    let root = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    assert!(FileImportSource::open(root.path().join("absent")).is_err());
    assert!(!root.path().join(KEY_FILE_NAME).exists());
}

#[test]
fn native_private_sources_preserve_empty_exact_bound_and_oversized_bytes() {
    for length in [0, 1_048_576, 1_048_577] {
        let (_root, path, store, source) = setup();
        drop(source);
        drop(store);
        let bytes = vec![b'x'; length];
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let owned = FileImportSource::open(path.clone());
        let borrowed = FileImportSource::open(&path);
        for result in [owned, borrowed] {
            if length <= 1_048_576 {
                let admitted = result.unwrap();
                let admitted_bytes_match = admitted.bytes() == bytes;
                assert!(admitted_bytes_match, "admission must preserve source bytes");
            } else {
                assert_eq!(
                    result.unwrap_err().kind(),
                    tinystoragedrivers_types::ErrorKind::InvalidInput
                );
            }
        }
        let source_bytes_retained = std::fs::read(&path).unwrap() == bytes;
        assert!(source_bytes_retained, "admission must retain source bytes");
        let parent = path.parent().unwrap();
        assert!(!parent.join(KEY_FILE_NAME).exists());
        assert!(!parent.join(super::super::SECRETS_FILE_NAME).exists());
        assert_eq!(std::fs::read_dir(parent).unwrap().count(), 1);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn an_exact_completed_import_retry_reopens_without_requiring_removed_source() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("operation-one", "fixture", b"imported", &source)
        .await
        .unwrap();
    store.cleanup_file_import(&receipt).await.unwrap();
    assert!(!path.exists());
    let reopened = EncryptedFileSecrets::at_path(store.path(), crate::crypto::tests::fixture_key());
    let retry = reopened
        .prepare_file_import("operation-one", "fixture", b"imported", &source)
        .await
        .unwrap();
    assert_eq!(
        reopened.cleanup_file_import(&retry).await.unwrap(),
        CleanupAcknowledgment::AlreadyCompleted
    );
}

#[cfg(unix)]
#[tokio::test]
async fn an_unacknowledged_removal_retries_the_namespace_barrier_without_another_source() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("removal-fault", "fixture", b"imported", &source)
        .await
        .unwrap();
    let failed = cleanup_with_barrier(
        &receipt.inner,
        &receipt.source,
        &receipt.intent,
        receipt.destination,
        receipt.namespace,
        &mut |_| Err(std::io::Error::from_raw_os_error(5)),
    )
    .unwrap();
    assert_eq!(failed, CleanupAcknowledgment::RemovedUnacknowledged);
    assert!(!path.exists());
    assert_eq!(
        store
            .recover_file_import_source("removal-fault", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
    assert_eq!(
        store.cleanup_file_import(&receipt).await.unwrap(),
        CleanupAcknowledgment::RemovedAndDirectorySynced
    );
    assert_eq!(
        store.cleanup_file_import(&receipt).await.unwrap(),
        CleanupAcknowledgment::AlreadyCompleted
    );
}

#[tokio::test]
async fn an_import_source_cannot_alias_destination_or_import_bookkeeping() {
    let (_root, path, store, source) = setup();
    store.set("seed", b"preserved").await.unwrap();
    let destination = path.parent().unwrap().join(super::super::SECRETS_FILE_NAME);
    let before = std::fs::read(&destination).unwrap();
    let aliased = FileImportSource::open(&destination).unwrap();
    assert!(
        store
            .prepare_file_import("alias", "fixture", b"imported", &aliased)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&destination).unwrap(), before);
    store
        .prepare_file_import("normal", "fixture", b"imported", &source)
        .await
        .unwrap();
    let bookkeeping = FileImportSource::open(ledger_path(&store.inner)).unwrap();
    let before = std::fs::read(&bookkeeping.0.path).unwrap();
    assert!(
        store
            .prepare_file_import("ledger-alias", "second", b"imported", &bookkeeping)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&bookkeeping.0.path).unwrap(), before);
    let lock = store.inner.lock().unwrap();
    drop(
        lock.namespace
            .create_file(Path::new(".secret_key.lock"))
            .unwrap(),
    );
    drop(lock);
    let key_lock = FileImportSource::open(path.parent().unwrap().join(".secret_key.lock")).unwrap();
    assert!(
        store
            .prepare_file_import("key-lock-alias", "key-lock-value", b"imported", &key_lock)
            .await
            .is_err()
    );
    assert!(store.get("key-lock-value").await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_import_requests_and_mutated_source_never_publish() {
    let (_root, path, store, source) = setup();
    assert!(
        store
            .prepare_file_import("", "fixture", b"value", &source)
            .await
            .is_err()
    );
    assert!(
        store
            .prepare_file_import("operation", "", b"value", &source)
            .await
            .is_err()
    );
    assert!(
        store
            .prepare_file_import("operation", "fixture", &[255], &source)
            .await
            .is_err()
    );
    assert!(
        store
            .recover_file_import_source("", "fixture")
            .await
            .is_err()
    );
    assert!(
        store
            .recover_file_import_source("operation", "")
            .await
            .is_err()
    );
    assert!(
        store
            .recover_file_import_source("missing", "fixture")
            .await
            .is_err()
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    file.write_all(b"changed fixture").unwrap();
    file.sync_all().unwrap();
    assert!(
        store
            .prepare_file_import("operation", "fixture", b"value", &source)
            .await
            .is_err()
    );
    assert!(store.get("fixture").await.unwrap().is_none());
    assert_eq!(std::fs::read(&path).unwrap(), b"changed fixture");
}

#[tokio::test]
async fn encrypted_ledger_and_intent_corruption_fail_without_source_cleanup() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("operation", "fixture", b"value", &source)
        .await
        .unwrap();
    let lock = store.inner.lock().unwrap();
    let ledger = ledger_path(&store.inner);
    write_atomic_in(&lock.namespace, &ledger, b"invalid ciphertext").unwrap();
    drop(lock);
    assert!(
        store
            .recover_file_import_source("operation", "fixture")
            .await
            .is_err()
    );
    let lock = store.inner.lock().unwrap();
    let blob = crypto::encrypt(&store.inner.key, b"[]").unwrap();
    write_atomic_in(&lock.namespace, &ledger, &blob).unwrap();
    drop(lock);
    assert!(
        store
            .recover_file_import_source("operation", "fixture")
            .await
            .is_err()
    );
    let lock = store.inner.lock().unwrap();
    let mut map = SecretMap::default();
    map.0
        .insert("fixture".to_string(), "invalid intent".to_string());
    write_ledger(&store.inner, &lock.namespace, &map).unwrap();
    drop(lock);
    assert!(
        store
            .recover_file_import_source("operation", "fixture")
            .await
            .is_err()
    );
    #[cfg(unix)]
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    #[cfg(windows)]
    assert_eq!(
        store.cleanup_file_import(&receipt).await.unwrap(),
        CleanupAcknowledgment::UnsupportedBoundary
    );
    assert!(path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn conflicting_quarantines_preserve_every_unverified_object() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("quarantine-conflict", "fixture", b"value", &source)
        .await
        .unwrap();
    let quarantine = quarantine_name(&receipt.intent);
    let mut file = source.0.namespace.create_file(&quarantine).unwrap();
    file.write_all(b"another object").unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
    assert_eq!(
        std::fs::read(path.parent().unwrap().join(&quarantine)).unwrap(),
        b"another object"
    );
    std::fs::rename(&path, path.with_file_name("original-held")).unwrap();
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"another object");
    assert!(!path.parent().unwrap().join(&quarantine).exists());
    assert_eq!(
        std::fs::read(path.with_file_name("original-held")).unwrap(),
        source.bytes()
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn native_io_fault_preserves_source_and_reconciles_publication() {
    // Publication faults target the driver, not fixture creation. This source
    // is preserved and read back; this test makes no source durability claim.
    let (_root, path, store, source) = setup_with_sync(false);
    let first = store
        .prepare_file_import("native-fault", "fixture", b"value", &source)
        .await;
    if std::env::var_os("TSD_EXPECT_NATIVE_IO_FAULT").is_some() {
        let error = first.unwrap_err();
        let native = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(native.raw_os_error(), Some(5));
        println!("OBSERVED_NATIVE_IO_ERROR=5");
    } else {
        first.unwrap();
    }
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
    let receipt = store
        .prepare_file_import("native-fault", "fixture", b"value", &source)
        .await
        .unwrap();
    assert_eq!(receipt.boundary(), PublicationBoundary::UnixDirectorySync);
    assert_eq!(
        store.get("fixture").await.unwrap().unwrap().as_slice(),
        b"value"
    );
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
}

#[tokio::test]
async fn an_in_place_destination_mutation_fails_verified_readback() {
    let (_root, path, store, source) = setup();
    let _receipt = store
        .prepare_file_import("in-place", "fixture", b"value", &source)
        .await
        .unwrap();
    let lock = store.inner.lock().unwrap();
    let intent = get_intent(
        &read_ledger(&store.inner, &lock.namespace).unwrap(),
        "fixture",
    )
    .unwrap()
    .unwrap();
    let mut map = SecretMap::default();
    map.0
        .insert("fixture".to_string(), "changed value".to_string());
    let bytes = Zeroizing::new(serde_json::to_vec(&map.0).unwrap());
    let blob = crypto::encrypt(&store.inner.key, &bytes).unwrap();
    let mut file = lock
        .namespace
        .open_file(Path::new(super::super::SECRETS_FILE_NAME), true)
        .unwrap();
    file.set_len(0).unwrap();
    file.write_all(&blob).unwrap();
    file.sync_all().unwrap();
    assert!(
        verify_destination(
            &store.inner,
            &lock.namespace,
            &intent,
            Some(secure::identity(&file).unwrap())
        )
        .is_err()
    );
    drop(lock);
    assert!(
        store
            .prepare_file_import("in-place", "fixture", b"value", &source)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
}

#[tokio::test]
async fn an_import_from_another_private_namespace_retains_its_source() {
    let (_root, path, _store, source) = setup();
    let target = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let store = EncryptedFileSecrets::new(target.path(), crate::crypto::tests::fixture_key());
    store
        .prepare_file_import("other-namespace", "fixture", b"value", &source)
        .await
        .unwrap();
    assert_eq!(
        store.get("fixture").await.unwrap().unwrap().as_slice(),
        b"value"
    );
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
}

#[cfg(unix)]
#[tokio::test]
async fn a_changed_encrypted_intent_does_not_authorize_cleanup() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("intent", "fixture", b"value", &source)
        .await
        .unwrap();
    let lock = store.inner.lock().unwrap();
    let mut ledger = read_ledger(&store.inner, &lock.namespace).unwrap();
    let mut changed = get_intent(&ledger, "fixture").unwrap().unwrap();
    changed.operation = "other operation".to_string();
    record_intent(&mut ledger, &changed).unwrap();
    write_ledger(&store.inner, &lock.namespace, &ledger).unwrap();
    drop(lock);
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
}

#[tokio::test]
async fn a_hard_linked_import_ledger_is_rejected_before_publication() {
    let (_root, path, store, source) = setup();
    store
        .prepare_file_import("ledger-link", "fixture", b"value", &source)
        .await
        .unwrap();
    let ledger = ledger_path(&store.inner);
    std::fs::hard_link(&ledger, ledger.with_file_name("foreign-ledger-link")).unwrap();
    assert!(
        store
            .recover_file_import_source("ledger-link", "fixture")
            .await
            .is_err()
    );
    assert!(
        store
            .prepare_file_import("ledger-link", "fixture", b"value", &source)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
}

#[cfg(unix)]
#[tokio::test]
async fn a_replacement_after_unacknowledged_removal_is_never_reconciled_as_deleted() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("removal-replacement", "fixture", b"value", &source)
        .await
        .unwrap();
    assert_eq!(
        cleanup_with_barrier(
            &receipt.inner,
            &receipt.source,
            &receipt.intent,
            receipt.destination,
            receipt.namespace,
            &mut |_| Err(std::io::Error::from_raw_os_error(5))
        )
        .unwrap(),
        CleanupAcknowledgment::RemovedUnacknowledged
    );
    let mut replacement = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    replacement.write_all(b"replacement object").unwrap();
    replacement.sync_all().unwrap();
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement object");
    assert_eq!(
        store
            .recover_file_import_source("removal-replacement", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_failed_final_cleanup_ledger_write_remains_unacknowledged_and_retryable() {
    use std::os::unix::fs::PermissionsExt;
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("ledger-fault", "fixture", b"value", &source)
        .await
        .unwrap();
    let ledger = ledger_path(&store.inner);
    let failed = cleanup_with_barrier(
        &receipt.inner,
        &receipt.source,
        &receipt.intent,
        receipt.destination,
        receipt.namespace,
        &mut |namespace| {
            std::fs::set_permissions(&ledger, std::fs::Permissions::from_mode(0o644))?;
            namespace.sync()
        },
    )
    .unwrap();
    assert_eq!(failed, CleanupAcknowledgment::RemovedUnacknowledged);
    assert!(!path.exists());
    assert!(
        store
            .recover_file_import_source("ledger-fault", "fixture")
            .await
            .is_err()
    );
    std::fs::set_permissions(&ledger, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        store
            .recover_file_import_source("ledger-fault", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
    assert_eq!(
        store.cleanup_file_import(&receipt).await.unwrap(),
        CleanupAcknowledgment::RemovedAndDirectorySynced
    );
}

#[tokio::test]
async fn subprocess_restart_recovers_authenticated_backup_without_deleting_a_fresh_source() {
    const OPERATION: &str = "restart-recovery";
    if let Some(directory) = std::env::var_os("TSD_RECOVERY_FIXTURE_DIR") {
        let directory = PathBuf::from(directory);
        let store = EncryptedFileSecrets::new(&directory, crate::crypto::tests::fixture_key());
        let backup = store
            .recover_file_import_source(OPERATION, "fixture")
            .await
            .unwrap();
        assert_eq!(backup.as_slice(), b"disposable legacy fixture");
        let source_path = directory.join("legacy");
        let fresh = FileImportSource::open(&source_path).unwrap();
        assert!(
            store
                .prepare_file_import(OPERATION, "fixture", b"value", &fresh)
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read(&source_path).unwrap(),
            b"fresh replacement fixture"
        );
        assert_eq!(
            store.get("fixture").await.unwrap().unwrap().as_slice(),
            b"value"
        );
        println!("AUTHENTICATED_BACKUP_RECOVERED_FRESH_SOURCE_RETAINED");
        return;
    }
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import(OPERATION, "fixture", b"value", &source)
        .await
        .unwrap();
    #[cfg(unix)]
    {
        assert_eq!(
            cleanup_with_barrier(
                &receipt.inner,
                &receipt.source,
                &receipt.intent,
                receipt.destination,
                receipt.namespace,
                &mut |_| Err(std::io::Error::from_raw_os_error(5))
            )
            .unwrap(),
            CleanupAcknowledgment::RemovedUnacknowledged
        );
        assert!(!path.exists());
    }
    #[cfg(windows)]
    {
        assert_eq!(
            store.cleanup_file_import(&receipt).await.unwrap(),
            CleanupAcknowledgment::UnsupportedBoundary
        );
        std::fs::rename(&path, path.with_file_name("retained-original")).unwrap();
    }
    let mut replacement = source.0.namespace.create_file(Path::new("legacy")).unwrap();
    replacement.write_all(b"fresh replacement fixture").unwrap();
    replacement.sync_all().unwrap();
    drop(replacement);
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["file::migration::tests::subprocess_restart_recovers_authenticated_backup_without_deleting_a_fresh_source", "--exact", "--nocapture"])
        .env("TSD_RECOVERY_FIXTURE_DIR", path.parent().unwrap()).output().unwrap();
    assert!(
        child.status.success(),
        "restart recovery fixture failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        String::from_utf8_lossy(&child.stdout)
            .contains("AUTHENTICATED_BACKUP_RECOVERED_FRESH_SOURCE_RETAINED")
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"fresh replacement fixture");
}

#[tokio::test]
async fn a_fresh_source_handle_cannot_recreate_live_cleanup_authority() {
    let (_root, path, store, source) = setup();
    store
        .prepare_file_import("live-authority", "fixture", b"value", &source)
        .await
        .unwrap();
    let fresh = FileImportSource::open(&path).unwrap();
    assert!(
        store
            .prepare_file_import("live-authority", "fixture", b"value", &fresh)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(path).unwrap(), source.bytes());
    assert_eq!(
        store
            .recover_file_import_source("live-authority", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
}

#[tokio::test]
async fn subprocess_restart_cannot_recreate_authority_from_unchanged_or_same_byte_source() {
    const NAME: &str = "file::migration::tests::subprocess_restart_cannot_recreate_authority_from_unchanged_or_same_byte_source";
    if let Some(directory) = std::env::var_os("TSD_LIVE_AUTHORITY_DIR") {
        let directory = PathBuf::from(directory);
        let store = EncryptedFileSecrets::new(&directory, crate::crypto::tests::fixture_key());
        assert_eq!(
            store
                .recover_file_import_source("restart-authority", "fixture")
                .await
                .unwrap()
                .as_slice(),
            b"disposable legacy fixture"
        );
        let path = directory.join("legacy");
        let fresh = FileImportSource::open(&path).unwrap();
        assert!(
            store
                .prepare_file_import("restart-authority", "fixture", b"value", &fresh)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(path).unwrap(), b"disposable legacy fixture");
        println!("BACKUP_RECOVERED_WITHOUT_LIVE_AUTHORITY");
        return;
    }
    let (_root, path, store, source) = setup();
    store
        .prepare_file_import("restart-authority", "fixture", b"value", &source)
        .await
        .unwrap();
    let mut results = Vec::new();
    for replace in [false, true] {
        if replace {
            std::fs::rename(&path, path.with_file_name("retained-original")).unwrap();
            let mut fresh = source.0.namespace.create_file(Path::new("legacy")).unwrap();
            fresh.write_all(source.bytes()).unwrap();
            fresh.sync_all().unwrap();
        }
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([NAME, "--exact", "--nocapture"])
            .env("TSD_LIVE_AUTHORITY_DIR", path.parent().unwrap())
            .output()
            .unwrap();
        results.push((replace, child));
        assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
    }
    for (replace, child) in &results {
        println!(
            "RESTART_AUTHORITY_CHILD replacement={replace} success={}",
            child.status.success()
        );
    }
    for (replace, child) in results {
        assert!(
            child.status.success(),
            "fresh subprocess recreated authority (replacement={replace}): {}",
            String::from_utf8_lossy(&child.stdout)
        );
        assert!(
            String::from_utf8_lossy(&child.stdout)
                .contains("BACKUP_RECOVERED_WITHOUT_LIVE_AUTHORITY")
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn absent_cleanup_names_do_not_acknowledge_a_still_linked_captured_object() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("moved-capture", "fixture", b"value", &source)
        .await
        .unwrap();
    let quarantine = quarantine_name(&receipt.intent);
    // Reproduce the persisted pre-unlink state, without claiming unlink occurred.
    capture_source(&receipt.source, Path::new("legacy"), &quarantine).unwrap();
    let lock = store.inner.lock().unwrap();
    let mut ledger = read_ledger(&store.inner, &lock.namespace).unwrap();
    let mut pending = get_intent(&ledger, "fixture").unwrap().unwrap();
    pending.removing = true;
    record_intent(&mut ledger, &pending).unwrap();
    write_ledger(&store.inner, &lock.namespace, &ledger).unwrap();
    drop(lock);
    let moved = path.with_file_name("still-linked-capture");
    std::fs::rename(path.parent().unwrap().join(quarantine), &moved).unwrap();
    assert!(!path.exists());
    assert!(store.cleanup_file_import(&receipt).await.is_err());
    assert_eq!(std::fs::read(moved).unwrap(), source.bytes());
    assert_eq!(
        store
            .recover_file_import_source("moved-capture", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
}

#[cfg(any(target_os = "macos", windows))]
#[tokio::test]
async fn native_case_aliases_of_all_reserved_objects_never_publish_or_delete() {
    #[cfg(target_os = "macos")]
    let (_root, path, store, source) = setup();
    #[cfg(windows)]
    let (_root, path, store, source) = {
        // Ordinary local spelling isolates alias admission from VerbatimDisk.
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("private");
        clack_private_fs::create_dir(&directory).unwrap();
        let path = directory.join("legacy");
        let mut file = clack_private_fs::create_new_file(&path).unwrap();
        file.write_all(b"disposable legacy fixture").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let source = FileImportSource::open(&path).unwrap();
        let store = EncryptedFileSecrets::new(&directory, crate::crypto::tests::fixture_key());
        (root, path, store, source)
    };
    let directory = path.parent().unwrap();
    let key_path = directory.join(KEY_FILE_NAME);
    super::super::load_or_create_key_file(&key_path).unwrap();
    store
        .prepare_file_import("seed-aliases", "seed", b"seed", &source)
        .await
        .unwrap();
    let mut outcomes = Vec::new();
    for name in [
        "secrets.enc",
        ".secret_key",
        "secrets.enc.imports",
        "secrets.enc.lock",
        ".secret_key.lock",
    ] {
        let original = directory.join(name);
        let alias = directory.join(name.to_uppercase());
        let original_file = std::fs::File::open(&original).unwrap();
        let alias_file = std::fs::File::open(&alias)
            .expect("native fixture requires a case-insensitive filesystem");
        assert_eq!(
            secure::identity(&original_file).unwrap(),
            secure::identity(&alias_file).unwrap()
        );
        let before = std::fs::read(store.path()).unwrap();
        let ledger_before = std::fs::read(ledger_path(&store.inner)).unwrap();
        let key_before = std::fs::read(&key_path).unwrap();
        let aliased = FileImportSource::open(&alias).unwrap();
        let denied = store
            .prepare_file_import(
                &format!("reserved-{name}"),
                &format!("alias-{name}"),
                b"value",
                &aliased,
            )
            .await
            .is_err();
        let destination_unchanged = std::fs::read(store.path()).unwrap() == before;
        let ledger_unchanged = std::fs::read(ledger_path(&store.inner)).unwrap() == ledger_before;
        let key_unchanged = std::fs::read(&key_path).unwrap() == key_before;
        let retained = alias.exists();
        println!(
            "NATIVE_RESERVED_ALIAS name={name} denied={denied} destination_unchanged={destination_unchanged} ledger_unchanged={ledger_unchanged} key_unchanged={key_unchanged} retained={retained}"
        );
        outcomes.push((
            name,
            denied,
            destination_unchanged,
            ledger_unchanged,
            key_unchanged,
            retained,
        ));
    }
    for (name, denied, destination_unchanged, ledger_unchanged, key_unchanged, retained) in outcomes
    {
        assert!(
            denied && destination_unchanged && ledger_unchanged && key_unchanged && retained,
            "reserved native object did not fail closed: {name}"
        );
    }
}

#[cfg(unix)]
thread_local! {
    static AFTER_REMOVING_LEDGER: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(unix)]
struct LedgerPause;

#[cfg(unix)]
impl LedgerPause {
    fn install(callback: impl FnOnce() + 'static) -> Self {
        AFTER_REMOVING_LEDGER.with(|slot| {
            assert!(slot.borrow_mut().replace(Box::new(callback)).is_none());
        });
        Self
    }
}

#[cfg(unix)]
impl Drop for LedgerPause {
    fn drop(&mut self) {
        AFTER_REMOVING_LEDGER.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(unix)]
pub(super) fn after_removing_ledger() {
    let callback = AFTER_REMOVING_LEDGER.with(|slot| slot.borrow_mut().take());
    if let Some(callback) = callback {
        callback();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn writable_source_mutation_at_the_ledger_boundary_retains_changed_material() {
    let (_root, path, store, source) = setup();
    // Producer's writable descriptor predates capture and remains open through
    // the callback; a namespace lock cannot serialize this writer.
    let mut writer = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let receipt = store
        .prepare_file_import("ledger-writer", "fixture", b"value", &source)
        .await
        .unwrap();
    let quarantine = path.with_file_name(quarantine_name(&receipt.intent));
    let _pause = LedgerPause::install(move || {
        writer.set_len(0).unwrap();
        writer.write_all(b"producer changed after capture").unwrap();
        writer.sync_all().unwrap();
    });
    let result = cleanup_with_barrier(
        &receipt.inner,
        &receipt.source,
        &receipt.intent,
        receipt.destination,
        receipt.namespace,
        &mut secure::Namespace::sync,
    );
    assert!(
        result.is_err(),
        "changed source received removal acknowledgment: {result:?}"
    );
    assert_eq!(
        std::fs::read(quarantine).unwrap(),
        b"producer changed after capture"
    );
    assert_eq!(
        store
            .recover_file_import_source("ledger-writer", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn quarantine_replacement_at_the_ledger_boundary_retains_both_objects() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("ledger-replacement", "fixture", b"value", &source)
        .await
        .unwrap();
    let quarantine = path.with_file_name(quarantine_name(&receipt.intent));
    let retained = path.with_file_name("retained-captured-original");
    let q = quarantine.clone();
    let original = retained.clone();
    let _pause = LedgerPause::install(move || {
        std::fs::rename(&q, &original).unwrap();
        let mut substitute = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&q)
            .unwrap();
        substitute.write_all(b"substituted after capture").unwrap();
        substitute.sync_all().unwrap();
    });
    let result = cleanup_with_barrier(
        &receipt.inner,
        &receipt.source,
        &receipt.intent,
        receipt.destination,
        receipt.namespace,
        &mut secure::Namespace::sync,
    );
    assert!(
        result.is_err(),
        "replacement received removal acknowledgment: {result:?}"
    );
    assert_eq!(
        std::fs::read(quarantine).unwrap(),
        b"substituted after capture"
    );
    assert_eq!(std::fs::read(retained).unwrap(), source.bytes());
    assert_eq!(
        store
            .recover_file_import_source("ledger-replacement", "fixture")
            .await
            .unwrap()
            .as_slice(),
        source.bytes()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_refuses_a_source_held_by_a_cooperating_writer() {
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("coordinated-source", "fixture", b"value", &source)
        .await
        .unwrap();
    let producer = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    fs4::FileExt::lock(&producer).unwrap();
    let denied = store.cleanup_file_import(&receipt).await;
    fs4::FileExt::unlock(&producer).unwrap();
    assert!(denied.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), source.bytes());
    assert_eq!(
        store.cleanup_file_import(&receipt).await.unwrap(),
        CleanupAcknowledgment::RemovedAndDirectorySynced
    );
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_quarantine_is_keyed_by_the_import_name_not_its_operation() {
    // Intents are stored and looked up by name, so two imports may share an
    // operation. An object parked at the operation-named quarantine (another
    // import's stalled cleanup) must not block this import's cleanup.
    let (_root, path, store, source) = setup();
    let receipt = store
        .prepare_file_import("shared-operation", "fixture", b"value", &source)
        .await
        .unwrap();
    let operation_named = PathBuf::from(format!(
        ".import-cleanup-{}",
        digest(receipt.intent.operation.as_bytes())
    ));
    let mut parked = source.0.namespace.create_file(&operation_named).unwrap();
    parked.write_all(b"another import's capture").unwrap();
    drop(parked);

    assert_eq!(
        store.cleanup_file_import(&receipt).await.unwrap(),
        CleanupAcknowledgment::RemovedAndDirectorySynced
    );
    assert!(!path.exists());
    assert_eq!(
        std::fs::read(path.parent().unwrap().join(&operation_named)).unwrap(),
        b"another import's capture"
    );
}

#[test]
fn quarantine_names_differ_per_import_name_and_source_object() {
    let base = Intent {
        operation: "operation".to_string(),
        name: "fixture".to_string(),
        live_instance: String::new(),
        source_identity: [1, 2],
        namespace_identity: [3, 4],
        source_hash: String::new(),
        value_hash: String::new(),
        source_backup: String::new(),
        cleaned: false,
        removing: false,
    };
    let mut other_name = base.clone();
    other_name.name = "other".to_string();
    let mut other_source = base.clone();
    other_source.source_identity = [1, 9];
    assert_ne!(quarantine_name(&base), quarantine_name(&other_name));
    assert_ne!(quarantine_name(&base), quarantine_name(&other_source));
    assert_eq!(quarantine_name(&base), quarantine_name(&base.clone()));
}
