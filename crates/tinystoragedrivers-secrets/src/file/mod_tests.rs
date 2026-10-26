//! The encrypted file driver: conformance, OpenHuman file compatibility,
//! key files, and every failure path.

use super::*;

use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::sync::atomic::AtomicU64;

use tinystoragedrivers_types::ErrorKind;

use crate::crypto::hex_decode;
use crate::crypto::tests::{OPENHUMAN_SECRETS_ENC_HEX, fixture_key};
use crate::testkit::{secrets_conformance, versioned_secrets_conformance};

/// A fresh directory under the target dir's temp area, removed on drop.
pub(super) struct TempDir(pub(super) PathBuf);

impl TempDir {
    pub(super) fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("tsd-secrets-{label}-{}-{seq}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        }
        #[cfg(windows)]
        clack_private_fs::create_dir(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// Compatibility fixtures themselves must meet admission; the bytes and format
// assertions are unchanged. Unsafe-permission fixtures use direct native APIs.
pub(super) fn fixture_write(
    path: impl AsRef<Path>,
    contents: impl AsRef<[u8]>,
) -> std::io::Result<()> {
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?
    };
    #[cfg(windows)]
    let mut file = clack_private_fs::create_new_file(path.as_ref())?;
    file.write_all(contents.as_ref())
}

/// Make `dir` read-only and report whether that is enforced. It is not for
/// root, where the write failures under test cannot be produced.
#[cfg(unix)]
fn make_readonly(dir: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o500)).unwrap();
    let probe = dir.join("probe");
    if fixture_write(&probe, b"").is_ok() {
        fs::remove_file(probe).unwrap();
        make_writable(dir);
        return false;
    }
    true
}

#[cfg(unix)]
fn make_writable(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
}

#[tokio::test]
async fn passes_the_secrets_conformance_suite() {
    let dir = TempDir::new("conformance");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key().unwrap());
    secrets_conformance(&store).await;
    assert_eq!(store.backend_name(), "encrypted_file");
    assert_eq!(store.path(), dir.0.join("secrets.enc"));
}

#[tokio::test]
async fn passes_versioned_secret_conformance() {
    let dir = TempDir::new("versioned-conformance");
    versioned_secrets_conformance(&EncryptedFileSecrets::new(&dir.0, fixture_key())).await;
}

#[tokio::test]
async fn conditional_writes_arbitrate_independent_file_handles() {
    let dir = TempDir::new("conditional-write");
    let first = EncryptedFileSecrets::new(&dir.0, fixture_key());
    let second = EncryptedFileSecrets::new(&dir.0, fixture_key());
    let initial = first
        .set_if("credential", b"initial", SecretPrecondition::Absent)
        .await
        .unwrap();
    let read = second.get_versioned("credential").await.unwrap().unwrap();
    assert_eq!(read.0.as_slice(), b"initial");
    assert_eq!(read.1, initial);
    let updated = second
        .set_if(
            "credential",
            b"updated",
            SecretPrecondition::Version(initial),
        )
        .await
        .unwrap();
    assert_ne!(updated, initial);
    assert_eq!(
        first
            .set_if("credential", b"stale", SecretPrecondition::Version(initial),)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    let current = first.get_versioned("credential").await.unwrap().unwrap();
    assert_eq!(current.0.as_slice(), b"updated");
    assert_eq!(current.1, updated);
    let forged = SecretVersion::from_bytes([0; 32]);
    assert_eq!(
        first
            .set_if("credential", b"forged", SecretPrecondition::Version(forged))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
}

#[tokio::test]
async fn opens_a_secrets_enc_file_openhuman_wrote() {
    let dir = TempDir::new("openhuman");
    fixture_write(
        dir.0.join("secrets.enc"),
        hex_decode(OPENHUMAN_SECRETS_ENC_HEX).unwrap(),
    )
    .unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key());

    assert_eq!(
        store
            .get("alice:api_key")
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"sk-live-abc"
    );
    assert_eq!(
        store.get("alice:oauth").await.unwrap().unwrap().as_slice(),
        "tok-\u{e9}".as_bytes()
    );
    assert_eq!(
        store.list("alice:").await.unwrap(),
        ["alice:api_key", "alice:oauth"]
    );

    // A write keeps the existing entries and stays in OpenHuman's format:
    // the raw blob decrypts to a JSON object of strings, as its `read_map`
    // expects.
    store.set("bob:token", b"t0k").await.unwrap();
    let blob = fs::read(dir.0.join("secrets.enc")).unwrap();
    let json = crypto::decrypt(&fixture_key(), &blob).unwrap();
    let map: HashMap<String, String> = serde_json::from_slice(&json).unwrap();
    assert_eq!(map.len(), 3);
    assert_eq!(map["alice:api_key"], "sk-live-abc");
    assert_eq!(map["bob:token"], "t0k");
}

#[cfg(unix)]
#[tokio::test]
async fn the_written_file_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new("perms");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key().unwrap());
    store.set("a", b"1").await.unwrap();
    let mode = fs::metadata(store.path()).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    assert!(dir.0.join("secrets.enc.lock").exists());
}

#[tokio::test]
async fn a_missing_or_empty_file_is_an_empty_store() {
    let dir = TempDir::new("empty");
    let store = EncryptedFileSecrets::new(dir.0.join("nested"), crypto::generate_key().unwrap());
    assert_eq!(store.list("").await.unwrap(), Vec::<String>::new());
    fs::create_dir_all(dir.0.join("nested")).unwrap();
    fixture_write(store.path(), b"").unwrap();
    assert!(store.get("a").await.unwrap().is_none());
}

#[tokio::test]
async fn corrupt_files_fail_closed_by_default() {
    let dir = TempDir::new("corrupt-default");
    let path = dir.0.join("secrets.enc");
    fixture_write(&path, b"not ciphertext").unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key());

    assert_eq!(store.get("a").await.unwrap_err().kind(), ErrorKind::Crypto);
    assert_eq!(fs::read(&path).unwrap(), b"not ciphertext");
    assert!(fs::read_dir(&dir.0).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".corrupt.")
    }));
}

#[cfg(windows)]
#[test]
fn windows_no_replace_move_preserves_the_existing_destination() {
    let dir = TempDir::new("windows-no-replace");
    let source = dir.0.join("source");
    let target = dir.0.join("archive");
    fixture_write(&source, b"source bytes").unwrap();
    fixture_write(&target, b"existing archive").unwrap();
    let namespace = open_parent(&source, false).unwrap();

    let error = namespace
        .move_noreplace(Path::new("source"), Path::new("archive"))
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read(source).unwrap(), b"source bytes");
    assert_eq!(fs::read(target).unwrap(), b"existing archive");
}

#[tokio::test]
async fn opt_in_corrupt_file_recovery_quarantines_and_continues() {
    let dir = TempDir::new("corrupt-recovery");
    let path = dir.0.join("secrets.enc");
    fixture_write(&path, b"not ciphertext").unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);

    assert_eq!(store.get("a").await.unwrap(), None);
    assert!(!path.exists());
    let archived = fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".corrupt.")
        })
        .expect("corrupt bytes are preserved in quarantine");
    assert_eq!(fs::read(archived).unwrap(), b"not ciphertext");

    store.set("a", b"value").await.unwrap();
    assert_eq!(store.get("a").await.unwrap().unwrap().as_slice(), b"value");
}

#[tokio::test]
async fn opt_in_recovery_also_quarantines_decrypted_non_json() {
    let dir = TempDir::new("corrupt-json");
    let path = dir.0.join("secrets.enc");
    let key = fixture_key();
    fixture_write(&path, crypto::encrypt(&key, b"not json").unwrap()).unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, key)
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);

    assert_eq!(store.list("").await.unwrap(), Vec::<String>::new());
    assert!(!path.exists());
    let archived = fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".corrupt.")
        })
        .expect("decrypted invalid JSON is preserved");
    let archive_bytes = fs::read(archived).unwrap();
    assert_eq!(
        crypto::decrypt(&fixture_key(), &archive_bytes)
            .unwrap()
            .as_slice(),
        b"not json"
    );
}

#[tokio::test]
async fn opt_in_recovery_does_not_quarantine_non_corruption_io_errors() {
    let dir = TempDir::new("corrupt-policy-permission");
    let path = dir.0.join("secrets.enc");
    fs::create_dir(&path).unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);

    assert_eq!(store.get("a").await.unwrap_err().kind(), ErrorKind::Backend);
    assert!(path.is_dir());
    assert!(fs::read_dir(&dir.0).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".corrupt.")
    }));
}

#[tokio::test]
async fn quarantine_move_failure_preserves_corrupt_source_and_fails_closed() {
    let dir = TempDir::new("quarantine-move-failure");
    let path = dir.0.join(SECRETS_FILE_NAME);
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    store.set("healthy", b"credential").await.unwrap();
    fs::write(&path, b"corrupt bytes").unwrap();

    let _failure = quarantine_move_failure::Guard::install();
    let lock = store.inner.lock().unwrap();
    assert!(store.inner.read_map_in(&lock.namespace).is_err());
    drop(lock);

    assert_eq!(fs::read(path).unwrap(), b"corrupt bytes");
    assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 2);
}

#[test]
fn a_quarantine_name_collision_preserves_both_files() {
    let dir = TempDir::new("quarantine-collision");
    let path = dir.0.join("secrets.enc");
    fixture_write(&path, b"corrupt bytes").unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    let sentinel = b"pre-existing archive";
    let sentinel_bytes = sentinel.to_vec();
    let archive_dir = dir.0.clone();
    let _pause = quarantine_pause::Guard::install(move |candidate| {
        fixture_write(archive_dir.join(candidate), &sentinel_bytes).unwrap();
    });

    let lock = store.inner.lock().unwrap();
    assert!(
        store
            .inner
            .read_map_in(&lock.namespace)
            .unwrap()
            .0
            .is_empty()
    );
    assert!(!path.exists());
    let mut archives = fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".corrupt.")
        })
        .map(|path| fs::read(path).unwrap())
        .collect::<Vec<_>>();
    archives.sort();
    assert_eq!(archives, [b"corrupt bytes".to_vec(), sentinel.to_vec()]);
}

#[tokio::test]
async fn quarantine_rechecks_archive_identity_and_keeps_a_later_source() {
    let dir = TempDir::new("quarantine-archive-identity");
    let path = dir.0.join(SECRETS_FILE_NAME);
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    store.set("healthy", b"credential").await.unwrap();
    let healthy = fs::read(&path).unwrap();
    fs::write(&path, b"corrupt bytes").unwrap();

    let directory = dir.0.clone();
    let healthy_replacement = healthy.clone();
    let _pause = quarantine_after_move_pause::Guard::install(move |source_name, archive_name| {
        let archive = directory.join(archive_name);
        fs::rename(&archive, directory.join("displaced-corrupt")).unwrap();
        fixture_write(directory.join(source_name), healthy_replacement).unwrap();
        fixture_write(archive, b"archive identity changed").unwrap();
    });

    let lock = store.inner.lock().unwrap();
    assert!(store.inner.read_map_in(&lock.namespace).is_err());
    drop(lock);
    assert_eq!(fs::read(path).unwrap(), healthy);
    assert_eq!(
        fs::read(dir.0.join("displaced-corrupt")).unwrap(),
        b"corrupt bytes"
    );
}

#[test]
fn quarantine_restores_the_archive_when_the_source_name_stays_absent() {
    let dir = TempDir::new("quarantine-restore-archive");
    let path = dir.0.join(SECRETS_FILE_NAME);
    fixture_write(&path, b"corrupt bytes").unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);

    let directory = dir.0.clone();
    let _pause = quarantine_after_move_pause::Guard::install(move |_, archive_name| {
        let archive = directory.join(archive_name);
        fs::rename(&archive, directory.join("displaced-corrupt")).unwrap();
        fixture_write(archive, b"changed archive").unwrap();
    });

    let lock = store.inner.lock().unwrap();
    assert!(store.inner.read_map_in(&lock.namespace).is_err());
    drop(lock);
    assert_eq!(fs::read(path).unwrap(), b"changed archive");
    assert_eq!(
        fs::read(dir.0.join("displaced-corrupt")).unwrap(),
        b"corrupt bytes"
    );
}

#[test]
fn unsafe_quarantine_archive_candidate_fails_without_moving_source() {
    let dir = TempDir::new("quarantine-archive-error");
    let path = dir.0.join(SECRETS_FILE_NAME);
    fixture_write(&path, b"corrupt bytes").unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    let directory = dir.0.clone();
    let _pause = quarantine_fill_pause::Guard::install(move |candidate| {
        fs::create_dir(directory.join(candidate)).unwrap();
    });

    let lock = store.inner.lock().unwrap();
    assert!(store.inner.read_map_in(&lock.namespace).is_err());
    drop(lock);
    assert_eq!(fs::read(path).unwrap(), b"corrupt bytes");
}

#[test]
fn a_full_quarantine_namespace_fails_without_moving_the_corrupt_source() {
    let dir = TempDir::new("quarantine-exhaustion");
    let path = dir.0.join("secrets.enc");
    fixture_write(&path, b"corrupt bytes").unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    let archive_dir = dir.0.clone();
    let _fill = quarantine_fill_pause::Guard::install(move |candidate| {
        fixture_write(archive_dir.join(candidate), b"existing archive").unwrap();
    });

    let lock = store.inner.lock().unwrap();
    let Err(error) = store.inner.read_map_in(&lock.namespace) else {
        panic!("quarantine must stop when all candidate archives are occupied");
    };
    assert_eq!(
        error.message(),
        "could not find an unused secrets quarantine name"
    );
    assert_eq!(fs::read(path).unwrap(), b"corrupt bytes");
    assert_eq!(
        fs::read_dir(&dir.0).unwrap().count(),
        130,
        "all 128 occupied archive candidates, the original and its lock remain"
    );
}

#[tokio::test]
async fn a_source_replaced_before_quarantine_remains_active() {
    let dir = TempDir::new("quarantine-precheck-replacement");
    let path = dir.0.join("secrets.enc");
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    store.set("healthy", b"credential").await.unwrap();
    let healthy = fs::read(&path).unwrap();
    write_atomic(&path, b"corrupt bytes").unwrap();

    let replacement = path.clone();
    let healthy_bytes = healthy.clone();
    let retained = path.with_file_name("replaced-corrupt");
    let _pause = quarantine_pause::Guard::install(move |_| {
        fs::rename(&replacement, retained).unwrap();
        fixture_write(&replacement, healthy_bytes).unwrap();
    });
    let lock = store.inner.lock().unwrap();
    assert!(store.inner.read_map_in(&lock.namespace).is_err());
    drop(lock);

    assert_eq!(fs::read(&path).unwrap(), healthy);
    assert_eq!(
        store.get("healthy").await.unwrap().unwrap().as_slice(),
        b"credential"
    );
}

#[tokio::test]
async fn a_replaced_healthy_file_remains_available_after_quarantine_race() {
    let dir = TempDir::new("quarantine-source-replacement");
    let path = dir.0.join("secrets.enc");
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    store.set("healthy", b"credential").await.unwrap();
    let healthy = fs::read(&path).unwrap();
    write_atomic(&path, b"corrupt bytes").unwrap();

    let replacement_bytes = healthy.clone();
    let source_path = path.clone();
    let _pause = quarantine_source_pause::Guard::install(move || {
        let displaced = source_path.with_file_name("displaced-corrupt");
        fs::rename(&source_path, displaced).unwrap();
        fixture_write(&source_path, replacement_bytes).unwrap();
    });

    let lock = store.inner.lock().unwrap();
    let result = store.inner.read_map_in(&lock.namespace);
    assert!(
        result.is_err(),
        "the first operation reports the source race"
    );
    drop(lock);
    assert_eq!(
        store.get("healthy").await.unwrap().unwrap().as_slice(),
        b"credential",
        "a later read must not mistake the moved healthy source for an empty store"
    );
    assert_eq!(fs::read(&path).unwrap(), healthy);
}

#[cfg(unix)]
#[tokio::test]
async fn a_file_that_becomes_readable_again_is_not_quarantined() {
    let dir = TempDir::new("quarantine-readable-again");
    let path = dir.0.join(SECRETS_FILE_NAME);
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    store.set("healthy", b"credential").await.unwrap();
    let healthy = fs::read(&path).unwrap();
    fs::write(&path, b"corrupt bytes").unwrap();

    let replacement_path = path.clone();
    let healthy_replacement = healthy.clone();
    let _pause = quarantine_source_pause::Guard::install(move || {
        fs::write(replacement_path, healthy_replacement).unwrap();
    });
    let lock = store.inner.lock().unwrap();
    let recovered = store.inner.read_map_in(&lock.namespace).unwrap();
    drop(lock);

    assert_eq!(
        recovered.0.get("healthy").map(String::as_str),
        Some("credential")
    );
    assert_eq!(fs::read(path).unwrap(), healthy);
    assert_eq!(
        fs::read_dir(&dir.0)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".corrupt."))
            .count(),
        0
    );
}

#[tokio::test]
async fn a_readable_replacement_is_left_untouched_when_quarantine_is_reconsidered() {
    let dir = TempDir::new("quarantine-later-writer");
    let path = dir.0.join("secrets.enc");
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);
    store.set("healthy", b"first writer").await.unwrap();
    let first_writer = fs::read(&path).unwrap();
    write_atomic(&path, b"corrupt bytes").unwrap();

    let first_path = path.clone();
    let first_bytes = first_writer.clone();
    let _source_pause = quarantine_source_pause::Guard::install(move || {
        let displaced = first_path.with_file_name("displaced-corrupt");
        fs::rename(&first_path, displaced).unwrap();
        fixture_write(&first_path, first_bytes).unwrap();
    });

    let lock = store.inner.lock().unwrap();
    assert!(store.inner.read_map_in(&lock.namespace).is_err());
    drop(lock);
    assert_eq!(
        fs::read(&path).unwrap(),
        first_writer,
        "a source that became readable again must not be quarantined"
    );
}

#[tokio::test]
async fn binary_values_are_rejected() {
    let dir = TempDir::new("binary");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key().unwrap());
    let error = store.set("bin", &[0xff, 0x00]).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!store.path().exists());
}

#[tokio::test]
async fn a_wrong_key_fails_closed_and_leaves_the_file_alone() {
    let dir = TempDir::new("wrong-key");
    let writer = EncryptedFileSecrets::new(&dir.0, crypto::generate_key().unwrap());
    writer.set("keep", b"me").await.unwrap();
    let before = fs::read(writer.path()).unwrap();

    let reader = EncryptedFileSecrets::new(&dir.0, crypto::generate_key().unwrap());
    assert_eq!(
        reader.get("keep").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(
        reader.set("other", b"x").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(
        reader.delete("keep").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(fs::read(writer.path()).unwrap(), before);
}

#[tokio::test]
async fn decrypted_garbage_is_a_serialization_error_without_plaintext() {
    let dir = TempDir::new("garbage");
    let key = crypto::generate_key().unwrap();
    let path = dir.0.join("secrets.enc");
    fixture_write(&path, crypto::encrypt(&key, b"[\"sk-leak\"]").unwrap()).unwrap();
    let store = EncryptedFileSecrets::at_path(&path, key);
    let error = store.list("").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(!error.to_string().contains("sk-leak"));
    assert!(std::error::Error::source(&error).is_none());
}

#[tokio::test]
async fn an_unreadable_path_is_a_backend_error() {
    let dir = TempDir::new("unreadable");
    // The "file" is a directory: reading it fails with something other than
    // NotFound.
    let store = EncryptedFileSecrets::at_path(&dir.0, crypto::generate_key().unwrap());
    assert_eq!(store.get("a").await.unwrap_err().kind(), ErrorKind::Backend);
}

#[tokio::test]
async fn a_parent_that_is_a_file_fails_the_lock() {
    let dir = TempDir::new("parent-file");
    let blocker = dir.0.join("blocker");
    fixture_write(&blocker, b"").unwrap();
    let store = EncryptedFileSecrets::new(&blocker, crypto::generate_key().unwrap());
    assert_eq!(
        store.set("a", b"1").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[cfg(unix)]
#[tokio::test]
async fn an_unwritable_directory_fails_the_lock_open() {
    let dir = TempDir::new("readonly");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key().unwrap());
    if !make_readonly(&dir.0) {
        return;
    }
    let result = store.set("a", b"1").await;
    make_writable(&dir.0);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
}

#[cfg(unix)]
#[test]
fn staging_fails_cleanly_in_an_unwritable_directory() {
    let dir = TempDir::new("stage");
    if !make_readonly(&dir.0) {
        return;
    }
    let result = write_atomic(&dir.0.join("secrets.enc"), b"x");
    make_writable(&dir.0);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
}

#[test]
fn a_failed_rename_removes_the_staged_file() {
    let dir = TempDir::new("rename");
    let target = dir.0.join("occupied");
    fs::create_dir_all(target.join("child")).unwrap();
    let error = write_atomic(&target, b"x").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    let leftovers: Vec<_> = fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().ends_with(".tmp"))
        .collect();
    assert_eq!(leftovers, Vec::<std::ffi::OsString>::new(), "{leftovers:?}");
}

#[test]
fn staging_skips_leftover_temp_files() {
    let dir = TempDir::new("leftover");
    let path = dir.0.join("secrets.enc");
    // Leftovers from a crashed writer with this pid at sequence 0 and 1.
    for seq in 0..2 {
        fixture_write(
            dir.0
                .join(format!("secrets.enc.{}.{seq}.tmp", std::process::id())),
            b"stale",
        )
        .unwrap();
    }
    // A private sequence, so no concurrent test can move it.
    let mut next = 0..;
    let tmp = stage(&path, b"fresh", &mut || next.next().unwrap()).unwrap();
    assert!(tmp.to_string_lossy().ends_with(".2.tmp"), "{tmp:?}");
    assert_eq!(fs::read(&tmp).unwrap(), b"fresh");
}

#[test]
fn write_atomic_replaces_an_existing_file() {
    let dir = TempDir::new("replace");
    let path = dir.0.join("secrets.enc");
    write_atomic(&path, b"old").unwrap();
    write_atomic(&path, b"new").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"new");
}

#[cfg(unix)]
#[test]
fn sync_parent_dir_admits_and_syncs_the_file_parent() {
    let dir = TempDir::new("sync-parent");
    let path = dir.0.join("published");
    fixture_write(&path, b"payload").unwrap();
    sync_parent_dir(&path).unwrap();
}

#[test]
fn a_key_published_concurrently_wins_and_the_temp_file_goes() {
    let dir = TempDir::new("key-race");
    let path = dir.0.join(".secret_key");
    fixture_write(&path, crate::crypto::tests::FIXTURE_KEY_HEX).unwrap();
    let loser = crypto::generate_key().unwrap();
    let tmp = stage(&path, key_to_hex(&loser).as_bytes(), &mut next_temp_seq).unwrap();
    let adopted = publish_key(&tmp, &path, loser).unwrap();
    assert_eq!(*adopted, *fixture_key());
    assert!(!tmp.exists());
}

#[test]
fn a_key_that_cannot_be_published_is_a_backend_error() {
    let dir = TempDir::new("key-publish");
    let tmp = stage(&dir.0.join("staged"), b"00", &mut next_temp_seq).unwrap();
    // The destination's directory does not exist.
    let error = publish_key(
        &tmp,
        &dir.0.join("missing/.secret_key"),
        crypto::generate_key().unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(!tmp.exists());
}

#[test]
fn a_bare_file_name_has_no_parent_to_create() {
    if std::env::var_os("TSD_PRIVATE_CWD_CHILD").is_some() {
        create_parent(Path::new("secrets.enc")).unwrap();
        assert_eq!(fs::read_dir(".").unwrap().count(), 0);
        let store = EncryptedFileSecrets::with_key_file(".").unwrap();
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                store.set("relative", b"value").await.unwrap();
                assert_eq!(
                    store.get("relative").await.unwrap().unwrap().as_slice(),
                    b"value"
                );
            });
        return;
    }
    let directory = TempDir::new("private-cwd");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "file::tests::a_bare_file_name_has_no_parent_to_create",
            "--exact",
            "--nocapture",
        ])
        .env("TSD_PRIVATE_CWD_CHILD", "1")
        .current_dir(&directory.0)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "private CWD child failed: {}",
        String::from_utf8_lossy(&child.stdout)
    );
}

#[tokio::test]
async fn owned_key_file_constructor_reuses_the_master_and_refuses_its_loss() {
    let dir = TempDir::new("keyfile");
    let first = EncryptedFileSecrets::with_key_file(dir.0.clone()).unwrap();
    first.set("a", b"1").await.unwrap();

    let key_hex = fs::read_to_string(dir.0.join(".secret_key")).unwrap();
    assert_eq!(key_hex.len(), 64);
    assert!(key_hex.bytes().all(|b| b.is_ascii_hexdigit()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(dir.0.join(".secret_key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    let second = EncryptedFileSecrets::with_key_file(dir.0.clone()).unwrap();
    assert_eq!(second.get("a").await.unwrap().unwrap().as_slice(), b"1");
    let master_retained = fs::read_to_string(dir.0.join(KEY_FILE_NAME)).unwrap() == key_hex;
    assert!(master_retained, "reopening must retain the master bytes");
    let ciphertext = fs::read(dir.0.join(SECRETS_FILE_NAME)).unwrap();
    drop(first);
    drop(second);
    fs::remove_file(dir.0.join(KEY_FILE_NAME)).unwrap();
    let Err(error) = EncryptedFileSecrets::with_key_file(dir.0.clone()) else {
        panic!("lost master must deny reopening existing ciphertext");
    };
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let ciphertext_retained = fs::read(dir.0.join(SECRETS_FILE_NAME)).unwrap() == ciphertext;
    assert!(
        ciphertext_retained,
        "failed reopening must retain ciphertext"
    );
    assert!(!dir.0.join(KEY_FILE_NAME).exists());
}

#[test]
fn read_only_key_file_constructor_never_creates_key_or_lock_files() {
    let dir = TempDir::new("readonly-keyfile");
    let error = EncryptedFileSecrets::with_key_file_read_only(&dir.0).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(!dir.0.join(KEY_FILE_NAME).exists());
    assert!(!dir.0.join(format!("{KEY_FILE_NAME}.lock")).exists());

    let _writer = EncryptedFileSecrets::with_key_file(&dir.0).unwrap();
    let lock_existed_before = dir.0.join(format!("{KEY_FILE_NAME}.lock")).exists();
    let read_only = EncryptedFileSecrets::with_key_file_read_only(&dir.0).unwrap();
    assert!(dir.0.join(KEY_FILE_NAME).is_file());
    assert_eq!(
        dir.0.join(format!("{KEY_FILE_NAME}.lock")).exists(),
        lock_existed_before
    );
    drop(read_only);
}

#[tokio::test]
async fn key_file_constructor_preserves_secrets_for_owned_and_borrowed_paths() {
    // Every accepted path form must create and reopen the same protected store.
    // In particular, borrowed Path and str callers must exercise creation as
    // well as rejection; native LLVM reports each generic instantiation.
    macro_rules! round_trip {
        ($label:literal, $directory:ident, $path:expr) => {{
            let $directory = TempDir::new($label);
            let directory = &$directory;
            let path = $path;
            let first = EncryptedFileSecrets::with_key_file(path).unwrap();
            first.set("retained", b"disposable fixture").await.unwrap();
            let key = fs::read(directory.0.join(KEY_FILE_NAME)).unwrap();
            let reopened = EncryptedFileSecrets::with_key_file($path).unwrap();
            assert_eq!(
                reopened.get("retained").await.unwrap().unwrap().as_slice(),
                b"disposable fixture"
            );
            assert_eq!(fs::read(directory.0.join(KEY_FILE_NAME)).unwrap(), key);
            assert_eq!(reopened.list("").await.unwrap(), ["retained"]);
            assert!(reopened.delete("retained").await.unwrap());
            assert!(first.get("retained").await.unwrap().is_none());
        }};
    }
    round_trip!("owned-path", dir, dir.0.clone());
    round_trip!("borrowed-pathbuf", dir, &dir.0);
    round_trip!("borrowed-path", dir, dir.0.as_path());
    round_trip!("borrowed-str", dir, dir.0.to_str().unwrap());
}

#[test]
fn an_existing_openhuman_key_file_with_a_newline_loads() {
    let dir = TempDir::new("oh-key");
    let path = dir.0.join(".secret_key");
    fixture_write(
        &path,
        format!("{}\n", crate::crypto::tests::FIXTURE_KEY_HEX),
    )
    .unwrap();
    assert_eq!(*load_or_create_key_file(&path).unwrap(), *fixture_key());
}

#[test]
fn a_corrupt_key_file_is_a_crypto_error() {
    let dir = TempDir::new("bad-key");
    let path = dir.0.join(".secret_key");
    fixture_write(&path, "not hex").unwrap();
    assert_eq!(
        load_or_create_key_file(&path).unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[test]
fn an_unreadable_key_file_is_a_backend_error() {
    let dir = TempDir::new("key-dir");
    // A directory where the key file should be.
    assert_eq!(
        load_or_create_key_file(&dir.0).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[cfg(unix)]
#[test]
fn an_uncreatable_key_file_is_a_backend_error() {
    let dir = TempDir::new("key-readonly");
    if !make_readonly(&dir.0) {
        return;
    }
    let result = load_or_create_key_file(&dir.0.join(".secret_key"));
    make_writable(&dir.0);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
}

#[test]
fn debug_shows_the_path_but_not_the_key() {
    let store = EncryptedFileSecrets::at_path("/x/secrets.enc", fixture_key());
    let debug = format!("{store:?}");
    assert!(debug.contains("/x/secrets.enc"));
    assert!(!debug.contains("0001020304"));
}

#[cfg(unix)]
#[path = "unix_birth_tests.rs"]
mod unix_birth;

#[cfg(windows)]
#[path = "windows_birth_tests.rs"]
mod windows_birth;

#[test]
fn guarded_names_reject_root_parent_and_multiple_components() {
    for name in ["", ".", "..", "one/two", "/"] {
        assert!(secure::leaf(Path::new(name)).is_err());
    }
    assert!(secure::file_name(Path::new("/")).is_err());
}

#[test]
fn locked_debug_redacts_admitted_paths() {
    let dir = TempDir::new("lock-debug");
    let path = dir.0.join("private-secrets.lock");
    let lock = lock_file(&path).unwrap();
    let debug = format!("{lock:?}");

    assert_eq!(debug, "Locked { .. }");
    assert!(!debug.contains(path.to_str().unwrap()));
}

#[cfg(unix)]
#[test]
fn a_replaced_lock_object_is_rejected_while_the_original_handle_is_held() {
    let dir = TempDir::new("lock-replacement");
    let path = dir.0.join("fixture.lock");
    let lock = lock_file(&path).unwrap();
    fs::rename(&path, dir.0.join("old.lock")).unwrap();
    fixture_write(&path, b"").unwrap();
    assert!(lock.verify().is_err());
    assert!(dir.0.join("old.lock").exists());
}

#[tokio::test]
async fn an_empty_private_ciphertext_file_is_an_empty_store() {
    let dir = TempDir::new("empty-private-store");
    fixture_write(dir.0.join(SECRETS_FILE_NAME), b"").unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key());
    assert!(store.get("missing").await.unwrap().is_none());
    store.set("fixture", b"value").await.unwrap();
    assert_eq!(
        store.get("fixture").await.unwrap().unwrap().as_slice(),
        b"value"
    );
}

#[test]
fn staged_key_readback_must_match_the_requested_generated_key() {
    let dir = TempDir::new("key-readback-mismatch");
    let path = dir.0.join(KEY_FILE_NAME);
    let staged = stage(
        &path,
        key_to_hex(&fixture_key()).as_bytes(),
        &mut next_temp_seq,
    )
    .unwrap();
    let result = publish_key(&staged, &path, Zeroizing::new([42; KEY_LEN]));
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Conflict);
    assert!(!staged.exists());
    assert_eq!(*load_or_create_key_file(&path).unwrap(), *fixture_key());
}

#[test]
fn a_staged_key_cannot_publish_into_a_different_admitted_directory() {
    let left = TempDir::new("key-left");
    let right = TempDir::new("key-right");
    let path = left.0.join(KEY_FILE_NAME);
    let stage = stage(
        &path,
        key_to_hex(&fixture_key()).as_bytes(),
        &mut next_temp_seq,
    )
    .unwrap();
    let result = publish_key(&stage, &right.0.join(KEY_FILE_NAME), fixture_key());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Conflict);
    assert!(!right.0.join(KEY_FILE_NAME).exists());
    assert!(stage.exists());
}

#[cfg(unix)]
#[test]
fn a_namespace_substitution_before_staging_writes_no_payload() {
    use std::os::unix::fs::DirBuilderExt;
    let root = TempDir::new("stage-substitution");
    let private = root.0.join("private");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&private)
        .unwrap();
    let target = private.join(SECRETS_FILE_NAME);
    let mut substitute = || {
        fs::rename(&private, root.0.join("original")).unwrap();
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&private)
            .unwrap();
        0
    };
    assert!(stage(&target, b"disposable payload", &mut substitute).is_err());
    for entry in fs::read_dir(&private).unwrap() {
        assert_eq!(entry.unwrap().metadata().unwrap().len(), 0);
    }
    assert!(
        fs::read_dir(root.0.join("original"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_key_reader_waits_for_an_in_progress_no_clobber_publication() {
    let Some(path) = std::env::var_os("TSD_KEY_RACE_FIXTURE") else {
        let dir = TempDir::new("key-race-normal");
        assert!(load_or_create_key_file(&dir.0.join(KEY_FILE_NAME)).is_ok());
        return;
    };
    let key = load_or_create_key_file(&PathBuf::from(path)).unwrap();
    assert_eq!(key.len(), KEY_LEN);
    println!("KEY_CREATION_SUCCEEDED");
}

#[cfg(target_os = "linux")]
#[test]
fn native_key_publication_faults_preserve_an_uncertain_winner() {
    let dir = TempDir::new("native-key-fault");
    let path = dir.0.join(KEY_FILE_NAME);
    let first = load_or_create_key_file(&path);
    match std::env::var("TSD_EXPECT_NATIVE_KEY_FAULT").ok().as_deref() {
        Some(kind) => {
            let error = first.unwrap_err();
            let native = std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap();
            assert_eq!(native.raw_os_error(), Some(5));
            println!("OBSERVED_NATIVE_IO_ERROR=5");
            if kind == "unlink" {
                assert!(path.exists());
                let before = fs::read(&path).unwrap();
                let files = fs::read_dir(&dir.0).unwrap().count();
                assert!(load_or_create_key_file(&path).is_err());
                assert_eq!(fs::read(&path).unwrap(), before);
                assert_eq!(fs::read_dir(&dir.0).unwrap().count(), files);
            } else if kind == "link" {
                assert!(!path.exists());
                assert!(load_or_create_key_file(&path).is_ok());
            } else {
                let before = fs::read(&path).unwrap();
                assert!(load_or_create_key_file(&path).is_ok());
                assert_eq!(fs::read(&path).unwrap(), before);
            }
        }
        None => {
            assert!(first.is_ok());
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn native_generic_publication_faults_preserve_or_report_the_published_bytes() {
    use std::os::unix::fs::OpenOptionsExt;

    let dir = TempDir::new("native-generic-publication-fault");
    let path = dir.0.join(SECRETS_FILE_NAME);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .unwrap()
        .write_all(b"previous ciphertext")
        .unwrap();

    let result = write_atomic(&path, b"replacement ciphertext");
    let Some(fault) = std::env::var("TSD_EXPECT_NATIVE_IO_FAULT").ok() else {
        result.unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"replacement ciphertext");
        return;
    };

    let error = result.unwrap_err();
    let native = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .unwrap();
    assert_eq!(native.raw_os_error(), Some(5));
    println!("OBSERVED_NATIVE_IO_ERROR=5");

    match fault.as_str() {
        "stage-flush" | "rename" => {
            assert_eq!(fs::read(&path).unwrap(), b"previous ciphertext");
            assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 1);
        }
        "publication-barrier" | "published-file-flush" => {
            assert_eq!(fs::read(&path).unwrap(), b"replacement ciphertext");
            assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 1);
        }
        other => panic!("unknown native fault fixture: {other}"),
    }
}

#[test]
fn an_absent_secret_file_name_is_rejected_before_any_filesystem_access() {
    assert!(write_atomic(Path::new(""), b"disposable fixture").is_err());
    assert!(stage(Path::new(""), b"disposable fixture", &mut next_temp_seq).is_err());
}

#[tokio::test]
async fn published_objects_are_ready_for_a_separate_user_access_probe() {
    let Some(path) = std::env::var_os("TSD_USER_DENIAL_FIXTURE") else {
        let dir = TempDir::new("ordinary-user-normal");
        let store = EncryptedFileSecrets::with_key_file(&dir.0).unwrap();
        store
            .set("fixture:value", b"disposable-fixture")
            .await
            .unwrap();
        return;
    };
    let store = EncryptedFileSecrets::with_key_file(PathBuf::from(path)).unwrap();
    store
        .set("fixture:value", b"disposable-fixture")
        .await
        .unwrap();
    eprintln!("PRIVATE_PUBLIC_OBJECTS_CREATED");
}

#[test]
fn a_missing_master_with_existing_store_objects_never_mints_a_new_key() {
    let mut outcomes = Vec::new();
    for name in [SECRETS_FILE_NAME, "secrets.enc.lock", "secrets.enc.imports"] {
        let directory = TempDir::new("missing-master");
        let path = directory.0.join(name);
        fixture_write(&path, b"existing material").unwrap();
        let before = fs::read(&path).unwrap();
        let denied = EncryptedFileSecrets::with_key_file(directory.0.clone())
            .is_err_and(|error| error.kind() == ErrorKind::Conflict);
        let unchanged = fs::read(path).unwrap() == before;
        let no_key = !directory.0.join(KEY_FILE_NAME).exists();
        println!(
            "MISSING_MASTER_VARIANT name={name} denied={denied} unchanged={unchanged} no_key={no_key}"
        );
        outcomes.push((name, denied, unchanged, no_key));
    }
    for (name, denied, unchanged, no_key) in outcomes {
        assert!(
            denied && unchanged && no_key,
            "existing {name} did not fail closed before key creation"
        );
    }
}

#[cfg(unix)]
#[test]
fn unsafe_existing_data_lock_and_ledger_fail_before_master_creation() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let mut outcomes = Vec::new();
    for name in [SECRETS_FILE_NAME, "secrets.enc.lock", "secrets.enc.imports"] {
        for kind in ["broad", "symlink", "hardlink"] {
            let directory = TempDir::new("unsafe-before-master");
            let path = directory.0.join(name);
            let other = directory.0.join("preserved-object");
            match kind {
                "broad" => {
                    fixture_write(&path, b"existing material").unwrap();
                    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
                }
                "symlink" => {
                    fixture_write(&other, b"existing material").unwrap();
                    symlink(&other, &path).unwrap();
                }
                "hardlink" => {
                    fixture_write(&other, b"existing material").unwrap();
                    fs::hard_link(&other, &path).unwrap();
                }
                _ => unreachable!(),
            }
            let metadata = fs::symlink_metadata(&path).unwrap();
            let denied = EncryptedFileSecrets::with_key_file(&directory.0).is_err();
            let no_key = !directory.0.join(KEY_FILE_NAME).exists();
            let unchanged = fs::read(&path).unwrap() == b"existing material"
                && fs::symlink_metadata(&path).unwrap().permissions().mode()
                    == metadata.permissions().mode();
            println!(
                "UNSAFE_MASTER_VARIANT name={name} kind={kind} denied={denied} unchanged={unchanged} no_key={no_key}"
            );
            outcomes.push((name, kind, denied, unchanged, no_key));
        }
    }
    for (name, kind, denied, unchanged, no_key) in outcomes {
        assert!(
            denied && unchanged && no_key,
            "unsafe {name} ({kind}) did not fail closed before key creation"
        );
    }
}

#[cfg(unix)]
#[test]
fn an_unpublishable_key_name_removes_only_its_staged_object() {
    let directory = TempDir::new("key-name-too-long");
    let normal = directory.0.join(KEY_FILE_NAME);
    let staged = stage(
        &normal,
        key_to_hex(&fixture_key()).as_bytes(),
        &mut next_temp_seq,
    )
    .unwrap();
    let target = directory.0.join("k".repeat(300));
    let error = publish_key(&staged, &target, fixture_key()).unwrap_err();
    assert_eq!(
        error.message(),
        "could not publish the key file",
        "{error:?}"
    );
    let native = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .unwrap();
    assert_eq!(
        native.raw_os_error(),
        Some(rustix::io::Errno::NAMETOOLONG.raw_os_error()),
        "{error:?}"
    );
    assert!(!staged.exists());
    assert!(!normal.exists());
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
}

mod publication_pause {
    use std::cell::RefCell;
    type Callback = Box<dyn FnOnce()>;
    thread_local! {
        static PUBLICATION: RefCell<Option<Callback>> = const { RefCell::new(None) };
        static KEY: RefCell<Option<Callback>> = const { RefCell::new(None) };
    }
    pub(super) struct Guard(bool);
    impl Guard {
        pub(super) fn install(key: bool, callback: impl FnOnce() + 'static) -> Self {
            let slot = if key { &KEY } else { &PUBLICATION };
            slot.with(|slot| assert!(slot.borrow_mut().replace(Box::new(callback)).is_none()));
            Self(key)
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            let slot = if self.0 { &KEY } else { &PUBLICATION };
            slot.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    pub(super) fn run(key: bool) {
        let slot = if key { &KEY } else { &PUBLICATION };
        let callback = slot.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback();
        }
    }
}

mod quarantine_pause {
    use super::Path;
    use std::cell::RefCell;

    type Callback = Box<dyn FnOnce(&Path)>;

    thread_local! {
        static CALLBACK: RefCell<Option<Callback>> = const { RefCell::new(None) };
    }

    pub(super) struct Guard;

    impl Guard {
        pub(super) fn install(callback: impl FnOnce(&Path) + 'static) -> Self {
            CALLBACK.with(|slot| assert!(slot.borrow_mut().replace(Box::new(callback)).is_none()));
            Self
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }

    pub(super) fn after_absence_check(candidate: &Path) {
        let callback = CALLBACK.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback(candidate);
        }
    }
}

mod quarantine_fill_pause {
    use super::Path;
    use std::cell::RefCell;

    type Callback = Box<dyn FnMut(&Path)>;
    thread_local! {
        static CALLBACK: RefCell<Option<Callback>> = const { RefCell::new(None) };
    }

    pub(super) struct Guard;
    impl Guard {
        pub(super) fn install(callback: impl FnMut(&Path) + 'static) -> Self {
            CALLBACK.with(|slot| assert!(slot.borrow_mut().replace(Box::new(callback)).is_none()));
            Self
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    pub(super) fn run(candidate: &Path) {
        CALLBACK.with(|slot| {
            if let Some(callback) = slot.borrow_mut().as_mut() {
                callback(candidate);
            }
        });
    }
}

mod quarantine_source_pause {
    use std::cell::RefCell;
    type Callback = Box<dyn FnOnce()>;
    thread_local! {
        static CALLBACK: RefCell<Option<Callback>> = const { RefCell::new(None) };
    }
    pub(super) struct Guard;
    impl Guard {
        pub(super) fn install(callback: impl FnOnce() + 'static) -> Self {
            CALLBACK.with(|slot| assert!(slot.borrow_mut().replace(Box::new(callback)).is_none()));
            Self
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    pub(super) fn run() {
        let callback = CALLBACK.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback();
        }
    }
}

mod quarantine_move_failure {
    use std::cell::Cell;

    thread_local! {
        static FAIL_NEXT: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) struct Guard;

    impl Guard {
        pub(super) fn install() -> Self {
            FAIL_NEXT.with(|flag| flag.set(true));
            Self
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            FAIL_NEXT.with(|flag| flag.set(false));
        }
    }

    pub(super) fn take() -> bool {
        FAIL_NEXT.with(|flag| flag.replace(false))
    }
}

mod quarantine_after_move_pause {
    use super::Path;
    use std::cell::RefCell;

    type Callback = Box<dyn FnOnce(&Path, &Path)>;

    thread_local! {
        static CALLBACK: RefCell<Option<Callback>> = const { RefCell::new(None) };
    }

    pub(super) struct Guard;

    impl Guard {
        pub(super) fn install(callback: impl FnOnce(&Path, &Path) + 'static) -> Self {
            CALLBACK.with(|slot| assert!(slot.borrow_mut().replace(Box::new(callback)).is_none()));
            Self
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }

    pub(super) fn run(source: &Path, archive: &Path) {
        let callback = CALLBACK.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback(source, archive);
        }
    }
}

pub(super) fn after_quarantine_absence_check(candidate: &Path) {
    quarantine_pause::after_absence_check(candidate);
}

pub(super) fn before_quarantine_archive_check(candidate: &Path) {
    quarantine_fill_pause::run(candidate);
}

pub(super) fn after_quarantine_source_check() {
    quarantine_source_pause::run();
}

pub(super) fn fail_quarantine_move() -> bool {
    quarantine_move_failure::take()
}

pub(super) fn after_quarantine_move(source: &Path, archive: &Path) {
    quarantine_after_move_pause::run(source, archive);
}

pub(super) fn after_staged_publication_identity() {
    publication_pause::run(false);
}

pub(super) fn after_staged_key_identity() {
    publication_pause::run(true);
}

#[cfg(unix)]
fn substitute_private_directory(private: PathBuf, retained: PathBuf) -> impl FnOnce() {
    move || {
        use std::os::unix::fs::DirBuilderExt;
        fs::rename(&private, retained).unwrap();
        fs::DirBuilder::new().mode(0o700).create(private).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn namespace_substitution_after_staging_preserves_the_previous_and_staged_publications() {
    use std::os::unix::fs::DirBuilderExt;
    let root = TempDir::new("publication-substitution");
    let private = root.0.join("private");
    let retained = root.0.join("retained");
    fs::DirBuilder::new().mode(0o700).create(&private).unwrap();
    let target = private.join(SECRETS_FILE_NAME);
    write_atomic(&target, b"previous ciphertext").unwrap();
    let _pause = publication_pause::Guard::install(
        false,
        substitute_private_directory(private.clone(), retained.clone()),
    );
    let error = write_atomic(&target, b"staged ciphertext").unwrap_err();
    assert_eq!(
        error.message(),
        "could not publish the secrets file",
        "{error:?}"
    );
    assert_eq!(
        fs::read(retained.join(SECRETS_FILE_NAME)).unwrap(),
        b"previous ciphertext"
    );
    let staged: Vec<_> = fs::read_dir(&retained)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "tmp"))
        .collect();
    assert_eq!(staged.len(), 1);
    assert_eq!(fs::read(&staged[0]).unwrap(), b"staged ciphertext");
    assert_eq!(fs::read_dir(private).unwrap().count(), 0);
}

#[cfg(unix)]
#[test]
fn namespace_substitution_during_key_publication_retains_the_unpublished_staged_key() {
    use std::os::unix::fs::DirBuilderExt;
    let root = TempDir::new("key-publication-substitution");
    let private = root.0.join("private");
    let retained = root.0.join("retained");
    fs::DirBuilder::new().mode(0o700).create(&private).unwrap();
    let target = private.join(KEY_FILE_NAME);
    let bytes = key_to_hex(&fixture_key());
    let staged = stage(&target, bytes.as_bytes(), &mut next_temp_seq).unwrap();
    let staged_name = staged.file_name().unwrap().to_owned();
    let _pause = publication_pause::Guard::install(
        true,
        substitute_private_directory(private.clone(), retained.clone()),
    );
    let error = publish_key(&staged, &target, fixture_key()).unwrap_err();
    assert_eq!(
        error.message(),
        "could not remove the staged key",
        "{error:?}"
    );
    assert_eq!(
        fs::read(retained.join(staged_name)).unwrap(),
        bytes.as_bytes()
    );
    assert!(!retained.join(KEY_FILE_NAME).exists());
    assert!(!target.exists());
    assert_eq!(fs::read_dir(private).unwrap().count(), 0);
    assert_eq!(fs::read_dir(retained).unwrap().count(), 1);
}

#[test]
fn a_substituted_staged_ciphertext_is_never_acknowledged_as_the_verified_publication() {
    let root = TempDir::new("staged-ciphertext-replacement");
    let target = root.0.join(SECRETS_FILE_NAME);
    let preserved = root.0.join("preserved-staged-ciphertext");
    let directory = root.0.clone();
    let original = preserved.clone();
    let _pause = publication_pause::Guard::install(false, move || {
        let staged = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|extension| extension == "tmp"))
            .unwrap();
        fs::rename(&staged, original).unwrap();
        fixture_write(staged, b"substitute ciphertext").unwrap();
    });
    let error = write_atomic(&target, b"verified staged ciphertext").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
    assert_eq!(error.message(), "published secrets identity changed");
    assert_eq!(fs::read(preserved).unwrap(), b"verified staged ciphertext");
    assert_eq!(fs::read(target).unwrap(), b"substitute ciphertext");
}

#[test]
fn changed_staged_ciphertext_bytes_fail_published_readback_without_deleting_material() {
    let root = TempDir::new("staged-ciphertext-mutation");
    let target = root.0.join(SECRETS_FILE_NAME);
    let directory = root.0.clone();
    let _pause = publication_pause::Guard::install(false, move || {
        let staged = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|extension| extension == "tmp"))
            .unwrap();
        // Mutate the admitted object in place, preserving its private ACL and identity.
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(staged)
            .unwrap()
            .write_all(b"changed ciphertext")
            .unwrap();
    });
    let error = write_atomic(&target, b"expected ciphertext").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
    assert_eq!(error.message(), "published secrets readback differs");
    assert_eq!(fs::read(target).unwrap(), b"changed ciphertext");
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 1);
}

#[test]
fn a_substituted_staged_key_retains_both_objects_without_accepting_the_publication() {
    let root = TempDir::new("staged-key-replacement");
    let target = root.0.join(KEY_FILE_NAME);
    let expected = key_to_hex(&fixture_key());
    let substitute = key_to_hex(&[42; KEY_LEN]);
    let staged = stage(&target, expected.as_bytes(), &mut next_temp_seq).unwrap();
    let preserved = root.0.join("preserved-staged-key");
    let original = preserved.clone();
    let source = staged.clone();
    let changed = substitute.clone();
    let _pause = publication_pause::Guard::install(true, move || {
        fs::rename(&source, original).unwrap();
        fixture_write(source, changed.as_bytes()).unwrap();
    });
    let error = publish_key(&staged, &target, fixture_key()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
    assert_eq!(error.message(), "published key object changed");
    assert_eq!(fs::read(preserved).unwrap(), expected.as_bytes());
    assert_eq!(fs::read(target).unwrap(), substitute.as_bytes());
    assert!(!staged.exists());
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 2);
}

#[test]
fn borrowed_namespace_path_reopens_the_same_private_master_key() {
    let directory = TempDir::new("borrowed-master");
    let path: &Path = directory.0.as_path();
    let first = EncryptedFileSecrets::with_key_file(path).unwrap();
    let bytes = fs::read(path.join(KEY_FILE_NAME)).unwrap();
    let second = EncryptedFileSecrets::with_key_file(path).unwrap();
    assert_eq!(*first.inner.key, *second.inner.key);
    assert_eq!(fs::read(path.join(KEY_FILE_NAME)).unwrap(), bytes);
    assert_eq!(
        fs::read_dir(path)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                path.extension()
                    .is_some_and(|value| value == "tmp")
                    .then_some(path)
            })
            .count(),
        0
    );
}

mod staging_pause {
    use super::{File, Path};
    use std::cell::RefCell;
    type Callback = Box<dyn FnOnce(&Path, &File)>;
    thread_local! {
        static CALLBACK: RefCell<Option<Callback>> = const { RefCell::new(None) };
    }
    pub(super) struct Guard;
    impl Guard {
        pub(super) fn install(callback: impl FnOnce(&Path, &File) + 'static) -> Self {
            CALLBACK.with(|slot| assert!(slot.borrow_mut().replace(Box::new(callback)).is_none()));
            Self
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    pub(super) fn run(path: &Path, file: &File) {
        let callback = CALLBACK.with(|slot| slot.borrow_mut().take());
        if let Some(callback) = callback {
            callback(path, file);
        }
    }
}

pub(super) fn after_staged_reservation(path: &Path, file: &File) {
    staging_pause::run(path, file);
}

fn weaken_reserved_fixture(path: &Path, file: &File) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = file;
        fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    }
    #[cfg(windows)]
    {
        use windows_permissions::{
            LocalBox, SecurityDescriptor,
            constants::{SeObjectType, SecurityInformation},
            wrappers,
        };
        let descriptor = wrappers::GetSecurityInfo(
            file,
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner,
        )
        .unwrap();
        let owner = wrappers::ConvertSidToStringSid(descriptor.owner().unwrap())
            .unwrap()
            .into_string()
            .unwrap();
        // Grant Everyone access only to this empty, disposable fixture. The
        // production admission must reject it before writing secret material.
        let broad: LocalBox<SecurityDescriptor> =
            format!("D:P(A;;FA;;;{owner})(A;;FA;;;WD)").parse().unwrap();
        wrappers::SetNamedSecurityInfo(
            path.as_os_str(),
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
            None,
            None,
            broad.dacl(),
            None,
        )
        .unwrap();
    }
}

#[test]
fn private_admission_lost_after_reservation_never_writes_staged_secret_bytes() {
    let root = TempDir::new("staging-admission-loss");
    let target = root.0.join(SECRETS_FILE_NAME);
    let _pause = staging_pause::Guard::install(weaken_reserved_fixture);
    let error = stage(
        &target,
        b"must never reach the staged file",
        &mut next_temp_seq,
    )
    .unwrap_err();
    assert_eq!(error.message(), "could not stage the secrets file");
    assert!(!target.exists());
    // Cleanup is best effort if native I/O refuses it. Even a retained staged
    // object must remain empty because admission preceded every byte write.
    for entry in fs::read_dir(&root.0).unwrap() {
        assert_eq!(entry.unwrap().metadata().unwrap().len(), 0);
    }
}

#[test]
fn an_unreservable_staging_name_leaves_no_private_objects() {
    let root = TempDir::new("staging-name-limit");
    let target = root.0.join("x".repeat(300));
    let error = reserve_temp_file(&target, &mut || 0).unwrap_err();
    assert_eq!(error.message(), "could not reserve a private staging file");
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 0);
}

#[test]
fn substituted_staging_object_is_rejected_before_secret_bytes_are_written() {
    let root = TempDir::new("staging-object-substitution");
    let target = root.0.join(SECRETS_FILE_NAME);
    let preserved = root.0.join("original-empty-reservation");
    let retained = preserved.clone();
    let _pause = staging_pause::Guard::install(move |path, file| {
        assert_eq!(file.metadata().unwrap().len(), 0);
        // Keep the original handle alive while replacing its visible leaf with
        // another genuinely private object. Permission checks alone cannot
        // distinguish the replacement; the retained native identity must.
        fs::rename(path, &retained).unwrap();
        fixture_write(path, b"").unwrap();
    });
    let error = stage(&target, b"must not enter either object", &mut next_temp_seq).unwrap_err();
    assert_eq!(error.message(), "could not stage the secrets file");
    assert_eq!(fs::metadata(preserved).unwrap().len(), 0);
    assert!(!target.exists());
    for entry in fs::read_dir(&root.0).unwrap() {
        assert_eq!(entry.unwrap().metadata().unwrap().len(), 0);
    }
}

// Fixture extraction must never format native values or errors on failure.
fn native_fixture_ok<T, E>(result: std::result::Result<T, E>, phase: &'static str) -> T {
    match result {
        Ok(value) => value,
        Err(_) => std::panic::panic_any(phase),
    }
}

fn native_fixture_error<T, E>(result: std::result::Result<T, E>, phase: &'static str) -> E {
    match result {
        Err(error) => error,
        Ok(_) => std::panic::panic_any(phase),
    }
}

struct PublicDebugProbe<'a>(&'a std::cell::Cell<usize>);

impl std::fmt::Debug for PublicDebugProbe<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.set(self.0.get() + 1);
        formatter.write_str("public synthetic debug probe")
    }
}

#[test]
fn native_fixture_success_extraction_never_formats_unexpected_error() {
    let formatted = std::cell::Cell::new(0);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        native_fixture_ok::<(), _>(Err(PublicDebugProbe(&formatted)), "native success phase");
    }))
    .unwrap_err();
    assert_eq!(
        formatted.get(),
        0,
        "unexpected errors must not be formatted"
    );
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"native success phase"));
    assert_eq!(
        native_fixture_ok::<_, PublicDebugProbe<'_>>(Ok(7), "expected success phase"),
        7
    );
}

#[test]
fn native_fixture_error_extraction_never_formats_unexpected_success() {
    let formatted = std::cell::Cell::new(0);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        native_fixture_error::<_, ()>(Ok(PublicDebugProbe(&formatted)), "native rejection phase");
    }))
    .unwrap_err();
    assert_eq!(
        formatted.get(),
        0,
        "unexpected values must not be formatted"
    );
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"native rejection phase")
    );
    assert_eq!(
        native_fixture_error::<PublicDebugProbe<'_>, _>(Err(7), "expected rejection phase"),
        7
    );
}

#[cfg(windows)]
fn native_fixture_directory(label: &'static str) -> TempDir {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let seq = NEXT.fetch_add(1, Ordering::Relaxed);
    let directory = native_fixture_ok(
        std::env::temp_dir().canonicalize(),
        "resolve native fixture root",
    )
    .join(format!("tsd-secrets-{label}-{}-{seq}", std::process::id()));
    let _ = fs::remove_dir_all(&directory);
    native_fixture_ok(
        clack_private_fs::create_dir(&directory),
        "create private native fixture root",
    );
    TempDir(directory)
}

#[cfg(windows)]
fn windows_preserve_and_replace(source: &Path, preserved: &Path, bytes: &[u8]) -> File {
    let namespace = native_fixture_ok(open_parent(source, false), "replacement parent admission");
    let original = native_fixture_ok(
        namespace.open_file(
            native_fixture_ok(secure::file_name(source), "replacement file name"),
            false,
        ),
        "replacement object admission",
    );
    let original_id = native_fixture_ok(secure::identity(&original), "original native identity");
    native_fixture_ok(fs::rename(source, preserved), "preserve original object");
    native_fixture_ok(fixture_write(source, bytes), "create private replacement");
    let replacement = native_fixture_ok(
        namespace.open_file(
            native_fixture_ok(secure::file_name(source), "replacement file name"),
            false,
        ),
        "replacement object admission",
    );
    let distinct = native_fixture_ok(
        secure::identity(&replacement),
        "replacement native identity",
    ) != original_id;
    assert!(distinct, "replacement must have a distinct native identity");
    assert_eq!(
        native_fixture_ok(
            winapi_util::file::information(&original),
            "original native link count"
        )
        .number_of_links(),
        1
    );
    assert_eq!(
        native_fixture_ok(
            winapi_util::file::information(&replacement),
            "replacement native link count"
        )
        .number_of_links(),
        1
    );
    original
}
#[cfg(windows)]
fn sole_staged_path(directory: &Path) -> PathBuf {
    let paths: Vec<_> = native_fixture_ok(fs::read_dir(directory), "staging inventory")
        .map(|entry| native_fixture_ok(entry, "staging inventory entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "tmp"))
        .collect();
    assert_eq!(paths.len(), 1);
    paths.into_iter().next().unwrap()
}
#[cfg(windows)]
fn assert_private_bytes(path: &Path, expected: &[u8]) {
    let unchanged =
        native_fixture_ok(fs::read(path), "private fixture readback").as_slice() == expected;
    assert!(unchanged, "private fixture bytes differ");
}

#[cfg(windows)]
#[test]
fn replaced_private_staging_object_is_rejected_before_payload_and_only_substitute_is_cleaned() {
    let root = native_fixture_directory("stage-reservation-substitution");
    let target = root.0.join(SECRETS_FILE_NAME);
    let preserved = root.0.join("preserved-empty-stage");
    let original = preserved.clone();
    let observer = std::rc::Rc::new(std::cell::RefCell::new(None));
    let retained = observer.clone();
    let _pause = staging_pause::Guard::install(move |source, reserved| {
        assert_eq!(
            native_fixture_ok(reserved.metadata(), "reserved object metadata").len(),
            0
        );
        let file = windows_preserve_and_replace(source, &original, b"");
        let same_reserved = native_fixture_ok(secure::identity(&file), "retained native identity")
            == native_fixture_ok(secure::identity(reserved), "reserved native identity");
        assert!(
            same_reserved,
            "retained observer must identify the reserved object"
        );
        retained.borrow_mut().replace(file);
    });
    let error = native_fixture_error(
        stage(&target, b"must never be written", &mut next_temp_seq),
        "changed staging identity must be rejected",
    );
    let correct_message = error.message() == "could not stage the secrets file";
    assert!(correct_message, "fixture rejection message differs");
    let native = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .unwrap();
    assert_eq!(native.kind(), std::io::ErrorKind::PermissionDenied);
    let correct_reason = native.to_string() == "staged file identity changed";
    assert!(correct_reason, "staging must reject changed identity");
    assert_private_bytes(&preserved, b"");
    assert_eq!(
        native_fixture_ok(fs::read_dir(&root.0), "fixture final inventory").count(),
        1
    );
    assert!(!target.exists());
    let guard = observer.borrow();
    let file = guard.as_ref().unwrap();
    assert_eq!(
        native_fixture_ok(
            winapi_util::file::information(file),
            "preserved native link count"
        )
        .number_of_links(),
        1
    );
    let same_preserved = native_fixture_ok(secure::identity(file), "preserved native identity")
        == native_fixture_ok(
            secure::identity(&native_fixture_ok(
                File::open(&preserved),
                "open preserved observer",
            )),
            "preserved path native identity",
        );
    assert!(same_preserved, "preserved object identity changed");
}

#[cfg(windows)]
#[test]
fn windows_substituted_staged_ciphertext_is_not_acknowledged() {
    let root = native_fixture_directory("ciphertext-identity-substitution");
    let target = root.0.join(SECRETS_FILE_NAME);
    native_fixture_ok(
        write_atomic(&target, b"previous ciphertext"),
        "write initial ciphertext",
    );
    let preserved = root.0.join("preserved-staged-ciphertext");
    let directory = root.0.clone();
    let original = preserved.clone();
    let observer = std::rc::Rc::new(std::cell::RefCell::new(None));
    let retained = observer.clone();
    let _pause = publication_pause::Guard::install(false, move || {
        let source = sole_staged_path(&directory);
        retained.borrow_mut().replace(windows_preserve_and_replace(
            &source,
            &original,
            b"substitute ciphertext",
        ));
    });
    let error = native_fixture_error(
        write_atomic(&target, b"verified staged ciphertext"),
        "substituted ciphertext must be rejected",
    );
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let correct_message = error.message() == "published secrets identity changed";
    assert!(correct_message, "fixture rejection message differs");
    assert_private_bytes(&preserved, b"verified staged ciphertext");
    assert_private_bytes(&target, b"substitute ciphertext");
    assert_eq!(
        native_fixture_ok(fs::read_dir(&root.0), "fixture final inventory").count(),
        2
    );
    assert_eq!(
        native_fixture_ok(
            winapi_util::file::information(observer.borrow().as_ref().unwrap()),
            "retained observer link count"
        )
        .number_of_links(),
        1
    );
}

#[cfg(windows)]
#[test]
fn windows_same_identity_staged_byte_mutation_fails_published_readback() {
    let root = native_fixture_directory("ciphertext-readback-mutation");
    let target = root.0.join(SECRETS_FILE_NAME);
    let directory = root.0.clone();
    let observer = std::rc::Rc::new(std::cell::RefCell::new(None));
    let retained = observer.clone();
    let _pause = publication_pause::Guard::install(false, move || {
        let source = sole_staged_path(&directory);
        let namespace = native_fixture_ok(open_parent(&source, false), "mutation parent admission");
        let mut file = native_fixture_ok(
            namespace.open_file(
                native_fixture_ok(secure::file_name(&source), "mutation file name"),
                true,
            ),
            "admit writable staged object",
        );
        let before = native_fixture_ok(secure::identity(&file), "retained native identity");
        native_fixture_ok(file.set_len(0), "truncate admitted staged object");
        native_fixture_ok(
            file.write_all(b"changed ciphertext"),
            "mutate admitted staged bytes",
        );
        native_fixture_ok(file.sync_all(), "flush admitted staged bytes");
        let same_object =
            native_fixture_ok(secure::identity(&file), "retained native identity") == before;
        assert!(same_object, "mutation must preserve native identity");
        retained.borrow_mut().replace(file);
    });
    let error = native_fixture_error(
        write_atomic(&target, b"expected ciphertext"),
        "changed readback must be rejected",
    );
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let correct_message = error.message() == "published secrets readback differs";
    assert!(correct_message, "fixture rejection message differs");
    assert_private_bytes(&target, b"changed ciphertext");
    assert_eq!(
        native_fixture_ok(fs::read_dir(&root.0), "fixture final inventory").count(),
        1
    );
    let file = observer.borrow();
    let same_published = native_fixture_ok(
        secure::identity(file.as_ref().unwrap()),
        "published observer native identity",
    ) == native_fixture_ok(
        secure::identity(&native_fixture_ok(
            File::open(&target),
            "open published observer",
        )),
        "published path native identity",
    );
    assert!(
        same_published,
        "mutation observer must identify published object"
    );
    assert_eq!(
        native_fixture_ok(
            winapi_util::file::information(file.as_ref().unwrap()),
            "published observer link count"
        )
        .number_of_links(),
        1
    );
}

#[cfg(windows)]
#[test]
fn windows_substituted_staged_key_retains_both_objects_without_acceptance() {
    let root = native_fixture_directory("key-identity-substitution");
    let target = root.0.join(KEY_FILE_NAME);
    let expected = key_to_hex(&fixture_key());
    let substitute = key_to_hex(&[42; KEY_LEN]);
    let staged = native_fixture_ok(
        stage(&target, expected.as_bytes(), &mut next_temp_seq),
        "stage private key",
    );
    let preserved = root.0.join("preserved-staged-key");
    let original = preserved.clone();
    let source = staged.clone();
    let changed = substitute.clone();
    let observer = std::rc::Rc::new(std::cell::RefCell::new(None));
    let retained = observer.clone();
    let _pause = publication_pause::Guard::install(true, move || {
        retained.borrow_mut().replace(windows_preserve_and_replace(
            &source,
            &original,
            changed.as_bytes(),
        ));
    });
    let result = publish_key(&staged, &target, fixture_key());
    assert!(result.is_err(), "substituted key must not be accepted");
    let error = result.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let correct_message = error.message() == "published key object changed";
    assert!(correct_message, "fixture rejection message differs");
    assert_private_bytes(&preserved, expected.as_bytes());
    assert_private_bytes(&target, substitute.as_bytes());
    assert!(!staged.exists());
    assert_eq!(
        native_fixture_ok(fs::read_dir(&root.0), "fixture final inventory").count(),
        2
    );
    assert_eq!(
        native_fixture_ok(
            winapi_util::file::information(observer.borrow().as_ref().unwrap()),
            "retained observer link count"
        )
        .number_of_links(),
        1
    );
}

#[cfg(windows)]
#[test]
fn missing_staging_parent_fails_once_without_creating_sibling() {
    let root = native_fixture_directory("reservation-missing-parent");
    let missing = root.0.join("absent");
    let target = missing.join(SECRETS_FILE_NAME);
    let mut attempts = 0;
    let result = reserve_temp_file(&target, &mut || {
        attempts += 1;
        7
    });
    assert!(result.is_err(), "missing parent must fail reservation");
    let error = result.err().unwrap();
    assert_eq!(attempts, 1);
    let correct_message = error.message() == "could not reserve a private staging file";
    assert!(correct_message, "fixture rejection message differs");
    let native = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .unwrap();
    assert_eq!(native.kind(), std::io::ErrorKind::NotFound);
    assert!(!missing.exists());
    assert_eq!(
        native_fixture_ok(fs::read_dir(&root.0), "fixture final inventory").count(),
        0
    );
}
