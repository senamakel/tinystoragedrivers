use std::fs;

use tempfile::tempdir;
use tinystoragedrivers_secrets::{CorruptFilePolicy, EncryptedFileSecrets, SecretStore, crypto};

#[tokio::test]
async fn public_quarantine_recovers_corrupt_files_in_the_production_build() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("secrets.enc");
    fs::write(&path, b"corrupt ciphertext").unwrap();
    let store = EncryptedFileSecrets::new(directory.path(), crypto::generate_key().unwrap())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);

    assert_eq!(store.get("missing").await.unwrap(), None);
    assert!(!path.exists());
    assert!(fs::read_dir(directory.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .any(|entry| entry.file_name().to_string_lossy().contains(".corrupt.")));
}
