//! Public integration regressions for fail-closed quarantine behavior.

#![cfg(feature = "file")]

use std::fs;

use tempfile::tempdir;
use tinystoragedrivers_secrets::{
    CorruptFilePolicy, EncryptedFileSecrets, SecretStore, crypto, write_atomic_private,
};

#[tokio::test]
async fn public_quarantine_recovers_corrupt_files_in_the_production_build() {
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let path = root.join("secrets.enc");
    write_atomic_private(&path, b"corrupt ciphertext").unwrap();
    let store = EncryptedFileSecrets::new(&root, crypto::generate_key().unwrap())
        .with_corrupt_file_policy(CorruptFilePolicy::QuarantineAndContinue);

    assert_eq!(store.get("missing").await.unwrap(), None);
    assert!(!path.exists());
    assert!(
        fs::read_dir(&root)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains(".corrupt."))
    );
}
