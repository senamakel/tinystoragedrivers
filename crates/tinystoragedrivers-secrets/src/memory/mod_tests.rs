//! The memory driver against the conformance suite, plus binary values.

use super::*;
use crate::testkit::{secrets_conformance, versioned_secrets_conformance};

#[tokio::test]
async fn passes_the_secrets_conformance_suite() {
    let store = MemorySecrets::new();
    secrets_conformance(&store).await;
    versioned_secrets_conformance(&store).await;
    assert_eq!(store.backend_name(), "memory");
}

#[tokio::test]
async fn versions_are_bound_to_the_memory_store_instance() {
    let first = MemorySecrets::new();
    let second = MemorySecrets::new();
    first.set("shared", b"same").await.unwrap();
    second.set("shared", b"same").await.unwrap();
    let (_, first_version) = first.get_versioned("shared").await.unwrap().unwrap();
    let (_, second_version) = second.get_versioned("shared").await.unwrap().unwrap();
    assert_ne!(first_version, second_version);
    let error = second
        .set_if(
            "shared",
            b"wrong-store",
            crate::Precondition::Version(first_version),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Conflict);
}

#[tokio::test]
async fn holds_binary_values_and_redacts_debug() {
    let store = MemorySecrets::new();
    store.set("bin", &[0, 0xff, 0x80]).await.unwrap();
    assert_eq!(
        store.get("bin").await.unwrap().unwrap().as_slice(),
        [0, 0xff, 0x80]
    );
    assert_eq!(format!("{store:?}"), "MemorySecrets { len: 1 }");
}

#[test]
fn a_poisoned_lock_is_recovered() {
    let store = std::sync::Arc::new(MemorySecrets::new());
    let clone = std::sync::Arc::clone(&store);
    let _ = std::thread::spawn(move || {
        let _guard = clone.entries();
        panic!("poison the lock");
    })
    .join();
    assert!(store.entries().values.is_empty());
}
