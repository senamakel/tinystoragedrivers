//! Compare-and-set checks every native secret store runs.

use std::sync::Arc;

use tokio::sync::Barrier;

use super::{CatchUnwind, unique_prefix};
use crate::store::{Precondition, SecretStore};
use tinystoragedrivers_core::ErrorKind;

/// Check versioned reads and atomic conditional writes against `store`.
///
/// Use this for stores that implement conditional writes. Keyring backends do
/// not expose an atomic compare-and-set primitive and must not claim support.
///
/// # Panics
///
/// Panics with the failed contract check's name.
pub async fn versioned_secrets_conformance(store: &dyn SecretStore) {
    let name = format!("{}cas", unique_prefix());
    let checks = CatchUnwind(Box::pin(check_versioned(store, &name)));
    let outcome = checks.await;
    let _ = store.delete(&name).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn check_versioned(store: &dyn SecretStore, name: &str) {
    assert!(
        store
            .get_versioned(name)
            .await
            .expect("read missing secret with version")
            .is_none(),
        "an absent secret has neither a value nor a version"
    );

    let created_version = store
        .set_if(name, b"first", Precondition::Absent)
        .await
        .expect("create absent secret");
    let (value, version) = store
        .get_versioned(name)
        .await
        .expect("read created secret with version")
        .expect("created secret is present");
    assert_eq!(value.as_slice(), &b"first"[..]);
    assert_eq!(
        created_version, version,
        "set_if returns its own committed revision"
    );
    assert_eq!(format!("{version:?}"), "SecretVersion(..)");

    let duplicate_create = store
        .set_if(name, b"duplicate", Precondition::Absent)
        .await
        .expect_err("absent precondition must reject an existing secret");
    assert_eq!(duplicate_create.kind(), ErrorKind::Conflict);

    store.set(name, b"first").await.expect("rewrite same value");
    let (_, rewritten) = store
        .get_versioned(name)
        .await
        .expect("read rewritten secret")
        .expect("rewritten secret is present");
    assert_ne!(
        rewritten, version,
        "rewriting identical plaintext advances version"
    );
    let stale_rewrite = store
        .set_if(name, b"stale", Precondition::Version(version))
        .await
        .expect_err("rewriting identical plaintext invalidates the old version");
    assert_eq!(stale_rewrite.kind(), ErrorKind::Conflict);

    let replaced_version = store
        .set_if(name, b"second", Precondition::Version(rewritten))
        .await
        .expect("replace the secret at its observed version");
    let stale = store
        .set_if(name, b"stale", Precondition::Version(rewritten))
        .await
        .expect_err("a stale version must not overwrite the secret");
    assert_eq!(stale.kind(), ErrorKind::Conflict);
    let (_, committed) = store.get_versioned(name).await.unwrap().unwrap();
    assert_eq!(
        replaced_version, committed,
        "set_if returns its committed revision"
    );
    assert!(!stale.to_string().contains("stale"));
    assert!(!stale.to_string().contains(name));

    let (_, before_delete) = store
        .get_versioned(name)
        .await
        .expect("read before delete")
        .expect("the secret exists before delete");
    assert!(store.delete(name).await.expect("delete secret"));
    store
        .set(name, b"second")
        .await
        .expect("recreate same value");
    let (_, recreated) = store
        .get_versioned(name)
        .await
        .expect("read recreated secret")
        .expect("recreated secret is present");
    assert_ne!(
        recreated, before_delete,
        "delete and recreate advances version"
    );
    let stale_recreation = store
        .set_if(name, b"stale", Precondition::Version(before_delete))
        .await
        .expect_err("a deleted and recreated secret rejects its old version");
    assert_eq!(stale_recreation.kind(), ErrorKind::Conflict);

    let (_, concurrent_version) = store
        .get_versioned(name)
        .await
        .expect("read version before racing writers")
        .expect("the secret still exists");
    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let first = async {
        first_barrier.wait().await;
        store
            .set_if(name, b"writer-a", Precondition::Version(concurrent_version))
            .await
    };
    let second = async {
        barrier.wait().await;
        store
            .set_if(name, b"writer-b", Precondition::Version(concurrent_version))
            .await
    };
    let (first, second) = tokio::join!(first, second);
    let results = [first, second];
    let winning_version = results
        .iter()
        .find_map(|result| result.as_ref().ok().copied())
        .expect("one concurrent write returned its committed revision");
    assert_eq!(
        results.iter().filter(|result| result.is_ok()).count(),
        1,
        "exactly one concurrent writer must win"
    );
    assert_eq!(
        results
            .iter()
            .filter_map(|result| result.as_ref().err())
            .map(|error| error.kind())
            .collect::<Vec<_>>(),
        [ErrorKind::Conflict],
        "the losing writer must receive a typed conflict"
    );

    let (final_value, final_version) = store
        .get_versioned(name)
        .await
        .expect("read after concurrent writers")
        .expect("a concurrent writer left the secret present");
    let final_value = final_value.as_slice();
    assert!(
        final_value == &b"writer-a"[..] || final_value == &b"writer-b"[..],
        "the winning writer's value must be stored"
    );
    assert_eq!(
        winning_version, final_version,
        "the winning call returns the revision of its own committed write"
    );
}
