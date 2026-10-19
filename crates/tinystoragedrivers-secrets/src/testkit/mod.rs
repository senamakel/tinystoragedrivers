//! The behavioral suite every [`SecretStore`] must pass.
//!
//! A driver's tests call [`secrets_conformance`] with a freshly opened store.
//! Every name the suite writes starts with a prefix unique to the run, so it
//! can point at a long-lived store (a real OS keychain behind
//! `TSD_LIVE_KEYRING=1`, a shared database) without colliding with anything
//! else, and it deletes what it wrote, also when a check fails part way. Values are UTF-8 text, the subset every
//! driver can hold.
//!
//! Failures panic with a message naming the check, which is how a Rust test
//! reports them.

#![allow(
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    reason = "the conformance suite reports a failed check by panicking inside the driver's test"
)]

mod document;

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use tinystoragedrivers_core::ErrorKind;

use crate::store::{MAX_SECRET_NAME_LEN, SecretStore};

pub use document::document_secrets_conformance;

/// Run every check against `store`.
///
/// [`SecretStore::list`] is checked only when the store reports
/// [`SecretStore::enumerable`]; otherwise the suite checks that `list` fails
/// with [`ErrorKind::Backend`] instead of returning a partial answer.
pub async fn secrets_conformance(store: &dyn SecretStore) {
    assert!(
        !store.backend_name().is_empty(),
        "backend_name must not be empty"
    );
    let names = Names(unique_prefix());
    let checks = CatchUnwind(Box::pin(async {
        round_trip(store, &names).await;
        listing(store, &names).await;
        deletion(store, &names).await;
        invalid_names(store).await;
    }));
    let outcome = checks.await;
    // Best effort and unconditional: a failed check must not leave test
    // secrets behind in a long-lived store.
    for suffix in WRITTEN {
        let _ = store.delete(&names.of(suffix)).await;
    }
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// Every suffix the suite may write.
const WRITTEN: [&str; 3] = ["a", "b", "b:nested"];

/// Polls a future, turning a panic inside it into an `Err`, so cleanup can
/// run before the panic is resumed.
struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::thread::Result<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.0.as_mut();
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}

/// The names one run writes, all under one unique prefix.
struct Names(String);

impl Names {
    fn of(&self, suffix: &str) -> String {
        format!("{}{suffix}", self.0)
    }
}

async fn read(store: &dyn SecretStore, name: &str) -> Option<Vec<u8>> {
    store
        .get(name)
        .await
        .expect("get")
        .map(|value| value.to_vec())
}

async fn round_trip(store: &dyn SecretStore, names: &Names) {
    assert_eq!(
        read(store, &names.of("missing")).await,
        None,
        "a missing secret must read as None"
    );
    assert!(
        !store.delete(&names.of("missing")).await.expect("delete"),
        "deleting a missing secret must report false"
    );

    store.set(&names.of("a"), b"first").await.expect("set");
    assert_eq!(
        read(store, &names.of("a")).await.as_deref(),
        Some(&b"first"[..]),
        "a secret must read back as written"
    );
    let second = "s\u{e9}cond \u{2713}".as_bytes();
    store.set(&names.of("a"), second).await.expect("overwrite");
    assert_eq!(
        read(store, &names.of("a")).await.as_deref(),
        Some(second),
        "set must replace the previous value"
    );
    store.set(&names.of("b"), b"bee").await.expect("set");
    store
        .set(&names.of("b:nested"), b"nest")
        .await
        .expect("set");
    assert_eq!(
        read(store, &names.of("b")).await.as_deref(),
        Some(&b"bee"[..]),
        "secrets must not overwrite each other"
    );
}

async fn listing(store: &dyn SecretStore, names: &Names) {
    if !store.enumerable() {
        let error = store.list(&names.0).await.expect_err("list must fail");
        assert_eq!(
            error.kind(),
            ErrorKind::Backend,
            "a store that cannot enumerate must say so"
        );
        return;
    }
    assert_eq!(
        store.list(&names.0).await.expect("list"),
        [names.of("a"), names.of("b"), names.of("b:nested")],
        "list must return every name under the prefix, sorted"
    );
    assert_eq!(
        store.list(&names.of("b")).await.expect("list"),
        [names.of("b"), names.of("b:nested")],
        "list must filter by prefix"
    );
    let everything = store.list("").await.expect("list all");
    assert!(
        everything.contains(&names.of("a")),
        "an empty prefix must list every secret"
    );
    assert!(
        everything.windows(2).all(|pair| pair[0] < pair[1]),
        "list must be sorted"
    );
}

async fn deletion(store: &dyn SecretStore, names: &Names) {
    assert!(
        store.delete(&names.of("a")).await.expect("delete"),
        "delete must report true"
    );
    assert_eq!(
        read(store, &names.of("a")).await,
        None,
        "a deleted secret must read as None"
    );
    assert!(
        !store.delete(&names.of("a")).await.expect("delete"),
        "a second delete must report false"
    );
    assert!(
        read(store, &names.of("b")).await.is_some(),
        "delete must not touch other secrets"
    );
    if store.enumerable() {
        assert_eq!(
            store.list(&names.0).await.expect("list"),
            [names.of("b"), names.of("b:nested")],
            "a deleted secret must leave the listing"
        );
    }
}

async fn invalid_names(store: &dyn SecretStore) {
    let too_long = "x".repeat(MAX_SECRET_NAME_LEN + 1);
    for bad in ["", "nul\0byte", "line\nbreak", too_long.as_str()] {
        expect_invalid(store.get(bad).await, "get must reject an invalid name");
        expect_invalid(
            store.set(bad, b"v").await,
            "set must reject an invalid name",
        );
        expect_invalid(
            store.delete(bad).await,
            "delete must reject an invalid name",
        );
    }
    // Checked whether or not the store can enumerate: an invalid prefix is
    // the caller's error either way.
    expect_invalid(
        store.list("bad\0prefix").await,
        "list must reject an invalid prefix",
    );
}

fn expect_invalid<T>(result: tinystoragedrivers_core::Result<T>, check: &str) {
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(ErrorKind::InvalidInput),
        "{check}"
    );
}

/// A prefix unique to this process and call.
fn unique_prefix() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let seq = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("tsd-conformance:{}:{nanos}:{seq}:", std::process::id())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
