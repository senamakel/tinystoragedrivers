//! The behavioral suite every driver must pass.
//!
//! A driver's tests call [`run`] with a freshly opened backend. Each check uses
//! collection, stream and key names unique to the run, so the suite can point
//! at a long-lived database (a shared `MongoDB` in CI) without cleanup races.
//! Checks for an optional [`Capability`] assert the documented
//! [`ErrorKind::Unsupported`] when the driver lacks it.
//!
//! Failures panic with a message naming the check, which is how a Rust test
//! reports them.

#![allow(
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    reason = "the conformance suite reports a failed check by panicking inside the driver's test"
)]

mod blobs;
mod documents;
mod fencing;
mod isolation;
mod ledger;
mod maintenance;
mod migrations;
mod primitives;
mod purge;
mod purge_history;
mod queue;
mod queue_port;
mod schema;
mod seal;
mod semantics;
mod shared;
mod streams;
mod validated;
mod watch;

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend::{ScopedStorage, StorageBackend};
use crate::capabilities::Capability;
use crate::error::{ErrorKind, Result};
use crate::scope::Scope;

/// Run the port checks (documents, validated batches, watch, streams, blobs,
/// queues, and [`run_queue`] on [`ScopedStorage::queues`]) against one
/// already-scoped handle.
///
/// This is the part of [`run`] that needs nothing from the backend itself, so
/// a client that cannot choose scopes or fences (a storage-bus
/// client bound by its host) runs it directly. The scope, purge and
/// fencing checks stay in [`run`].
pub async fn run_ports(storage: &ScopedStorage) {
    documents::run(storage).await;
    ledger::run(storage).await;
    semantics::run(storage).await;
    schema::run(storage).await;
    validated::run(storage).await;
    watch::run(storage).await;
    streams::run(storage).await;
    seal::run(storage).await;
    blobs::run(storage).await;
    primitives::run(storage).await;
    queue::run(storage).await;
    queue_port::run(storage.queues().as_ref()).await;
}

/// Run the queue checks that need no control over time against `queue`:
/// ordering, acknowledgement, nacks and dead letters, redrive, extension,
/// stats and purge, plus delays, groups and dedupe when the queue reports
/// them in its [`QueueFeatures`](crate::queue::QueueFeatures).
///
/// [`run_ports`] runs these against [`ScopedStorage::queues`]; a queue
/// backend that is not part of a [`StorageBackend`] (a broker) runs them
/// directly. Leases are taken for a minute and never left to lapse, so the
/// checks hold whoever owns the clock. Queue names are unique to the call, so
/// the queue can be long-lived and shared.
///
/// # Panics
///
/// On the first check the queue fails, naming it.
pub async fn run_queue(queue: &dyn crate::queue::QueueStore) {
    queue_port::run(queue).await;
}

/// Run every check against `backend`.
///
/// The backend must accept [`Scope::local`] and at least one other scope
/// (`conformance-b`) unless `single_scope` is set, in which case the isolation
/// checks are skipped (a single-operator SQLite file).
///
/// # Panics
///
/// On the first check the driver fails, naming it. That is the suite's
/// purpose: it is called from a driver's tests. Also panics if
/// [`StorageBackend::driver`] is not a registered native driver;
/// external drivers should call [`run_with_process_capabilities`] instead.
pub async fn run(backend: &dyn StorageBackend, single_scope: bool) {
    // These expectations cover every built-in native record driver. A new
    // driver must choose its guarantees explicitly instead of silently
    // skipping the declaration checks.
    let expected = match backend.driver() {
        "memory" | "file" => Some((false, false)),
        "sqlite" | "mongodb" => Some((true, true)),
        _ => None,
    }
    .unwrap_or_else(|| {
        panic!(
            "unregistered backend driver {:?}; call run_with_process_capabilities with its expected declarations",
            backend.driver()
        )
    });
    run_with_process_capabilities(backend, single_scope, expected.0, expected.1).await;
}

/// Run the behavioral suite with explicit process capability expectations.
///
/// Built-in drivers should use [`run`], which binds expectations to their
/// registered native driver identity. External drivers should use this entry
/// point and state both declarations so they cannot accidentally bypass the
/// capability conformance checks. It validates the process flags on both the
/// backend and its scoped document handles (`local`, and `conformance-b` for
/// a multi-scope backend). [`Capability::SharedAcrossProcesses`]
/// includes the guarantees of [`Capability::CrossProcessCas`], so declaring
/// shared visibility without cross-process CAS is invalid.
///
/// # Panics
///
/// Panics when the backend or scoped document capability flags do not match
/// the supplied expectations, or when any behavioral check fails.
pub async fn run_with_process_capabilities(
    backend: &dyn StorageBackend,
    single_scope: bool,
    shared_across_processes: bool,
    cross_process_cas: bool,
) {
    let capabilities = backend.capabilities();
    assert!(
        !capabilities.contains(Capability::SharedAcrossProcesses)
            || capabilities.contains(Capability::CrossProcessCas),
        "{} declares SharedAcrossProcesses without required CrossProcessCas",
        backend.document_driver()
    );
    assert_eq!(
        capabilities.contains(Capability::SharedAcrossProcesses),
        shared_across_processes,
        "{} shared_across_processes capability declaration",
        backend.document_driver()
    );
    assert_eq!(
        capabilities.contains(Capability::CrossProcessCas),
        cross_process_cas,
        "{} cross_process_cas capability declaration",
        backend.document_driver()
    );
    let local = backend
        .for_scope(&Scope::local())
        .expect("for_scope(local) must succeed");
    check_document_process_capabilities(&local, shared_across_processes, cross_process_cas);
    if single_scope {
        let other = Scope::new("conformance-b").expect("valid scope");
        assert!(
            backend.for_scope(&other).is_err(),
            "single-scope backend accepted a foreign scope"
        );
    } else {
        let other_scope = Scope::new("conformance-b").expect("valid scope");
        let other = backend
            .for_scope(&other_scope)
            .expect("multi-scope backend must accept conformance-b");
        check_document_process_capabilities(&other, shared_across_processes, cross_process_cas);
    }
    run_ports(&local).await;
    seal::run_host(&local).await;
    if !single_scope {
        isolation::run(backend).await;
        isolation::nested(backend).await;
        validated::isolation(backend).await;
        purge::run(backend).await;
        purge_history::run(backend).await;
        maintenance::run(backend).await;
    }
    migrations::run(backend).await;
    fencing::run(backend, single_scope).await;
}

fn check_document_process_capabilities(
    storage: &ScopedStorage,
    shared_across_processes: bool,
    cross_process_cas: bool,
) {
    let capabilities = storage.documents().capabilities();
    assert!(
        !capabilities.contains(Capability::SharedAcrossProcesses)
            || capabilities.contains(Capability::CrossProcessCas),
        "{} scoped documents declare SharedAcrossProcesses without required CrossProcessCas",
        storage.driver()
    );
    assert_eq!(
        capabilities.contains(Capability::SharedAcrossProcesses),
        shared_across_processes,
        "{} scoped documents shared_across_processes capability declaration",
        storage.driver()
    );
    assert_eq!(
        capabilities.contains(Capability::CrossProcessCas),
        cross_process_cas,
        "{} scoped documents cross_process_cas capability declaration",
        storage.driver()
    );
}

/// Run the shared-handle checks against backends `open` yields: each call
/// opens a new, independent backend on the same store (another connection to
/// one SQLite file or one `MongoDB` database, a directory reopened).
///
/// A driver that reports [`Capability::SharedAcrossProcesses`] must show that
/// handles see each other's writes at once and arbitrate races between them:
/// racing lease acquires have exactly one winner (at epoch 1 for a fresh
/// key), and racing version-guarded puts do too. A driver that reports
/// [`Capability::CrossProcessCas`] must also arbitrate racing claims,
/// version-guarded puts and create-only puts, even if it does not promise
/// immediate visibility across all storage ports. Drivers with neither
/// capability get only the reopen check. Every driver, with or without either
/// capability, must show that a backend opened after another one was dropped
/// reads what it wrote and continues its versions, offsets and lease epochs.
///
/// `open` is called many times; it must not clear the store.
///
/// # Panics
///
/// On the first check the driver fails, naming it.
pub async fn run_shared<B, F, Fut>(open: F)
where
    B: StorageBackend,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = B>,
{
    shared::run(open).await;
}

/// A name unique to this process and call, safe as a collection, stream or
/// blob key segment.
fn unique(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    // `RandomState` is seeded from the OS per process, so two processes with
    // the same PID (separate containers) still get different names.
    let salt = RandomState::new().build_hasher().finish();
    format!(
        "conf_{label}_{salt:016x}_{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Unwrap a port result, naming the check on failure.
fn ok<T>(result: Result<T>, check: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{check}: unexpected error {error}"),
    }
}

/// Assert a port result failed with `kind`.
fn fails<T: std::fmt::Debug>(result: Result<T>, kind: ErrorKind, check: &str) {
    match result {
        Ok(value) => panic!("{check}: expected {kind}, got Ok({value:?})"),
        Err(error) => assert_eq!(error.kind(), kind, "{check}: {error}"),
    }
}

/// Assert a listing came back empty, showing what it held otherwise.
fn empty<T: std::fmt::Debug>(items: &[T], check: &str) {
    assert_eq!(items.len(), 0, "{check}: expected nothing, got {items:?}");
}

/// Whether the handles' driver claims `capability`.
fn has(storage: &ScopedStorage, capability: Capability) -> bool {
    storage.documents().capabilities().contains(capability)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
