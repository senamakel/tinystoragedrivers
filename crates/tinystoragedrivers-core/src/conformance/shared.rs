//! Shared-handle checks: independently opened backends on one store.
//!
//! [`run_shared`](super::run_shared) is what a host runs to learn whether
//! several of its own handles on one store (several connections in a process,
//! or several processes) can be trusted to agree. A driver that cannot (the
//! memory driver is one heap, the file driver serializes only inside one
//! process) does not claim [`Capability::SharedAcrossProcesses`] and gets the
//! reopen check alone.

use std::collections::BTreeSet;
use std::future::Future;

use serde_json::json;

use super::primitives::{contended, join_error};
use super::{fails, ok, unique};
use crate::backend::{ScopedStorage, StorageBackend};
use crate::capabilities::Capability;
use crate::document::Precondition;
use crate::error::ErrorKind;
use crate::primitives::{self, Leases};
use crate::scope::Scope;

/// Handles opened for each race.
pub(super) const RACERS: usize = 6;

/// Rounds of each race, each on a fresh key.
const ROUNDS: usize = 3;

/// Run the shared-handle checks over backends `open` yields.
pub(super) async fn run<B, F, Fut>(open: F)
where
    B: StorageBackend,
    F: Fn() -> Fut,
    Fut: Future<Output = B>,
{
    let first = open().await;
    let shared = first
        .capabilities()
        .contains(Capability::SharedAcrossProcesses);
    let cross_process_cas = first.capabilities().contains(Capability::CrossProcessCas);
    let fencing = first.capabilities().contains(Capability::Fencing);
    if shared || cross_process_cas {
        let second = open().await;
        assert!(
            second
                .capabilities()
                .contains(Capability::SharedAcrossProcesses)
                == shared
                && second.capabilities().contains(Capability::CrossProcessCas) == cross_process_cas
                && second.capabilities().contains(Capability::Fencing) == fencing,
            "independently opened backends report different capabilities"
        );
        if fencing {
            stale_fence_race(&first, &second).await;
        }
        if shared {
            visibility(&handle(&first), &handle(&second)).await;
        }
        let mut racers = vec![first, second];
        while racers.len() < RACERS {
            racers.push(open().await);
        }
        let racers: Vec<ScopedStorage> = racers.iter().map(handle).collect();
        claim_race(&racers, shared).await;
        lease_race(&racers, shared).await;
        version_races(&racers, shared).await;
    } else {
        drop(first);
    }
    reopen(&open).await;
}

fn handle(backend: &impl StorageBackend) -> ScopedStorage {
    ok(backend.for_scope(&Scope::local()), "shared handle")
}

/// A single candidate claimed concurrently is returned to exactly one handle.
async fn claim_race(handles: &[ScopedStorage], shared: bool) {
    let coll = unique("shared_claim");
    let id = "candidate";
    ok(
        handles[0]
            .documents()
            .put(&coll, id, json!({"state": "ready"}), Precondition::Absent)
            .await,
        "claim race seed",
    );
    let tasks: Vec<_> = handles
        .iter()
        .map(|storage| {
            let docs = std::sync::Arc::clone(storage.documents());
            let coll = coll.clone();
            tokio::spawn(async move {
                docs.claim(
                    &coll,
                    &crate::Filter::eq("state", "ready"),
                    &[],
                    &json!({"state": "claimed"}),
                )
                .await
            })
        })
        .collect();
    let mut winners = Vec::new();
    for task in tasks {
        if let Some(claimed) = ok(
            ok(task.await.map_err(join_error), "claim race task"),
            "claim race",
        ) {
            winners.push(claimed);
        }
    }
    assert_eq!(
        winners.len(),
        1,
        "exactly one racing document claim must win: {winners:?}"
    );
    if shared {
        for storage in handles {
            let seen = ok(
                storage.documents().get(&coll, id).await,
                "shared claim re-read",
            )
            .expect("claimed document remains present");
            assert_eq!(seen.doc["state"], "claimed");
        }
    }
}

/// A stale fence remains invalid when a different backend handle changes its guard.
async fn stale_fence_race(first: &impl StorageBackend, second: &impl StorageBackend) {
    let scope = Scope::local();
    let guards = unique("shared_fence_guard");
    let data = unique("shared_fence_data");
    ok(
        handle(first)
            .documents()
            .put(&guards, "lease", json!({"epoch": 1}), Precondition::Absent)
            .await,
        "fence race seed",
    );
    let fence = crate::Fence::epoch(scope.clone(), &guards, "lease", "epoch", 1);
    let stale = ok(
        first.for_scope_fenced(&scope, &fence),
        "open first fenced handle",
    );
    ok(
        handle(second)
            .documents()
            .put(&guards, "lease", json!({"epoch": 2}), Precondition::None)
            .await,
        "move fence through second handle",
    );
    fails(
        stale
            .documents()
            .put(&data, "stale", json!({"write": true}), Precondition::Absent)
            .await,
        ErrorKind::Fenced,
        "cross-open stale fence",
    );

    // Race an already-started old-holder write against takeover. Either the
    // old write linearizes before takeover or it is fenced afterward; once
    // the new holder writes, the old value must never become final.
    ok(
        handle(first)
            .documents()
            .put(&guards, "lease", json!({"epoch": 1}), Precondition::None)
            .await,
        "reset fence for in-flight race",
    );
    let old = ok(
        first.for_scope_fenced(&scope, &fence),
        "open in-flight old holder",
    );
    let old_docs = old.documents().clone();
    let old_data = data.clone();
    let old_write = old_docs.put(&old_data, "race", json!({"by": "old"}), Precondition::None);
    tokio::pin!(old_write);
    // Poll once before takeover. A backend may already have read the old
    // guard and be waiting to commit under only its per-open lock.
    let first_poll =
        std::future::poll_fn(|cx| std::task::Poll::Ready(old_write.as_mut().poll(cx))).await;
    let (old_result, ()) = tokio::join!(
        async {
            match first_poll {
                std::task::Poll::Ready(result) => result,
                std::task::Poll::Pending => old_write.await,
            }
        },
        async {
            ok(
                handle(second)
                    .documents()
                    .put(&guards, "lease", json!({"epoch": 2}), Precondition::None)
                    .await,
                "take over during old write",
            );
            let new_fence = crate::Fence::epoch(scope.clone(), &guards, "lease", "epoch", 2);
            let new_holder = ok(
                second.for_scope_fenced(&scope, &new_fence),
                "open new holder",
            );
            ok(
                new_holder
                    .documents()
                    .put(&data, "race", json!({"by": "new"}), Precondition::None)
                    .await,
                "new holder write after takeover",
            );
        }
    );
    assert!(
        old_result.is_ok()
            || old_result
                .as_ref()
                .is_err_and(|error| error.kind() == ErrorKind::Fenced),
        "in-flight old-holder write must succeed before takeover or be fenced afterward: {old_result:?}"
    );
    let final_doc = ok(
        handle(first).documents().get(&data, "race").await,
        "read final fenced race value",
    )
    .expect("new-holder value remains present");
    assert_eq!(
        final_doc.doc["by"], "new",
        "stale in-flight write overwrote the new holder's value"
    );
}

/// Each handle sees what the other wrote, deletions included.
async fn visibility(a: &ScopedStorage, b: &ScopedStorage) {
    let coll = unique("shared_vis");
    let stream = unique("shared_vis_stream");
    let blob = format!("{}/k", unique("shared_vis_blob"));

    let v1 = ok(
        a.documents()
            .put(&coll, "d", json!({"by": "a"}), Precondition::Absent)
            .await,
        "shared a put",
    );
    let seen = ok(b.documents().get(&coll, "d").await, "shared b get")
        .expect("a write is visible to the other handle at once");
    assert_eq!((seen.version, &seen.doc["by"]), (v1, &json!("a")));
    fails(
        b.documents()
            .put(&coll, "d", json!({"by": "b"}), Precondition::Absent)
            .await,
        ErrorKind::Conflict,
        "shared b create over a's document",
    );
    let v2 = ok(
        b.documents()
            .put(&coll, "d", json!({"by": "b"}), Precondition::Version(v1))
            .await,
        "shared b update at the version a wrote",
    );
    assert!(v2 > v1, "an update moves the version on");
    assert_eq!(
        ok(a.documents().get(&coll, "d").await, "shared a re-read")
            .expect("document")
            .doc["by"],
        "b",
        "an update is visible to the other handle at once"
    );
    assert_eq!(
        ok(
            a.documents().count(&coll, &crate::Filter::All).await,
            "shared a count"
        ),
        1
    );

    ok(
        a.streams().append(&stream, json!(1)).await,
        "shared a append",
    );
    assert_eq!(
        ok(
            b.streams().append(&stream, json!(2)).await,
            "shared b append"
        ),
        1,
        "appends from two handles share one offset sequence"
    );
    assert_eq!(ok(a.streams().len(&stream).await, "shared a len"), 2);

    ok(
        a.blobs().put(&blob, vec![1, 2], None).await,
        "shared a blob",
    );
    assert_eq!(
        ok(b.blobs().get(&blob).await, "shared b blob")
            .expect("a blob is visible to the other handle")
            .bytes,
        vec![1, 2]
    );

    assert!(
        ok(
            b.documents().delete(&coll, "d", Precondition::None).await,
            "shared b delete"
        ),
        "shared b deleted a's document"
    );
    assert!(
        ok(a.documents().get(&coll, "d").await, "shared a after delete").is_none(),
        "a delete is visible to the other handle at once"
    );
}

/// Handles racing to acquire one lease get exactly one winner, at epoch 1.
async fn lease_race(handles: &[ScopedStorage], shared: bool) {
    ok(primitives::ensure(&handles[0]).await, "shared primitives");
    let owner = unique("shared_lease");
    for round in 0..ROUNDS {
        let key = format!("p{round}");
        let tasks: Vec<_> = handles
            .iter()
            .enumerate()
            .map(|(index, storage)| {
                let leases = Leases::new(storage.documents()).with_retry(contended());
                let (owner, key) = (owner.clone(), key.clone());
                tokio::spawn(async move {
                    leases
                        .acquire(&owner, &key, &format!("holder-{index}"), 60_000)
                        .await
                })
            })
            .collect();
        let mut granted = Vec::new();
        for task in tasks {
            let outcome = ok(
                ok(task.await.map_err(join_error), "lease task"),
                "lease acquire",
            );
            granted.extend(outcome);
        }
        assert_eq!(
            granted.len(),
            1,
            "exactly one racing acquire across handles must win (round {round}): {granted:?}"
        );
        let winner = &granted[0];
        assert_eq!(winner.epoch, 1, "a fresh lease starts at epoch 1");
        // General shared visibility is a separate promise from CAS. When it
        // is declared, every handle must also observe the winning claim.
        if shared {
            for storage in handles {
                let seen = ok(
                    Leases::new(storage.documents()).current(&owner, &key).await,
                    "shared lease current",
                );
                assert_eq!(
                    seen.as_ref(),
                    Some(winner),
                    "handles disagree on the holder"
                );
            }
        }
    }
}

/// Version-guarded and create-only writes from racing handles: one winner.
async fn version_races(handles: &[ScopedStorage], shared: bool) {
    let coll = unique("shared_cas");
    for round in 0..ROUNDS {
        let id = format!("d{round}");
        let seed = ok(
            handles[0]
                .documents()
                .put(&coll, &id, json!({"by": "seed"}), Precondition::Absent)
                .await,
            "shared cas seed",
        );
        let tasks: Vec<_> = handles
            .iter()
            .enumerate()
            .map(|(index, storage)| {
                let docs = std::sync::Arc::clone(storage.documents());
                let (coll, id) = (coll.clone(), id.clone());
                tokio::spawn(async move {
                    docs.put(
                        &coll,
                        &id,
                        json!({"by": index}),
                        Precondition::Version(seed),
                    )
                    .await
                    .map(|version| (index, version))
                })
            })
            .collect();
        let mut winners = Vec::new();
        for task in tasks {
            match ok(task.await.map_err(join_error), "cas task") {
                Ok(won) => winners.push(won),
                Err(error) => assert_eq!(
                    error.kind(),
                    ErrorKind::Conflict,
                    "a lost version race is a conflict: {error}"
                ),
            }
        }
        assert_eq!(
            winners.len(),
            1,
            "exactly one racing version-guarded put must win (round {round}): {winners:?}"
        );
        let (index, version) = winners[0];
        assert!(version > seed, "the winner's version moves on");
        if shared {
            for storage in handles {
                let read =
                    ok(storage.documents().get(&coll, &id).await, "cas re-read").expect("document");
                assert_eq!(
                    (read.version, &read.doc["by"]),
                    (version, &json!(index)),
                    "every handle sees the winner's write"
                );
            }
        }
    }

    // Create-only: racing `Absent` puts create the document once.
    let id = "created";
    let tasks: Vec<_> = handles
        .iter()
        .enumerate()
        .map(|(index, storage)| {
            let docs = std::sync::Arc::clone(storage.documents());
            let (coll, id) = (coll.clone(), id.to_owned());
            tokio::spawn(async move {
                docs.put(&coll, &id, json!({"by": index}), Precondition::Absent)
                    .await
                    .map(|_| index)
            })
        })
        .collect();
    let mut created = BTreeSet::new();
    for task in tasks {
        match ok(task.await.map_err(join_error), "create task") {
            Ok(index) => {
                created.insert(index);
            }
            Err(error) => assert!(
                error.kind() == ErrorKind::Conflict,
                "a lost create race is refused: {error}"
            ),
        }
    }
    assert_eq!(
        created.len(),
        1,
        "exactly one racing create must win: {created:?}"
    );
}

/// What one backend wrote, a backend opened later reads, and the lease epoch
/// continues from where the first left it.
async fn reopen<B, F, Fut>(open: &F)
where
    B: StorageBackend,
    F: Fn() -> Fut,
    Fut: Future<Output = B>,
{
    let coll = unique("shared_reopen");
    let stream = unique("shared_reopen_stream");
    let blob = format!("{}/k", unique("shared_reopen_blob"));
    let owner = unique("shared_reopen_lease");

    let first = open().await;
    let storage = handle(&first);
    ok(primitives::ensure(&storage).await, "reopen primitives");
    let version = ok(
        storage
            .documents()
            .put(&coll, "d", json!({"n": 1}), Precondition::Absent)
            .await,
        "reopen put",
    );
    ok(
        storage
            .streams()
            .append_batch(&stream, vec![json!(1), json!(2)])
            .await,
        "reopen append",
    );
    ok(
        storage.blobs().put(&blob, vec![7], None).await,
        "reopen blob",
    );
    let leases = Leases::new(storage.documents()).with_retry(contended());
    let lease = ok(
        leases.acquire(&owner, "k", "first", 60_000).await,
        "reopen acquire",
    )
    .expect("a free lease is acquirable");
    assert!(ok(
        leases.release(&owner, "k", "first", lease.epoch).await,
        "reopen release"
    ));
    drop((leases, storage, first));

    let again = open().await;
    let storage = handle(&again);
    let seen = ok(storage.documents().get(&coll, "d").await, "reopen get")
        .expect("a reopened backend sees the earlier document");
    assert_eq!((seen.version, &seen.doc["n"]), (version, &json!(1)));
    let next = ok(
        storage
            .documents()
            .put(&coll, "d", json!({"n": 2}), Precondition::Version(version))
            .await,
        "reopen update at the old version",
    );
    assert!(next > version, "versions continue after a reopen");
    assert_eq!(ok(storage.streams().len(&stream).await, "reopen len"), 2);
    assert_eq!(
        ok(
            storage.streams().append(&stream, json!(3)).await,
            "reopen append again"
        ),
        2,
        "stream offsets continue after a reopen"
    );
    assert_eq!(
        ok(storage.blobs().get(&blob).await, "reopen blob get")
            .expect("a reopened backend sees the earlier blob")
            .bytes,
        vec![7]
    );
    let leases = Leases::new(storage.documents()).with_retry(contended());
    let second = ok(
        leases.acquire(&owner, "k", "second", 60_000).await,
        "reopen reacquire",
    )
    .expect("a released lease is acquirable");
    assert_eq!(
        second.epoch,
        lease.epoch + 1,
        "the lease epoch continues across a reopen"
    );
}
