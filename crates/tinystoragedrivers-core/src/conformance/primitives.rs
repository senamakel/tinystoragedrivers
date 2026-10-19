//! Contention checks for the shared primitives: racing counters, lease
//! acquirers and compare-and-swap updates must lose nothing on any driver.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{ok, unique};
use crate::backend::ScopedStorage;
use crate::primitives::{self, Acquire, Acquisition, Counters, Kv, Leases, Repo, RetryPolicy};

/// Increments each task performs in the counter and update races.
const ROUNDS: u64 = 100;

pub(super) async fn run(storage: &ScopedStorage) {
    ok(primitives::ensure(storage).await, "primitives ensure");
    ok(primitives::ensure(storage).await, "primitives ensure twice");
    counters(storage).await;
    leases(storage).await;
    lease_cluster_features(storage).await;
    updates(storage).await;
    kv(storage).await;
}

/// Two tasks drawing 100 numbers each get 1..=200 with no repeat and no gap.
async fn counters(storage: &ScopedStorage) {
    let owner = unique("counter");
    let draw = |storage: ScopedStorage, owner: String| async move {
        let counters = Counters::new(storage.documents()).with_retry(contended());
        let mut seen = Vec::new();
        for _ in 0..ROUNDS {
            seen.push(ok(counters.next(&owner, "n").await, "counter next"));
        }
        seen
    };
    let (a, b) = tokio::join!(
        tokio::spawn(draw(storage.clone(), owner.clone())),
        tokio::spawn(draw(storage.clone(), owner.clone())),
    );
    let (a, b) = (
        ok(a.map_err(join_error), "task a"),
        ok(b.map_err(join_error), "task b"),
    );
    for task in [&a, &b] {
        assert!(
            task.windows(2).all(|w| w[0] < w[1]),
            "a task's counter values must increase"
        );
    }
    let all: BTreeSet<u64> = a.iter().chain(&b).copied().collect();
    assert_eq!(all.len(), 2 * 100, "counter handed out a duplicate");
    assert_eq!(all.first(), Some(&1), "counter must start at 1");
    assert_eq!(all.last(), Some(&200), "counter must not skip a value");
    let counters = Counters::new(storage.documents());
    assert_eq!(ok(counters.peek(&owner, "n").await, "counter peek"), 200);
    assert_eq!(
        ok(
            counters.set_at_least(&owner, "n", 500).await,
            "counter set_at_least"
        ),
        500
    );
    assert_eq!(
        ok(counters.next(&owner, "n").await, "counter after set"),
        501
    );
}

/// Racing acquirers: exactly one wins per round, and epochs only go up.
async fn leases(storage: &ScopedStorage) {
    let owner = unique("lease");
    let leases = Leases::new(storage.documents()).with_retry(contended());
    let mut last_epoch = 0;
    for round in 0..5 {
        let key = format!("p{round}");
        let race = |holder: &'static str| {
            let leases = leases.clone();
            let (owner, key) = (owner.clone(), key.clone());
            tokio::spawn(async move { leases.acquire(&owner, &key, holder, 60_000).await })
        };
        let (a, b) = tokio::join!(race("a"), race("b"));
        let a = ok(ok(a.map_err(join_error), "lease task a"), "lease acquire a");
        let b = ok(ok(b.map_err(join_error), "lease task b"), "lease acquire b");
        assert!(
            a.is_some() ^ b.is_some(),
            "exactly one racing acquire must win (round {round}): {a:?} / {b:?}"
        );
        let winner = a.or(b).expect("one winner");
        assert_eq!(winner.epoch, 1, "a fresh lease starts at epoch 1");
        let held = ok(leases.current(&owner, &key).await, "lease current");
        assert_eq!(held.as_ref(), Some(&winner), "current must be the winner");
    }
    // Release and re-acquire by the rival: the epoch only moves forward.
    let key = "handover";
    let first = ok(leases.acquire(&owner, key, "a", 60_000).await, "acquire a")
        .expect("free lease must be acquirable");
    assert!(ok(
        leases.release(&owner, key, "a", first.epoch).await,
        "release"
    ));
    let second = ok(leases.acquire(&owner, key, "b", 60_000).await, "acquire b")
        .expect("released lease must be acquirable");
    assert!(
        second.epoch > first.epoch,
        "takeover must raise the epoch ({} -> {})",
        first.epoch,
        second.epoch
    );
    last_epoch = last_epoch.max(second.epoch);
    // A stale actor of the released tenure cannot renew or release the new one,
    // even though the holder string is reusable.
    let again = ok(
        leases.release(&owner, key, "b", second.epoch).await,
        "release b",
    );
    assert!(again, "the current tenure releases");
    let third = ok(
        leases.acquire(&owner, key, "b", 60_000).await,
        "reacquire b",
    )
    .expect("released lease must be acquirable by the same holder");
    assert!(third.epoch > second.epoch, "a reacquire raises the epoch");
    assert!(
        ok(
            leases.renew(&owner, key, "b", second.epoch, 60_000).await,
            "stale renew"
        )
        .is_none(),
        "a stale tenure must not renew the new one"
    );
    assert!(
        !ok(
            leases.release(&owner, key, "b", second.epoch).await,
            "stale release"
        ),
        "a stale tenure must not release the new one"
    );
    let renewed = ok(
        leases.renew(&owner, key, "b", third.epoch, 60_000).await,
        "renew",
    )
    .expect("holder can renew");
    last_epoch = last_epoch.max(third.epoch);
    assert_eq!(renewed.epoch, last_epoch, "renew must keep the epoch");
}

fn lease_request<'a>(owner: &'a str, holder: &'a str, held_epoch: Option<u64>) -> Acquire<'a> {
    Acquire {
        owner,
        key: "k",
        holder,
        ttl_ms: 100,
        endpoint: Some("http://node:1"),
        held_epoch,
    }
}

fn lease_grant(outcome: Acquisition) -> (crate::Lease, bool) {
    match outcome {
        Acquisition::Granted {
            lease,
            previous_unclean,
        } => (lease, previous_unclean),
        Acquisition::Held(record) => panic!("expected a grant, held by {record:?}"),
    }
}

/// Endpoint, released versus expired, unclean takeover and compaction behave
/// the same on every driver. Time is injected so expiry needs no sleeping.
async fn lease_cluster_features(storage: &ScopedStorage) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let owner = unique("lease_cf");
    let now = Arc::new(AtomicU64::new(1_000_000));
    let clock = Arc::clone(&now);
    let leases = Leases::new(storage.documents())
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)))
        .with_retry(contended());
    // Fresh key: epoch 1, clean, endpoint recorded and kept across a renew.
    let (a1, unclean) = lease_grant(ok(
        leases.acquire_with(lease_request(&owner, "a", None)).await,
        "acquire a",
    ));
    assert_eq!((a1.epoch, unclean), (1, false));
    assert_eq!(a1.endpoint.as_deref(), Some("http://node:1"));
    let renewed = ok(leases.renew(&owner, "k", "a", 1, 100).await, "renew");
    assert_eq!(
        renewed.expect("renew").endpoint.as_deref(),
        Some("http://node:1")
    );

    // Another holder is refused with the live record.
    match ok(
        leases.acquire_with(lease_request(&owner, "b", None)).await,
        "acquire b",
    ) {
        Acquisition::Held(record) => {
            assert_eq!((record.holder.as_str(), record.epoch), ("a", 1));
            assert_eq!(record.endpoint.as_deref(), Some("http://node:1"));
        }
        other @ Acquisition::Granted { .. } => panic!("live lease must hold: {other:?}"),
    }

    // Re-entrant: the same instance keeps its epoch, clean.
    let (again, unclean) = lease_grant(ok(
        leases
            .acquire_with(lease_request(&owner, "a", Some(1)))
            .await,
        "reenter",
    ));
    assert_eq!((again.epoch, unclean), (1, false));

    // Unclean takeover of a live record the caller does not hold.
    let (fresh, unclean) = lease_grant(ok(
        leases.acquire_with(lease_request(&owner, "a", None)).await,
        "unclean takeover",
    ));
    assert_eq!((fresh.epoch, unclean), (2, true));
    assert!(
        ok(leases.renew(&owner, "k", "a", 1, 100).await, "stale renew").is_none(),
        "the fenced-out incarnation must not renew"
    );
    assert!(!ok(
        leases.release(&owner, "k", "a", 1).await,
        "stale release"
    ));

    // A release is its own state, distinct from expiry; the next acquire is clean.
    assert!(ok(leases.release(&owner, "k", "a", 2).await, "release"));
    let record =
        ok(leases.inspect(&owner, "k").await, "inspect").expect("a released record is kept");
    assert!(record.released && !record.is_live(1_000_000));
    let (b3, unclean) = lease_grant(ok(
        leases.acquire_with(lease_request(&owner, "b", None)).await,
        "after release",
    ));
    assert_eq!((b3.epoch, unclean), (3, false));

    // An abandoned lease is expired, not released; the takeover is unclean.
    now.store(2_000_000, Ordering::SeqCst);
    let record = ok(leases.inspect(&owner, "k").await, "inspect").expect("kept");
    assert!(!record.released && !record.is_live(2_000_000));
    let (c4, unclean) = lease_grant(ok(
        leases.acquire_with(lease_request(&owner, "c", None)).await,
        "after expiry",
    ));
    assert_eq!((c4.epoch, unclean), (4, true));

    lease_compaction(&leases, &owner).await;
}

/// Compaction is conditional on holder and epoch, and races have one winner.
async fn lease_compaction(leases: &Leases, owner: &str) {
    let record = ok(leases.inspect(owner, "k").await, "inspect").expect("kept");
    assert_eq!(record.epoch, 4, "the unclean takeover left epoch 4");
    assert!(
        !ok(leases.compact(owner, "k", "c", 3).await, "wrong epoch"),
        "a mismatched epoch must not drop the record"
    );
    assert!(
        !ok(leases.compact(owner, "k", "b", 4).await, "wrong holder"),
        "a mismatched holder must not drop the record"
    );
    assert!(ok(leases.inspect(owner, "k").await, "inspect").is_some());
    assert!(ok(leases.compact(owner, "k", "c", 4).await, "compact"));
    assert!(ok(leases.inspect(owner, "k").await, "inspect").is_none());
    assert!(!ok(
        leases.compact(owner, "k", "c", 4).await,
        "compact twice"
    ));

    // Racing compactions: exactly one removes the record.
    let (g, _) = lease_grant(ok(
        leases.acquire_with(lease_request(owner, "d", None)).await,
        "reacquire",
    ));
    let race = || {
        let leases = leases.clone();
        let owner = owner.to_owned();
        let epoch = g.epoch;
        tokio::spawn(async move { leases.compact(&owner, "k", "d", epoch).await })
    };
    let (x, y) = tokio::join!(race(), race());
    let x = ok(ok(x.map_err(join_error), "task"), "compact x");
    let y = ok(ok(y.map_err(join_error), "task"), "compact y");
    assert!(
        x ^ y,
        "exactly one racing compaction must remove the record"
    );
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tally {
    n: u64,
}

/// Concurrent `Repo::update` increments lose nothing.
async fn updates(storage: &ScopedStorage) {
    let collection = unique("tally").replace('_', ".");
    let id = "t";
    let bump = |storage: ScopedStorage, collection: String| async move {
        let repo =
            ok(Repo::<Tally>::new(storage.documents(), collection), "repo").with_retry(contended());
        for _ in 0..ROUNDS {
            ok(
                repo.update(id, |t| {
                    Ok(Some(Tally {
                        n: t.map_or(1, |t| t.n + 1),
                    }))
                })
                .await,
                "repo update",
            );
        }
    };
    let (a, b) = tokio::join!(
        tokio::spawn(bump(storage.clone(), collection.clone())),
        tokio::spawn(bump(storage.clone(), collection.clone())),
    );
    ok(a.map_err(join_error), "update task a");
    ok(b.map_err(join_error), "update task b");
    let repo = ok(Repo::<Tally>::new(storage.documents(), collection), "repo");
    let stored = ok(repo.get(id).await, "repo get").expect("tally exists");
    assert_eq!(
        stored.doc.n,
        2 * ROUNDS,
        "concurrent updates lost an increment"
    );
}

/// Namespaces list by prefix in key order and stay apart.
async fn kv(storage: &ScopedStorage) {
    let owner = unique("kv");
    let one = ok(Kv::ns(storage.documents(), owner.as_str(), "one"), "kv ns");
    let two = ok(Kv::ns(storage.documents(), owner.as_str(), "two"), "kv ns");
    for key in ["b/2", "a/1", "b/1", "c"] {
        ok(one.put(key, &key.len()).await, "kv put");
    }
    ok(two.put("b/9", &"other").await, "kv put two");
    let listed = ok(one.list::<usize>("b/").await, "kv list");
    let keys: Vec<_> = listed.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(
        keys,
        ["b/1", "b/2"],
        "kv list must filter by prefix in key order"
    );
    assert_eq!(ok(one.list::<usize>("").await, "kv list all").len(), 4);
    assert_eq!(ok(two.list::<String>("").await, "kv list two").len(), 1);
    assert!(ok(one.delete("c").await, "kv delete"));
    assert_eq!(ok(one.get::<usize>("c").await, "kv get deleted"), None);
}

/// A generous budget with a short backoff: the checks assert on outcomes, so
/// a slow driver may take many attempts but must not fail.
pub(super) fn contended() -> RetryPolicy {
    RetryPolicy::new(
        512,
        std::time::Duration::from_micros(200),
        std::time::Duration::from_millis(4),
    )
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a `map_err` callback"
)]
pub(super) fn join_error(error: tokio::task::JoinError) -> crate::StorageError {
    crate::StorageError::backend(format!("task failed: {error}"))
}
