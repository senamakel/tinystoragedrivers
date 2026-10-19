use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::{ErrorKind, MemoryStorage, StorageBackend};

struct Rig {
    storage: crate::ScopedStorage,
    leases: Leases,
    now: Arc<AtomicU64>,
}

fn rig() -> Rig {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&now);
    let leases = Leases::new(storage.documents())
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)))
        .with_retry(RetryPolicy::immediate());
    Rig {
        storage,
        leases,
        now,
    }
}

#[tokio::test]
async fn the_first_acquire_starts_at_epoch_one() {
    let rig = rig();
    let lease = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (lease.holder.as_str(), lease.epoch, lease.expires_ms),
        ("a", 1, 1_100)
    );
    assert_eq!(rig.leases.current("host", "p1").await.unwrap(), Some(lease));
}

#[tokio::test]
async fn a_live_lease_blocks_other_holders_but_not_its_own() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(
        rig.leases
            .acquire("host", "p1", "b", 100)
            .await
            .unwrap()
            .is_none()
    );
    rig.now.store(1_050, Ordering::SeqCst);
    let again = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((again.epoch, again.expires_ms), (1, 1_150));
}

#[tokio::test]
async fn takeover_after_expiry_bumps_the_epoch_and_renew_keeps_it() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    rig.now.store(1_100, Ordering::SeqCst);
    assert!(rig.leases.current("host", "p1").await.unwrap().is_none());
    let taken = rig
        .leases
        .acquire("host", "p1", "b", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((taken.holder.as_str(), taken.epoch), ("b", 2));
    rig.now.store(1_150, Ordering::SeqCst);
    let renewed = rig
        .leases
        .renew("host", "p1", "b", 2, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((renewed.epoch, renewed.expires_ms), (2, 1_250));
    // The old holder returns after another lapse and takeover: epoch 3.
    rig.now.store(1_300, Ordering::SeqCst);
    let back = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(back.epoch, 3);
}

#[tokio::test]
async fn renew_refuses_a_stranger_a_lapsed_lease_and_a_missing_one() {
    let rig = rig();
    assert!(
        rig.leases
            .renew("host", "p1", "a", 1, 100)
            .await
            .unwrap()
            .is_none()
    );
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(
        rig.leases
            .renew("host", "p1", "b", 1, 100)
            .await
            .unwrap()
            .is_none()
    );
    rig.now.store(1_100, Ordering::SeqCst);
    assert!(
        rig.leases
            .renew("host", "p1", "a", 1, 100)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn only_the_holder_releases_and_the_epoch_continues() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(!rig.leases.release("host", "p1", "b", 1).await.unwrap());
    assert!(rig.leases.current("host", "p1").await.unwrap().is_some());
    assert!(rig.leases.release("host", "p1", "a", 1).await.unwrap());
    assert!(!rig.leases.release("host", "p1", "a", 1).await.unwrap());
    assert!(rig.leases.current("host", "p1").await.unwrap().is_none());
    let next = rig
        .leases
        .acquire("host", "p1", "b", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.epoch, 2);
}

#[tokio::test]
async fn leases_are_per_owner_and_key() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(
        rig.leases
            .acquire("host", "p2", "b", 100)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        rig.leases
            .acquire("cron", "p1", "b", 100)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn a_non_positive_ttl_is_refused() {
    let rig = rig();
    for ttl in [0, -5] {
        let error = rig
            .leases
            .acquire("host", "p1", "a", ttl)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn the_fence_follows_the_epoch() {
    let rig = rig();
    let scope = rig.storage.scope().clone();
    let fence = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap()
        .fence(&scope);
    assert_eq!(fence.collection(), LEASES);
    assert!(fence.validate().is_ok());

    let backend = MemoryStorage::new();
    // A fresh backend whose leases the fence guards.
    let storage = backend.for_scope(&scope).unwrap();
    let clock_now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&clock_now);
    let leases =
        Leases::new(storage.documents()).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let a = leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    let fenced = backend.for_scope_fenced(&scope, &a.fence(&scope)).unwrap();
    fenced
        .documents()
        .put(
            "work",
            "w",
            serde_json::json!({"n": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
    clock_now.store(2_000, Ordering::SeqCst);
    leases
        .acquire("host", "p1", "b", 100)
        .await
        .unwrap()
        .unwrap();
    let error = fenced
        .documents()
        .put(
            "work",
            "w",
            serde_json::json!({"n": 2}),
            crate::Precondition::None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
}

#[test]
fn debug_does_not_need_the_clock() {
    assert!(format!("{:?}", rig().leases).contains("Leases"));
}

#[tokio::test]
async fn reacquiring_a_lapsed_or_released_lease_advances_the_epoch_for_the_same_holder() {
    let rig = rig();
    let first = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    // Lapse: the earlier tenure's fence must not authorize the new one.
    rig.now.store(2_000, Ordering::SeqCst);
    let second = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((first.epoch, second.epoch), (1, 2));
    // Release, then the same holder takes it again.
    assert!(rig.leases.release("host", "p1", "a", 2).await.unwrap());
    let third = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.epoch, 3);
    // A live re-acquire and a renew keep the epoch.
    let live = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    let renewed = rig
        .leases
        .renew("host", "p1", "a", 3, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((live.epoch, renewed.epoch), (3, 3));
}

#[tokio::test]
async fn a_stale_tenure_cannot_renew_or_release_the_new_one() {
    let rig = rig();
    let old = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    // The lease lapses and the same holder string reacquires it.
    rig.now.store(2_000, Ordering::SeqCst);
    let new = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((old.epoch, new.epoch), (1, 2));
    // The stalled actor of the first tenure is refused.
    assert!(
        rig.leases
            .renew("host", "p1", "a", old.epoch, 100)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !rig.leases
            .release("host", "p1", "a", old.epoch)
            .await
            .unwrap()
    );
    // The new tenure is untouched: same expiry, still live, still renewable.
    assert_eq!(rig.leases.current("host", "p1").await.unwrap(), Some(new));
    let renewed = rig
        .leases
        .renew("host", "p1", "a", 2, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.epoch, 2);
    assert!(rig.leases.release("host", "p1", "a", 2).await.unwrap());
}

fn request(holder: &str, held_epoch: Option<u64>) -> Acquire<'_> {
    Acquire {
        owner: "host",
        key: "p1",
        holder,
        ttl_ms: 100,
        endpoint: Some("http://node-a:7788"),
        held_epoch,
    }
}

fn granted(outcome: Acquisition) -> (Lease, bool) {
    match outcome {
        Acquisition::Granted {
            lease,
            previous_unclean,
        } => (lease, previous_unclean),
        Acquisition::Held(record) => panic!("expected a grant, held by {record:?}"),
    }
}

#[tokio::test]
async fn acquire_with_records_the_endpoint_and_a_fresh_key_is_clean() {
    let rig = rig();
    let (lease, unclean) = granted(rig.leases.acquire_with(request("a", None)).await.unwrap());
    assert_eq!((lease.epoch, unclean), (1, false));
    assert_eq!(lease.endpoint.as_deref(), Some("http://node-a:7788"));
    let record = rig.leases.inspect("host", "p1").await.unwrap().unwrap();
    assert_eq!(record.endpoint.as_deref(), Some("http://node-a:7788"));
    assert!(record.is_live(1_050) && !record.released);
    // Renewing keeps the endpoint.
    let renewed = rig
        .leases
        .renew("host", "p1", "a", 1, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.endpoint.as_deref(), Some("http://node-a:7788"));
}

#[tokio::test]
async fn acquire_with_reports_the_live_record_of_another_holder() {
    let rig = rig();
    granted(rig.leases.acquire_with(request("a", None)).await.unwrap());
    match rig.leases.acquire_with(request("b", None)).await.unwrap() {
        Acquisition::Held(record) => {
            assert_eq!((record.holder.as_str(), record.epoch), ("a", 1));
            assert_eq!(record.endpoint.as_deref(), Some("http://node-a:7788"));
        }
        other @ Acquisition::Granted { .. } => panic!("must be held: {other:?}"),
    }
}

#[tokio::test]
async fn a_release_is_a_distinct_state_from_expiry_and_hands_over_clean() {
    let rig = rig();
    let (lease, _) = granted(rig.leases.acquire_with(request("a", None)).await.unwrap());
    assert!(
        rig.leases
            .release("host", "p1", "a", lease.epoch)
            .await
            .unwrap()
    );
    let record = rig.leases.inspect("host", "p1").await.unwrap().unwrap();
    assert!(record.released && !record.is_live(1_000));
    assert!(rig.leases.current("host", "p1").await.unwrap().is_none());
    let (next, unclean) = granted(rig.leases.acquire_with(request("b", None)).await.unwrap());
    assert_eq!((next.epoch, unclean), (2, false));

    // An abandoned lease is expired, not released.
    rig.now.store(2_000, Ordering::SeqCst);
    let record = rig.leases.inspect("host", "p1").await.unwrap().unwrap();
    assert!(!record.released && !record.is_live(2_000));
    let (after, unclean) = granted(rig.leases.acquire_with(request("c", None)).await.unwrap());
    assert_eq!((after.epoch, unclean), (3, true));
}

#[tokio::test]
async fn the_same_holder_reenters_only_with_the_epoch_it_holds() {
    let rig = rig();
    let (first, _) = granted(rig.leases.acquire_with(request("a", None)).await.unwrap());
    // Same instance, still live: the epoch stays and it is clean.
    rig.now.store(1_050, Ordering::SeqCst);
    let (again, unclean) = granted(
        rig.leases
            .acquire_with(request("a", Some(first.epoch)))
            .await
            .unwrap(),
    );
    assert_eq!((again.epoch, unclean, again.expires_ms), (1, false, 1_150));
}

#[tokio::test]
async fn a_live_record_left_by_a_dead_incarnation_is_taken_over_and_fenced() {
    let scope = Scope::local();
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&scope).unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&now);
    let leases =
        Leases::new(storage.documents()).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let (old, _) = granted(leases.acquire_with(request("a", None)).await.unwrap());
    let fenced = backend
        .for_scope_fenced(&scope, &old.fence(&scope))
        .unwrap();
    // A new incarnation with the same holder id, record still live.
    now.store(1_010, Ordering::SeqCst);
    let (successor_lease, unclean) =
        granted(leases.acquire_with(request("a", None)).await.unwrap());
    assert_eq!((successor_lease.epoch, unclean), (2, true));
    // The old incarnation can neither renew nor release the new tenure.
    assert!(
        leases
            .renew("host", "p1", "a", old.epoch, 100)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!leases.release("host", "p1", "a", old.epoch).await.unwrap());
    // And its fence is dead.
    let error = fenced
        .documents()
        .put(
            "work",
            "w",
            serde_json::json!({"n": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
}

#[tokio::test]
async fn compact_drops_only_the_named_tenure() {
    let rig = rig();
    let (first, _) = granted(rig.leases.acquire_with(request("a", None)).await.unwrap());
    rig.now.store(2_000, Ordering::SeqCst);
    let (second, _) = granted(rig.leases.acquire_with(request("b", None)).await.unwrap());
    // The earlier tenure's token no longer matches: nothing is removed.
    assert!(
        !rig.leases
            .compact("host", "p1", "a", first.epoch)
            .await
            .unwrap()
    );
    assert!(
        !rig.leases
            .compact("host", "p1", "b", first.epoch)
            .await
            .unwrap()
    );
    assert!(rig.leases.inspect("host", "p1").await.unwrap().is_some());
    assert!(
        rig.leases
            .compact("host", "p1", "b", second.epoch)
            .await
            .unwrap()
    );
    assert!(rig.leases.inspect("host", "p1").await.unwrap().is_none());
    assert!(
        !rig.leases
            .compact("host", "p1", "b", second.epoch)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_compacted_tenure_is_fenced_out() {
    let rig = rig();
    let scope = rig.storage.scope().clone();
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&scope).unwrap();
    let leases = Leases::new(storage.documents()).with_clock(Arc::new(|| 1_000));
    let a = leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    let fenced = backend.for_scope_fenced(&scope, &a.fence(&scope)).unwrap();
    assert!(leases.compact("host", "p1", "a", a.epoch).await.unwrap());
    let error = fenced
        .documents()
        .put(
            "work",
            "w",
            serde_json::json!({"n": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
}

#[tokio::test]
async fn a_stale_held_epoch_cannot_evict_the_live_replacement() {
    let rig = rig();
    granted(rig.leases.acquire_with(request("a", None)).await.unwrap());
    rig.now.store(1_010, Ordering::SeqCst);
    let (replacement, _) = granted(rig.leases.acquire_with(request("a", None)).await.unwrap());
    assert_eq!(replacement.epoch, 2);
    // A paused older incarnation still believes it holds epoch 1.
    match rig
        .leases
        .acquire_with(request("a", Some(1)))
        .await
        .unwrap()
    {
        Acquisition::Held(record) => assert_eq!(record.epoch, 2),
        other @ Acquisition::Granted { .. } => panic!("a stale actor must not win: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_same_holder_call_that_loses_the_swap_is_refused_not_re_taken() {
    use std::sync::atomic::AtomicBool;

    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let rival = Leases::new(storage.documents()).with_clock(Arc::new(|| 1_000));
    let injected = Arc::new(AtomicBool::new(false));
    let handle = tokio::runtime::Handle::current();
    let leases = Leases::new(storage.documents())
        .with_retry(RetryPolicy::immediate())
        .with_clock({
            let (rival, injected) = (rival.clone(), Arc::clone(&injected));
            Arc::new(move || {
                // On the first attempt, after it read "absent", a competing
                // call of the same holder lands its write.
                if !injected.swap(true, Ordering::SeqCst) {
                    tokio::task::block_in_place(|| {
                        handle.block_on(rival.acquire_with(request("a", None)))
                    })
                    .unwrap();
                }
                1_000
            })
        });
    match leases.acquire_with(request("a", None)).await.unwrap() {
        Acquisition::Held(record) => assert_eq!((record.holder.as_str(), record.epoch), ("a", 1)),
        other @ Acquisition::Granted { .. } => panic!("the loser must not re-take: {other:?}"),
    }
    let stored = rival.inspect("host", "p1").await.unwrap().unwrap();
    assert_eq!(stored.epoch, 1, "the winner's grant stands");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_swap_is_a_race_even_when_the_winner_wrote_an_identical_body() {
    use std::sync::atomic::AtomicBool;

    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let rival = Leases::new(storage.documents()).with_clock(Arc::new(|| 1_000));
    granted(rival.acquire_with(request("a", None)).await.unwrap());
    let injected = Arc::new(AtomicBool::new(false));
    let handle = tokio::runtime::Handle::current();
    let leases = Leases::new(storage.documents())
        .with_retry(RetryPolicy::immediate())
        .with_clock({
            let (rival, injected) = (rival.clone(), Arc::clone(&injected));
            Arc::new(move || {
                // After the first attempt read the live epoch-1 record, the
                // running instance re-enters it with the very same body.
                if !injected.swap(true, Ordering::SeqCst) {
                    tokio::task::block_in_place(|| {
                        handle.block_on(rival.acquire_with(request("a", Some(1))))
                    })
                    .unwrap();
                }
                1_000
            })
        });
    match leases.acquire_with(request("a", None)).await.unwrap() {
        Acquisition::Held(record) => assert_eq!(record.epoch, 1),
        other @ Acquisition::Granted { .. } => panic!("must not evict the winner: {other:?}"),
    }
}

#[test]
fn the_body_wire_format_is_pinned() {
    let full = Body {
        holder: "a".into(),
        epoch: 3,
        expires_ms: 9,
        endpoint: Some("http://n".into()),
        released: true,
    };
    assert_eq!(
        serde_json::to_value(&full).unwrap(),
        serde_json::json!({
            "holder": "a", "epoch": 3, "expires_ms": 9,
            "endpoint": "http://n", "released": true
        })
    );
    // Optional fields are absent, not null or false, when unused.
    let bare = Body {
        endpoint: None,
        released: false,
        ..full
    };
    assert_eq!(
        serde_json::to_value(&bare).unwrap(),
        serde_json::json!({"holder": "a", "epoch": 3, "expires_ms": 9})
    );
    // A record written before these fields existed still decodes.
    let legacy: Body =
        serde_json::from_value(serde_json::json!({"holder": "a", "epoch": 3, "expires_ms": 9}))
            .unwrap();
    assert_eq!(legacy, bare);
}

#[tokio::test]
async fn the_holder_fence_checks_epoch_holder_and_release() {
    let rig = rig();
    let scope = rig.storage.scope().clone();
    let a = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    let fence = a.holder_fence(&scope);
    assert_eq!(fence.collection(), LEASES);
    assert_eq!(fence.id(), a.fence(&scope).id());
    assert!(fence.validate().is_ok());
    assert_ne!(fence.filter(), a.fence(&scope).filter());
    assert_eq!(
        fence.filter(),
        &Filter::eq("epoch", 1_u64)
            .and(Filter::eq("holder", "a"))
            .and(Filter::exists("released", false))
    );
}
