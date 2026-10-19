//! Tests for the shared lease contract.

use super::*;

#[cfg(feature = "file")]
#[tokio::test]
async fn a_local_lock_excludes_an_independent_store_until_release() {
    let root = tempfile::tempdir().unwrap();
    let first = LocalLeases::new(root.path(), "node-a").unwrap();
    let second = LocalLeases::new(root.path(), "node-b").unwrap();

    let grant = first.acquire("alice", 1_000).await.unwrap();
    let live = second.holder("alice").await.unwrap().unwrap();
    assert!(is_live(&live, 1_000));
    assert_eq!(live.epoch, grant.epoch);
    assert!(matches!(
        second.acquire("alice", 1_000).await,
        Err(LeaseError::Held(_))
    ));

    first.release(grant).await.unwrap();
    let next = second.acquire("alice", 1_000).await.unwrap();
    assert_eq!(next.epoch, 2);
    assert!(!next.previous_unclean);
}

#[cfg(feature = "file")]
#[tokio::test]
async fn local_lock_excludes_a_separate_process() {
    if std::env::var_os("TSD_LOCAL_LOCK_CHILD").is_some() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let first = LocalLeases::new(root.path(), "parent").unwrap();
    let grant = first.acquire("alice", 1_000).await.unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "tests::local_lock_child_observes_parent",
        ])
        .env("TSD_LOCAL_LOCK_CHILD", "1")
        .env("TSD_LOCAL_LOCK_ROOT", root.path())
        .status()
        .unwrap();
    assert!(result.success());
    first.release(grant).await.unwrap();
}

#[cfg(feature = "file")]
#[tokio::test]
async fn local_holder_observes_a_dead_process_without_erasing_its_epoch() {
    if std::env::var_os("TSD_LOCAL_LOCK_OWNER_CHILD").is_some() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "tests::local_lock_child_exits_while_holding",
        ])
        .env("TSD_LOCAL_LOCK_OWNER_CHILD", "1")
        .env("TSD_LOCAL_LOCK_ROOT", root.path())
        .status()
        .unwrap();
    assert!(result.success());

    let observer = LocalLeases::new(root.path(), "observer").unwrap();
    let raw = observer.inspect("alice").await.unwrap().unwrap();
    assert!(is_live(&raw, 1_001));
    assert_eq!(raw.epoch, 1);

    let holder_view = observer.holder("alice").await.unwrap().unwrap();
    assert!(!is_live(&holder_view, 1_001));
    assert_eq!(holder_view.epoch, 1);
    assert_eq!(holder_view.holder, "child");
    assert!(!holder_view.released);

    let takeover = observer.acquire("alice", 1_002).await.unwrap();
    assert_eq!(takeover.epoch, 2);
    assert!(takeover.previous_unclean);
    assert_eq!(observer.inspect("alice").await.unwrap().unwrap().epoch, 2);
}

#[cfg(feature = "file")]
#[tokio::test]
#[ignore = "invoked as a child process by local_holder_observes_a_dead_process_without_erasing_its_epoch"]
async fn local_lock_child_exits_while_holding() {
    let root = std::path::PathBuf::from(std::env::var_os("TSD_LOCAL_LOCK_ROOT").unwrap());
    let child = LocalLeases::new(root, "child").unwrap();
    let grant = child.acquire("alice", 1_000).await.unwrap();
    assert_eq!(grant.epoch, 1);
}

#[cfg(feature = "file")]
#[tokio::test]
#[ignore = "invoked as a child process by local_lock_excludes_a_separate_process"]
async fn local_lock_child_observes_parent() {
    let root = std::path::PathBuf::from(std::env::var_os("TSD_LOCAL_LOCK_ROOT").unwrap());
    let child = LocalLeases::new(root, "child").unwrap();
    assert!(matches!(
        child.acquire("alice", 1_000).await,
        Err(LeaseError::Held(_))
    ));
}

#[cfg(feature = "file")]
#[tokio::test]
async fn local_reentry_preserves_the_current_grant_generation() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalLeases::new(root.path(), "node-a").unwrap();
    let grant = store.acquire("alice", 1_000).await.unwrap();
    let reentrant = store.acquire("alice", 1_025).await.unwrap();
    assert_eq!(reentrant.epoch, grant.epoch);
    assert_eq!(reentrant.version, grant.version);
    assert!(!reentrant.previous_unclean);
    let renewed = store.renew(&grant, 1_050).await.unwrap();
    assert_eq!(renewed.version.0, reentrant.version.0 + 1);
    assert!(matches!(
        store.renew(&grant, 1_051).await,
        Err(LeaseError::Lost)
    ));
    let other = LocalLeases::new(root.path(), "node-b").unwrap();
    assert!(matches!(
        other.acquire("alice", 1_200).await,
        Err(LeaseError::Held(_))
    ));
    assert_eq!(store.driver_fence("alice", "node-a", grant.epoch), None);
}

#[cfg(feature = "file")]
#[tokio::test]
async fn concurrent_local_renewals_have_one_latest_grant_winner() {
    use std::sync::{Arc, Barrier};

    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalLeases::new(root.path(), "node-a").unwrap());
    let mut grant = store.acquire("alice", 1_000).await.unwrap();
    for now_ms in 1_001..1_033 {
        let barrier = Arc::new(Barrier::new(3));
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let store = store.clone();
                let grant = grant.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    barrier.wait();
                    runtime.block_on(store.renew(&grant, now_ms))
                })
            })
            .collect();
        barrier.wait();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(LeaseError::Lost)))
                .count(),
            1
        );
        grant = results.into_iter().find_map(Result::ok).unwrap();
    }
    store.release(grant).await.unwrap();
}

#[cfg(feature = "file")]
#[tokio::test]
async fn local_renew_release_and_takeover_are_linearized() {
    use std::sync::{Arc, Barrier};

    let root = tempfile::tempdir().unwrap();
    let current = Arc::new(LocalLeases::new(root.path(), "node-a").unwrap());
    let next = Arc::new(LocalLeases::new(root.path(), "node-b").unwrap());
    let grant = current.acquire("alice", 1_000).await.unwrap();
    let barrier = Arc::new(Barrier::new(3));

    let renew_thread = {
        let store = current.clone();
        let grant = grant.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            barrier.wait();
            runtime.block_on(store.renew(&grant, 1_001))
        })
    };
    let release_thread = {
        let store = current.clone();
        let next = next.clone();
        let grant = grant.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            barrier.wait();
            runtime.block_on(async {
                let released = store.release(grant).await;
                let takeover = next.acquire("alice", 1_002).await;
                (released, takeover)
            })
        })
    };
    barrier.wait();
    let renewed = renew_thread.join().unwrap();
    let (released, takeover) = release_thread.join().unwrap();
    match renewed {
        Ok(latest) => {
            assert!(matches!(released, Err(LeaseError::Lost)));
            assert!(matches!(takeover, Err(LeaseError::Held(_))));
            current.release(latest).await.unwrap();
            let next_grant = next.acquire("alice", 1_003).await.unwrap();
            assert_eq!(next_grant.epoch, 2);
            assert!(!next_grant.previous_unclean);
        }
        Err(LeaseError::Lost) => {
            released.unwrap();
            let next_grant = takeover.unwrap();
            assert_eq!(next_grant.epoch, 2);
            assert!(!next_grant.previous_unclean);
        }
        Err(other) => panic!("unexpected renew error: {other}"),
    }
}

#[cfg(feature = "file")]
#[tokio::test]
async fn local_lock_and_epoch_migrate_from_the_existing_host_format() {
    use fs2::FileExt;
    use sha2::Digest;

    let root = tempfile::tempdir().unwrap();
    let key = "alice";
    let digest = sha2::Sha256::digest(key.as_bytes());
    let dir = root.path().join(format!("{digest:x}"));
    std::fs::create_dir_all(&dir).unwrap();
    let old_lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(".lease"))
        .unwrap();
    old_lock.lock_exclusive().unwrap();
    std::fs::write(
        dir.join(".lease.json"),
        serde_json::to_vec(&serde_json::json!({
            "owner":"old-node", "endpoint":"https://old.example", "epoch":41,
            "expires_at_ms":u64::MAX, "released":false
        }))
        .unwrap(),
    )
    .unwrap();

    let store = LocalLeases::new(root.path(), "new-node")
        .unwrap()
        .with_endpoint(Some("https://new.example".to_owned()));
    let held = match store.acquire(key, 1_000).await {
        Err(LeaseError::Held(record)) => record,
        other => panic!("expected existing host lock to exclude acquisition, got {other:?}"),
    };
    assert_eq!(held.holder, "old-node");
    assert_eq!(held.endpoint.as_deref(), Some("https://old.example"));
    assert_eq!(held.epoch, 41);
    assert_eq!(held.expires_ms, i64::MAX);

    FileExt::unlock(&old_lock).unwrap();
    let grant = store.acquire(key, 1_001).await.unwrap();
    assert_eq!(grant.epoch, 42);
    assert!(grant.previous_unclean);
    let record = store.inspect(key).await.unwrap().unwrap();
    assert_eq!(record.endpoint.as_deref(), Some("https://new.example"));
    assert_eq!(record.expires_ms, i64::MAX);
    let written: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join(".lease.json")).unwrap()).unwrap();
    assert_eq!(written["owner"], "new-node");
    assert_eq!(written["expires_at_ms"], u64::MAX);
    assert!(written.get("holder").is_none());
    assert!(written.get("expires_ms").is_none());
}

#[test]
fn retry_hint_is_zero_for_released_and_lapsed_records() {
    let live = LeaseRecord {
        holder: "node".into(),
        endpoint: None,
        epoch: 3,
        expires_ms: 1_500,
        released: false,
    };
    assert!(is_live(&live, 1_499));
    assert_eq!(retry_after_ms(&live, 1_000), 500);
    assert!(!is_live(&live, 1_500));
    assert_eq!(retry_after_ms(&live, 1_500), 0);
    let released = LeaseRecord {
        released: true,
        ..live
    };
    assert_eq!(retry_after_ms(&released, 1_000), 0);
}

#[test]
fn canonical_lease_record_reads_the_previous_host_field_names() {
    let record: LeaseRecord = serde_json::from_value(serde_json::json!({
        "owner": "node-a", "endpoint":"https://node.example", "epoch": 9,
        "expires_at_ms":u64::MAX
    }))
    .unwrap();
    assert_eq!(record.holder, "node-a");
    assert_eq!(record.expires_ms, i64::MAX);
    assert_eq!(record.endpoint.as_deref(), Some("https://node.example"));
    assert!(!record.released);
}
