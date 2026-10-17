use super::*;
use crate::memory::MemoryStorage;
use crate::{Filter, Scope, StorageBackend, WatchOptions};
use std::sync::Barrier;
use std::time::Duration;

#[tokio::test]
async fn shared_watch_entries_are_removed_after_the_last_receiver_drops() {
    let storage = MemoryStorage::with_native_watch();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let first = docs
        .watch("notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();
    let second = docs
        .watch("notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();

    assert_eq!(storage.db.watchers.entry_count(), 1);
    assert_eq!(
        storage
            .db
            .watchers
            .subscription_count(&("local".into(), "notes".into())),
        Some(2)
    );
    drop(first);
    assert_eq!(storage.db.watchers.entry_count(), 1);
    drop(second);
    assert_eq!(storage.db.watchers.entry_count(), 0);
}

#[test]
fn invalidation_ignores_unregistered_keys() {
    let registry = MemoryWatchRegistry::default();
    registry.invalidate("alice", "notes");
    assert_eq!(registry.entry_count(), 0);
    registry.invalidate_scope("alice");
    assert_eq!(registry.entry_count(), 0);
}

#[tokio::test]
async fn concurrent_final_watch_drops_remove_the_registered_entry() {
    let storage = MemoryStorage::with_native_watch();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let first = docs
        .watch("notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();
    let second = docs
        .watch("notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();
    let registry = Arc::clone(&storage.db.watchers);
    let key = ("local".to_owned(), "notes".to_owned());
    assert_eq!(registry.subscription_count(&key), Some(2));
    let barrier = Arc::new(Barrier::new(3));
    *registry.drop_barrier.lock().unwrap() = Some(Arc::clone(&barrier));

    let first_drop = std::thread::spawn(move || drop(first));
    let second_drop = std::thread::spawn(move || drop(second));
    barrier.wait();
    first_drop.join().unwrap();
    second_drop.join().unwrap();

    assert_eq!(registry.entry_count(), 0);
    assert_eq!(registry.subscription_count(&key), None);
}

#[test]
fn registry_recovers_a_poisoned_internal_lock() {
    let registry = Arc::new(MemoryWatchRegistry::default());
    let poisoner = Arc::clone(&registry);
    let _ = std::thread::spawn(move || {
        let _entries = poisoner.entries.lock().unwrap();
        panic!("poison the private registry lock");
    })
    .join();

    assert_eq!(registry.entry_count(), 0);
}

#[test]
fn cleanup_leaves_a_replacement_sender_alone() {
    let registry = MemoryWatchRegistry::default();
    let key = ("alice".to_owned(), "notes".to_owned());
    let (old_sender, old_receiver) = watch::channel(());
    let old_sender = Arc::new(old_sender);
    drop(old_receiver);
    let (new_sender, _new_receiver) = watch::channel(());
    let new_sender = Arc::new(new_sender);
    registry.entries().insert(
        key.clone(),
        WatchEntry {
            sender: Arc::clone(&new_sender),
            subscriptions: 1,
        },
    );

    registry.unsubscribe(&key, &old_sender);
    assert_eq!(registry.entry_count(), 1);
}

#[test]
fn cleanup_handles_an_already_removed_entry() {
    let registry = MemoryWatchRegistry::default();
    let (sender, receiver) = watch::channel(());
    drop(receiver);

    registry.unsubscribe(&("alice".to_owned(), "notes".to_owned()), &Arc::new(sender));
    assert_eq!(registry.entry_count(), 0);
}

#[tokio::test]
async fn a_closed_signal_requires_reset_then_stops() {
    let storage = MemoryStorage::with_native_watch();
    let scope = Scope::local();
    let documents = MemoryDocuments::new(
        Arc::clone(&storage.db),
        scope.clone(),
        Arc::clone(&storage.clock),
        None,
        false,
    )
    .with_native_watch(true);
    let key = (scope.as_str().to_owned(), "notes".to_owned());
    let mut watch = storage
        .db
        .watchers
        .subscribe(key.clone(), documents, Duration::from_secs(1))
        .unwrap();
    let (replacement, _) = watch::channel(());
    let old_sender = std::mem::replace(&mut watch.sender, Arc::new(replacement));
    let old_entry = storage.db.watchers.entries().remove(&key).unwrap();
    drop(old_sender);
    drop(old_entry);

    assert!(matches!(
        watch.next().await.unwrap(),
        Some(crate::WatchNotice::ResetRequired {
            reason: crate::WatchReset::Invalidated,
            ..
        })
    ));
    assert!(watch.next().await.unwrap().is_none());
}
