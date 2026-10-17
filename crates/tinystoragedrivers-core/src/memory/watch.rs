//! Native document invalidations for the opt-in test memory backend.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::watch;

use super::MemoryDocuments;
use crate::{DocumentWatch, ErrorKind, Result, StorageError, WatchNotice, WatchReset};

type WatchKey = (String, String);

/// Coalesced invalidation senders keyed by scope and collection.
#[derive(Debug, Default)]
pub(super) struct MemoryWatchRegistry {
    entries: Mutex<BTreeMap<WatchKey, WatchEntry>>,
    #[cfg(test)]
    drop_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
}

#[derive(Debug)]
struct WatchEntry {
    sender: Arc<watch::Sender<()>>,
    subscriptions: usize,
}

impl MemoryWatchRegistry {
    fn entries(&self) -> MutexGuard<'_, BTreeMap<WatchKey, WatchEntry>> {
        match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(super) fn subscribe(
        self: &Arc<Self>,
        key: WatchKey,
        documents: MemoryDocuments,
        interval: Duration,
    ) -> Result<MemoryWatch> {
        let mut entries = self.entries();
        let sender = if let Some(entry) = entries.get_mut(&key) {
            entry.subscriptions = entry
                .subscriptions
                .checked_add(1)
                .ok_or_else(|| StorageError::backend("memory watch count is exhausted"))?;
            Arc::clone(&entry.sender)
        } else {
            let (sender, receiver) = watch::channel(());
            drop(receiver);
            let sender = Arc::new(sender);
            entries.insert(
                key.clone(),
                WatchEntry {
                    sender: Arc::clone(&sender),
                    subscriptions: 1,
                },
            );
            sender
        };
        let receiver = sender.subscribe();
        drop(entries);
        Ok(MemoryWatch {
            documents,
            interval,
            key,
            registry: Arc::clone(self),
            sender,
            receiver,
            terminal: false,
        })
    }

    pub(super) fn invalidate(&self, scope: &str, collection: &str) {
        let key = (scope.to_owned(), collection.to_owned());
        let entries = self.entries();
        let Some(entry) = entries.get(&key) else {
            return;
        };
        entry.sender.send_replace(());
    }

    pub(super) fn invalidate_scope(&self, scope: &str) {
        let entries = self.entries();
        let keys: Vec<_> = entries
            .iter()
            .filter(|((entry_scope, _), _)| entry_scope == scope)
            .map(|(key, _)| key)
            .cloned()
            .collect();
        for key in keys {
            if let Some(entry) = entries.get(&key) {
                entry.sender.send_replace(());
            }
        }
    }

    fn unsubscribe(&self, key: &WatchKey, sender: &Arc<watch::Sender<()>>) {
        #[cfg(test)]
        {
            let has_other_subscriptions = {
                let entries = self.entries();
                entries.get(key).is_some_and(|entry| {
                    Arc::ptr_eq(&entry.sender, sender) && entry.subscriptions > 1
                })
            };
            self.wait_for_concurrent_test_drops(has_other_subscriptions);
        }
        let mut entries = self.entries();
        let remove = match entries.get_mut(key) {
            Some(current) if Arc::ptr_eq(&current.sender, sender) => {
                if current.subscriptions > 1 {
                    current.subscriptions -= 1;
                    false
                } else {
                    true
                }
            }
            _ => false,
        };
        if remove {
            entries.remove(key);
        }
    }

    #[cfg(test)]
    fn wait_for_concurrent_test_drops(&self, has_other_subscriptions: bool) {
        if !has_other_subscriptions {
            return;
        }
        let barrier = match self.drop_barrier.lock() {
            Ok(barrier) => barrier.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        if let Some(barrier) = barrier {
            barrier.wait();
        }
    }

    #[cfg(test)]
    pub(super) fn entry_count(&self) -> usize {
        self.entries().len()
    }

    #[cfg(test)]
    pub(super) fn subscription_count(&self, key: &WatchKey) -> Option<usize> {
        self.entries().get(key).map(|entry| entry.subscriptions)
    }
}

/// Owned subscription with no worker and no document payload.
pub(super) struct MemoryWatch {
    documents: MemoryDocuments,
    interval: Duration,
    key: WatchKey,
    registry: Arc<MemoryWatchRegistry>,
    sender: Arc<watch::Sender<()>>,
    receiver: watch::Receiver<()>,
    terminal: bool,
}

impl std::fmt::Debug for MemoryWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryWatch").finish_non_exhaustive()
    }
}

impl MemoryWatch {
    fn reset_if_generation_changed(&mut self) -> Result<Option<WatchNotice>> {
        match self.documents.validate_watch() {
            Ok(()) => Ok(None),
            Err(error) if error.kind() == ErrorKind::ScopeGenerationRejected => {
                self.terminal = true;
                Ok(Some(WatchNotice::ResetRequired {
                    reason: WatchReset::GenerationChanged,
                    source: Some(error),
                }))
            }
            Err(error) => Err(error),
        }
    }

    fn invalidated() -> WatchNotice {
        WatchNotice::Invalidated { cursor: None }
    }
}

#[crate::async_trait]
impl DocumentWatch for MemoryWatch {
    async fn next(&mut self) -> Result<Option<WatchNotice>> {
        if self.terminal {
            return Ok(None);
        }
        if let Some(reset) = self.reset_if_generation_changed()? {
            return Ok(Some(reset));
        }

        match tokio::time::timeout(self.interval, self.receiver.changed()).await {
            Ok(Ok(())) | Err(_) => {
                if let Some(reset) = self.reset_if_generation_changed()? {
                    return Ok(Some(reset));
                }
                Ok(Some(Self::invalidated()))
            }
            Ok(Err(_)) => {
                self.terminal = true;
                Ok(Some(WatchNotice::ResetRequired {
                    reason: WatchReset::Invalidated,
                    source: None,
                }))
            }
        }
    }
}

impl Drop for MemoryWatch {
    fn drop(&mut self) {
        self.registry.unsubscribe(&self.key, &self.sender);
    }
}

#[cfg(test)]
#[path = "watch_tests.rs"]
mod tests;
