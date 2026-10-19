//! [`DocumentQueue`]: the [`QueueStore`] every document driver gets for free.
//!
//! # Records
//!
//! Every message of every queue on a scope is one document in [`MESSAGES`],
//! with its `queue`, a `state` and a `ready_at` time whose meaning follows the
//! state:
//!
//! | `state`  | `ready_at` means                               |
//! | -------- | ---------------------------------------------- |
//! | `ready`  | when it becomes deliverable (delay, backoff)   |
//! | `leased` | when the lease lapses and it is deliverable again |
//! | `dead`   | when it was dead-lettered                      |
//! | `done`   | when its dedupe tombstone expires              |
//!
//! So "deliverable now" is one filter, `state in (ready, leased) and ready_at
//! <= now`, and [`receive`](QueueStore::receive) is the document port's atomic
//! [`claim`](crate::DocumentStore::claim) over it, highest `priority` then
//! lowest `seq` first. The claim cannot raise the attempt count (a merge patch
//! sets, it does not add), so the claimed record is rewritten once more under
//! its claimed version; a consumer that dies between the two writes leaves the
//! attempt uncounted, which only ever costs one extra delivery.
//!
//! A message with a dedupe key gets a deterministic id derived from it, so a
//! second enqueue is a conflicting create. Acknowledging such a message within
//! a dedupe window turns it into a `done` tombstone instead of deleting it;
//! [`DocumentQueue::prune`] removes expired tombstones.
//!
//! # Groups
//!
//! One record per `(queue, group)` in [`GROUPS`] names the message that holds
//! the group and until when. A message is leased only after it takes the group
//! (free, its own, or lapsed); otherwise the claim is reverted and the group is
//! skipped for the rest of the call. A nack with a delay keeps the group pinned
//! to the message until the retry is due, so later messages of the group do
//! not overtake it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::QueueStore;
use super::types::{
    DeadLetter, Delivery, Enqueued, NewMessage, QueueFeatures, QueueSpec, QueueStats, Receipt,
    Retry, validate_queue_name,
};
use crate::backend::ScopedStorage;
use crate::document::{CollectionSpec, DocumentStore, IndexSpec, Precondition, Query};
use crate::error::{ErrorKind, Result, StorageError};
use crate::filter::{Filter, Sort};
use crate::key::Key;
use crate::memory::{Clock, system_clock};
use crate::primitives::Repo;

/// The message collection.
pub const MESSAGES: &str = "tsd.queue";
/// The group-lock collection.
pub const GROUPS: &str = "tsd.queue_groups";
/// The queue-spec collection.
pub const SPECS: &str = "tsd.queue_specs";

const READY: &str = "ready";
const LEASED: &str = "leased";
const DEAD: &str = "dead";
const DONE: &str = "done";

/// How many times an enqueue re-reads a dedupe conflict before giving up.
const ENQUEUE_ATTEMPTS: u32 = 4;

/// The collections a [`DocumentQueue`] uses, with their indexes.
#[must_use]
pub fn collection_specs() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(MESSAGES).index(IndexSpec::new(
            "queue_due",
            ["queue", "state", "ready_at", "priority", "seq"],
        )),
        CollectionSpec::new(GROUPS),
        CollectionSpec::new(SPECS),
    ]
}

/// Declare [`collection_specs`] on `storage`'s scope. Idempotent.
///
/// # Errors
///
/// The first failing [`ensure_collection`](crate::DocumentStore::ensure_collection).
pub async fn ensure(storage: &ScopedStorage) -> Result<()> {
    for spec in collection_specs() {
        storage.documents().ensure_collection(&spec).await?;
    }
    Ok(())
}

/// The stored message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Body {
    queue: String,
    state: String,
    priority: u8,
    ready_at: i64,
    seq: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dedupe_key: Option<String>,
    attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    consumer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    enqueued_ms: i64,
    payload: Value,
}

impl Body {
    fn holds(&self, queue: &str, receipt: &Receipt) -> bool {
        self.queue == queue
            && self.state == LEASED
            && self.token.as_deref() == Some(receipt.token.as_str())
    }

    fn dead(self, now: i64, reason: impl Into<String>) -> Self {
        Self {
            state: DEAD.to_owned(),
            ready_at: now,
            token: None,
            consumer: None,
            reason: Some(reason.into()),
            ..self
        }
    }

    fn ready_at(self, at: i64) -> Self {
        Self {
            state: READY.to_owned(),
            ready_at: at,
            token: None,
            consumer: None,
            ..self
        }
    }
}

/// Which message holds a group, and until when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GroupLock {
    queue: String,
    owner: String,
    until_ms: i64,
}

/// A [`QueueStore`] over a [`DocumentStore`]: one document per message in
/// [`MESSAGES`], whose deliverable set is a single atomic
/// [`claim`](crate::DocumentStore::claim), plus one group lock per
/// `(queue, group)` in [`GROUPS`] and one spec per queue in [`SPECS`].
///
/// Time comes from an injectable [`Clock`] so tests drive leases and delays
/// without sleeping. Specs declared through this handle are cached in it; a
/// spec changed through another handle is seen by this one after
/// [`ensure_queue`](QueueStore::ensure_queue) is called on it again.
#[derive(Clone)]
pub struct DocumentQueue {
    docs: Arc<dyn DocumentStore>,
    messages: Repo<Body>,
    groups: Repo<GroupLock>,
    clock: Clock,
    specs: Arc<Mutex<HashMap<String, QueueSpec>>>,
    seq: Arc<Mutex<(u64, u64)>>,
}

impl std::fmt::Debug for DocumentQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentQueue")
            .field("docs", &self.docs)
            .finish_non_exhaustive()
    }
}

impl DocumentQueue {
    /// Queues on `docs`, timed by the system clock.
    ///
    /// # Panics
    ///
    /// Never: the collection names are valid constants.
    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "the collection names are valid constants"
    )]
    pub fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            docs: Arc::clone(docs),
            messages: Repo::new(docs, MESSAGES).expect("tsd.queue is a valid collection"),
            groups: Repo::new(docs, GROUPS).expect("tsd.queue_groups is a valid collection"),
            clock: Arc::new(system_clock),
            specs: Arc::default(),
            seq: Arc::default(),
        }
    }

    /// Time the queue with `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Remove `queue`'s expired dedupe tombstones and report how many went.
    ///
    /// # Errors
    ///
    /// An invalid name or a backend error.
    pub async fn prune(&self, queue: &str) -> Result<u64> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        self.docs
            .delete_where(
                MESSAGES,
                &in_queue(queue)
                    .and(Filter::eq("state", DONE))
                    .and(Filter::lte("ready_at", now)),
            )
            .await
    }

    fn now(&self) -> Result<i64> {
        i64::try_from((self.clock)()).map_err(|_| StorageError::invalid_input("clock out of range"))
    }

    fn lock_specs(&self) -> Result<std::sync::MutexGuard<'_, HashMap<String, QueueSpec>>> {
        self.specs
            .lock()
            .map_err(|_| StorageError::backend("queue spec cache poisoned"))
    }

    pub(super) async fn spec(&self, queue: &str) -> Result<QueueSpec> {
        if let Some(spec) = self.lock_specs()?.get(queue) {
            return Ok(*spec);
        }
        let spec = match self.docs.get(SPECS, queue).await? {
            Some(found) => serde_json::from_value(found.doc)?,
            None => QueueSpec::default(),
        };
        self.lock_specs()?.insert(queue.to_owned(), spec);
        Ok(spec)
    }

    /// A per-handle increasing sequence: enqueue order within one handle is
    /// exact, across handles it is millisecond order.
    fn next_seq(&self, now: i64) -> Result<String> {
        let mut seq = self
            .seq
            .lock()
            .map_err(|_| StorageError::backend("queue sequence poisoned"))?;
        let now = u64::try_from(now).unwrap_or(0);
        if now > seq.0 {
            *seq = (now, 0);
        } else {
            seq.1 += 1;
        }
        Ok(format!("{:013}{:08}", seq.0, seq.1))
    }

    fn after(now: i64, ms: u64) -> Result<i64> {
        i64::try_from(ms)
            .ok()
            .and_then(|ms| now.checked_add(ms))
            .ok_or_else(|| StorageError::invalid_input("queue delay out of range"))
    }

    async fn enqueue_one(&self, queue: &str, message: &NewMessage) -> Result<Enqueued> {
        message.validate()?;
        let now = self.now()?;
        let body = Body {
            queue: queue.to_owned(),
            state: READY.to_owned(),
            priority: message.priority,
            ready_at: Self::after(now, message.delay_ms)?,
            seq: self.next_seq(now)?,
            group: message.group.clone(),
            dedupe_key: message.dedupe_key.clone(),
            attempt: 0,
            token: None,
            consumer: None,
            reason: None,
            enqueued_ms: now,
            payload: message.payload.clone(),
        };
        let id = match &message.dedupe_key {
            Some(key) => Key::of([queue, "d", key.as_str()])?,
            None => Key::of([queue, "m", Key::ulid().as_str()])?,
        };
        let id = id.as_str().to_owned();
        let doc = serde_json::to_value(&body)?;
        for _ in 0..ENQUEUE_ATTEMPTS {
            match self
                .docs
                .put(MESSAGES, &id, doc.clone(), Precondition::Absent)
                .await
            {
                Ok(_) => {
                    return Ok(Enqueued {
                        id,
                        duplicate: false,
                    });
                }
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(error),
            }
            // Only a dedupe id can already exist. A live message or an
            // unexpired tombstone makes this a duplicate; an expired
            // tombstone is replaced.
            let Some(found) = self.messages.get(&id).await? else {
                continue;
            };
            if found.doc.state != DONE || found.doc.ready_at > now {
                return Ok(Enqueued {
                    id,
                    duplicate: true,
                });
            }
            match self
                .docs
                .put(
                    MESSAGES,
                    &id,
                    doc.clone(),
                    Precondition::Version(found.version),
                )
                .await
            {
                Ok(_) => {
                    return Ok(Enqueued {
                        id,
                        duplicate: false,
                    });
                }
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::conflict(
            "enqueue kept racing another writer of the same dedupe key",
        ))
    }

    /// Take `group` for message `id` until `until`. Free, held by `id` itself
    /// or lapsed: taken. Held by another message: refused.
    async fn take_group(
        &self,
        queue: &str,
        group: &str,
        id: &str,
        until: i64,
        now: i64,
    ) -> Result<bool> {
        let key = group_key(queue, group)?;
        let taken = self
            .groups
            .update(&key, |current| {
                Ok(match current {
                    Some(lock) if lock.owner != id && lock.until_ms > now => None,
                    _ => Some(GroupLock {
                        queue: queue.to_owned(),
                        owner: id.to_owned(),
                        until_ms: until,
                    }),
                })
            })
            .await?;
        Ok(taken.is_some())
    }

    /// Keep `group` pinned to `id` until `until`, if `id` still holds it.
    async fn pin_group(&self, queue: &str, group: &str, id: &str, until: i64) -> Result<()> {
        let key = group_key(queue, group)?;
        self.groups
            .update(&key, |current| {
                Ok(current
                    .filter(|lock| lock.owner == id)
                    .map(|lock| GroupLock {
                        until_ms: until,
                        ..lock
                    }))
            })
            .await?;
        Ok(())
    }

    /// Free `group` if `id` holds it.
    async fn free_group(&self, queue: &str, group: &str, id: &str) -> Result<()> {
        let key = group_key(queue, group)?;
        self.groups.delete_if(&key, |lock| lock.owner == id).await?;
        Ok(())
    }

    async fn free_group_of(&self, queue: &str, body: &Body, id: &str) -> Result<()> {
        match &body.group {
            Some(group) => self.free_group(queue, group, id).await,
            None => Ok(()),
        }
    }

    /// Rewrite the message `receipt` names, if the receipt still holds it.
    /// `next` sees the held body and returns the body to store. Returns the
    /// body that was replaced, or `None` when the receipt lost the message.
    async fn settle<F>(&self, queue: &str, receipt: &Receipt, mut next: F) -> Result<Option<Body>>
    where
        F: FnMut(Body) -> Body + Send,
    {
        let mut before = None;
        let stored = self
            .messages
            .update(&receipt.id, |current| {
                before = None;
                Ok(match current {
                    Some(body) if body.holds(queue, receipt) => {
                        before = Some(body.clone());
                        Some(next(body))
                    }
                    _ => None,
                })
            })
            .await?;
        Ok(stored.and(before))
    }
}

fn in_queue(queue: &str) -> Filter {
    Filter::eq("queue", queue)
}

fn group_key(queue: &str, group: &str) -> Result<String> {
    Ok(Key::of([queue, group])?.as_str().to_owned())
}

fn check_window(what: &str, ms: u64) -> Result<()> {
    if ms == 0 {
        return Err(StorageError::invalid_input(format!(
            "a {what} must be positive"
        )));
    }
    Ok(())
}

#[async_trait]
impl QueueStore for DocumentQueue {
    fn features(&self) -> QueueFeatures {
        QueueFeatures {
            dedupe: true,
            groups: true,
            priorities: true,
            delay: true,
        }
    }

    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()> {
        validate_queue_name(queue)?;
        spec.validate()?;
        self.docs
            .put(
                SPECS,
                queue,
                serde_json::to_value(spec)?,
                Precondition::None,
            )
            .await?;
        self.lock_specs()?.insert(queue.to_owned(), *spec);
        Ok(())
    }

    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>> {
        validate_queue_name(queue)?;
        let mut out = Vec::with_capacity(messages.len());
        for message in messages {
            out.push(self.enqueue_one(queue, message).await?);
        }
        Ok(out)
    }

    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> Result<Vec<Delivery>> {
        validate_queue_name(queue)?;
        if max == 0 {
            return Err(StorageError::invalid_input("receive at least one message"));
        }
        check_window("visibility window", visibility_ms)?;
        let spec = self.spec(queue).await?;
        let now = self.now()?;
        let until = Self::after(now, visibility_ms)?;
        let due = in_queue(queue)
            .and(Filter::one_of("state", [READY, LEASED]))
            .and(Filter::lte("ready_at", now));
        let order = [Sort::desc("priority"), Sort::asc("seq")];
        let wanted = usize::try_from(max).unwrap_or(usize::MAX);
        let mut skipped: Vec<String> = Vec::new();
        let mut out = Vec::new();
        while out.len() < wanted {
            let filter = if skipped.is_empty() {
                due.clone()
            } else {
                due.clone()
                    .and(Filter::one_of("group", skipped.iter().map(String::as_str)).negate())
            };
            let token = Key::ulid().as_str().to_owned();
            let patch = json!({
                "state": LEASED,
                "ready_at": until,
                "token": token,
                "consumer": consumer,
            });
            let Some(claimed) = self.docs.claim(MESSAGES, &filter, &order, &patch).await? else {
                break;
            };
            let id = claimed.id;
            let version = claimed.version;
            let body: Body = serde_json::from_value(claimed.doc)?;

            if let Some(group) = body.group.clone()
                && !self.take_group(queue, &group, &id, until, now).await?
            {
                // Another message holds the group: give this one back as it
                // was found (deliverable now) and skip the group.
                let back = serde_json::to_value(body.ready_at(now))?;
                match self
                    .docs
                    .put(MESSAGES, &id, back, Precondition::Version(version))
                    .await
                {
                    Ok(_) => {}
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(error),
                }
                skipped.push(group);
                continue;
            }

            let attempt = body.attempt.saturating_add(1);
            if attempt > spec.max_attempts {
                let dead = body.clone().dead(now, "max attempts exceeded");
                match self
                    .docs
                    .put(
                        MESSAGES,
                        &id,
                        serde_json::to_value(&dead)?,
                        Precondition::Version(version),
                    )
                    .await
                {
                    Ok(_) => self.free_group_of(queue, &body, &id).await?,
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(error),
                }
                continue;
            }

            let leased = Body { attempt, ..body };
            match self
                .docs
                .put(
                    MESSAGES,
                    &id,
                    serde_json::to_value(&leased)?,
                    Precondition::Version(version),
                )
                .await
            {
                Ok(_) => out.push(Delivery {
                    receipt: Receipt { id, token },
                    payload: leased.payload,
                    attempt,
                    priority: leased.priority,
                    group: leased.group,
                    enqueued_ms: leased.enqueued_ms,
                    lease_until_ms: until,
                }),
                // Only a writer that saw the lease lapse can get in here, so
                // the message is someone else's now.
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(error),
            }
        }
        Ok(out)
    }

    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool> {
        validate_queue_name(queue)?;
        let spec = self.spec(queue).await?;
        let now = self.now()?;
        let Some(found) = self.messages.get(&receipt.id).await? else {
            return Ok(false);
        };
        if !found.doc.holds(queue, receipt) {
            return Ok(false);
        }
        let body = found.doc;
        let settled = if body.dedupe_key.is_some() && spec.dedupe_window_ms > 0 {
            let tombstone = Body {
                state: DONE.to_owned(),
                ready_at: Self::after(now, spec.dedupe_window_ms)?,
                token: None,
                consumer: None,
                payload: Value::Null,
                ..body.clone()
            };
            self.docs
                .put(
                    MESSAGES,
                    &receipt.id,
                    serde_json::to_value(&tombstone)?,
                    Precondition::Version(found.version),
                )
                .await
                .map(|_| true)
        } else {
            self.docs
                .delete(MESSAGES, &receipt.id, Precondition::Version(found.version))
                .await
        };
        match settled {
            Ok(true) => {
                self.free_group_of(queue, &body, &receipt.id).await?;
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(error) if error.kind() == ErrorKind::Conflict => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool> {
        validate_queue_name(queue)?;
        let spec = self.spec(queue).await?;
        let now = self.now()?;
        let retry_at = match &retry {
            Retry::After(ms) => Some(Self::after(now, *ms)?),
            Retry::DeadLetter(_) => None,
        };
        let stored = self
            .settle(queue, receipt, |body| match (&retry, retry_at) {
                (Retry::After(_), Some(at)) if body.attempt < spec.max_attempts => {
                    body.ready_at(at)
                }
                (Retry::DeadLetter(reason), _) => body.dead(now, reason.clone()),
                _ => body.dead(now, "max attempts exceeded"),
            })
            .await?;
        let Some(before) = stored else {
            return Ok(false);
        };
        let retried = matches!(retry, Retry::After(_)) && before.attempt < spec.max_attempts;
        if let Some(group) = &before.group {
            match retry_at {
                Some(at) if retried => self.pin_group(queue, group, &receipt.id, at).await?,
                _ => self.free_group(queue, group, &receipt.id).await?,
            }
        }
        Ok(true)
    }

    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool> {
        validate_queue_name(queue)?;
        check_window("visibility window", visibility_ms)?;
        let until = Self::after(self.now()?, visibility_ms)?;
        let stored = self
            .settle(queue, receipt, |body| Body {
                ready_at: until,
                ..body
            })
            .await?;
        let Some(before) = stored else {
            return Ok(false);
        };
        if let Some(group) = &before.group {
            self.pin_group(queue, group, &receipt.id, until).await?;
        }
        Ok(true)
    }

    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        let count = |filter: Filter| async move {
            self.docs
                .count(MESSAGES, &in_queue(queue).and(filter))
                .await
        };
        let due = Filter::lte("ready_at", now);
        let later = Filter::gt("ready_at", now);
        Ok(QueueStats {
            ready: count(Filter::one_of("state", [READY, LEASED]).and(due)).await?,
            delayed: count(Filter::eq("state", READY).and(later.clone())).await?,
            leased: count(Filter::eq("state", LEASED).and(later)).await?,
            dead: count(Filter::eq("state", DEAD)).await?,
        })
    }

    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>> {
        validate_queue_name(queue)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let page = self
            .messages
            .query(
                &Query::filter(in_queue(queue).and(Filter::eq("state", DEAD)))
                    .sort(Sort::asc("seq"))
                    .limit(limit),
            )
            .await?;
        Ok(page
            .items
            .into_iter()
            .map(|found| DeadLetter {
                id: found.id,
                payload: found.doc.payload,
                attempt: found.doc.attempt,
                reason: found.doc.reason.unwrap_or_default(),
                group: found.doc.group,
                enqueued_ms: found.doc.enqueued_ms,
            })
            .collect())
    }

    async fn redrive(&self, queue: &str, id: &str) -> Result<bool> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        let stored = self
            .messages
            .update(id, |current| {
                Ok(current
                    .filter(|body| body.queue == queue && body.state == DEAD)
                    .map(|body| Body {
                        attempt: 0,
                        reason: None,
                        ..body.ready_at(now)
                    }))
            })
            .await?;
        Ok(stored.is_some())
    }

    async fn purge(&self, queue: &str) -> Result<u64> {
        validate_queue_name(queue)?;
        let removed = self.docs.delete_where(MESSAGES, &in_queue(queue)).await?;
        self.docs.delete_where(GROUPS, &in_queue(queue)).await?;
        Ok(removed)
    }
}

#[cfg(test)]
#[path = "document_tests.rs"]
mod tests;
