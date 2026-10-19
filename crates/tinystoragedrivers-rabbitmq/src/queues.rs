//! [`RabbitQueues`]: the [`QueueStore`] over one `RabbitMQ` connection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use lapin::Acker;
use lapin::Channel;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::ShortString;
use tinystoragedrivers_core::queue::{
    DeadLetter, Delivery, Enqueued, MAX_PRIORITY, NewMessage, QueueFeatures, QueueSpec, QueueStats,
    QueueStore, Receipt, Retry, validate_queue_name,
};
use tinystoragedrivers_core::{Key, Result, Scope, StorageError};
use tokio::sync::Mutex as AsyncMutex;

use crate::envelope::{Envelope, MAX_ATTEMPTS};
use crate::link::{self, Link, Topology, amqp};
use crate::names::{self, Names};

/// How often the handle looks for lapsed leases.
const SWEEP_EVERY: Duration = Duration::from_millis(25);

/// What `RabbitMQ` gives a queue; see `docs/specs/queue-rabbitmq.md`.
const FEATURES: QueueFeatures = QueueFeatures {
    dedupe: false,
    groups: false,
    priorities: true,
    delay: true,
};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

fn poisoned() -> StorageError {
    StorageError::backend("rabbitmq handle lock poisoned")
}

/// One delivery this handle holds unacknowledged.
struct Lease {
    queue: String,
    acker: Acker,
    generation: u64,
    deadline: Instant,
    attempt: u32,
    priority: u8,
    envelope: Envelope,
}

/// What settling a lease does with its message.
enum Next {
    /// Deliver again, through the delay bucket if any, or dead-letter when
    /// out of attempts.
    Retry(Option<u64>),
    /// Dead-letter with this reason.
    Dead(String),
}

struct Inner {
    prefix: String,
    link: AsyncMutex<Link>,
    topology: Topology,
    leases: Mutex<HashMap<String, Lease>>,
    specs: Mutex<HashMap<String, QueueSpec>>,
}

/// A [`QueueStore`] on `RabbitMQ`, bound to one scope.
///
/// Every queue is a durable classic priority queue named after the scope (see
/// the crate docs). Leases live in this handle: a [`Receipt`] works only on
/// the handle that issued it, and a lease that lapses is settled by the
/// handle's own timer. Clones share the connection and the leases.
#[derive(Clone)]
pub struct RabbitQueues {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for RabbitQueues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitQueues")
            .field("prefix", &self.inner.prefix)
            .finish_non_exhaustive()
    }
}

impl RabbitQueues {
    /// Connect to the broker at `url` (`amqp://` or `amqps://`) and bind the
    /// handle to `scope`. Must be called inside a Tokio runtime: the handle
    /// runs a task that settles lapsed leases.
    ///
    /// # Errors
    ///
    /// `Unavailable` when the broker cannot be reached. The message never
    /// contains the URL.
    pub async fn connect(url: &str, scope: &Scope) -> Result<Self> {
        let link = Link::open(url).await?;
        let inner = Arc::new(Inner {
            prefix: names::prefix(scope),
            link: AsyncMutex::new(link),
            topology: Topology::default(),
            leases: Mutex::new(HashMap::new()),
            specs: Mutex::new(HashMap::new()),
        });
        tokio::spawn(sweep(Arc::downgrade(&inner)));
        Ok(Self { inner })
    }

    /// The broker queue that holds `queue`'s deliverable messages, for
    /// operators and tools that publish to it directly. Its dead letters are
    /// `<name>/dead`.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an invalid queue name.
    pub fn broker_name(&self, queue: &str) -> Result<String> {
        validate_queue_name(queue)?;
        Ok(Names::new(&self.inner.prefix, queue).main)
    }

    /// Close the connection without settling anything, as a process that
    /// dies does: the broker requeues every delivery this handle held.
    ///
    /// # Errors
    ///
    /// `Unavailable` when the close fails.
    pub async fn close(&self) -> Result<()> {
        self.inner.link.lock().await.close().await
    }
}

/// Settle lapsed leases until the handle is dropped.
async fn sweep(inner: Weak<Inner>) {
    loop {
        tokio::time::sleep(SWEEP_EVERY).await;
        let Some(inner) = inner.upgrade() else { return };
        let now = Instant::now();
        let due: Vec<Lease> = match inner.leases.lock() {
            Ok(mut leases) => {
                let tokens: Vec<String> = leases
                    .iter()
                    .filter(|(_, lease)| lease.deadline <= now)
                    .map(|(token, _)| token.clone())
                    .collect();
                tokens
                    .iter()
                    .filter_map(|token| leases.remove(token))
                    .collect()
            }
            Err(_) => return,
        };
        for lease in due {
            // A failure leaves the delivery unacknowledged; the broker
            // requeues it when the channel closes.
            let _ = inner.settle(lease, Next::Retry(None)).await;
        }
    }
}

impl Inner {
    fn leases(&self) -> Result<MutexGuard<'_, HashMap<String, Lease>>> {
        self.leases.lock().map_err(|_| poisoned())
    }

    /// The live channel and its generation. A reopened channel lost every
    /// lease of the old one (the broker requeued them), so they are dropped.
    async fn channel(&self) -> Result<(Channel, u64)> {
        let mut link = self.link.lock().await;
        if link.ensure().await? {
            let generation = link.generation();
            self.leases()?
                .retain(|_, lease| lease.generation == generation);
        }
        Ok((link.channel(), link.generation()))
    }

    async fn names(&self, channel: &Channel, queue: &str) -> Result<Names> {
        let names = Names::new(&self.prefix, queue);
        self.topology.declare(channel, &names).await?;
        Ok(names)
    }

    fn spec(&self, queue: &str) -> Result<QueueSpec> {
        Ok(self
            .specs
            .lock()
            .map_err(|_| poisoned())?
            .get(queue)
            .copied()
            .unwrap_or_default())
    }

    /// Take the lease `receipt` names, if this handle holds it.
    fn take(&self, queue: &str, receipt: &Receipt) -> Result<Option<Lease>> {
        let mut leases = self.leases()?;
        let held = leases
            .get(&receipt.token)
            .is_some_and(|lease| lease.queue == queue && lease.envelope.id == receipt.id);
        Ok(if held {
            leases.remove(&receipt.token)
        } else {
            None
        })
    }

    /// Publish the message's next state, then acknowledge the delivery.
    async fn settle(&self, lease: Lease, next: Next) -> Result<bool> {
        let (channel, generation) = self.channel().await?;
        if generation != lease.generation {
            return Ok(false);
        }
        let names = self.names(&channel, &lease.queue).await?;
        let mut envelope = lease.envelope;
        envelope.attempt = lease.attempt;
        match next {
            Next::Retry(delay) if lease.attempt < envelope.max_attempts => {
                let target = delay.map_or_else(|| names.main.clone(), |bucket| names.delay(bucket));
                link::publish(&channel, &target, &envelope, lease.priority).await?;
            }
            Next::Retry(_) => {
                envelope.reason = Some(MAX_ATTEMPTS.to_owned());
                link::publish(&channel, &names.dead, &envelope, 0).await?;
            }
            Next::Dead(reason) => {
                envelope.reason = Some(reason);
                link::publish(&channel, &names.dead, &envelope, 0).await?;
            }
        }
        lease
            .acker
            .ack(BasicAckOptions::default())
            .await
            .map_err(amqp)
    }

    /// Browse up to `limit` dead letters, handing each to `pick`, which
    /// returns whether to keep (acknowledge) it. Unkept ones are requeued in
    /// place.
    async fn browse_dead(
        &self,
        channel: &Channel,
        names: &Names,
        limit: usize,
        mut pick: impl FnMut(&Envelope) -> bool,
    ) -> Result<Vec<Envelope>> {
        let mut held = Vec::new();
        let mut kept = Vec::new();
        let mut result = Ok(());
        while held.len() + kept.len() < limit {
            let delivery = match link::get(channel, &names.dead).await {
                Ok(Some(delivery)) => delivery,
                Ok(None) => break,
                Err(error) => {
                    result = Err(error);
                    break;
                }
            };
            let id = delivery
                .properties
                .message_id()
                .as_ref()
                .map(ShortString::as_str);
            let envelope = Envelope::from_bytes(&delivery.data, id, now_ms());
            if pick(&envelope) {
                kept.push((delivery.acker, envelope));
            } else {
                held.push((delivery.acker, envelope));
            }
        }
        let requeue = BasicNackOptions {
            multiple: false,
            requeue: true,
        };
        let mut seen = Vec::with_capacity(held.len() + kept.len());
        for (acker, envelope) in held {
            acker.nack(requeue).await.map_err(amqp)?;
            seen.push(envelope);
        }
        result?;
        for (acker, envelope) in kept {
            link::publish(
                channel,
                &names.main,
                &Envelope {
                    attempt: 0,
                    reason: None,
                    ..envelope.clone()
                },
                0,
            )
            .await?;
            acker.ack(BasicAckOptions::default()).await.map_err(amqp)?;
            seen.push(envelope);
        }
        Ok(seen)
    }
}

fn check_message(message: &NewMessage) -> Result<()> {
    if message.priority > MAX_PRIORITY {
        return Err(StorageError::invalid_input(format!(
            "a message priority runs 0 to {MAX_PRIORITY}"
        )));
    }
    if message.group.is_some() {
        return Err(StorageError::invalid_input(
            "rabbitmq queues do not honor message groups (QueueFeatures::groups is false)",
        ));
    }
    if message.dedupe_key.is_some() {
        return Err(StorageError::invalid_input(
            "rabbitmq queues do not honor dedupe keys (QueueFeatures::dedupe is false)",
        ));
    }
    names::bucket(message.delay_ms)?;
    Ok(())
}

fn positive(value: u64, what: &str) -> Result<()> {
    if value == 0 {
        return Err(StorageError::invalid_input(format!(
            "{what} must be positive"
        )));
    }
    Ok(())
}

#[async_trait]
impl QueueStore for RabbitQueues {
    fn features(&self) -> QueueFeatures {
        FEATURES
    }

    /// Declares the durable topology and records the spec in this handle;
    /// `dedupe_window_ms` is ignored (no dedupe).
    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()> {
        validate_queue_name(queue)?;
        spec.validate()?;
        let (channel, _) = self.inner.channel().await?;
        self.inner.names(&channel, queue).await?;
        self.inner
            .specs
            .lock()
            .map_err(|_| poisoned())?
            .insert(queue.to_owned(), *spec);
        Ok(())
    }

    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>> {
        validate_queue_name(queue)?;
        for message in messages {
            check_message(message)?;
        }
        let spec = self.inner.spec(queue)?;
        let (channel, _) = self.inner.channel().await?;
        let names = self.inner.names(&channel, queue).await?;
        let mut out = Vec::with_capacity(messages.len());
        for message in messages {
            let envelope = Envelope {
                id: Key::ulid().as_str().to_owned(),
                payload: message.payload.clone(),
                attempt: 0,
                max_attempts: spec.max_attempts,
                enqueued_ms: now_ms(),
                reason: None,
            };
            let target = match names::bucket(message.delay_ms)? {
                Some(bucket) => names.delay(bucket),
                None => names.main.clone(),
            };
            link::publish(&channel, &target, &envelope, message.priority).await?;
            out.push(Enqueued {
                id: envelope.id,
                duplicate: false,
            });
        }
        Ok(out)
    }

    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        _consumer: &str,
    ) -> Result<Vec<Delivery>> {
        validate_queue_name(queue)?;
        positive(u64::from(max), "a receive batch")?;
        positive(visibility_ms, "a visibility window")?;
        let (channel, generation) = self.inner.channel().await?;
        let names = self.inner.names(&channel, queue).await?;
        let mut out = Vec::new();
        let max = usize::try_from(max).unwrap_or(usize::MAX);
        while out.len() < max {
            let Some(delivery) = link::get(&channel, &names.main).await? else {
                break;
            };
            let id = delivery
                .properties
                .message_id()
                .as_ref()
                .map(ShortString::as_str);
            let envelope = Envelope::from_bytes(&delivery.data, id, now_ms());
            let priority = delivery.properties.priority().unwrap_or(0);
            // A redelivery is a return after a channel closed with the
            // message unacknowledged: one more attempt it never settled.
            let attempt = envelope
                .attempt
                .saturating_add(1)
                .saturating_add(u32::from(delivery.redelivered));
            // Until it is handed out, the lease counts only the deliveries the
            // message already had, which is what a dead letter records.
            let lease = Lease {
                queue: queue.to_owned(),
                acker: delivery.acker,
                generation,
                deadline: Instant::now() + Duration::from_millis(visibility_ms),
                attempt: attempt.saturating_sub(1),
                priority,
                envelope,
            };
            if lease.envelope.is_foreign() {
                let reason = lease.envelope.reason.clone().unwrap_or_default();
                self.inner.settle(lease, Next::Dead(reason)).await?;
                continue;
            }
            if attempt > lease.envelope.max_attempts {
                self.inner.settle(lease, Next::Retry(None)).await?;
                continue;
            }
            let token = Key::ulid().as_str().to_owned();
            let delivered = Delivery {
                receipt: Receipt {
                    id: lease.envelope.id.clone(),
                    token: token.clone(),
                },
                payload: lease.envelope.payload.clone(),
                attempt,
                priority,
                group: None,
                enqueued_ms: lease.envelope.enqueued_ms,
                lease_until_ms: now_ms()
                    .saturating_add(i64::try_from(visibility_ms).unwrap_or(i64::MAX)),
            };
            self.inner
                .leases()?
                .insert(token, Lease { attempt, ..lease });
            out.push(delivered);
        }
        Ok(out)
    }

    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool> {
        validate_queue_name(queue)?;
        let Some(lease) = self.inner.take(queue, receipt)? else {
            return Ok(false);
        };
        lease
            .acker
            .ack(BasicAckOptions::default())
            .await
            .map_err(amqp)
    }

    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool> {
        validate_queue_name(queue)?;
        let next = match retry {
            Retry::After(ms) => Next::Retry(names::bucket(ms)?),
            Retry::DeadLetter(reason) => Next::Dead(reason),
        };
        let Some(lease) = self.inner.take(queue, receipt)? else {
            return Ok(false);
        };
        self.inner.settle(lease, next).await
    }

    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool> {
        validate_queue_name(queue)?;
        positive(visibility_ms, "a visibility window")?;
        let mut leases = self.inner.leases()?;
        Ok(match leases.get_mut(&receipt.token) {
            Some(lease) if lease.queue == queue && lease.envelope.id == receipt.id => {
                lease.deadline = Instant::now() + Duration::from_millis(visibility_ms);
                true
            }
            _ => false,
        })
    }

    /// `ready`, `delayed` and `dead` are the broker's counts; `leased` counts
    /// this handle's leases only.
    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        validate_queue_name(queue)?;
        let (channel, _) = self.inner.channel().await?;
        let names = self.inner.names(&channel, queue).await?;
        let mut delayed = 0;
        for (_, name) in names.delays() {
            delayed += link::count(&channel, &name).await?;
        }
        let leased = self
            .inner
            .leases()?
            .values()
            .filter(|lease| lease.queue == queue)
            .count() as u64;
        Ok(QueueStats {
            ready: link::count(&channel, &names.main).await?,
            delayed,
            leased,
            dead: link::count(&channel, &names.dead).await?,
        })
    }

    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>> {
        validate_queue_name(queue)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (channel, _) = self.inner.channel().await?;
        let names = self.inner.names(&channel, queue).await?;
        let seen = self
            .inner
            .browse_dead(&channel, &names, limit, |_| false)
            .await?;
        Ok(seen
            .into_iter()
            .map(|envelope| DeadLetter {
                id: envelope.id,
                payload: envelope.payload,
                attempt: envelope.attempt,
                reason: envelope.reason.unwrap_or_default(),
                group: None,
                enqueued_ms: envelope.enqueued_ms,
            })
            .collect())
    }

    /// Scans the whole dead queue for `id`, holding what it browses until it
    /// is done.
    async fn redrive(&self, queue: &str, id: &str) -> Result<bool> {
        validate_queue_name(queue)?;
        let (channel, _) = self.inner.channel().await?;
        let names = self.inner.names(&channel, queue).await?;
        let mut found = false;
        self.inner
            .browse_dead(&channel, &names, usize::MAX, |envelope| {
                let hit = !found && envelope.id == id;
                found |= hit;
                hit
            })
            .await?;
        Ok(found)
    }

    /// Purges the broker queues and drops this handle's own leases; another
    /// process's unacknowledged deliveries survive.
    async fn purge(&self, queue: &str) -> Result<u64> {
        validate_queue_name(queue)?;
        let (channel, _) = self.inner.channel().await?;
        let names = self.inner.names(&channel, queue).await?;
        let mut removed = link::purge(&channel, &names.main).await?;
        removed += link::purge(&channel, &names.dead).await?;
        for (_, name) in names.delays() {
            removed += link::purge(&channel, &name).await?;
        }
        let held: Vec<Lease> = {
            let mut leases = self.inner.leases()?;
            let tokens: Vec<String> = leases
                .iter()
                .filter(|(_, lease)| lease.queue == queue)
                .map(|(token, _)| token.clone())
                .collect();
            tokens.iter().filter_map(|t| leases.remove(t)).collect()
        };
        for lease in held {
            if lease
                .acker
                .ack(BasicAckOptions::default())
                .await
                .map_err(amqp)?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
}
