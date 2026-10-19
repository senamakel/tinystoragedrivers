# RabbitMQ queue backend

**Status:** Implemented. **Owner:** tinystoragedrivers. **Parent:**
[`queue-store.md`](queue-store.md).

## Problem

A deployment that already runs RabbitMQ wants its inbound queues there, not in
the document database. `tinystoragedrivers-rabbitmq` implements `QueueStore`
over AMQP 0-9-1 (`lapin`). RabbitMQ is a queue, not a document store, so it is
selected by its own URL, never by the storage URL.

## Mapping

| `QueueStore` | AMQP 0-9-1 |
| --- | --- |
| scope | name prefix `tsd/<h>/`, `<h>` = 16 hex digits of FNV-1a 64 of the scope string |
| queue `q` | durable classic queue `tsd/<h>/q`, `x-max-priority: 9` |
| dead letters | durable classic queue `tsd/<h>/q/dead` |
| delay / `Retry::After` | durable queues `tsd/<h>/q/delay/<ms>`, `x-message-ttl: <ms>`, dead-lettering to `tsd/<h>/q` through the default exchange |
| `receive` | `basic.get` (manual ack) up to `max` times; the lease lives in the handle |
| `ack` | `basic.ack` |
| `nack` / lapse | publish the next state (retry, delay or dead), then `basic.ack` the original |
| `stats` | passive `queue.declare` message counts |
| `purge` | `queue.purge` of the queue, its dead and delay queues, plus the handle's own leases |

`/` cannot occur in a queue name, so no queue's name collides with another's
dead or delay queue, and the scope hash keeps two scopes' queues apart.

Every message body is a JSON envelope the driver owns: `id`, `payload`, the
number of deliveries already counted (`attempt`), `max_attempts` (from the
enqueuing handle's spec), `enqueued_ms`, and `reason` once dead. The message
id is also the AMQP `message_id`; priority is the AMQP priority. A body that is
not an envelope (published by another tool) is dead-lettered on receipt with
reason `not a tinystoragedrivers message`.

### Leases and receipts

A delivery tag is valid only on the channel that received it, so a lease is
held by the handle: the receipt token maps to the tag in the handle's lease
table. A receipt works only on the handle that issued it; on any other handle
`ack`, `nack` and `extend` return `false`. The visibility window is a timer in
the handle: when it lapses the handle settles the message as a retry (or a
dead letter on its last attempt), exactly as a `nack` with no delay. If the
process dies, the channel closes and RabbitMQ requeues every unacknowledged
delivery itself; the next receiver sees `redelivered` and counts one extra
attempt. If the handle's channel closes while it runs, its leases are lost the
same way and the handle opens a fresh channel on its next call.

### Attempts and dead letters

Attempts are counted by the driver, not the broker: a retry republishes the
envelope with its count raised, because a requeue with `basic.nack` would
lose it (classic queues keep no delivery count). A delivery whose attempt
would exceed `max_attempts` is moved to the dead queue with reason
`max attempts exceeded`. Moving is publish-then-ack with publisher confirms,
so a crash between the two can duplicate a message, never lose it.

`dead_letters` browses the dead queue (`basic.get`, then `basic.nack` with
requeue, which keeps a classic queue's order). `redrive` scans the dead queue
for the id, republishes it with a fresh count and acknowledges the dead copy.
Both hold the dead messages they browse until they finish, so two concurrent
browsers can each see part of the dead queue.

### Delays

Each delay rounds **up** to a power of two milliseconds between 256 ms and
2^28 ms (about 3.1 days): 21 wait queues per queue, each with one TTL, so
expiry is first-in first-out and never blocked by a longer delay. A message
waits at least its delay and less than twice it. Longer delays are refused
with `InvalidInput`. A retried or delayed message re-enters at the back of its
priority level.

## Features

`QueueFeatures { dedupe: false, groups: false, priorities: true, delay: true }`.

- **dedupe**: AMQP has no keyed lookup. A message with a `dedupe_key` is
  refused with `InvalidInput` rather than silently stored twice; callers that
  need dedupe keep keys in a document store (the documented fallback).
- **groups**: single-active-consumer serializes a whole queue, not a group,
  and consistent-hash sharding needs a plugin and still lets two consumers
  hold one shard. A message with a `group` is refused with `InvalidInput`.
- **priorities**: 0 to 9 through `x-max-priority`, honored by `basic.get`.
- **delay**: through the wait queues above, rounded up.

## Other limits

- `stats.leased` counts this handle's leases only; other processes' leases
  are invisible to AMQP 0-9-1 without the management API.
  `ready`, `delayed` and `dead` are the broker's counts.
- `QueueSpec` lives in the handle (and in each envelope, as `max_attempts`),
  so every process should `ensure_queue` (or apply the same
  [`QueueManifest`](queue-store.md)) before enqueueing. `dedupe_window_ms` is
  accepted and ignored.
- `purge` cannot drop another process's unacknowledged deliveries; they are
  requeued if that process dies, or settled by it.

## Configuration

The facade parses a queue URL, separate from the storage URL, with
`QueueConfig::parse`: `storage` (or empty) keeps the scope's `DocumentQueue`,
and `amqp://…` / `amqps://…` select RabbitMQ (feature `rabbitmq`; without it
the URL is refused). `open_queues(&config, storage, manifest)` returns the
scope's `ScopedStorage` with its queue port replaced by a `RabbitQueues` and,
given a manifest path, applies the `QueueManifest`: `apply` calls
`ensure_queue`, which declares the durable topology. `QueueConfig` never
prints credentials. A storage-bus host serves those queues like any other
port.

## Acceptance

`conformance::run_queue` passes against a live broker
(`crates/tinystoragedrivers-rabbitmq/tests/live.rs`, `TSD_RABBITMQ_URL`), with
live tests for lease lapse, crash redelivery, delay buckets and foreign
messages. CI runs them in a RabbitMQ job and in the coverage job.
