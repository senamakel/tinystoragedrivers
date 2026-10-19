# QueueStore delivery plan

Specification: [`../specs/queue-store.md`](../specs/queue-store.md).

## Tasks

1. [x] Port and types in `crates/tinystoragedrivers-core/src/queue/`
   (`mod.rs`, `types.rs`), exported from `lib.rs`.
2. [x] `DocumentQueue` over any `DocumentStore`
   (`crates/tinystoragedrivers-core/src/queue/document.rs`), with collections
   `tsd.queue`, `tsd.queue_groups` and `tsd.queue_specs`.
3. [x] Conformance checks (`conformance/queue.rs`) wired into `run_ports`, so
   memory, SQLite, file and the bus client loopback run them.
3a. [x] Queue manifest: `queue/manifest.rs` (`QueueManifest`,
   `QueueDeclaration`), `schemas/queues.schema.json`,
   `docs/examples/queues.json`.
4. [x] MongoDB live job passes the queue checks.
5. [x] Storage bus: a `Queues` object and selectors in
   `tinystoragedrivers-bus` (contract version 5), `BusQueues` client,
   `StorageServer` selectors with manifest-bounded serving
   (`with_queue_manifest`), busproxy serving, and the clock-free
   `conformance::run_queue` across the loopback and a real bus.
6. [x] `tinystoragedrivers-rabbitmq` ([spec](../specs/queue-rabbitmq.md)):
   native queue backend behind the facade's `rabbitmq` feature and an
   `amqp://` queue URL, with live tests in a RabbitMQ CI job and the coverage
   job.
7. [x] `ScopedStorage::queues()` and facade wiring for a queue URL separate
   from the storage URL.
   - [x] `ScopedStorage::queues()` / `with_queues`.
   - [x] Facade `QueueConfig` and `open_queues(config, storage, manifest)`.

## Verification

```sh
cargo test -p tinystoragedrivers-core --all-features queue
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
TSD_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/%2f \
  cargo test -p tinystoragedrivers-rabbitmq --test live
```
