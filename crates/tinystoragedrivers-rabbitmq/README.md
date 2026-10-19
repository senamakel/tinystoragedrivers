# tinystoragedrivers-rabbitmq

`QueueStore` over RabbitMQ (AMQP 0-9-1, `lapin`). Queues only: RabbitMQ is not
a document store, so this crate is selected by its own `amqp://` / `amqps://`
URL (the facade's `rabbitmq` feature, `QueueConfig`, `open_queues`), never by
the storage URL.

- A scope's queue `q` is the durable classic queue `tsd/<hash>/q`
  (`x-max-priority: 9`), with `.../dead` for dead letters and 21 TTL wait
  queues `.../delay/<ms>` for delays and backoff.
- Leases live in the handle: a receipt works only on the handle that issued
  it, and the handle's own timer settles a lapsed lease. A crashed process's
  deliveries return when its channel closes.
- Features: priorities and delay (rounded up to a power of two, 256 ms to
  about 3.1 days). No groups and no dedupe; a message asking for either is
  refused with `InvalidInput`.

The full mapping and every limit are in
[`docs/specs/queue-rabbitmq.md`](../../docs/specs/queue-rabbitmq.md). Live
tests run with `TSD_RABBITMQ_URL` set:

```sh
docker run -d --rm --name tsd-rabbit -p 127.0.0.1:5672:5672 rabbitmq:4-management
TSD_RABBITMQ_URL='amqp://guest:guest@127.0.0.1:5672/%2f' \
  cargo test -p tinystoragedrivers-rabbitmq --test live
```
