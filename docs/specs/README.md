# Specifications

Specifications define what the system must do before implementation details
take over. Create one for behavior that changes a public API, crosses module
boundaries, introduces a durable data format, or has meaningful operational
constraints.

Use a short kebab-case filename such as `retry-policy.md`. Each specification
should contain:

1. **Status and owner** — Draft, Accepted, Implemented, or Superseded.
2. **Problem** — the user or system need, without prescribing a solution.
3. **Goals and non-goals** — the exact boundary of the work.
4. **Proposed behavior** — public API, inputs, outputs, errors, and examples.
5. **Invariants and constraints** — properties every implementation must keep.
6. **Acceptance criteria** — externally observable pass/fail conditions.
7. **Open questions** — unresolved decisions that block acceptance.

After the specification is accepted, create a linked implementation plan in
[`../plans/`](../plans/README.md). Keep code snippets small enough to clarify
the contract; production code still belongs under `src/`.

See [`storage-ports.md`](storage-ports.md),
[`secret-storage.md`](secret-storage.md) and
[`write-fencing.md`](write-fencing.md) and
[`generation-admission-errors.md`](generation-admission-errors.md) and
[`schema-v2.md`](schema-v2.md), the uniform record schema.

The test-only memory watch helper is specified in
[`memory-watch-test-util.md`](memory-watch-test-util.md).

[Confidential migration delivery](migration-delivery.md) preserves full-payload
migration semantics through bounded transport frames.

[`queue-store.md`](queue-store.md) specifies the `QueueStore` port, and
[`queue-rabbitmq.md`](queue-rabbitmq.md) its RabbitMQ backend.
