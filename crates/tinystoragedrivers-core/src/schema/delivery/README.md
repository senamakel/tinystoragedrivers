# Migration delivery

Enable `migration-delivery` in core or the facade. `MigrationDeliveryClient`
implements the existing `TransformExecutor`. `MigrationExchangeServer` accepts
pure DTOs from `tinystoragedrivers-schema`, without depending on TinyBus.

Owners provide a stateless `MigrationPayloadHandler`, a monotonic `ExchangeClock`,
a unique runtime `TransferIds` generator and a `MigrationTransport`. Before
accepting an exchange, their adapter negotiates `MIGRATION_EXCHANGE_VERSION = 1`.
Every transport call must use the framework's confidential, monitor-excluded API.
The generic transport trait makes this an implementation requirement; it cannot
prove framework confidentiality. Obtain `PeerIdentity` from actual trusted
request context, never a request field or an invented global caller.

Modules construct the server before ordinary lazy initialization, retain its
`cleanup_guard` for their lifetime, and forward trusted disconnect events. The
guard periodically reclaims idle receiving/ready buffers without another request,
holds a weak reference between ticks and aborts on drop. Default idle TTL is 60
seconds; the default admission policy is 16 simultaneous exchanges, configurable
through the constructor. Executing converters have no TTL deadline.

The server binds each token to a peer, enforces exact next offsets and admits
only 128 KiB raw fragments. It never reserves an advertised total size. Explicit
buffer growth and JSON output serialization use fallible incremental reserves.
Whole JSON values and mapped streams still consume whole-payload memory, as the
legacy runner did; serde's value decoding and owner conversion retain their
ordinary allocation behavior. There is no new total payload ceiling.

Execute atomically claims one complete input. An owned blocking job performs
conversion without holding the state mutex or blocking async control messages.
Its finalizer releases an abandoned queued job, including runtime shutdown before
execution. Cancellation/disconnect marks an executing job discarded but retains
its admission slot until the synchronous converter actually returns. Dropping an
Execute waiter cannot strand execution, and completion cannot resurrect cancelled
state. Handler errors and panic outcomes become static wire errors. No panic
payload is serialized; process panic-hook policy remains the runtime owner's
responsibility.

Client abandonment schedules best-effort confidential Cancel on the current
runtime, bounded to one second. Idle TTL covers lost replies, lost cancellation,
and runtime shutdown. Ambiguous sequences always fail; the durable runner retries
the original operation. This layer neither writes data nor advances checkpoints.

The client maps an Execute `TransformFailed` reply to static `InvalidInput`
(`migration transform failed`). Transport failures, resource failures, invalid
replies and ambiguous protocol sequences become static `Unavailable`
(`migration delivery unavailable`). Owner error details never cross the wire;
the server replaces returned transform errors and panics with `TransformFailed`.
These failures leave the durable migration pending with its source retained.

Blob delivery sends only operation/version/source and retains the original byte
vector and content type locally. Identity streams return their original entries
without sending them. Only explicitly independent entry mappings use groups of
256 entries. The client reconstructs every group into a complete ordered result
before the existing runner writes, preserving skip behavior and emitted offsets.
Document pages and independent groups serialize completely, then fragment both
request and reply. Source validation, conflict handling, leases, cursor updates,
sealing and cleanup remain in the unified runner.

Tests enforce the actual serialized 16 MiB frame ceiling on a synthetic transport.
Actual TinyBus peer context, confidential calls and monitor exclusion require
separate compiled-owner integration tests.
