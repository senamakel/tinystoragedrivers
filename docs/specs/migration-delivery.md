# Confidential migration delivery

Migration transport must preserve the legacy full-payload runner's accepted
payload sizes and durable failure/resume behavior through a bounded-frame bus.
No total payload ceiling, public stream mechanism, business conversion or
storage write is introduced by the delivery layer.

The pure schema defines a version-one tagged snake_case exchange: Begin,
Upload, Execute, Download, Finish and Cancel. Payloads carry document pages,
metadata-only blob destination queries, metadata-only stream plans or ordered
independent stream entry groups. Every exchange is confidential, including
metadata, errors and lifecycle messages. The owning transport adapter negotiates
protocol version one before allocation and proves actual framework monitoring
exclusion independently of the generic helper tests.

Runtime-generated bounded tokens belong to trusted contextual peers. Unknown
and wrong-peer tokens have the same response. Upload and download offsets must
match exactly; each raw fragment is at most 128 KiB. Declared lengths never
cause preallocation. All error responses contain static categories only; owner,
backend, serde, source identifier and credential diagnostics never cross the
wire. Expired tokens become unknown, avoiding an unbounded tombstone registry;
the Expired vocabulary is reserved for adapters with peer-safe expiry knowledge.

A configurable admission limit defaults to 16 transfers. Receiving and ready
states expire after 60 seconds of inactivity, refreshed only by valid requests.
An owner-retained cleanup guard periodically expires state without another
request and retains only a weak state reference between ticks. Executing work
never expires from the idle TTL. Converters run outside the state mutex and
outside the async executor's control path. Cancel/disconnect discards executing
results while retaining admission until actual synchronous work ends. Dropping
an Execute waiter must not strand execution. Server-owned completion and an
execution finalizer cover successful, failed, panicking and abandoned queued jobs.
Dropping the server releases all its transient state.

Clients abandon ambiguous exchanges and attempt bounded confidential Cancel on
their runtime. Idle expiry covers failed cancellation. Documents use fragmented
complete requests/replies. Blob bytes and content type remain local and are
returned without copying. Identity streams keep their original entries locally.
Independent-entry streams map ordered groups of 256 and reconstruct the complete
result before the durable runner writes. Skips preserve legacy emitted offsets;
input resume offsets are never derived from output counts.

Frames are bounded, total memory is not. Whole JSON assembly, value decoding,
owner conversion and reconstructed streams retain full-payload memory behavior.
Fallible explicit buffer reserves and serialization failures return resumable
Unavailable failures. Value decoding and owner allocations retain their existing
allocator behavior. Source/target checks, writes, conflicts, leases, progress,
readiness, sealing and source cleanup remain the runner's responsibility.

Synthetic tests prove framing and lifecycle behavior. They cannot prove a real
transport's confidentiality. Owning compiled modules must test real trusted peer
context, confidential dispatch and absence of private payloads in monitoring.
