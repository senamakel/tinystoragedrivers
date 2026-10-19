# Write fencing

**Status:** Implemented. **Owner:** storage ports.

## Problem

A lease names which node owns a piece of work (a SaaS profile, a queue
partition), but holding a lease does not stop a node that *lost* it from
writing. A holder that is paused (GC, a stalled VM, a partition) keeps running
its in-process work until it notices the loss. A host can check its lease
before every write, but the check and the write are two operations: a node
that passes the check and then pauses past the takeover lands one write on top
of the new holder's. Only the storage backend can make the check and the
write one atomic step.

## Goals and non-goals

Goals:

- An opt-in primitive that makes every write through a set of handles
  conditional, atomically and inside the driver, on a *guard document*
  (typically a lease record) still matching what the writer was granted.
- One typed refusal callers can branch on.
- Every driver enforces it or says it cannot, and the conformance suite checks
  both.
- No change for callers that do not opt in.

Non-goals:

- Lease acquisition, renewal or expiry policy. Those belong to the host.
- Fencing across independently connected backends. Guard and target must
  belong to the same connected backend.
- Clock-based expiry of the grant. A host that wants "and my grant has not
  expired" puts that in the filter it builds for each binding, or relies on
  the epoch, which a takeover always moves.

## Proposed behavior

```rust
pub struct Fence { /* scope, collection, id, filter */ }
impl Fence {
    pub fn new(scope: Scope, collection, id, filter: Filter) -> Self;
    pub fn epoch(scope: Scope, collection, id, field, epoch: u64) -> Self;
}

trait StorageBackend {
    // default: Err(Unsupported(Capability::Fencing))
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage>;
    // default: Err(Unsupported(Capability::Fencing)) unless generation 0
    fn for_scope_fenced_generation(&self, scope: &Scope, generation: ScopeGeneration,
        fence: &Fence) -> Result<ScopedStorage>;
}

ErrorKind::Fenced          // new, not retryable
Capability::Fencing
```

The guard lives in the same backend as the target. Its scope stays independent
of the target scope. The host chooses the fence when granting a handle; model
metadata does not choose a guard address. A host must propagate refusal rather
than replace native enforcement with a preflight read.

A fence *holds* while the document `collection/id` in the fence's scope
exists, is live (not expired under its collection's `ttl_field`), and matches
the filter.

Every write through handles from `for_scope_fenced` reads the guard in the same
atomic step as the write. When the fence does not hold, the write fails with
`ErrorKind::Fenced` and changes nothing, even when it would have changed
nothing anyway. The fence is checked before the write's own precondition.

Fenced writes: documents `put`, `delete`, `delete_where`, `claim`,
`atomic_batch`, `drop_collection`; streams `append`, `append_batch`,
`truncate_before`, `delete_stream`; blobs `put`, `delete`. Reads and
`ensure_collection` are not fenced. A driver that cannot make a write atomic
with the check refuses it with `Unsupported(Fencing)` instead of running it
unfenced.

An invalid fence (reserved or malformed collection, bad id, untranslatable
filter) is `InvalidInput` from `for_scope_fenced`.

## Per-driver guarantees

| Driver | Fencing | How | Limits |
| --- | --- | --- | --- |
| memory | yes | Checked under a shared native gate covering the guard and target states. | One process, by nature. |
| SQLite | yes | The guard is read inside the target write transaction. Directory-mode guards in other files are attached before BEGIN IMMEDIATE, locking both files; file-mode named guards use prefixed tables. | Holds across independent connections and processes. Only the target file is mutated. Guard attachments are removed after the operation. |
| file | yes | Guard check and target mutation hold both canonical directory locks in stable path order, including independently reopened directories; unrelated directories remain independent. | **Single process only**, like every other guarantee of this driver. |
| MongoDB | with transactions | Each fenced write runs in one multi-document transaction across guard and target collection prefixes that reads the guard and increments a hidden `_fence` counter on it, so a concurrent takeover write conflicts with the transaction. | Needs a replica set or sharded cluster. Fenced writes through one fence serialize on the guard document, and contention is retried (64 attempts) and then reported as `Unavailable`. Blob writes are refused with `Unsupported(Fencing)` because GridFS cannot join a transaction. |

## Invariants

- A refused fenced write changes no document, stream or blob.
- A write through a fenced handle never succeeds after a write that makes the
  guard stop matching has committed.
- `Fenced` is never retryable. The caller has lost the right to write and
  must re-acquire before binding a new fence.

## Acceptance

- `conformance::run` checks, on every driver: refusal while the guard is
  absent, success while it matches, refusal after it moves on (with no state
  change), the new holder's fence writing again, an expired guard (with
  `Ttl`), an invalid fence, and `Unsupported(Fencing)` without the capability.
- `conformance::run` also checks the compound fence `Lease::holder_fence`
  (`epoch == e AND holder == h AND released absent`, built with
  `Filter::and` and `Filter::exists`): on every driver with the capability,
  a write fenced by a tenure that was taken over, released with no successor,
  rewritten under another holder at the same epoch, or compacted away is
  refused, while the epoch-only `Lease::fence` still holds across a bare
  release.
- Persistent-driver tests cover reopening and independent SQLite connections.
  Mongo standalone never advertises the fence capability.
- Every source file keeps at least 90% line coverage.
