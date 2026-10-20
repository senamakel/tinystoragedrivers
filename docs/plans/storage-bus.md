# Storage bus implementation

Contract: [Storage bus](../specs/storage-bus.md). This plan is ordered; each
step can land as a separate upstream PR. No step claims a port complete until
its client, host adapter and shared conformance pass.

## Ordered work

1. **Documents wire contract.** In `crates/tinystoragedrivers-bus/src/`, add
   serde-only request/response shapes for every native Documents operation,
   including query cursor, claim, batches, search, watches and host schema
   operations. Collection names are explicit and checked against the captured
   grant. Reuse the established CRUD field spellings. Pin every public message
   shape with JSON roundtrip tests; reject unknown selectors and authority
   fields. Export the exact storage namespace and object constants. Verify the
   new wire module depends only on serde/serde_json; the crate currently retains
   its legacy TinyBus proxy dependency.
2. **Documents client and server.** Extend
   `crates/tinystoragedrivers-tinybus/src/` with a complete typed module client
   and host adapter over an already captured native handle. Add a memory-driver
   end-to-end test for every operation, conflicts, paging, capability errors,
   grant denial, fencing and absent-vs-failed outcomes. Run common Documents
   conformance through both surfaces. Do not advertise any missing capability.
3. **Watch and bulk transport.** Add TinyBus `SIGNAL` delivery for
   payload-free watch invalidation on the captured authority, and transfer
   payloads above 16 MiB with TinyBus Stream.
   Test stream binding, limit enforcement, cleanup after failure and reread
   semantics. Never send document payloads in watch notifications.
4. **Streams object.** Add versioned serde contract, typed client/host adapter,
   native conformance and conditional append coverage. Preserve dense offsets
   and truncation semantics.
5. **Blobs object.** Add versioned serde contract and same-port adapter. Route
   large data through TinyBus Stream, pin ranges/listing/prefix behavior and
   verify host-side S3 configuration forwarding without exposing endpoint or
   credentials to modules.
6. **Secrets object.** Add confidential-only contract and adapters backed by
   encrypted native `SecretStore`; verify no plaintext crosses non-confidential
   transport, and sanitized errors never reveal names or bytes.
7. **Security and full gates.** Exercise principal/grant/schema rights, stale
   fences, forbidden selectors, oversized payloads and unknown operations
   across compiled module boundaries. Run fmt, clippy, all-feature build/tests,
   docs and coverage. Update README and public docs alongside each step.

## Progress

Step 1 (Documents) and the Streams, Blobs and Secrets wire contracts plus
size limits and `CONTRACT_VERSION` are in `tinystoragedrivers-bus`. The bus
client (`BusStorage` and the port handles), the reference `StorageServer` and
the in-process `Loopback` are in `tinystoragedrivers-client`, and
`conformance::run_ports` passes through them (including a blob above the frame
cap over streams, and watch signals). Port gaps landed: keyset cursors
(already in the drivers), FTS ranking and reindex semantics, TTL visibility,
claim/CAS run-ledger patterns (all pinned by conformance), the blob tree view,
and the Vector decision (stays in TinyMemory). The S3 driver and its CI lane
already exist. Native blob expiry and its versioned migration wire selectors
are implemented in this TSD source; the remaining epic work includes a
`TinyBus` proxy transport, module-side server registration and release.

## Current step

Step 1 introduces data types and tests only. It does not claim that the
Documents bus client/server is complete, that CRUD selectors have been expanded
in the callback adapter, or that the other three objects are implemented.
