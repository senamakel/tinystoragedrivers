# Governed storage endpoint

**Status:** Accepted. **Owner:** `tinystoragedrivers-busproxy` and
`tinystoragedrivers-tinybus`. **Issue:**
[`senamakel/tinystoragedrivers#2`](https://github.com/senamakel/tinystoragedrivers/issues/2).
**Plan:** [`../plans/governed-storage-endpoint.md`](../plans/governed-storage-endpoint.md).

## Problem

`PerCallerEndpoint` currently creates one storage server per authenticated bus
sender. Its synchronous binder runs while a global map lock is held, the
binding identity does not include scope or backend identity, and policy is
applied only when that server is built. The host also needs a lifecycle-aware
TinyBus attachment and a structural way to carry a host-selected tenant/profile
context to storage without accepting a scope from request data or ambient task
state.

The endpoint must remain useful to existing single-context callers while
supporting independently authorized storage leases on one authenticated peer.
It must reuse the published `StorageGrant` behavior, including exact secret
selectors, rather than introducing a second authorization implementation.

## Goals and non-goals

Goals:

- Preserve `PerCallerEndpoint::new`, existing `StorageEndpoint` implementors,
  released `StorageCall` literals, `BusTransport::connect`, and fixed-root
  callers.
- Add asynchronous binding without awaiting while holding the shared binding
  map lock, and gate each request, stream operation, and watch by current
  authority.
- Bind by authenticated module, canonical resolved scope, backend identity,
  and authority epoch. Recheck identity and revocation before publishing a
  slow binding.
- Add a governed endpoint and lifecycle-owned `attach_storage` integration
  on TinyBus stateful linked setup. Resolve the caller through TinyBus's
  authoritative sender-to-module lookup.
- Carry explicit, host-created, revocable storage context through ordinary
  calls, streamed blobs, and watches. Do not accept tenant/profile names as
  authority from request JSON.
- Keep errors and audit records content-free, retain bounded streaming, and
  clean up bindings, watches, streams, and registration tasks on release.

Non-goals:

- OpenHuman-specific scope naming, lease policy, fences, backend selection,
  or audit policy. Those remain in the host adapter.
- A generic TinyBus tenant header or implicit propagation of an OpenHuman
  profile through an unchanged global storage route.
- Claiming that this endpoint alone completes OpenHuman #7608. Shared modules
  must first consume a host-issued bound descriptor and retain per-lease
  instances; process-global storage reconfiguration is not multi-profile
  isolation.
- Replacing native backend fencing with a host preflight check or claiming
  that dropping a task rolls back an admitted write.

## Proposed behavior

### Compatibility and binding

`PerCallerEndpoint::new(sync_bind)` and its current endpoint behavior remain
available. An additive asynchronous binder shares the endpoint's binding
machinery. No call to a binder or other awaitable operation occurs while the
shared binding-map mutex is held. Concurrent first calls may share one pending
bind or may race to build, but only the current binding epoch and backend
identity may be published; losing builds are discarded safely. The additive
`PerCallerEndpoint::new_async` accepts an owned unique peer and an awaitable
binder. Its caller cache defaults to 1024 peers and its concurrent bind limit
defaults to 64; both are configurable. At capacity it evicts only a binding
with no active operation, no retained download reader, and no open watches.
If every entry is active, a new peer is refused with a content-free capacity
error. `release` invalidates in-flight binds as well as removing the cached
entry.

The governed binding identity contains the authenticated module id, canonical
scope resolved by trusted host code, backend identity/version, and lease or
binding epoch. The endpoint defaults to 1024 live or pending leases per
registration and 64 concurrent asynchronous backend resolutions. Both limits
are configurable; pressure fails closed. Each resolution has a monotonic
ticket, and the endpoint verifies the ticket and backend epoch after a backend
operation before returning output or publishing a watch/download. Release or
detachment invalidates the identity and prevents late work from restoring it.

### Governance

`GovernedEndpoint` receives an `Environment`, `AuditSink`, stream ceiling, and
cache limits. `attach_storage` creates an owned
`Arc<dyn CallerDirectory + Send + Sync + 'static>` from
`host.sender_lookup()` and stores it in the TSD registration/lease registry.
The TinyBus adapter wraps a cloned `ModuleSenderLookup`, a weak handle that
shares `ModuleHost::module_for_sender`'s live-admission predicate and returns
`None` after the host is dropped. The environment resolves installed backend
identity, trusted module scope, declared `StorageGrant`, secret keys, and
per-call write admission. It uses the canonical grant's `authorize` and
`filter_reply` for ordinary methods and stream hooks. Host code initializes
only the declared collection schemas before exposing a binding; requests
cannot widen declarations.

Every method and stream operation is admitted against the current lease and
grant. Secret calls retain confidential routing and require an admitted
recipient. Prefix and exact-secret authorization retain the published grant
semantics: exact-only grants allow exact Get/Set/Delete names, never listing,
and exact names do not become prefixes.

### Explicit scope context

The host supplies a typed `HostStorageContext<C>` when it registers a lease.
TSD stores that context in the registration and passes it to
`Environment::resolve(context, module)`; it never derives profile identity
from ambient task state. Registration also takes an explicit intended unique
peer. The owned caller-directory handle resolves it through
`ModuleSenderLookup::module_for_sender(&peer) -> Option<ModuleId>`. TSD stores
the canonical module id beside the peer and context. Each call and delayed bind
publication rechecks the registration and current lookup, then matches the
registered root/id, authenticated peer, and module id. Detachment invalidates
the peer's leases.

The serializable `StorageDescriptor` is host-issued routing configuration. It
identifies the serving connection's authenticated unique service peer, a
registration id, and the exact object root. It contains no host context, scope,
database, backend, grant, or secret. Possessing or forging the descriptor
alone grants no authority; the server registry must find the live registration
and match the broker-authenticated caller peer and current admitted module
before dispatch. The host retains a separate opaque `StorageLease` revocation
handle; the module receives only the descriptor.

`BusTransport::connect_bound(connection, descriptor)` pins every request and
stream to the descriptor's unique service peer. It does not resolve
`HOST_NAME` or fall back to `connect` on a bound path. Documents, streams,
blobs, confidential Secrets, upload stream opens/writes, download requests and
accepted streams, and watch signal sender checks all use that same peer. TSD
registers the same
`STORAGE_INTERFACE` at four exact object paths on the same captured, attested
linked `Connection`, without prefix matching or root-magic routing. It returns
the descriptor only after all four exports are installed.

The original object-safe `StorageEndpoint` trait and `StorageCall` remain
source-compatible. A typed per-lease adapter captures the host context and
lease, implements the existing trait, and passes internal contextual calls to
the governed endpoint through calls, upload/download streams, and watch
routing. Do not add generic context methods to `dyn StorageEndpoint`.
`HostStorageContext<C>::new(context)` creates the host wrapper and `as_ref()`
borrows its opaque value for `Environment::resolve(context, module)`. A
request-body profile or scope field has no authority.

A legacy fixed-root governed endpoint may be created with one explicit,
trusted single-context authority. It is usable only when the host guarantees
that every call on that route belongs to that context. The route sees the
calling module peer; it cannot detect or reject a request that represents a
different business profile. Where the host cannot guarantee single-context
use, the fixed route is unavailable and the caller must use a bound lease
descriptor. Missing or revoked bound context fails before schema
initialization, key access, or backend access. There is no silent fallback to
`Scope::local()`.

### Lifecycle, streams, watches, and audit

`tinystoragedrivers_tinybus::attach_storage(host, &observer_connection,
endpoint, runtime)` owns the U5 linked attachment and returns a TSD-owned
`StorageRegistration`. Internally it calls `host.sender_lookup()`, invokes
`ModuleHost::attach_linked_with_setup` for TSD's static linked export, and
captures the exact service `Connection` supplied to its setup callback. That
attested linked connection serves the same `STORAGE_INTERFACE` at four exact
per-lease object paths and is the service peer named in each descriptor. The
helper transfers the completed registration from setup through an owned
one-shot channel and returns it only after TinyBus reports module readiness and
service setup succeeds. TinyBus `ModuleInfo` is internal admission state, not
the public registration owner. The host simply retains the returned TSD owner.

The owner retains the optional legacy `ServedStorage`, captured linked
`Connection`, lease registry, signal receiver, detach monitor, and watch
relays. Its explicit async shutdown revokes leases, stops and drains tasks,
removes only its per-lease paths, shuts down the optional fixed-root service,
and closes the linked connection after cleanup. Closing that connection
withdraws its bus peer; it does not assert or replace the enclosing
`ModuleHost` admission state. The caller retains `ModuleHost` and owns that
lifecycle. Dropping the registration also schedules cleanup on the captured
runtime. `observer_connection` is borrowed; its caller retains ownership and
lifetime. TSD installs a broker match and owns only the
returned signal receiver and monitor for the linked service peer and admitted
caller peers; the observer never serves storage or supplies caller authority.
Setup cancellation, attach failure, and failed one-shot transfer roll back
partial exports and close the owned linked connection.

Each `StorageRegistration::register_lease(context, peer)` is an owned
async transaction: resolve the live unique peer through
`self.callers.module_for_sender(peer)`; allocate an opaque registry id and four exact
per-lease paths; export all four objects; then publish the context/peer/module
mapping and return `(StorageLease, StorageDescriptor)`. The descriptor is
returned only after every path is served. A failure or cancellation before
publication rolls back every export owned by that transaction. The descriptor
is never returned for a partial registration.
TinyBus U5 `ModuleHost::attach_linked_with_setup` returns `ModuleInfo`; it does
not own or represent the TSD registration. `ModuleRuntime::serve_host_object`
returns an ordinary `HostObjectGuard` and grants no module attestation or
confidential-call authority, so it is not a substitute for this governed
registration. Runtime hops capture authority before scheduling work and check
the binding epoch before publishing results.

Uploads use `accept_stream_from`, enforce the declared byte count and a
configurable ceiling (256 MiB by default), and do not buffer an entire blob.
Downloads remain directed to the authenticated peer, and a returned download
reader pins its cache entry until the stream is consumed or dropped. Watch
identities and relays are scoped by lease and peer. Shared backend signals are
filtered by the lease-owned watch-id set; a `WatchClose` for another lease's
handle is refused before reaching the shared server. If a delayed `WatchOpen`
completes after revocation or epoch change, the native watch is closed and no
handle is returned. A binding replacement resets and closes native watches
owned by the old binding. Release invalidates pending binds, streams, watch
relays, and binding epochs. Detach is recognized only for an authenticated
unique peer; lagged observer streams trigger authoritative liveness
reconciliation before monitoring continues.

Audit records contain closed outcomes and authorized metadata only. They
exclude request documents, secret names and values, keys, tokens, raw driver
errors, and source chains. Backend failures and panics become content-free
failures. Runtime cancellation is not represented as a guarantee that a
started write was rolled back; backend fencing remains the authority for
atomic write admission.

## Invariants and constraints

- Existing public calls and implementors continue to compile unchanged.
- Binding and lease authority derive only from authenticated TinyBus identity
  and host-created registrations, never module-supplied ids or request fields.
- Each lease captures a typed host context plus an intended unique peer and
  canonical module id. Distinct contexts for one peer remain distinct, while a
  peer holding both descriptors may select either registered route.
- A descriptor is serializable routing data, not authority. The registry and
  authenticated peer/module match authorize each use.
- A lease descriptor is visible only after all four exact-path exports have
  been installed. Partial/cancelled registration rolls back its owned exports.
- The full binding key includes module, canonical scope, backend identity, and
  epoch; stale asynchronous work cannot publish after invalidation.
- No lock is held across an await, binder callback, backend call, or runtime
  hop.
- Grant authorization and reply filtering are identical for normal calls,
  streamed calls, and watch-related storage operations.
- No collection schema is created or expanded from a module request.
- Exact secret names stay exact; list permission still requires a declared
  prefix. Secret values and key material never enter logs or audits.
- A scoped registration cannot be used by a different authenticated peer or
  after revocation. Context is resolved before any backend/key/schema access.
- Blob streaming stays bounded and peer-bound. Watches cannot cross leases,
  even when two leases share one module peer.
- Existing fixed-root and `serve_collection` behavior remains supported.
- The pure `tinystoragedrivers-bus` crate remains transport-free and builds at
  Rust 1.85; implementation crates remain at the workspace Rust 1.88 floor.
- TinyBus producer and consumer use one canonical Git URL/revision and one
  SDK type identity. The endpoint must not copy TinyBus authority types.

## Acceptance criteria

1. Existing `PerCallerEndpoint::new`, object-safe `StorageEndpoint`
   implementations, `Arc<dyn StorageEndpoint>` usage, `StorageCall` and
   `StorageNeeds` struct literals, `BusTransport::connect`, and fixed-root
   consumers retain source and runtime behavior. A typed per-lease adapter
   implements the unchanged trait; no generic context methods are added to it.
2. A deterministic concurrency test proves a blocked bind for module A does
   not block module B, and that no late bind can replace a newer backend or
   recreate a released epoch.
3. Tests prove every call and stream hook checks current write admission and
   grant authorization, filters replies, refuses undeclared schema/targets,
   and preserves exact-secret semantics.
4. A two-lease test on one authenticated peer uses distinct typed host
   contexts, descriptors, and scopes. Each descriptor routes document, stream,
   blob, secret, and watch operations to its captured context; request-body
   scope data cannot redirect a call; revoking A leaves B usable and prevents
   A's delayed bind/watch work from publishing. A peer that legitimately holds
   both descriptors can choose either route. This proves state confinement,
   not business-tenant identity or authentication for the host's original
   business caller.
5. A deterministic service-name handover test binds descriptor A to unique
   service peer A, transfers the public `HOST_NAME` alias to service peer B,
   and proves bound calls, uploads, downloads, Secrets, and watches still reach
   A or fail. After A disconnects, every bound route fails and never reconnects
   through B. Legacy `connect(connection)` retains its existing public-name
   behavior.
6. Missing, expired, revoked, mismatched-caller-peer, or forged descriptors fail
   before key provider, schema, or backend access. A fixed-root route refuses
   absent trusted context and does not guess the local profile.
7. Lifecycle tests cover release, authenticated detach, delayed watch-open,
   stream cleanup, runtime hop, watcher shutdown, lag reconciliation, and
   bounded cache pressure without retaining dead serving connections.
8. Upload tests cover the configured ceiling, declared/head/body length
   mismatch, cancellation, errors, and stolen stream ids. Downloads remain
   directed to the authenticated peer and blobs are not fully buffered.
9. Audit/error tests prove payloads, secrets, keys, tokens, driver messages,
   and source chains are absent while refusal categories remain actionable.
10. Existing linked and actual loaded-cdylib storage tests continue to pass;
   reports distinguish linked test coverage from a real dynamic runtime
   proof.
11. Focused busproxy/tinybus tests, exact-secret regressions, workspace format,
    Clippy, build, test, docs, dependency guards, Rust 1.85 pure-bus and
    standalone-consumer checks pass. No result is claimed for a gate not run.

## Integration prerequisites

- TinyBus U5 is published and pinned at v0.3.2, source commit
  `cba2de0e573f99fe4c7e45ba14e2d6b5a2a7757f`. The public APIs are
  `ModuleHost::attach_linked_with_setup(...) -> ModuleInfo`,
  `ModuleHost::sender_lookup() -> ModuleSenderLookup`, and
  `RuntimeSetup(Connection, &ModuleHost)`. `ModuleSenderLookup` is a cloneable
  weak handle whose `module_for_sender(&BusName)` returns `None` after host
  drop. `ModuleRuntime::serve_host_object(...) -> HostObjectGuard` remains an
  ordinary host-object guard, not storage authority.
- Verify the typed per-lease adapter carries context through stream/watch
  client routes while keeping `StorageEndpoint` object-safe and preserving
  existing implementations and `StorageCall` struct literals.
- Keep the version change under the release workflow. Do not hand-edit the
  workspace version; review the finalized public diff for compatibility before
  invoking release.
