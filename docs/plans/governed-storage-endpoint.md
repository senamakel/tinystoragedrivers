# Governed storage endpoint implementation plan

**Status:** Accepted; TinyBus U5 v0.3.3 is pinned; final-cohort verification in progress.
**Spec:**
[`../specs/governed-storage-endpoint.md`](../specs/governed-storage-endpoint.md).
**Issue:**
[`senamakel/tinystoragedrivers#2`](https://github.com/senamakel/tinystoragedrivers/issues/2).

## Goal, boundary, and assumptions

Deliver a reusable governed storage host endpoint and TinyBus attachment while
preserving the released fixed-root API. The implementation must use the
published TinyBus stateful linked setup and authoritative sender-to-module
lookup at one canonical SDK revision. OpenHuman-specific scope construction,
fences, backend routing, and audit policy remain consumer adapters.

This work does not implement OpenHuman #7608 by itself. Multi-profile callers
must use separately host-issued leases/descriptors and retain one bound client
instance per lease. Existing global-root callers receive only explicitly
trusted single-context behavior.

The owning spec is accepted, TinyBus U5 is published, and the additive context
API preserves the released object-safe endpoint and struct literals.
Do not hand-edit the root workspace version; the repository release workflow
owns version changes.

## Proposed additive API shape

These names and signatures are the review target; finalize TinyBus-dependent
parameters after the U5 release. Keep context and lease fields private. The
descriptor is serializable routing data, not an authority token.

```rust
pub struct HostStorageContext<C>(C); // typed host state, never serialized
impl<C> HostStorageContext<C> {
    pub fn new(context: C) -> Self;
    pub fn as_ref(&self) -> &C;
}
pub struct StorageLease { /* registration id, peer/module, epoch */ }
#[derive(Serialize, Deserialize)]
pub struct StorageDescriptor { /* unique service peer + registration id + exact root */ }
pub trait HostContext: Send + Sync + 'static {}

#[async_trait]
pub trait Environment<C: HostContext>: Send + Sync + 'static {
    async fn resolve(&self, context: &C,
        module: &tinybus::module::ModuleId) -> Result<ResolvedStorage, HostFailure>;
    async fn admit_write(&self, context: &C,
        module: &tinybus::module::ModuleId) -> Result<(), HostFailure>;
}

pub trait CallerDirectory: Send + Sync + 'static {
    fn module_for_sender(&self, sender: &BusName) -> Option<tinybus::module::ModuleId>;
}
pub struct TinyBusCallerDirectory(tinybus::module::ModuleSenderLookup);
impl CallerDirectory for TinyBusCallerDirectory {
    fn module_for_sender(&self, sender: &BusName) -> Option<tinybus::module::ModuleId> {
        self.0.module_for_sender(sender)
    }
}

pub struct GovernedEndpoint<E, A, C> {
    /* environment, audit, registry */
}
impl<E, A, C> GovernedEndpoint<E, A, C> {
    pub fn new(environment: E, audit: A,
        limits: GovernedEndpointLimits) -> Self;
}
pub struct StorageRegistration<C> { /* ServedStorage, linked Connection,
    Arc<dyn CallerDirectory + Send + Sync + 'static>, leases,
    detach and relay tasks */ }
impl<C: HostContext> StorageRegistration<C> {
    pub async fn register_lease(
        &self, context: HostStorageContext<C>,
        intended_peer: BusName,
    ) -> Result<(StorageLease, StorageDescriptor), HostFailure>;
    pub async fn revoke(&self, lease: &StorageLease) -> bool;
    pub async fn shutdown(self) -> Result<(), HostFailure>;
}

impl BusTransport {
    pub async fn connect_bound(connection: Connection,
        descriptor: StorageDescriptor) -> tinybus::Result<Self>; // pins every route to service_peer
}

pub async fn attach_storage<C, E, A>(
    host: &ModuleHost,
    observer_connection: &Connection,
    endpoint: Arc<GovernedEndpoint<E, A, C>>,
    runtime: tokio::runtime::Handle,
) -> Result<StorageRegistration<C>, HostFailure>
where C: HostContext;
```

`HostContext` is a marker bound (`Send + Sync + 'static`) for a host-owned
context type, not a tenant string parsed from the descriptor. Its wrapper has
`new(C)` and `as_ref() -> &C`, allowing the environment to construct/read its
own opaque host context. `attach_storage` calls `host.sender_lookup()` while
`host` is borrowed, wraps the handle as `Arc<TinyBusCallerDirectory>`, and
stores that adapter in the returned `StorageRegistration` and its lease
registry. The current U5 source API returns a cloneable `ModuleSenderLookup`
holding `Weak<ModuleHostInner>`; its `module_for_sender(&BusName)` returns
`None` after host drop and applies the same live-admission predicate as the
host method. This needs no host clone, static runtime pointer, global callback
slot, or ownership cycle.

The descriptor is host-issued routing configuration, not an authority token.
It carries the serving connection's unique service peer, registration id, and
exact object root. The server registry must match that route to a live lease
and the broker-authenticated caller peer/module before granting access. A
descriptor copied or forged for another caller does not confer authority.

The per-lease adapter captures the context and lease and implements the
existing object-safe `StorageEndpoint` methods. It passes contextual state
internally to `GovernedEndpoint`; do not add generic context methods to the
trait object or alter existing `StorageCall` literals. The private registry
stores context, peer, module, epoch, and four exact paths. The serializable
descriptor is routing data only; every call and delayed publication rechecks
the weak caller lookup and captured registration. Any API that makes request
data or ambient state authoritative requires revising the spec first.

## Ordered tasks

### 1. Pin the TinyBus prerequisite and prove the boundary

Files: root `Cargo.toml`, `Cargo.lock`, `crates/tinystoragedrivers-tinybus/`
tests, and the appropriate `proof-workspaces/` consumer fixture.

- Add a source-compatibility compile test using unchanged `StorageCall` and
  `StorageNeeds` literals and an `Arc<dyn StorageEndpoint>` implementation.
  Keep this test green as subsequent API tests are added.
- After Cargo approval, write a consumer/API test that uses U5's
  `ModuleHost::sender_lookup() -> ModuleSenderLookup` and linked setup seams
  against TSD's current TinyBus pin. Confirm the real compile failure is the
  missing API, then update the single revision after U5 is published and prove
  the same test passes. Do not simulate RED with an artificial assertion.
- Add a runtime test that the weak lookup resolves the authenticated unique
  peer, returns `None` for aliases/stopped peers/host drop, and agrees with
  `ModuleHost::module_for_sender`. Test that `attach_storage` returns its
  TSD-owned cleanup registration after readiness even though U5's internal
  `attach_linked_with_setup` result is only `ModuleInfo`.
- Update the one canonical TinyBus Git revision and lockfile to the published
  U5 release. Do not introduce a second TinyBus source or duplicate its types.
- Keep the default and no-default pure-bus crate transport-free and compatible
  with Rust 1.85; use the implementation workspace's Rust 1.88 for host crates.
- Verify U5 APIs against upstream documentation and its tests before relying
  on exact names/signatures.

### 2. Add additive binding and contextual endpoint seams

Files: `crates/tinystoragedrivers-busproxy/src/per_caller.rs`, new focused
`per_caller_async.rs` / `per_caller_async_tests.rs` if needed,
`host.rs`, public exports in `lib.rs`, plus module README/spec updates.

- First add a deterministic test that blocks module A's binder and proves
  module B's first request completes; assert no endpoint map lock is held
  across asynchronous binding.
- Add a shared binding state keyed by authenticated module, canonical scope,
  backend identity/version, and binding epoch. Preserve `PerCallerEndpoint::new`
  and its current synchronous behavior through an adapter.
- Provide `PerCallerEndpoint::new_async` with an owned `BusName` argument and
  awaitable `Option<StorageServer>` binder. Bound in-flight binder work so
  capacity pressure fails closed without holding the shared cache lock.
- Add pending-bind deduplication or equivalent bounded duplicate work, with a
  publication check after every awaited seam. An obsolete result must be
  dropped if its epoch, lease, backend, or registration changed.
- Keep `StorageEndpoint` object-safe and unchanged. Add a typed per-lease
  adapter that implements the existing trait methods, captures its lease and
  context, and passes internal contextual state into `GovernedEndpoint`.
  Do not add generic context methods to `dyn StorageEndpoint` or change
  `StorageCall` struct literals.
- Bound the caller cache (default 1024) and concurrent binders (default 64).
  At capacity, evict only a cached server with no active operation, retained
  download reader, or open watches; otherwise refuse the new peer. Keep
  release as explicit invalidation and ensure pending caller tickets cannot
  publish after release.
- Test idle-entry eviction, active watch/download retention, full-cache
  refusal, release invalidation, concurrent unrelated async binds, and
  late-bind non-resurrection.

### 3. Implement governed per-call authorization

Files: new `crates/tinystoragedrivers-busproxy/src/governed/` module and
`mod_tests.rs`, exports in `lib.rs`, and module `README.md`.

- Define small host seams for trusted scope resolution, installed backend
  identity, declared `StorageGrant`, secret keys, write admission, authoritative
  caller lookup, and audit sink. Keep OpenHuman types and policy out.
- Use the proposed `Environment::resolve(context, module) -> ResolvedStorage`
  and `Environment::admit_write(context, module)` shape. `ResolvedStorage`
  owns the backend identity, canonical scope, grant, and key provider needed
  for one binding; it is never deserialized from a bus request.
- Resolve authenticated module plus captured lease before doing any schema,
  key, or backend work. Refuse missing, revoked, mismatched, stale, or
  multi-profile-without-context authority at this boundary.
- Build/cache a storage server for the canonical scope and declared grant.
  Recheck backend identity and epoch before publishing it. Initialize only
  host-declared schemas before the binding becomes visible.
- Run grant authorization, write admission, and reply filtering on every
  request. Apply the same checks to upload/download paths and watch-backed
  operations. Reuse `StorageGrant::authorize` and `filter_reply`; never fork
  secret grant semantics.
- Redact all errors and audit records to approved metadata and closed outcome
  labels. Map backend panics/failures to content-free host failures.
- TDD cases: unknown/ambiguous caller, undeclared object/target, read-only
  grant, malformed request, schema mutation/drop, exact secret access/list
  denial, retryable key-provider failure, backend replacement during bind,
  and no audit payload leakage.

### 4. Add host-created leases and bound module transport

Files: `crates/tinystoragedrivers-busproxy/src/transport.rs`, new lease and
descriptor module(s) under `busproxy/src/`, transport tests, public exports,
and `crates/tinystoragedrivers-client/` only if a source-compatible typed
adapter is required.

- Introduce a typed host context plus host-created lease and serializable
  descriptor. The descriptor identifies a registration and exact object root;
  the server registry binds it to context, intended peer, and module. It carries
  no caller-selected scope, backend, grant, or database and is not authority by
  itself.
- Add scoped registration and `BusTransport::connect_bound(connection,
  descriptor)` additively. Keep `connect(connection)` and fixed-root behavior.
- Include the serving connection's broker-assigned unique service peer in the
  host-issued serializable `StorageDescriptor`, beside its registration id and
  exact object root. `BusTransport::connect_bound` resolves and pins the
  service peer once; it never routes a bound request through the public
  `HOST_NAME` alias and never falls back to legacy `connect`.
- `StorageRegistration::register_lease(context, intended_peer)` is async. It
  resolves the peer through the stored weak sender lookup, installs four exact
  per-lease object paths as one owned transaction, and returns the host's
  revocation handle plus routing descriptor only after all four succeed.
  Revoke invalidates the registry record and lease-scoped streams/watches.
- The descriptor is serializable routing data, not authority. The per-lease
  server registry validates registration id/root and the authenticated peer
  against the intended peer, and rechecks the live sender lookup and lease
  epoch before each call and before publishing delayed bind/watch work. Never
  infer authority from request JSON or ambient task context.
- Carry captured context through the typed `StorageEndpoint` adapter for
  ordinary methods, confidential secrets, blob upload/download, and watch
  subscription/signal routing without changing the object-safe trait.
- Audit every `BusTransport` route: regular and confidential method proxies,
  upload stream-open target and body writes, download request and accepted
  stream sender, and watch signal subscription/sender validation all use the
  same descriptor-pinned service peer. Do not reuse `proxy()` or
  `streamed_call()` unchanged if they resolve `HOST_NAME` at call time.
- TDD two leases A/B for one authenticated peer with the same collection name
  but distinct scopes. Give separate transports A and B descriptors and
  interleave reads/writes/streams/watches and canaries. A payload scope cannot
  redirect either route; revoking A leaves B usable and prevents A's delayed
  bind/watch publication. A peer holding both descriptors may intentionally
  choose either route; this proves routing/state confinement, not
  business-tenant authentication. Also test a descriptor used by a different
  peer, forged registration id, and revoked/expired lease.
- Preserve `serve_collection` and fixed-root single-context use. The fixed
  route cannot identify a request's original business profile; use it only
  where the host guarantees one context. Where bound authority is required,
  callers use a lease descriptor. Never silently fall back to local scope.

### 5. Attach through TinyBus U5 with owned lifecycle

Files: `crates/tinystoragedrivers-tinybus/src/lib.rs`, focused module files as
needed, and `crates/tinystoragedrivers-tinybus/src/lib_tests.rs` or a public
integration test under `tests/`.

- Implement `attach_storage(host, &observer_connection, endpoint, runtime)` as
  the owner of TSD's static linked export and U5 attachment. The caller awaits
  it and retains the returned `StorageRegistration`; no generic host-owned
  registration slot or external linked-setup orchestration is part of the API.
- Inside the helper, call `host.sender_lookup()` while `host` is borrowed and
  adapt that owned weak handle to `Arc<dyn CallerDirectory + Send + Sync +
  'static>`. Store it in the TSD owner/lease registry. It contains
  `Weak<ModuleHostInner>`, implements `module_for_sender(&BusName) ->
  Option<ModuleId>`, and returns `None` after host drop. Do not use a host
  clone, static-runtime back-pointer, callback registry, or strong ownership
  cycle.
- The helper borrows `observer_connection` for broker lifecycle observation;
  the caller remains responsible for that connection. The setup callback
  captures its actual linked `Connection`, serves `STORAGE_INTERFACE` on the
  four exact per-lease paths, and creates the service owner. The observer
  never serves storage or supplies caller authority. Closing the linked
  connection withdraws its bus peer; the caller retains `ModuleHost` and owns
  the distinct module admission/lifecycle state.
- Transfer the created owner from the setup callback to `attach_storage` over
  a private owned one-shot channel. The helper awaits both successful U5
  readiness (`ModuleInfo` is internal only) and the setup result, then returns
  the TSD `StorageRegistration`. Attach failure, setup error/cancellation, or
  a dropped one-shot receiver must roll back partial object exports and close
  the linked connection; no partial owner escapes.
- Serve the same `STORAGE_INTERFACE` at the four exact per-lease object paths
  on that same captured, attested linked `Connection`. The serialized
  descriptor routes to those four paths; no root-prefix dispatcher or four
  independently named interfaces are introduced.
- Resolve caller identity only through the owned U5 sender lookup; never scan
  mutable well-known names or accept a caller-supplied module id.
- Test all four object paths and the one shared `STORAGE_INTERFACE`,
  confidential Secrets routing, linked registration
  shutdown, unique-peer detach, well-known alias release not detaching a live
  peer, and liveness reconciliation after missed detach events. Exercise
  Secrets through the real attested linked connection and verify all four
  paths use the same `STORAGE_INTERFACE`.
- Deterministic handover regression: bind descriptor A to service unique peer
  A, then let service peer B take over public `HOST_NAME` while A is alive.
  Bound calls, all upload/download routes, Secrets, and watch signals must
  still reach A or fail, never B. After A disconnects, each bound route fails
  without alias re-resolution. Keep a separate test proving legacy
  `connect(connection)` retains its existing alias behavior.
- Ensure an ordinary unattested fallback cannot expose secret data.
- `StorageRegistration` retains `Arc<dyn CallerDirectory + Send + Sync +
  'static>` and rechecks it for every call and delayed bind publication. A
  lease binds its host context, explicit intended unique peer, captured
  `ModuleId`, epoch, and four exact paths. A stopped/detached peer resolves to
  `None`, invalidating the lease before backend work.
- TDD cancellation and owner transfer: cancellation during setup, U5 attach
  failure, and a dropped result receiver each leave no partial exports or
  lease. The caller receives the owner only after readiness; its explicit
  shutdown revokes, unserves, stops relays, and closes the owned link. U5's
  `ModuleInfo` remains internal to the helper.
- TDD caller-peer mismatch and descriptor service-peer mismatch before any
  schema, key, backend, stream, or watch work. Recheck the server registration
  and live weak sender lookup before publishing a delayed bind/open.
- `ModuleRuntime::serve_host_object` yields an ordinary `HostObjectGuard`;
  it does not attest the peer or grant confidential-call authority. Do not use
  it as the storage registration or authority source. TSD's service checks
  `CallContext` and its own lease registry, including Secrets admission.
- `StorageRegistration` owns the `ServedStorage`, linked `Connection`, exact
  registered paths, and detach/relay tasks. `shutdown(self)` revokes leases,
  drains/aborts tasks, calls `Connection::unserve` only on its exact per-lease
  paths, calls `ServedStorage::shutdown` for its base objects/name, then closes
  its connection after service tasks release clones. Drop schedules equivalent
  cleanup through the captured runtime.
- `register_lease(context, peer)` is an owned async transaction. It resolves
  the endpoint's `callers.module_for_sender(peer)` before spawning owned work,
  creates an opaque registration id
  and four exact per-lease object paths, and exports all four before publishing
  the registry record or returning `(StorageLease,
  StorageDescriptor { registration_id, root })`. If the caller cancels, an
  owned transaction completes rollback of
  all paths; partial registration never produces a descriptor. Do not route
  through a path prefix. Test detach/revocation after export begins but before
  registry publication: the liveness recheck must fail, roll back all four
  paths, and publish neither lease nor descriptor.

### 6. Close streams, watches, and runtime-hop races

Files: `busproxy/src/host/mod.rs`, focused host/stream/watch tests, governed cache
tests, and TinyBus attachment tests.

- Port the proven hop/relay behavior with context captured before each hop.
  Check the lease epoch again after returning to publish results.
- Scope stream and watch state by lease and peer. Release revokes pending bind,
  queued watch open, stream routes, forwarders, and watcher tasks. No delayed
  operation can recreate released state.
- Preserve peer-bound `accept_stream_from`, exact declared lengths, directed
  downloads, unicast watch delivery, and chunked transfer without full-blob
  buffering. Default stream ceiling is 256 MiB and remains configurable.
- TDD declared/head/body mismatch, ceiling, malformed or stolen stream id,
  cancellation/error cleanup, queued watch-open detach, release during runtime
  hop, watch id reuse across two leases on one peer, and >256-peer churn with
  no retained serving connection.
- Do not claim task cancellation rolls back an admitted write; verify native
  fence remains on the backend write path.

### 7. Port existing generic host and cdylib regression coverage

Files: new `busproxy` integration/module test files, the
`tinystoragedrivers-tinybus/test-cdylib` consumer fixture, its loaded-module
test, and `scripts/test-governed-cdylib.sh`.

- Port the generic refusal, stream, cache, watch, and lifecycle behavior from
  OpenHuman's storage-host tests without porting OpenHuman policy or types.
- Keep tests aligned with the owning crate's layout: module-local tests in
  sibling `*_tests.rs`; public contract tests under crate `tests/`.
- Preserve actual loaded-cdylib coverage for reads/writes, cross-module and
  cross-scope refusal, payload-free audit, blobs larger than one frame, native
  watch isolation, and peer-bound streams. Label linked-only checks separately
  from an actual dynamic runtime proof.
- The dynamic proof must load the built fixture with a file-digest allowlist
  through `ModuleHost`; it receives a host-issued descriptor explicitly. Cover
  the `HOST_NAME` handover while the issuing peer remains live, then lease
  withdrawal and native module stop while B still owns the alias. Verify every
  bound route fails without reaching B, including ordinary calls, uploads,
  downloads, confidential Secrets, and watch open/signals. Include a
  multi-frame canary, declared/head mismatch, ceiling refusal, and a stolen
  stream handle from a different authenticated module.
- Keep test doubles deterministic, network-free, and independent of wall-clock
  sleeps. Use synchronization channels/barriers for races.

### 8. Document, verify, and hand off the release candidate

Files: busproxy/tinybus `README.md`, crate rustdoc, this spec and plan, proof
workspace fixture as applicable.

- Update public API docs and runnable examples in the same change as behavior.
- Run focused busproxy and tinybus tests, exact-secret regression tests,
  `cargo fmt --all -- --check`, Clippy, build, full test, `cargo doc`, and
  dependency guards. Also run pure-bus default/no-default and standalone
  consumer proof under Rust 1.85; implementation host crates use Rust 1.88.
- Record exact command/output evidence, distinguish linked tests from dynamic
  cdylib execution, and list any pre-existing failures without broadening
  claims.
- Do not tag or release by hand. After review, the release workflow owns the
  version bump/tag; downstream host/module consumers adopt the published
  revision afterward.

## Completion checklist

- [ ] Spec accepted; U5 API/revision published and confirmed.
- [ ] Existing public APIs remain source-compatible; additive bound API is
      documented.
- [ ] Per-call grant, write-admission, schema, exact-secret, stream, and audit
      behavior is covered.
- [ ] Two leases on one peer are isolated for calls, blobs, streams, and watches.
- [ ] Release, detach, stale binds, runtime hops, and cleanup are covered.
- [ ] Existing linked and actual cdylib coverage remains green.
- [ ] Rust 1.85 pure-bus and consumer proof plus Rust 1.88 owning workspace
      gates are green.
- [ ] Evidence is ready for review; release remains owned by the workflow.
