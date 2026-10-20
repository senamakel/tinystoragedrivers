# tinystoragedrivers-core

The storage ports (`DocumentStore`, `StreamStore`, `BlobStore`), the tenant
`Scope`, the shared filter and value semantics, the in-memory reference driver,
the `Blocking` bridge for synchronous callers, and the `conformance` suite that
every driver runs.

It depends on no database client and no transport, and CI checks this. Driver
crates and consumers both compile against it. Most code should depend on the
`tinystoragedrivers` facade instead, which re-exports everything here.

Pure schema declarations and transform wire contracts live in
[`tinystoragedrivers-schema`](../tinystoragedrivers-schema/README.md), reexported
here as `declarations`. The existing closure-backed `Schema`, `Step`, registry,
keys and storage ports remain in core.

Blob schema steps preserve the source's known absolute expiry: `get_with_expiry`
reads bytes and deadline together, and `put_until` writes that same deadline.
Drivers that cannot inspect or preserve it fail closed rather than treating an
unknown lifetime as permanent.

See [`../../docs/specs/storage-ports.md`](../../docs/specs/storage-ports.md).

Native document invalidation is optional through `DocumentStore::watch` and
`Capability::Watch`. Subscribe before reading authoritative state; notifications
are conservative, payload-free reread hints. Select the `polling-watch` feature
explicitly for `PollingWatch` over an admitted handle; it does not promise native
history or add Watch support to a backend. See [the watch module](src/watch/README.md).

Tests that need a native watch can enable `tinystoragedrivers-core/test-util`
and construct `MemoryStorage::with_native_watch()`. Default memory storage
continues to report Watch as unsupported. See the [test-only memory watch
specification](../../docs/specs/memory-watch-test-util.md).

The optional `migration-delivery` feature exports `MigrationDeliveryClient` and
`MigrationExchangeServer` for confidential fragmented external transforms. It
keeps storage writes in the existing runner and introduces no bus dependency.
Adapters must retain a periodic cleanup guard and supply actual contextual peer
identity and a confidential transport; see the
[module README](src/schema/delivery/README.md).
