# tinystoragedrivers-core

The storage ports (`DocumentStore`, `StreamStore`, `BlobStore`), the tenant
`Scope`, the shared filter and value semantics, the in-memory reference driver,
the `Blocking` bridge for synchronous callers, and the `conformance` suite that
every driver runs.

It depends on no database client and no transport, and CI checks this. Driver
crates and consumers both compile against it. Most code should depend on the
`tinystoragedrivers` facade instead, which re-exports everything here.

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
