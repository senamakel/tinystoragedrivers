# Native memory watch for tests

Status: accepted implementation target. Owner: `tinystoragedrivers-core`.

## Problem

The memory backend is useful for deterministic tests, but it intentionally does
not advertise native `Watch`. Tests for code that requires a native
`DocumentStore::watch` subscription therefore need a separate capable backend.

## Goals and non-goals

- Add an opt-in, test-only way to create a watch-capable `MemoryStorage`.
- Keep the helper and its dependency surface behind the `test-util` Cargo
  feature.
- Preserve the current default: ordinary `MemoryStorage` does not advertise
  `Watch` and its document watch method returns `Unsupported(Watch)`.
- Exercise the public storage and watch contracts without a database or network.

This is not a production memory-driver capability, durable change history,
filter-specific event stream, or substitute for a driver's native watch tests.

## Proposed behavior

With `tinystoragedrivers-core/test-util` enabled,
`MemoryStorage::with_native_watch()` constructs an opted-in memory backend using
the system clock. Its backend and scoped document capabilities include
`Watch`. A subscription is available through the existing
`DocumentStore::watch` API. Tests that need an injected clock can call
`MemoryStorage::with_clock(clock).enable_native_watch()`; ordinary
`new`, `with_clock`, and `Default` remain watch-free.

The watch binds to its memory database, scope, and collection. Successful
document mutations in that scope and collection produce coalesced,
payload-free `Invalidated` notices. A filter is validated at open; mutations
that do not match it may still invalidate the watch. Changes to another scope
or collection do not invalidate it. The watch periodically emits an
invalidation within the configured `WatchOptions::interval`, so tests can
exercise bounded rereads for logical expiry without a background worker.

The subscription is registered before `watch` returns, so callers can open it
before their initial authoritative read. It does not support resume cursors;
an `after` cursor is rejected as `InvalidInput`. Opening and delivering notices
check the captured scope generation and fence. Dropping the watch releases its
subscription state.

## Invariants and constraints

- Default `MemoryStorage::new`, `with_clock`, and `Default` remain watch-free.
- `test-util` does not become a default feature or alter production features.
- Notices contain no document data and confer no authority; callers reread
  state and apply their own rules.
- The implementation has no detached task, unbounded event queue, or
  cross-scope mutation signal.
- No persistent or portable resume history is implied.

## Acceptance criteria

- The builder is available only with `test-util`; feature-disabled core builds
  do not compile the helper or native-watch implementation.
- Default capability and unsupported behavior remain unchanged.
- Opted-in watches observe insert, replacement, delete, and bulk mutation
  invalidations, while mutations in another scope or collection do not signal
  them.
- Opening a watch before the initial read cannot miss a later successful
  mutation; notices are payload-free and extra same-collection invalidations
  are allowed.
- Quiet watches produce bounded periodic invalidations; cursors are rejected.
- Stale generations and fences fail closed, and dropping the final watcher
  releases the per-scope/per-collection subscription entry.

## Open questions

None.
