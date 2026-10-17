# Native memory watch for tests

Spec: [native memory watch for tests](../specs/memory-watch-test-util.md).

## Goal and assumptions

Implement the accepted test-only API in `tinystoragedrivers-core`. The existing
`DocumentStore::watch` contract remains authoritative: notifications are
payload-free reread hints, subscribe before the initial read, and validate
captured admission at the subscription boundary. The memory watch may
conservatively invalidate every query on one scope and collection and does not
provide resume history.

The API does not exist before this feature, so there is no meaningful failing
test against the current source. Add tests with the new public API and verify
both feature configurations; do not manufacture a compile-failure test as a
red phase.

## Tasks

1. Add `test-util = ["tokio/sync"]` in
   `crates/tinystoragedrivers-core/Cargo.toml`. Add the documented zero-argument
   `MemoryStorage::with_native_watch() -> Self` constructor behind that
   feature. Also add a clearly distinct `enable_native_watch(self) -> Self`
   opt-in for tests that start with an injected clock. Carry the opt-in state
   through scoped, existing-schema, and fenced handles. Keep default backend
   and document capabilities unchanged.
2. Add a feature-gated memory watch notifier and `DocumentWatch` implementation
   under `crates/tinystoragedrivers-core/src/memory/`. Register subscriptions
   under the database serialization boundary, key invalidations by scope and
   collection, coalesce notices, and periodically invalidate using the
   caller's interval. Reject resume cursors, and check the captured generation
   and fence both when opening and delivering a notice. Drop the final
   subscription without retaining a worker or stale map entry.
3. Signal successful document mutations from
   `crates/tinystoragedrivers-core/src/memory/documents.rs`, including single
   writes, claims, deletes, bulk deletes, and atomic batches. Notify only after
   the state operation succeeds; coalesce one event per affected collection
   for a batch.
4. Add public API tests under
   `crates/tinystoragedrivers-core/tests/` and focused internal tests beside
   the memory watch module. Cover default unsupported behavior, enabled
   capability, subscribe-before-read delivery, payload-free notices,
   collection/scope isolation, conservative filter invalidation, mutation
   paths, quiet interval hints, cursor rejection, stale admission, cancellation,
   and watcher cleanup. Use the existing sibling `*_tests.rs` convention for
   Rust unit tests.
5. Run formatting, focused default and `test-util` tests, and the TSD workspace
   contract gates (Clippy, build, all-feature tests, docs, and coverage) using
   the repository's prescribed cancellation wrapper for long commands. Record
   exact commands and terminal results; do not run Cargo concurrently with
   another workspace owner.

## Completion checklist

- [ ] Spec and linked plan are present before capability code.
- [ ] The test-only API and invalidation behavior meet the acceptance criteria.
- [ ] Default and `test-util` configurations pass their focused tests.
- [ ] Required workspace gates and format checks have terminal results.
- [ ] No version field or release tag was edited by hand; release remains
  workflow-owned.
