# Storage grants implementation

Contract: [Storage grants](../specs/storage-grants.md). This leaf implements
the generic declaration and authorization library only; hosts retain caller
admission and product-specific grant decisions.

## Ordered work

1. **Shared secret-name validation.** Add a focused core secret module with
   `MAX_SECRET_NAME_LEN`, `validate_secret_name`, and
   `validate_secret_prefix`. Keep it dependency-free beyond the existing core
   error type. Make the secrets crate's existing constant and validator names
   aliases so its public API and error messages remain unchanged.
2. **Borrowed needs declaration.** Add `StorageNeeds<'a>` to
   `tinystoragedrivers-bus`, borrowing collection specs and the three resource
   prefix slices. Reuse its existing `CollectionSpec`; do not add serde or a
   second schema shape. Export it from the bus crate.
3. **Owned grant API.** Add `tinystoragedrivers-busproxy::grant` with private,
   owned `StorageGrant` fields, explicit `GrantPolicy`, secret-safe formatting,
   content-free validation and denial errors, and `from_needs`. Reuse core
   validators including the shared secret-prefix contract. Export the grant API.
4. **Request authorization.** Port the host authorization behavior to the v1
   bus selectors and typed field names. Classify all writes exhaustively;
   validate all batch resources; preserve exact schema answers, filtered
   collection listings, null-only no-grant capabilities, and literal prefix
   matching. Keep caller identity, scope, backend, fence, and product mappings
   out of this crate.
5. **Behavior tests and checks.** First add the regression case proving grants
   reject an overlong/control-character secret prefix while accepting valid
   literal prefixes. Run it against the old grant implementation and record the
   expected failure. Then add module-local coverage for construction,
   validation, ownership, schema equality, selector classification,
   authorization, filtering, and content-free handling. Run focused bus and
   busproxy tests, then the repository format, Clippy, build, tests, docs, and
   per-file coverage gates.

## Boundaries

Do not add OpenHuman schema adapters or module grant tables here. Do not add a
second grant mechanism to TinyBus security, change wire payloads, hand-edit
versions, or publish from this implementation step.

## Release acceptance

After TinyBus PR #74 merges, update the root and callback-proof manifests to
its canonical merged revision and regenerate both lockfiles. Once the
canonical epic branch is green in hosted CI, merge it normally and dispatch the
repository release workflow with a minor bump on exactly
`epic/8056-modules-wiring`. In that workflow, update the nested callback-proof
lock after the root version bump, stage it with the root lock, and run the
existing `--locked` callback proof before committing and tagging. Verify the
version commit and published tag on that branch. Restore the release
workflow's main-only guard before the final rollup to main. The workflow
remains the only owner of version and lockfile changes.
