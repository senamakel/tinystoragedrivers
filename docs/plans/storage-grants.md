# Storage grants implementation

Contract: [Storage grants](../specs/storage-grants.md). This leaf implements
the generic declaration and authorization library only; hosts retain caller
admission and product-specific grant decisions.

## Ordered work

1. **Borrowed needs declaration.** Add `StorageNeeds<'a>` to
   `tinystoragedrivers-bus`, borrowing collection specs and the three resource
   prefix slices. Reuse its existing `CollectionSpec`; do not add serde or a
   second schema shape. Export it from the bus crate.
2. **Owned grant API.** Add `tinystoragedrivers-busproxy::grant` with private,
   owned `StorageGrant` fields, explicit `GrantPolicy`, secret-safe formatting,
   content-free validation and denial errors, and `from_needs`. Reuse core
   validation helpers where they match the contract. Export the grant API.
3. **Request authorization.** Port the host authorization behavior to the v1
   bus selectors and typed field names. Classify all writes exhaustively;
   validate all batch resources; preserve exact schema answers, filtered
   collection listings, null-only no-grant capabilities, and literal prefix
   matching. Keep caller identity, scope, backend, fence, and product mappings
   out of this crate.
4. **Behavior tests and checks.** Add module-local tests for construction,
   validation, ownership, schema equality, selector classification,
   authorization, filtering, and content-free handling. Run focused bus and
   busproxy tests, then the repository format, Clippy, build, tests, docs, and
   per-file coverage gates.

## Boundaries

Do not add OpenHuman schema adapters or module grant tables here. Do not add a
second grant mechanism to TinyBus security, change wire payloads, hand-edit
versions, or publish from this implementation step.

## Release acceptance

After the canonical epic branch is green in hosted CI, merge it normally and
dispatch the repository release workflow with a minor bump on exactly
`epic/8056-modules-wiring`. Verify the version commit and published tag on
that branch. Restore the release workflow's main-only guard before the final
rollup to main. The workflow remains the only owner of version and lockfile
changes.
