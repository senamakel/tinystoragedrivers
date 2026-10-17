# Optional TinyBus client in the storage contract

Status: accepted for implementation. Owner: tinystoragedrivers.

## Problem and boundary

`tinystoragedrivers-bus` contains portable storage wire types and the legacy
`DocumentClient`. The client uses TinyBus, so a host that needs only the data
contract currently links TinyBus even when it never constructs a client. Module
contracts such as `tinyconnectors-bus` need the canonical `StorageNeeds` and
`CollectionSpec` types without inheriting that client dependency.

The `client` Cargo feature gates only `DocumentClient` and its TinyBus
implementation. The default feature set includes `client` to preserve existing
imports and behavior. A data-only consumer sets `default-features = false` and
retains all storage request, response, schema, and needs types.

## Compatibility

- Default dependencies continue to export `DocumentClient` and compile its
  existing tests.
- `--no-default-features` exports every data type, including
  `StorageNeeds`, `CollectionSpec`, `IndexSpec`, and `SearchSpec`, but does not
  compile or include TinyBus in the normal/build dependency graph.
- No wire shape, selector, storage operation, or contract version changes.
- The feature controls the compatibility client only; the separate
  `tinystoragedrivers-client` and `tinystoragedrivers-busproxy` crates retain
  their existing responsibilities.
- The bus package supports Rust 1.85 in both feature profiles. The storage
  workspace and implementation packages continue to require Rust 1.88.

## Acceptance

An independent consumer workspace with only a path dependency on
`tinystoragedrivers-bus` and default features disabled must construct a complete
`StorageNeeds`, including indexed, TTL, and search collection metadata plus
blob, stream, and secret prefixes. Its normal/build dependency tree must contain
no TinyBus, async runtime, HTTP client, database driver, or storage
implementation crate. The repository CI runs this proof separately so feature
unification from other workspace members cannot hide a dependency leak.

The ordinary default-feature bus tests and the compiled document-callback proof
must keep passing, demonstrating that existing client users remain compatible.
The `bus-msrv` CI job runs the default and no-default bus tests and independent
consumer proof on Rust 1.85; the compiled callback proof also runs in the
workspace's Rust 1.88 lane.
