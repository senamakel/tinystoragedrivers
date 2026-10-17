# Storage bus contract MSRV

Status: accepted. Owner: tinystoragedrivers.

## Problem and boundary

`tinystoragedrivers-bus` is a serde data contract plus the default-on legacy
`DocumentClient`. Consumers such as TinyWallet support Rust 1.85, while the
storage implementation workspace needs Rust 1.88. Cargo reads each package's
`rust-version`, so inheriting the workspace value prevents those consumers from
using the bus even when the package source and its dependencies support 1.85.

Only `tinystoragedrivers-bus` lowers its package `rust-version` to 1.85. The
workspace and every implementation package retain Rust 1.88. The bus package's
data types, wire formats, default feature, client API, and dependencies do not
change. Its optional default `client` feature continues to compile against
TinyBus, whose declared MSRV is also 1.85. Data-only consumers can still disable
default features and omit TinyBus.

## Acceptance

- On Rust 1.85, the bus crate checks and tests both with default features and
  with `--no-default-features`.
- An independent consumer runs against the canonical `StorageNeeds` and full
  collection schema on Rust 1.85, with no TinyBus, runtime, HTTP client,
  database driver, or storage implementation in its normal/build graph.
- The existing compiled document-callback proof continues to pass on the
  workspace's Rust 1.88 validation lane, preserving the default client API.
- The workspace's `rust-version` remains 1.88; implementation crates do not
  claim the bus package's lower MSRV.
