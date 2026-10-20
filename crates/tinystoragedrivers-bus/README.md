# Document collection bus contract

A compiled module uses DocumentClient for confidential Get, Put and Delete on
one host-captured collection. Requests contain only document identifiers, JSON
data and native compare-and-swap conditions. Unknown fields are rejected.

No native storage crate, URL resolver, credentials, database client, schema
declaration, scope selector or admission issuer is linked by this contract.
Versions and categories have a separate minimal wire representation. Native
error messages and source chains are not serialized.

Transport errors are the outer result; native storage refusals are the inner
Outcome. A missing Get returns Ok(None), rather than fabricating a document.
See [the contract](../../docs/specs/document-callback.md).

The wire uses document_id and strictly parsed inline document_json text. This
preserves arbitrary business JSON without confusing id-only data with native
TinyBus stream references. No privacy check or stream API is bypassed.

The crate also pins the Streams, Blobs, Secrets and Queues wire shapes, finite
selector sets, size limits and `CONTRACT_VERSION`; see
[the storage bus spec](../../docs/specs/storage-bus.md).

Contract version 6 adds expiry-aware blob read and absolute-deadline write
selectors. Version 7 adds separate atomic document batch selectors that carry
one strict, host-safe metadata options DTO. Both versions leave established
request shapes unchanged.

## Dependency profiles

The default `client` feature preserves the legacy `DocumentClient` API and
enables TinyBus. A consumer that only needs portable wire or storage-needs
types can set `default-features = false`; this retains those data types without
including TinyBus in its normal/build dependency graph. The independent
[storage-needs consumer proof](../../proof-workspaces/storage-needs/README.md)
checks that dependency profile.

## Rust version

This package supports Rust 1.85 with both the default TinyBus compatibility
client and with default features disabled. The storage-driver workspace remains
at Rust 1.88; only this portable contract package advertises the lower MSRV.
The `bus-msrv` CI job checks both profiles and the independent consumer proof
on Rust 1.85.
