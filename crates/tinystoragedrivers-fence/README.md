# tinystoragedrivers-fence

Lease ownership helpers and native fenced backend binding for
`tinystoragedrivers`.

`DocumentLeases` adapts the core compare-and-swap lease primitive to the
`LeaseStore` contract, preserving the caller's owner, holder, lease scope and
TTL. Its fencing token is enforced by `StorageBackend::for_scope_fenced` in
the same atomic step as protected document, stream and blob writes.

Enable the `file` feature for `LocalLeases`, which holds a portable advisory
file lock for each key until release or process exit. `inspect` returns the
durable record, while `holder` also checks whether the process lock is still
held. This backend-less store does not issue a driver fence; a local lock
cannot atomically protect writes in an unrelated backend.
