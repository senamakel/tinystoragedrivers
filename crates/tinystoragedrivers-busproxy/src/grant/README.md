# Storage grant module

This module turns host-approved module storage needs into an immutable,
validated grant and decides whether one typed storage-bus call fits that
grant. It contains no caller authentication, caller-to-module mapping, scope
selection, backend choice, or fence management.

## Public surface

- `StorageGrant::from_needs` owns and validates selected `StorageNeeds` using
  `GrantPolicy`.
- `StorageGrant::from_needs_with_exact_secrets` accepts the additive exact-name
  wrapper while preserving the legacy `StorageNeeds` prefix behavior.
- `StorageGrant::with_queue_manifest` is a separate host-policy opt-in for the
  host-selected queues. Grants deny queue operations by default; an opted-in
  grant validates the selector and request, restricts the target to the
  manifest, and applies the grant's write policy. The host must supply the same
  manifest to its bound `StorageServer`, which enforces the queue declaration
  while executing the operation. The manifest participates in grant equality.
- `StorageGrant::authorize` checks the object, current selector, typed request,
  schema operation, target, and write permission. It returns a `Plan` to
  forward the call or answer a host-only operation locally. An ungranted
  `Queues.Features` request is answered locally with every optional feature
  disabled so clients can initialize; queue operations remain denied until the
  host opts in with `with_queue_manifest`.
- `StorageGrant::filter_reply` filters collection enumeration replies.
- `QueueGrantError`, `GrantError`, and `Denied` expose content-free error
  labels.

`StorageNeeds` borrows the bus crate's existing owned `CollectionSpec` values
and resource-prefix slices. A grant clones those values, so it does not borrow
the module schema. The collection schema, including indexes, TTL, and search
fields, is retained and compared exactly for ensure and validate requests.

## Host requirements

The host must select and approve canonical needs through module admission and
resource policy before calling `from_needs`. Never derive a grant from
request-supplied needs. The constructor applies generic syntax checks,
reserved-resource rules, the writable flag, and explicit secret admission; it
does not authenticate the source or maintain per-module allow-lists.

Queue access is independent of `StorageNeeds`: the host may attach a validated
queue manifest after selecting the module's authority. It must bind the
`StorageServer` to that same manifest and include manifest/backend changes in
the governed binding revision so a cached server cannot retain stale queue
policy.

The host then binds the returned grant to its authenticated caller, scope,
backend, and fence. Requests do not carry those values. Secret operations must
also be served only through the confidential caller route. Watch ids are safe
only when the host endpoint binds subscriptions to that same caller.

Resource prefixes use literal `starts_with` matching. Hosts must include any
delimiter they require in a prefix. The module does not normalize resource
names or infer filesystem path rules for streams and secrets.

Exact secret names authorize only Get, Set, and Delete for those names. They do
not grant `Secrets.List`, even when a list prefix equals an exact name; listing
continues to require a declared prefix so backend results cannot reveal
undeclared suffix names. In mixed grants, list calls remain bounded by one
declared secret prefix.
