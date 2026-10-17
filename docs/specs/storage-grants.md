# Storage grants

Status: accepted. Owner: tinystoragedrivers.

## Boundary

Loadable modules declare storage needs; the host decides which needs become
authority. A module declaration is untrusted input and never creates or widens
its own grant. The host checks the declaration against its module admission and
resource mapping before passing approved needs to this API. `GrantPolicy`
applies generic write, secret, and reserved-collection rules; it is not a
per-module resource allow-list. The authenticated host binds the resulting
immutable grant to the caller, scope, backend, and fence. Those bindings remain
outside this library contract: requests cannot select an identity, scope,
backend, or grant, and a grant value alone does not authenticate a caller.

`tinystoragedrivers-bus::StorageNeeds<'a>` is a borrowed declaration containing
collection specs and blob, stream, and secret prefixes. It is not a wire
envelope and does not need serde. Collection declarations reuse the bus
crate's owned `CollectionSpec`, including ordered indexes, uniqueness, TTL,
and search fields. `tinystoragedrivers-busproxy::grant::StorageGrant` clones
these declarations into owned private fields so the resulting grant remains
valid after the source schemas are dropped. Schema-to-bus conversion and
registry composition, preparation, and migration remain the host's job.

Modules that need authority for a finite set of secret keys use the additive
`StorageNeedsWithExactSecrets<'a>` declaration, formed with
`StorageNeeds::with_exact_secret_names`. It contains the complete existing
`StorageNeeds` value plus exact secret names. The original `StorageNeeds`
fields and struct-literal API remain unchanged: its `secret_prefixes` keep
their existing literal `starts_with` meaning. Grant construction validates
exact names with the shared secret-name validator and owns them separately.
Get, Set, and Delete allow a name listed exactly or covered by a declared
prefix. An exact name never implies permission to list: `List` still requires a
declared prefix that contains the requested list prefix, including when an
exact name is identical to that prefix. This prevents a backend listing from
revealing suffix names that were not declared.

## Host policy and validation

The host supplies `GrantPolicy`: writable access, permission to admit secrets,
and any additional reserved collection names. Its default is read-only, denies
secret access, and adds no product-specific reserved names. Nonempty secret
needs without explicit secret permission are an error. Product collection
names, including an encrypted-secrets collection, are host policy rather than
global storage-driver policy.

Grant construction validates and copies the complete declaration. It rejects
duplicate collections, prefixes, or exact secret names, malformed collection
specs, empty names and prefixes, NUL bytes, overlong resources, the reserved
`tsd.*` namespace, `_tsd` collection names, host-reserved collections, and blob
prefixes that overlap `_tsd/sealed/` in either direction. Blob prefixes may end
in `/` and are not treated as full blob keys. Actual blob keys are checked
with the core user-key validator. Prefix matching is literal `starts_with`:
callers must declare delimiters themselves; the library does not normalize
names or infer filesystem path semantics.

Secret names and prefixes use the shared core secret-name contract: at most
256 UTF-8 bytes and no Unicode control characters. Exact secret names and
grant prefixes must be nonempty; the SecretStore's empty list prefix is
intentionally not a whole-store grant. This rule does not treat `/` as a path
separator or alter literal prefix matching. `tinystoragedrivers-secrets`
preserves its existing `MAX_SECRET_NAME_LEN`, `validate_name`, and
`validate_prefix` exports and error messages by re-exporting the core
validators under those names.

Grant and needs debug output, validation errors, and denial errors must not
expose secret prefixes, request contents, names, or source errors. Errors use
stable content-free labels. Grant fields stay private and have no mutation API.

## Authorization

The proxy accepts only the four known objects and their finite v1 selector
sets. Unknown selectors fail closed. Write classification is closed over the
known selectors; a selector that has no classification is denied, never
treated as a read. Read-only grants deny every write, including writes in a
batch. Every resource in a checked batch's checks and operations must be
granted. `AtomicBatch` rejects nonempty checks; `AtomicBatchChecked` accepts
them after validating each dependency. Exact collection names and literal
prefixes are enforced for every operation.

`EnsureCollection` and `ValidateCollectionSpec` are answered locally with null
only when the request exactly matches the host-declared spec, even for a
read-only grant. They never perform backend DDL. `DropCollection` is always
denied. `Collections` replies are decoded and filtered to granted names;
malformed replies fail and are never passed through. Any well-formed
`Collections` prefix is allowed because its reply is filtered to exact granted
names. For Streams, Blobs, and Secrets prefix listings, the requested prefix
must be contained within one grant prefix; the library does not form a union
across prefixes. Exact secret-name declarations do not authorize `Secrets.List`,
and the grant does not filter list replies to exact names.

When no document or blob resource is granted, a null `Capabilities` request
receives the contract's empty document or zero blob capabilities. Non-null
capability requests are malformed. Streams and Secrets have no Capabilities
selector. All other calls on an ungranted object are denied.

Watch admission is scoped to the collection grant. Watch-handle ownership is
enforced by the host's per-caller endpoint, not by the grant; hosts must bind
and close handles per authenticated caller. This grant does not validate
caller identity or watch ids.

The current write selectors are Documents `Put`, `Delete`, `DeleteWhere`,
`Claim`, `AtomicBatch`, `AtomicBatchChecked`, and `DropCollection` (which is
classified as a write but denied by the host-only schema rule); Streams `Append`,
`AppendBatch`, `AppendBatchIfOffset`, `TruncateBefore`, `DeleteStream`, and
`Seal`; Blobs `Put`, `PutStream`, `Delete`, and `PresignPut`; Secrets `Set`
and `Delete`. `Info` is a stream read. The typed v1 request fields and
selector lists in `tinystoragedrivers-bus` are authoritative.

## Compatibility and coverage

The grant holds the full exact collection spec, including indexes and their
field order, TTL, and search declarations. Schema ensure/validate compares the
whole spec. Existing request and reply JSON shapes remain unchanged; this work
adds an in-process needs type and authorization API only.

Tests pin borrowed-to-owned lifetime behavior, complete schema preservation,
default policy and explicit secret admission, exact secret CRUD and denial of
unlisted names, prefix compatibility, exact-only list refusal, mixed prefix
list access, reserved resources, all selector write classifications,
read-only denials, batches, prefix boundaries and sealed-blob exclusions, exact
schema requests, capability handling, filtered collections, and content-free
secret-safe errors and debug output.
