# Captured document callback adapter

A trusted host calls endpoint, prepares a canonical TinyBus HostCallbackBinding,
then serves one already-admitted DocumentStore and fixed collection with
serve_collection. It loads the real verified module using that same binding.

The service implementation is private. There is no ordinary service constructor
or public direct dispatch method. Registration and calls cannot select another
collection, scope, URL, database or native admission. Native ports perform CAS,
data validation, generation checks and fenced writes; this crate only forwards.

Schema preparation and legacy import authority remain the owning host/module's
explicit responsibilities. Registration never calls ensure_collection. Native
error categories cross the wire, while backend messages and sources do not.
No persistence policy or module business reducer is implemented here.

Run the real artifact proof with scripts/test-document-callback.sh. This uses the
canonical callback binding, not an OpenHuman workaround or private API copy.
The proof does not establish TinyMemes persistence or unpublished lifecycle fixes.

The wire uses document_id and strictly parsed inline document_json text. This
preserves arbitrary business JSON without confusing id-only data with native
TinyBus stream references. No privacy check or stream API is bypassed.

## Governed storage

The governed storage adapter serves host-created, revocable leases over the
four storage objects. Each descriptor carries the exact object root and the
unique peer that issued it; a caller's authenticated peer and module identity
are checked for every method and streamed operation. The descriptor is routing
configuration, not authority, and request bodies never select a storage scope.

The linked integration suite exercises leases with real in-memory storage and
a broker. The separate loaded-cdylib proof builds a TinyBus consumer fixture,
allowlists its actual artifact digest, loads it through `ModuleHost`, and tests
the governed route from its own module connection. Run it with:

```sh
scripts/test-governed-cdylib.sh
```

That proof covers document and stream reads/writes, exact Secrets, a blob larger
than one TinyBus frame, native watch delivery, peer and scope refusals,
content-free audit records, upload ceiling and stream-handle refusals, and
`HOST_NAME` handover. It distinguishes actual dynamic loading from linked
module tests and uses no external services.
