# tinystoragedrivers-sqlite

This is the SQLite driver for `tinystoragedrivers`. It runs the desktop and CLI
backend and passes the shared conformance suite in both layouts.

## Layouts

One storage is one database file. The path decides which:

- A path ending in `.db` or `.sqlite` is that file.
- Any other path is a directory (created if missing), and the database is
  `<dir>/storage.db`. This keeps `sqlite:<workspace dir>` URLs working.

## Storage

- **Layout 2.** Tables are `docs`, `streams`, `stream_heads`, `blobs`, `fts`
  and `meta` (plus the internal `_tsd_collections`, `_tsd_scope_admissions` and
  `_tsd_purge_receipts`). `meta.layout` records `2`.
  - A file written by v0.x (`_tsd_docs` and friends) is upgraded in place, in one
    transaction, the first time it is opened: tombstones become `deleted` rows,
    `created_ms`/`updated_ms` are backfilled from a recognised body field
    (`created_ms`, RFC 3339 `created_at` or `createdAt`) or the open time, a
    declared v1 `ttl` field fills `expires_ms`, and the v0.x tables are dropped.
    The upgrade is not reversible; back the file up first. A layout-2 file that
    still holds v0.x tables is corrupt and refused.
  - `auto_vacuum` is not changed by the upgrade: a file still at `NONE` converts
    at the first `maintain` call (see below).
  - A deleted document keeps its row (`deleted = 1`, body `{}`) so versions
    continue; every read skips it.
  - `RecordMetadata` is supported: `put_with` records `schema` and `expires_ms`,
    and the reserved paths `_schema`, `_created_ms`, `_updated_ms` and
    `_expires_ms` filter and sort through the columns.
- **Documents** are stored in one `WITHOUT ROWID` table per database, keyed by
  `(scope, collection, id)`.
  - A body over 4 KiB is stored zstd-compressed. Declared index and search fields are projected into a `doc_fields` side table, which indexes and SQL filters read; other filters run in Rust.
  - Unique constraints are checked in the write transaction with the same
    equality the memory driver uses.
  - Declared search fields are mirrored into an FTS5 table.
- **Filters, sorts and paging** are pushed down where SQL gives the port's
  answer, and every row is still re-checked in Rust (`Filter::matches_with`),
  so SQL only ever narrows.
  - *Filters.* Equality and membership on strings and booleans, and ranges
    (`gt`, `gte`, `lt`, `lte`) on numbers, strings and booleans, over a
    projected field, the id or a metadata column. A projected range tests
    `kind` and `value`, so a bound of another JSON type matches nothing, and
    numbers compare numerically. Strings compare bytewise, which is the
    port's order and SQLite's default collation. Number equality is left to
    Rust, as are `null`, array and object bounds, `Ne`, `Or`, `Not`, `Exists`
    and any path that is not projected.
  - *Sorts.* A limited query whose sort keys are all projected fields, the id
    or metadata columns is ordered by SQL (`ORDER BY kind, value` per key,
    through one `LEFT JOIN` on `doc_fields` per field; a missing field is
    null, ties are by id) with `LIMIT`, and the keyset cursor seek is part of
    the `WHERE`, so only the page is read and decoded. A page the Rust check
    leaves short continues from its last row. A query stays in Rust when a key
    is not projected, when a sort field holds a `u64` above `i64::MAX` (stored
    as a rounded float, so SQL could tie values the port orders), or when the
    page contains an array or object sort value (their order is structural
    and the side table keeps no value for them). Unlimited queries, counts
    and deletes sort in Rust.
- **Streams** use dense offsets that are allocated in the inserting
  transaction.
- **Blobs** are stored as rows, and range reads use `substr`.
- **Expiry** is applied at read time against an injectable clock.

## Concurrency

Every handle on the same file within one process shares one connection. Writes
use immediate transactions. Other processes are serialized by WAL plus a
5-second busy timeout. Contention that outlasts the timeout returns a retryable
`Unavailable` error. SQLite's file locks coordinate independent processes, and
its immediate transactions make version preconditions atomic across them. The
backend reports `Capability::SharedAcrossProcesses` and
`Capability::CrossProcessCas`.

A fenced handle (`for_scope_fenced`) checks its guard in the same database.
The guard check and target document, stream or blob mutation run in one
immediate transaction, which holds the write lock from its first statement. The
fence therefore holds across reopened handles and independent connections.

## Maintenance

The driver reports `Capability::Maintenance` and `Capability::ScopeEnumeration`.

- **`auto_vacuum`.** A database file created by this driver sets
  `auto_vacuum=INCREMENTAL` before it writes anything, because SQLite fixes the
  setting at the first table. `maintain(budget)` then runs
  `PRAGMA incremental_vacuum(N)`, where `N` is the budget in pages (the whole
  free list when unbounded), finishes with a passive checkpoint so the file
  actually shrinks, and reports the pages that left the file times the page
  size. A budget under one page, or `Some(0)`, reclaims nothing.
- **Older files.** A file made by an earlier release has `auto_vacuum=NONE`.
  Opening it changes nothing. The first `maintain` call sets the pragma and
  runs a one-time `VACUUM`, which is the only way SQLite applies the change.
  That rewrites the whole file: it needs free disk space about the size of the
  database, holds the write lock while it runs, and ignores the budget (a zero
  budget still does nothing). Run the first call when the host is idle.
- **`checkpoint(mode)`.** `Passive` copies what it can without waiting.
  `Truncate` runs `PRAGMA wal_checkpoint(TRUNCATE)` and leaves the `-wal` file
  empty, so the database file alone holds the data; call it before moving or
  copying the file. If a reader or writer keeps it from finishing it fails with
  `ErrorKind::Unavailable`, which is safe to retry.
- **`scopes(prefix)`.** One `SELECT` over the scope column of the document,
  document, stream and blob tables, de-duplicated and ordered by byte. The
  prefix is literal (`%` and `_` have no special meaning). The purge ledger
  does not count: an erased scope drops out of the listing.
- `maintain` and `checkpoint` act on the whole database file.

## Native access

`SqliteNative` gives raw access to the same file. It exposes
`with_connection`, `with_transaction`, and `migrate`, where `migrate` is a
per-owner, append-only migration runner. Owners such as a session ledger with
its own FTS tables use it to keep their schema while sharing the connection.
`run_blocking` runs a closure on that same connection on the calling thread,
keeping the closure's own error type, for owners whose API is synchronous. It
blocks, so call it from sync code or a blocking task, not on a runtime worker.

## rusqlite version

`rusqlite` is required as `0.40.2` (caret) with `bundled` and is re-exported
as `tinystoragedrivers_sqlite::rusqlite`. `libsqlite3-sys` may appear only once
in a host's dependency graph, so the requirement must unify with the host's
own pin (OpenHuman pins `=0.40.2`).

## Limits

- Versions and offsets stop at `i64::MAX`, because that is the largest value
  SQLite's integer column can hold. A write past that limit fails instead of
  reusing a value.
- Paging is offset-based, with cursors bound to their query.
- Field paths with an all-digit segment (an array index or a numeric key) or
  a `"` / `\\` in a segment cannot be written as a SQLite JSON path. Filters
  and indexes on them are evaluated in Rust instead of SQL.
