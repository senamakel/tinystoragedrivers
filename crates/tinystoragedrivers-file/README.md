# tinystoragedrivers-file

The plain-file driver: every record is an ordinary file under one directory, so
the data stays human-inspectable and easy to back up or diff. Selected with the
`file:<dir>` URL through the facade's `file` feature, or opened directly with
`FileStorage::open(dir)`.

## Layout

```text
<root>/
├── _meta/collections/<collection>.json     collection declarations (CollectionSpec)
├── scopes/<scope>/
│   ├── docs/<collection>/<id>.json         {"id", "version", "doc"}
│   ├── streams/<name>.jsonl                one {"offset", "value"} line per entry
│   ├── streams/<name>.meta.json            {"name", "base"}
│   ├── blobs/<key>                         the raw bytes
│   └── blobs/<key>.meta.json               {"key", "content_type"}
```

Scopes sit under `scopes/`, so none can shadow `_meta`.

## Names on disk

Every name is percent-encoded: lowercase ASCII letters, digits, `_` and `-`
stay as they are; every other byte becomes `%XX` (uppercase hex). The result is
distinct on case-insensitive filesystems (macOS, Windows), never contains `.`
(so `.json` / `.meta.json` suffixes are unambiguous), and Windows device names
(`con`, `nul`, `com1`, ...) get their first character escaped.

- Files (documents, streams, blobs, declarations): an encoding longer than 200
  bytes becomes `~` plus a 128-bit FNV-1a hash. The real name is stored inside
  the file and checked on every access; a file holding a different name is
  reported as a `Backend` error instead of being overwritten.
- Directories (scopes, collections): a long encoding is split into 200-byte
  chunks, each continuation prefixed with `+`, so two names never share a
  directory and nothing is hashed.

## Durability

- Whole-file writes (documents, declarations, stream sidecars, blobs) go to a
  temporary file in the same directory, are fsynced, then renamed into place.
- A stream append writes whole lines and fsyncs. A crash can leave a torn final
  line; readers ignore it and the next append cuts it off. The stream length is
  derived from `base` and the last complete line, never stored separately.
- `truncate_before` raises `base` first, then rewrites the file; a crash in
  between leaves lines below `base`, which readers skip.
- Deleting a document (`delete`, `delete_where`, `drop_collection`) replaces
  its file with a tombstone, `{"id", "version"}` without `doc`, in one rename.
  A recreated id continues from its last version, so a compare-and-swap
  prepared before the deletion fails. Tombstones are never pruned.
- `ensure_collection` refuses (`AlreadyExists`, nothing written) a unique index
  that stored documents in any scope already violate.
- A blob's sidecar decides existence: put writes the bytes then the sidecar,
  delete removes the sidecar first.

## Capabilities

| Capability | Provided | Notes |
| --- | --- | --- |
| `Ttl` | yes | Expired documents stay on disk and read as absent, like the memory driver. |
| `FullText` | yes | Same tokenizer and scoring as the memory driver. |
| `Transactions` | no | A batch over several files cannot be crash-atomic with renames, so `atomic_batch` returns `Unsupported`. |
| `Fencing` | yes | The guard and target mutation share their native directory lock, within one process only. |
| `ScopeEnumeration` | yes | `scopes(prefix)` walks `scopes/`, decodes each directory name (rejoining the `+` continuation chunks of long names) and lists those with at least one file under `docs/`, `streams/` or `blobs/`. Undecodable stray directories are skipped. Cost is proportional to the number of scope directories. |
| `Maintenance` | no | `maintain` and `checkpoint` return `Unsupported`: there is nothing to reclaim or flush. |

## Concurrency

All operations on one directory are serialized by a mutex shared by every handle
in the process that opened the same (canonicalized) directory, which makes
`claim` and compare-and-swap atomic within the process. File IO runs on tokio's
blocking pool when a runtime is present, and inline otherwise. Several
processes writing the same directory are **not** coordinated.
The backend therefore omits `Capability::SharedAcrossProcesses` and
`Capability::CrossProcessCas`.

`for_scope_fenced` holds the directory lock through guard validation and the
document, stream or blob mutation. This fencing guarantee is process-local and
does not extend to other processes writing the directory.


## Limits

- Queries, counts, `delete_where`, `claim` and search read the whole
  collection; there are no on-disk indexes. Suited to small per-user stores.
- Windows' 260-character path limit applies unless long paths are enabled.
