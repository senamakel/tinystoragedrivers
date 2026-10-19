# Secret storage

Status: accepted (v0.3, `tinystoragedrivers-secrets`). Companion to
[`storage-ports.md`](storage-ports.md); delivered by step 4 of
[`../plans/storage-rollout.md`](../plans/storage-rollout.md).

## Problem

OpenHuman keeps API keys, OAuth tokens and encrypted config fields in
`security/keyring/` in three places:
- the OS keychain
- one encrypted `secrets.enc` file
- `enc2:` strings inside config

A cloud deployment needs the same secrets in a shared database, encrypted per
tenant. Existing installs must keep decrypting what they already stored.

## Goals and non-goals

Goals:
- One port that desktop and cloud hosts share.
- OpenHuman's byte formats reproduced exactly, in both directions.
- A distinct data key per tenant in the database-backed driver.

Non-goals:
- Which secrets exist, how names are namespaced per user, and when a legacy
  value is migrated. These are host policy.
- Key management services. A KMS-unwrapped key arrives as `StaticKey` or
  `DerivedKeys` material.
- Migrating OpenHuman's `dev-keychain.json`.

The port lives in `tinystoragedrivers-secrets`, not in the core, so the core
links no cryptography. The facade re-exports it as `tinystoragedrivers::secrets`
behind the `secrets` feature, and `keyring` adds the OS store.

## `SecretStore`

| Method | Contract |
| --- | --- |
| `get(name)` | The value, or `None` when absent. Wrapped in `Zeroizing`. |
| `get_versioned(name)` | The value and opaque `SecretVersion` from one snapshot, or `None` when absent. |
| `set(name, bytes)` | Store, replacing any previous value. |
| `set_if(name, bytes, condition)` | Store only if `Precondition::Absent` or the observed `Precondition::Version` still holds; stale conditions are `Conflict`. Returns the revision for its committed write. |
| `delete(name)` | Remove; report whether it existed. |
| `list(prefix)` | Names starting with `prefix`, sorted. `""` lists every secret. |
| `enumerable()` | Whether `list` works. Default `true`. |
| `backend_name()` | A stable label for logs. |

Names and prefixes:
- **Names** are 1–256 bytes with no control characters (NUL included). Any
  other name is `InvalidInput` from every method.
- **Prefixes** are at most 256 bytes with no control characters. An invalid
  prefix is `InvalidInput` whether or not the store can enumerate.
- **Opaque names.** A driver must never interpret a name as a path. It is a
  map key, a document id, or a credential attribute.

Values:
- Bytes in general. The file and keyring drivers hold UTF-8 text only and
  reject anything else with `InvalidInput`, because their formats are string
  maps and passwords.

Errors:
- Undecryptable data is `Crypto`.
- Malformed decrypted data is `Serialization`.
- A locked or denied store, and transient I/O, are `Unavailable`.
- A store that cannot enumerate fails `list` with `Backend`.
- Messages never contain key bytes, plaintext or secret names.

Versioned writes:
- `SecretVersion` is an opaque, redacted 256-bit token, not the document
  driver's monotonically increasing `Version`. It binds the secret name and
  storage domain to the stored encrypted representation. Handles for the same
  persistent store share a domain; independent stores do not. Rewriting identical
  plaintext and deleting/recreating a secret both produce a different token.
- `DocumentSecrets` includes scope, collection, name, native document revision
  and ciphertext in the token. `set_if` re-reads and checks that token, then
  uses the document store's native version precondition for the atomic write.
- `EncryptedFileSecrets` binds the stable file path and hashes the name and exact encrypted `secrets.enc`
  snapshot while holding the existing cross-process lock. Since the file is one
  encrypted map, writing any entry invalidates tokens for every entry. This
  conservative conflict avoids a new file format or version sidecar.
- `MemorySecrets` keeps per-name generations, including tombstones, under its
  mutex. Keyring stores do not support these methods and return typed
  `Unsupported(ConditionalSecretWrites)` errors.

## Formats

- **Blob.** ChaCha20-Poly1305: `nonce(12) ‖ ciphertext ‖ tag(16)`, a fresh
  OS-random nonce each time, no associated data. A failing OS random source is
  `Crypto`, never a panic.
- **`enc2:` string.** `enc2:` followed by the blob in lowercase hex. Upper-case
  hex is accepted on read.
- **`secrets.enc`.** The JSON object `{"<name>": "<value>"}` as one raw blob.
  A missing or empty file is an empty store. A file that does not decrypt or
  parse is an error and is never overwritten.
- **Keys.** 32 bytes as 64 hex characters, surrounding whitespace ignored:
  `.secret_key`, and the keychain entry `openhuman` / `app:master_key`.
- **Legacy `enc:`.** Repeating-key XOR. Read-only, for migration.

## Drivers

- **`MemorySecrets`.** For tests.
- **`EncryptedFileSecrets`.**
  - Read → modify → write runs under an exclusive advisory lock on
    `secrets.enc.lock`, which interoperates with OpenHuman's.
  - Each write stages a synced `0600` temp file and renames it into place.
  - A new `.secret_key` is staged, then published with a hard link that fails
    if the file exists, so a reader never sees a partial key.
- **`DocumentSecrets`.**
  - Each secret is the document `{"ciphertext": "enc2:…"}`, its id the name,
    in collection `secrets` by default.
  - The key comes from a `KeyProvider` for the handle's scope:
    - `StaticKey` is one key everywhere.
    - `DerivedKeys` is `HKDF-SHA256(ikm = master, info =
      "tinystoragedrivers-secrets/v1:" ‖ scope)`, 32 bytes.
- **`KeyringSecrets`.**
  - One credential per secret: the store's service, with the name as user.
  - Values go through the password API.
  - It cannot enumerate.
  - `load_or_create_key(name)` mints a key only on a genuine absence and
    reads it back before returning. It serializes within the process, and
    across processes with `with_key_creation_lock(path)`.

## Acceptance

- Every driver passes `testkit::secrets_conformance`. The suite removes what
  it wrote even when a check fails.
- Golden vectors written by OpenHuman's own crypto code decrypt through
  `crypto`, `EncryptedFileSecrets` and `DocumentSecrets`.
- A ciphertext copied between tenants under `DerivedKeys` fails with `Crypto`.
- The real OS keychain is touched only by `live_*` tests behind
  `TSD_LIVE_KEYRING=1`.
