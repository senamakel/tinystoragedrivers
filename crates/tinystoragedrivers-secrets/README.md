# tinystoragedrivers-secrets

The contract is specified in
[`docs/specs/secret-storage.md`](../../docs/specs/secret-storage.md).

Secret storage primitives: the API `SecretStore` port and engines for memory,
encrypted files, documents, OS keyrings, and cloud KMS. Engines depend on the
new API and types crates. The host chooses which secrets exist, how names are
scoped per user, and when a legacy value is migrated. The file feature also
provides protected source admission and verified import primitives so hosts do
not duplicate native filesystem identity and publication checks.

| Driver | Where the bytes live | Use |
| --- | --- | --- |
| `MemorySecrets` | process memory | tests and ephemeral use |
| `EncryptedFileSecrets` | `secrets.enc`, one ChaCha20-Poly1305 blob | desktop, CLI |
| `DocumentSecrets` | `{"ciphertext": "enc2:…"}` per secret in any `DocumentStore` | cloud, multi-tenant |
| `KeyringSecrets` (feature `keyring`) | the OS credential store | desktop |

## Explicit Linux credential providers

Defaults are empty. Enable `keyring` for `KeyringSecrets::linux_kernel(service)`,
which selects the kernel keyring explicitly. Enable `keyring-secret-service`
for `KeyringSecrets::secret_service(service)`, which selects the synchronous
Secret Service provider with encrypted D-Bus transport (`crypto-rust`). The
facade forwards both features. Secret Service builds on Linux require libdbus
development headers; no OpenSSL or vendored libdbus is selected here.

```rust,no_run
# #[cfg(all(target_os = "linux", feature = "keyring-secret-service"))]
# fn example() {
use tinystoragedrivers_secrets::KeyringSecrets;
let kernel = KeyringSecrets::linux_kernel("my-app");
let desktop = KeyringSecrets::secret_service("my-app");
# }
```

Both factories retain their own immutable credential builder, construct lazily,
and never set the process default or fall back to another provider. Availability
is established by an operation, not by construction. `backend_name() == "keyring"`
does not distinguish these providers. Permission and human consent remain the
caller's responsibility.

`KeyringSecrets::new` keeps the `keyring` crate's compiled default. Cargo unifies
features across consumers: enabling Secret Service alongside `linux-native`
can change that Linux default even without a global setter. Review ambient
`new` call sites before enabling the feature in a shared graph, and use explicit
factories for provider-sensitive code. Custom injected builders retain their
existing behavior. Consumer feature unions must not combine synchronous and
asynchronous Secret Service or `crypto-rust` and `crypto-openssl`.

## Formats

Every format is OpenHuman's own, so existing data opens unchanged:

- **`enc2:` values.** `enc2:` + lowercase hex of `nonce(12) ‖ ciphertext ‖
  tag(16)`, ChaCha20-Poly1305, no associated data. Upper-case hex is accepted
  on read. See `crypto::{encrypt_enc2, decrypt_enc2}`.
- **`secrets.enc`.** The JSON object `{"<name>": "<value>"}` encrypted as one
  raw `nonce ‖ ciphertext ‖ tag` blob. Values are text, so the file and keyring
  drivers reject non-UTF-8 values.
- **Keys.** Key acquisition is supplied by the host through `KeyProvider` or
  a caller-owned encrypted-file key.

The crate's tests decrypt golden vectors that OpenHuman's crypto code
produced, including an actual `secrets.enc` fixture from TSD 1.0.0.

## Keys per tenant

`DocumentSecrets` asks a `KeyProvider` for the data key of its handle's scope
on every call:

- `StaticKey`: one key everywhere (from the environment, or KMS-unwrapped).
- `DerivedKeys`: `HKDF-SHA256(ikm = master, info =
  "tinystoragedrivers-secrets/v1:" ‖ scope)`. Each tenant gets a distinct
  key, and a ciphertext copied between tenants does not decrypt.

## Operational notes

- `EncryptedFileSecrets` takes the same `secrets.enc.lock` advisory lock as
  OpenHuman for its read → modify → write cycle. It writes through a unique
  temp file (`0600`, synced) and renames it into place.
- By default, a `secrets.enc` that does not decrypt or parse fails the call
  (`ErrorKind::Crypto` / `Serialization`) and is left untouched. Hosts may
  explicitly select `CorruptFilePolicy::QuarantineAndContinue`; that mode
  durably moves the corrupt bytes to a unique `secrets.enc.corrupt.*` sibling
  before treating the store as empty. On Unix it syncs the directory; on
  Windows it uses a no-replace, write-through native move. An incorrect key is
  indistinguishable from damaged ciphertext and is also quarantined under this
  opt-in policy. Quarantine failures and non-corruption I/O errors still fail
  closed.
- Hosts that write plaintext secrets can use `write_atomic_private`, which
  creates owner-only staging files before writing secret bytes. Generic file
  storage writes remain available separately.
- `KeyringSecrets` cannot enumerate (`enumerable() == false`, `list` fails with
  `ErrorKind::Backend`). A locked store is `ErrorKind::Unavailable`.
- File and keyring calls run on the tokio blocking pool when a runtime is
  present.
- Error messages never contain key bytes, plaintext, or secret names.

## Tests

`testkit::secrets_conformance` (feature `testkit`) runs the same suite
against every driver. The keyring driver runs it over an in-memory
credential builder. Keyring tests never access a user's actual credential
store.

The encrypted-file engine's format, locking, and conditional-write behavior
are documented in [the file module](src/file/README.md).
