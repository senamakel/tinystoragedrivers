# tinystoragedrivers-secrets

The contract is specified in
[`docs/specs/secret-storage.md`](../../docs/specs/secret-storage.md).

Encrypted secret storage for OpenHuman: one `SecretStore` port (`get`,
`get_versioned`, `set`, `set_if`, `delete`, `list`), with drivers for each
place a secret can live. Desktop and cloud builds share the port and the byte
formats.

| Driver | Where the bytes live | Use |
| --- | --- | --- |
| `MemorySecrets` | process memory | tests |
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
- **Keys.** 32 bytes written as 64 hex characters: OpenHuman's `.secret_key`
  file (`load_or_create_key_file`) and its keychain master key (service
  `openhuman`, user `app:master_key`; `KeyringSecrets::load_or_create_key`).
- **Legacy `enc:`.** Repeating-key XOR. Readable through
  `crypto::decrypt_legacy_enc` for migration only; never written.

The crate's tests decrypt golden vectors that OpenHuman's own crypto code
produced, and the reverse direction (this crate writes, OpenHuman's code
reads) was checked with the same code.

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
- `get_versioned` pairs a value with an opaque `SecretVersion`; `set_if` uses
  `Precondition::Absent` or `Precondition::Version`. A stale precondition is a
  typed conflict. File versions hash the encrypted whole-file snapshot, so a
  change to any entry conservatively invalidates every version from that file.
  The hash contains ciphertext, not plaintext, and adds no persisted format.
- `KeyringSecrets` does not claim `ConditionalSecretWrites`: OS keyrings do
  not expose an atomic compare-and-set operation.
- A `secrets.enc` that does not decrypt or parse fails the call
  (`ErrorKind::Crypto` / `Serialization`) and is left untouched. It is never
  treated as empty or overwritten.
- `KeyringSecrets` cannot enumerate (`enumerable() == false`, `list` fails with
  `ErrorKind::Backend`). A locked store is `ErrorKind::Unavailable`.
  `load_or_create_key` creates a key only when the entry is genuinely absent,
  and serializes creation within the process. The OS stores have no
  create-if-absent, so `with_key_creation_lock(path)` serializes it across
  processes through a shared lock file.
- `load_or_create_key_file` writes and syncs a temp sibling, then publishes it
  with a hard link that fails if the key file exists. Readers never see a
  partial key, and racing creators adopt the winner's key.
- File and keyring calls run on the tokio blocking pool when a runtime is
  present.
- Error messages never contain key bytes, plaintext, or secret names.

## Tests

`testkit::secrets_conformance` (feature `testkit`) runs the same suite
against every driver. The keyring driver runs it over an in-memory
credential builder. The real OS store is only touched by
`live_keyring_conformance`, and only with `TSD_LIVE_KEYRING=1`, or by the isolated
Linux Secret Service launcher:

```sh
cargo test --locked -p tinystoragedrivers-secrets --features keyring-secret-service --lib
python3 .github/scripts/check-native-secret-service.py
```

Run those commands from the repository root. The launcher requires
`dbus-run-session`, `gdbus` and `gnome-keyring-daemon`. It creates private XDG
state under this checkout's `target/`, leaves HOME unchanged, starts an owned
bus with service activation disabled and an owned disposable daemon, proves
bus-owner/default-collection state, and exercises real set/get/exact readback/
delete/NoEntry plus locked and absent-service denial. It stops and waits for
its daemon, disposes scratch credential state and retains only boolean receipts.
`--coverage` uses the already instrumented secrets test executable and report
profile directory. A timeout fails the probe and terminates its owned process
group. Kernel access, macOS Keychain and Windows Credential Manager runtime
proof are separate; they are not inferred from this Linux probe.

Private file namespaces, atomic restrictive birth, and the bounded verified file
import API are documented in [the file module](src/file/README.md). Import
receipts retain live object identity; authenticated backup recovery after a
restart returns bytes, not permission to delete a newly resolved source path.
