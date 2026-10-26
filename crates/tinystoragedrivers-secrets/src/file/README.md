# Encrypted file engine

`EncryptedFileSecrets` stores a JSON name-to-text map in one ChaCha20-Poly1305
blob. It keeps the established `secrets.enc` bytes unchanged, serializes
read-modify-write operations with the sibling `.lock` file, and uses private
staging files with verified atomic publication.

File-backed secret values are UTF-8. Reads return zeroizing byte buffers;
invalid names, invalid text, unreadable ciphertext, and failed publication
return typed errors without logging secret values or names.

Conditional writes hold the same cross-process lock while checking the secret
revision and publishing the replacement. Their opaque revision includes the
file identity and ciphertext snapshot, so an unrelated file update may
conservatively conflict with a prior read.

Windows uses published `clack-private-fs` to pass an owner-only protected DACL
at the original native create call and obtain an owned `std::fs::File`.
Admission supports rooted ordinary and verbatim local NTFS disk paths with
persistent ACLs, preserving verbatim spelling for every component pin. UNC,
remote/device and drive-relative paths are rejected. It requires a process
identity without thread impersonation, trusted ancestor ownership and no
foreign effective mutation grants. Retained directory handles deny delete
sharing. Leaf handles retain actual FileIDs while allowing legitimate atomic
replacement. Reparse points, unsafe owners/ACLs and hard links are rejected.
Existing permissions are checked, never repaired after exposure.

Reads and mutations hold `<secrets filename>.lock`. Key creation holds
`<key filename>.lock`, including the short two-link no-clobber publication
interval. New staging, key, data and lock files are private before bytes.

`EncryptedFileSecrets` fails closed on decryption and serialization errors by
default. A host that wants desktop recovery can opt into
`CorruptFilePolicy::QuarantineAndContinue`: before quarantine the driver
rechecks the opened identity and probes the current bytes again under the same
held lock. If the original object has become readable, it returns those
secrets and leaves the file in place. Otherwise it moves the corrupt file to a
unique `.corrupt.*` sibling and syncs the directory on Unix before continuing
with an empty store. A failed move is an error. Windows uses a
no-replace `MoveFileExW` write-through move. It never removes archived bytes.
An incorrect key is indistinguishable from damaged ciphertext and is also
quarantined under this opt-in policy. Permission, namespace and other I/O
failures remain errors.

`with_key_file_read_only` reads an existing `.secret_key` without creating the
key or its key-creation lock. `ProtectedHexKeyFile` provides bounded,
read-only admission for a host-selected legacy key file. `FileImportSource`
and `prepare_file_import` bind a verified source to a destination publication
and retain an encrypted recovery backup; the host still owns source selection,
user consent, and conversion policy. `cleanup_file_import` only removes the
exact source represented by a live verified receipt and reports the native
publication boundary.

Hosts that need to atomically write plaintext secrets can use the public
`write_atomic_private` helper. Its staging file is owner-only before bytes are
written. Publication flushes staged content, publishes, then verifies the
actual published identity and content. Unix also syncs the admitted directory.
Windows uses `std::fs::rename`, including its POSIX replacement fallback, with
bounded retries for transient antivirus sharing violations. The
write-through, no-replace move is used only for corruption quarantine.
Namespace verification surrounds replacement. Published FileID verification,
explicit file flush and exact byte readback precede acknowledgment. Unsupported
extended replacement fails closed, potentially reporting the original access
denial. These acknowledgments describe native APIs; they do not prove physical
power-loss survival. A failure after publication may leave visible state.
Failed staging cleanup is attempted and can itself fail; private leftovers
are never permission-broadened or treated as committed data.
