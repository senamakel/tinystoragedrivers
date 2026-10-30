//! [`EncryptedFileSecrets`]: every secret in one encrypted file, in
//! OpenHuman's `secrets.enc` format.
//!
//! # Format
//!
//! The file is exactly what OpenHuman's `EncryptedFileBackend` writes: the
//! JSON object `{"<name>": "<value>", ...}` (string values), encrypted as one
//! [`crypto::encrypt`] blob (`nonce ‖ ciphertext ‖ tag`, raw bytes, no hex).
//! A missing or empty file is an empty store. Values are therefore UTF-8 text;
//! [`SecretStore::set`] rejects anything else.
//!
//! # Concurrency
//!
//! Reads and whole-file mutations retain the exclusive advisory sidecar lock
//! `secrets.enc.lock`, interoperable with OpenHuman's lock.
//! New objects are private at the native create call, before payload writes.
//! Namespace pins and opened-file identities guard admission and readback.
//! Unix acknowledgment includes file and directory sync; Windows verifies
//! visible publication and file flush without claiming durable directory sync.
//! Errors after replacement can describe a visible, unacknowledged outcome.
//!
//! # Corruption
//!
//! A file that fails to decrypt or parse is an error
//! ([`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto) or
//! [`ErrorKind::Serialization`](tinystoragedrivers_types::ErrorKind::Serialization))
//! and is left untouched: no read degrades to "empty", and no write replaces
//! secrets it could not read.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tinystoragedrivers_types::{
    Result, SecretPrecondition, SecretVersion, StorageError, validate_secret_name as validate_name,
    validate_secret_prefix as validate_prefix,
};
use zeroize::{Zeroize, Zeroizing};

use crate::crypto::{self, KEY_LEN, key_from_hex, key_to_hex};
use crate::store::{SecretStore, require_utf8};
use crate::task::{io_error, run_blocking};

mod hex_key_source;
mod migration;
mod secure;
pub use hex_key_source::{HexKeyFileSource, MAX_HEX_KEY_FILE_BYTES, ProtectedHexKeyFile};
pub use migration::{
    CleanupAcknowledgment, FileImportSource, PublicationBoundary, VerifiedFileImport,
};
pub use secure::Locked;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

/// The secrets file name inside a workspace directory.
pub const SECRETS_FILE_NAME: &str = "secrets.enc";

/// The key file name for a generic file-backed master key.
pub const KEY_FILE_NAME: &str = ".secret_key";

/// Policy for a secrets file that cannot be decrypted or parsed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CorruptFilePolicy {
    /// Return the corruption error and preserve the file. This is the default.
    #[default]
    FailClosed,
    /// Move corrupt bytes to a unique sibling archive and continue with an
    /// empty store. The archive is never deleted by the driver.
    QuarantineAndContinue,
}

/// Distinguishes concurrent temp files written by one process.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Secrets in one ChaCha20-Poly1305 file.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use tinystoragedrivers_secrets::{EncryptedFileSecrets, SecretStore, crypto};
///
/// let dir = std::env::temp_dir().canonicalize().unwrap()
///     .join(format!("tsd-doc-{}", std::process::id()));
/// let secrets = EncryptedFileSecrets::new(&dir, crypto::generate_key()?);
/// secrets.set("alice:api_key", b"sk-live-abc").await?;
/// assert!(dir.join("secrets.enc").exists());
/// # std::fs::remove_dir_all(&dir).unwrap();
/// # Ok::<(), tinystoragedrivers_types::StorageError>(())
/// # }).unwrap();
/// ```
pub struct EncryptedFileSecrets {
    inner: Arc<Inner>,
}

#[derive(Clone)]
struct Inner {
    path: PathBuf,
    key: Zeroizing<[u8; KEY_LEN]>,
    corrupt_file_policy: CorruptFilePolicy,
}

impl EncryptedFileSecrets {
    /// The store at `dir/secrets.enc`, under `key`.
    ///
    #[must_use]
    pub fn new(dir: impl AsRef<Path>, key: Zeroizing<[u8; KEY_LEN]>) -> Self {
        Self::at_path(dir.as_ref().join(SECRETS_FILE_NAME), key)
    }

    /// The store at an explicit file path, under `key`.
    #[must_use]
    pub fn at_path(path: impl Into<PathBuf>, key: Zeroizing<[u8; KEY_LEN]>) -> Self {
        Self {
            inner: Arc::new(Inner {
                path: path.into(),
                key,
                corrupt_file_policy: CorruptFilePolicy::FailClosed,
            }),
        }
    }

    /// Select what this store does when the encrypted file is corrupt.
    ///
    /// The default is [`CorruptFilePolicy::FailClosed`]. Recovery is opt-in
    /// because quarantining the file makes reads look empty and subsequent
    /// writes create a fresh store.
    #[must_use]
    pub fn with_corrupt_file_policy(mut self, policy: CorruptFilePolicy) -> Self {
        Arc::make_mut(&mut self.inner).corrupt_file_policy = policy;
        self
    }

    /// The store at `dir/secrets.enc`, keyed by `dir/.secret_key`, which is
    /// created (`0600`, 32 random bytes as hex) when absent. Blocks on disk
    /// I/O; call it at boot.
    ///
    /// # Errors
    ///
    /// As [`load_or_create_key_file`]. Existing unsafe data, ledger or store-lock
    /// objects are rejected before master creation. Existing store objects with
    /// a missing master are a conflict; recover the original key first.
    pub fn with_key_file(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let key_path = dir.join(KEY_FILE_NAME);
        let key_lock = key_creation_lock(&key_path)?;
        // Admit all existing store objects before considering key creation.
        // Existing material plus a lost key is not an empty target.
        let existing = admit_existing_store(&key_lock.namespace, &dir.join(SECRETS_FILE_NAME))?;
        let key = match read_key_file(&key_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !existing.is_empty() {
                    return Err(StorageError::conflict(
                        "existing secret store has no master key",
                    ));
                }
                create_key_file(&key_path, &key_lock)?
            }
            other => parse_key_file(other)?,
        };
        key_lock
            .verify()
            .map_err(|e| io_error("verify the constructor key lock", e))?;
        Ok(Self::new(dir, key))
    }

    /// Open the store using an existing `dir/.secret_key` without creating
    /// the key or any key-creation lock file.
    ///
    /// This is intended for read-only inspection paths that must not turn a
    /// missing key into a new identity. Store operations still use the normal
    /// encrypted-file locking and write behavior.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory or key file is missing or cannot
    /// be read, and a crypto error if the key is not valid hex.
    pub fn with_key_file_read_only(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let key = parse_key_file(read_key_file(&dir.join(KEY_FILE_NAME)))?;
        Ok(Self::new(dir, key))
    }

    /// The secrets file this store reads and writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }
}

impl fmt::Debug for EncryptedFileSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedFileSecrets")
            .field("path", &self.inner.path)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretStore for EncryptedFileSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        validate_name(name)?;
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        // Retain the admitted namespace and stable sidecar object throughout readback.
        run_blocking(move || {
            let lock = inner.lock()?;
            let map = inner.read_map_in(&lock.namespace)?;
            lock.verify()
                .map_err(|e| io_error("revalidate the read lock", e))?;
            Ok(map
                .0
                .get(&name)
                .map(|value| Zeroizing::new(value.as_bytes().to_vec())))
        })
        .await
    }

    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        validate_name(name)?;
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        run_blocking(move || {
            let lock = inner.lock()?;
            let map = inner.read_map_in(&lock.namespace)?;
            let Some(value) = map.0.get(&name) else {
                return Ok(None);
            };
            let (identity, ciphertext) = inner.snapshot_in(&lock.namespace)?;
            lock.verify()
                .map_err(|e| io_error("revalidate the versioned read lock", e))?;
            let version = crate::version::version_from_parts(&[
                &identity.0.to_be_bytes(),
                &identity.1.to_be_bytes(),
                name.as_bytes(),
                &ciphertext,
            ]);
            Ok(Some((Zeroizing::new(value.as_bytes().to_vec()), version)))
        })
        .await
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        let value = Zeroizing::new(require_utf8(value, "encrypted file")?.to_string());
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        run_blocking(move || {
            let lock = inner.lock()?;
            let mut map = inner.read_map_in(&lock.namespace)?;
            map.0.insert(name, value.to_string());
            lock.verify()
                .map_err(|e| io_error("revalidate the write lock", e))?;
            inner.write_map_in(&lock.namespace, &map)?;
            lock.verify()
                .map_err(|e| io_error("verify the published lock", e))
        })
        .await
    }

    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: SecretPrecondition,
    ) -> Result<SecretVersion> {
        validate_name(name)?;
        let value = Zeroizing::new(require_utf8(value, "encrypted file")?.to_string());
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        run_blocking(move || {
            let lock = inner.lock()?;
            let mut map = inner.read_map_in(&lock.namespace)?;
            match condition {
                SecretPrecondition::Absent if map.0.contains_key(&name) => {
                    return Err(StorageError::conflict("secret precondition did not match"));
                }
                SecretPrecondition::Version(expected) => {
                    if !map.0.contains_key(&name) {
                        return Err(StorageError::conflict("secret precondition did not match"));
                    }
                    let (identity, ciphertext) = inner.snapshot_in(&lock.namespace)?;
                    let current = crate::version::version_from_parts(&[
                        &identity.0.to_be_bytes(),
                        &identity.1.to_be_bytes(),
                        name.as_bytes(),
                        &ciphertext,
                    ]);
                    if current != expected {
                        return Err(StorageError::conflict("secret precondition did not match"));
                    }
                }
                SecretPrecondition::Absent => {}
            }
            map.0.insert(name.clone(), value.to_string());
            lock.verify()
                .map_err(|e| io_error("revalidate the conditional write lock", e))?;
            inner.write_map_in(&lock.namespace, &map)?;
            lock.verify()
                .map_err(|e| io_error("verify the published conditional write", e))?;
            let (identity, ciphertext) = inner.snapshot_in(&lock.namespace)?;
            Ok(crate::version::version_from_parts(&[
                &identity.0.to_be_bytes(),
                &identity.1.to_be_bytes(),
                name.as_bytes(),
                &ciphertext,
            ]))
        })
        .await
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        run_blocking(move || {
            let lock = inner.lock()?;
            let mut map = inner.read_map_in(&lock.namespace)?;
            let Some(mut removed) = map.0.remove(&name) else {
                return Ok(false);
            };
            removed.zeroize();
            lock.verify()
                .map_err(|e| io_error("revalidate the write lock", e))?;
            inner.write_map_in(&lock.namespace, &map)?;
            lock.verify()
                .map_err(|e| io_error("verify the published lock", e))?;
            Ok(true)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.to_string();
        run_blocking(move || {
            let lock = inner.lock()?;
            let map = inner.read_map_in(&lock.namespace)?;
            lock.verify()
                .map_err(|e| io_error("revalidate the read lock", e))?;
            Ok(map
                .0
                .keys()
                .filter(|name| name.starts_with(&prefix))
                .cloned()
                .collect())
        })
        .await
    }

    fn backend_name(&self) -> &'static str {
        "encrypted_file"
    }

    fn enumerable(&self) -> bool {
        true
    }
}

/// The decrypted name → value map, wiped on drop.
#[derive(Default)]
struct SecretMap(BTreeMap<String, String>);

impl Drop for SecretMap {
    fn drop(&mut self) {
        for value in self.0.values_mut() {
            value.zeroize();
        }
    }
}

impl Inner {
    fn snapshot_in(&self, namespace: &secure::Namespace) -> Result<(secure::Identity, Vec<u8>)> {
        let name =
            secure::file_name(&self.path).map_err(|e| io_error("admit the secrets file", e))?;
        let mut file = namespace
            .open_file(name, false)
            .map_err(|e| io_error("open the versioned secrets file", e))?;
        let identity = secure::identity(&file)
            .map_err(|e| io_error("read the versioned secrets identity", e))?;
        let mut ciphertext = Vec::new();
        file.read_to_end(&mut ciphertext)
            .map_err(|e| io_error("read the versioned secrets file", e))?;
        namespace
            .verify()
            .map_err(|e| io_error("revalidate the versioned secrets namespace", e))?;
        Ok((identity, ciphertext))
    }

    fn read_map_in(&self, namespace: &secure::Namespace) -> Result<SecretMap> {
        let (blob, source_identity) = match read_private(namespace, &self.path) {
            Ok(blob) => blob,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SecretMap::default()),
            Err(e) => return Err(io_error("read the secrets file", e)),
        };
        if blob.is_empty() {
            return Ok(SecretMap::default());
        }
        let decrypted = crypto::decrypt(&self.key, &blob);
        // The quarantine probe re-reads the file; keep one ciphertext copy live.
        drop(blob);
        let json = match decrypted {
            Ok(json) => json,
            Err(_error) if self.corrupt_file_policy == CorruptFilePolicy::QuarantineAndContinue => {
                return Ok(self
                    .quarantine_corrupt_in(namespace, source_identity)?
                    .unwrap_or_default());
            }
            Err(error) => return Err(error),
        };
        // The serde message can quote the offending input, which here is
        // plaintext, so it is neither kept nor chained as the source.
        match serde_json::from_slice(&json) {
            Ok(map) => Ok(SecretMap(map)),
            Err(_) if self.corrupt_file_policy == CorruptFilePolicy::QuarantineAndContinue => {
                Ok(self
                    .quarantine_corrupt_in(namespace, source_identity)?
                    .unwrap_or_default())
            }
            Err(_) => Err(StorageError::serialization(
                "decrypted secrets file is not a json object of strings",
            )),
        }
    }

    fn quarantine_corrupt_in(
        &self,
        namespace: &secure::Namespace,
        corrupt_identity: secure::Identity,
    ) -> Result<Option<SecretMap>> {
        static QUARANTINE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let name =
            secure::file_name(&self.path).map_err(|e| io_error("admit the secret name", e))?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        for _ in 0..128 {
            let sequence = QUARANTINE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let mut archived = name.as_os_str().to_os_string();
            archived.push(format!(".corrupt.{timestamp}.{sequence}"));
            let archived = PathBuf::from(archived);
            #[cfg(test)]
            tests::before_quarantine_archive_check(&archived);
            match namespace.open_file(&archived, false) {
                Ok(_) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error("inspect the corruption archive", error)),
            }
            #[cfg(test)]
            tests::after_quarantine_absence_check(&archived);
            let source = namespace
                .open_file(name, false)
                .map_err(|error| io_error("revalidate the corrupt secrets file", error))?;
            if secure::identity(&source)
                .map_err(|error| io_error("identify the corrupt secrets file", error))?
                != corrupt_identity
            {
                return Err(StorageError::conflict(
                    "secrets file changed before corruption quarantine",
                ));
            }
            #[cfg(test)]
            tests::after_quarantine_source_check();
            let mut current = namespace
                .open_file(name, false)
                .map_err(|error| io_error("probe the corrupt secrets file", error))?;
            if secure::identity(&current)
                .map_err(|error| io_error("identify the corruption probe", error))?
                != corrupt_identity
            {
                return Err(StorageError::conflict(
                    "secrets file changed before corruption probe",
                ));
            }
            let mut current_bytes = Vec::new();
            current
                .read_to_end(&mut current_bytes)
                .map_err(|error| io_error("probe the corrupt secrets file", error))?;
            namespace
                .verify()
                .map_err(|error| io_error("revalidate the corruption probe", error))?;
            if let Ok(json) = crypto::decrypt(&self.key, &current_bytes)
                && let Ok(map) = serde_json::from_slice(&json)
            {
                return Ok(Some(SecretMap(map)));
            }
            drop(current);
            #[cfg(test)]
            let move_result = if tests::fail_quarantine_move() {
                Err(std::io::Error::other("injected quarantine move failure"))
            } else {
                namespace.move_noreplace(name, &archived)
            };
            #[cfg(not(test))]
            let move_result = namespace.move_noreplace(name, &archived);
            match move_result {
                Ok(()) => {
                    #[cfg(test)]
                    tests::after_quarantine_move(name, &archived);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(io_error("quarantine the corrupt secrets file", error)),
            }
            let archived_file = namespace
                .open_file(&archived, false)
                .map_err(|error| io_error("verify the corruption archive", error))?;
            if secure::identity(&archived_file)
                .map_err(|error| io_error("identify the corruption archive", error))?
                != corrupt_identity
            {
                Self::restore_replaced_source(namespace, &archived, name)?;
                return Err(StorageError::conflict(
                    "quarantined secrets file identity changed",
                ));
            }
            #[cfg(unix)]
            namespace
                .sync()
                .map_err(|error| io_error("persist the secrets quarantine", error))?;
            return Ok(None);
        }
        Err(StorageError::backend(
            "could not find an unused secrets quarantine name",
        ))
    }

    fn restore_replaced_source(
        namespace: &secure::Namespace,
        archived: &Path,
        source: &Path,
    ) -> Result<()> {
        match namespace.move_noreplace(archived, source) {
            Ok(()) => {}
            // A later writer won the source name. Keep its file and the
            // mismatched archived object; neither may be overwritten.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(error) => return Err(io_error("restore the replaced secrets file", error)),
        }
        #[cfg(unix)]
        namespace
            .sync()
            .map_err(|error| io_error("persist the restored secrets file", error))?;
        Ok(())
    }

    fn write_map_in(&self, namespace: &secure::Namespace, map: &SecretMap) -> Result<()> {
        let json = Zeroizing::new(serde_json::to_vec(&map.0)?);
        let blob = crypto::encrypt(&self.key, &json)?;
        write_atomic_in(namespace, &self.path, &blob)
    }

    /// Take the exclusive cross-process lock on `<path>.lock`, held until the
    /// returned file is dropped.
    fn lock(&self) -> Result<secure::Locked> {
        let mut name = self.path.file_name().unwrap_or_default().to_os_string();
        name.push(".lock");
        lock_file(&self.path.with_file_name(name))
    }
}

/// Take an exclusive advisory lock on `lock_path` (created if absent), held
/// until the returned file is dropped. The same `flock`/`LockFileEx` lock
/// OpenHuman's `fs2` lock takes.
///
/// # Errors
///
/// Returns an error if the lock path is unsafe, the lock cannot be created or
/// opened, or the advisory lock cannot be acquired.
pub fn lock_file(lock_path: &Path) -> Result<Locked> {
    let namespace = open_parent(lock_path, true)?;
    let name = secure::file_name(lock_path).map_err(|e| io_error("admit the lock name", e))?;
    let file = match namespace.create_file(name) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => namespace
            .open_file(name, true)
            .map_err(|e| io_error("open the lock file", e))?,
        Err(error) => return Err(io_error("create the lock file", error)),
    };
    fs4::FileExt::lock(&file).map_err(|e| io_error("take the lock", e))?;
    let lock = Locked {
        namespace,
        file,
        name: name.to_path_buf(),
    };
    lock.verify()
        .map_err(|e| io_error("verify the held lock", e))?;
    Ok(lock)
}

fn open_parent(path: &Path, create: bool) -> Result<secure::Namespace> {
    secure::file_name(path).map_err(|e| io_error("admit the secret name", e))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    secure::Namespace::open(parent, create).map_err(|e| io_error("admit the secrets directory", e))
}

#[cfg(test)]
fn create_parent(path: &Path) -> Result<()> {
    open_parent(path, true).map(|_| ())
}

fn read_private(
    namespace: &secure::Namespace,
    path: &Path,
) -> std::io::Result<(Vec<u8>, secure::Identity)> {
    let mut file = namespace.open_file(secure::file_name(path)?, false)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let identity = secure::identity(&file)?;
    namespace.verify()?;
    Ok((bytes, identity))
}

/// Replace a private object and verify the published identity and bytes.
/// The staged file is owner-only before any payload bytes are written, and the
/// containing directory is synced on Unix. Windows preserves the platform's
/// retained-handle replacement semantics; corruption quarantine separately
/// uses write-through no-replace moves.
///
/// # Errors
///
/// Returns an error if the path is unsafe, the parent cannot be admitted, the
/// private staging file cannot be written or synced, publication fails, or
/// the visible file fails identity/content verification.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let namespace = open_parent(path, true)?;
    write_atomic_in(&namespace, path, bytes)
}

/// Sync the parent directory of `path` after publishing a file entry.
///
/// Unix filesystems use this to make a rename or link durable. Windows does
/// not expose an equivalent directory sync through this driver, so this helper
/// is available on Unix only.
///
/// # Errors
///
/// Returns an I/O error if the parent directory cannot be admitted or synced.
#[cfg(unix)]
pub fn sync_parent_dir(path: &Path) -> Result<()> {
    let namespace = open_parent(path, false)?;
    namespace
        .sync()
        .map_err(|error| io_error("sync the parent directory", error))
}

fn write_atomic_in(namespace: &secure::Namespace, path: &Path, bytes: &[u8]) -> Result<()> {
    let name = secure::file_name(path).map_err(|e| io_error("admit the secret name", e))?;
    match namespace.open_file(name, false) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("admit the existing secrets file", error)),
    }
    let tmp_path = stage(path, bytes, &mut next_temp_seq)?;
    let tmp_name =
        secure::file_name(&tmp_path).map_err(|e| io_error("admit the staged name", e))?;
    let staged = namespace
        .open_file(tmp_name, false)
        .map_err(|e| io_error("verify the staged file", e))?;
    let expected =
        secure::identity(&staged).map_err(|e| io_error("read the staged identity", e))?;
    drop(staged);
    #[cfg(test)]
    tests::after_staged_publication_identity();
    namespace.replace(tmp_name, name).map_err(|e| {
        let _ = namespace.remove(tmp_name);
        io_error("publish the secrets file", e)
    })?;
    // A failure beyond this point is an uncertain published outcome, not rollback.
    #[cfg(unix)]
    namespace
        .sync()
        .map_err(|e| io_error("persist the published secrets namespace", e))?;
    let mut published = namespace
        .open_file(name, true)
        .map_err(|e| io_error("verify the published file", e))?;
    if secure::identity(&published).map_err(|e| io_error("read the published identity", e))?
        != expected
    {
        return Err(StorageError::conflict("published secrets identity changed"));
    }
    published
        .sync_all()
        .map_err(|e| io_error("flush the published file", e))?;
    let mut observed = Vec::new();
    published
        .read_to_end(&mut observed)
        .map_err(|e| io_error("read back the published file", e))?;
    if observed != bytes {
        return Err(StorageError::conflict("published secrets readback differs"));
    }
    namespace
        .verify()
        .map_err(|e| io_error("verify the publication namespace", e))
}

/// The next temp-file sequence number for this process.
fn next_temp_seq() -> u64 {
    TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Write `bytes` to a fresh, synced, `0600` temp sibling of `path` and return
/// its path. Failed staging attempts remove their private sibling when I/O permits.
fn stage(path: &Path, bytes: &[u8], next_seq: &mut dyn FnMut() -> u64) -> Result<PathBuf> {
    let namespace = open_parent(path, true)?;
    let (tmp_path, mut file) = reserve_temp_file(path, next_seq)?;
    #[cfg(test)]
    tests::after_staged_reservation(&tmp_path, &file);
    let staged = (|| -> std::io::Result<()> {
        namespace.verify()?;
        let reopened = namespace.open_file(secure::file_name(&tmp_path)?, false)?;
        if secure::identity(&reopened)? != secure::identity(&file)? {
            return Err(secure::denied("staged file identity changed"));
        }
        drop(reopened);
        file.write_all(bytes)?;
        file.sync_all()
    })();
    staged.map(|()| tmp_path.clone()).map_err(|e| {
        let _ =
            namespace.remove(secure::file_name(&tmp_path).unwrap_or_else(|_| Path::new("invalid")));
        io_error("stage the secrets file", e)
    })
}

/// The native create establishes private permissions before returning a handle.
fn reserve_temp_file(path: &Path, next_seq: &mut dyn FnMut() -> u64) -> Result<(PathBuf, File)> {
    loop {
        let mut name = path.file_name().unwrap_or_default().to_os_string();
        name.push(format!(".{}.{}.tmp", std::process::id(), next_seq()));
        let tmp_path = path.with_file_name(&name);
        #[cfg(unix)]
        let created = rustix::fs::open(
            &tmp_path,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map(File::from)
        .map_err(std::io::Error::from);
        #[cfg(windows)]
        let created = clack_private_fs::create_new_file(&tmp_path);
        match created {
            Ok(file) => return Ok((tmp_path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error("reserve a private staging file", error)),
        }
    }
}

/// Load the hex key at `path`, or create it with 32 random bytes when absent.
///
/// This is the `.secret_key` format of OpenHuman's `SecretStore`: 64 hex
/// characters, surrounding whitespace ignored. A new key is written and synced
/// to a temp sibling (`0600` on Unix) and only then published under `path`
/// with a hard link, which fails if the file already exists. A reader never
/// sees a partial key file, and two processes racing to create it agree on the
/// winner's key. `<key filename>.lock` serializes the temporary two-link
/// interval; arbitrary existing hard links are rejected. New key and lock
/// objects are private at creation. Blocks on disk I/O.
///
/// # Errors
///
/// [`ErrorKind::Crypto`](tinystoragedrivers_types::ErrorKind::Crypto) for a
/// key file that is not 32 bytes of hex, or an I/O error mapped as the file
/// driver maps them (including a filesystem without hard links).
pub fn load_or_create_key_file(path: &Path) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let lock = key_creation_lock(path)?;
    match read_key_file(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        other => return parse_key_file(other),
    }
    create_key_file(path, &lock)
}

fn key_creation_lock(path: &Path) -> Result<secure::Locked> {
    let mut lock_name = path.file_name().unwrap_or_default().to_os_string();
    lock_name.push(".lock");
    // Serialize the short two-link publication interval across processes.
    // A retained stale alias still fails admission rather than minting a key.
    lock_file(&path.with_file_name(lock_name))
}

fn create_key_file(path: &Path, lock: &secure::Locked) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let key = crypto::generate_key()?;
    let tmp_path = stage(path, key_to_hex(&key).as_bytes(), &mut next_temp_seq)?;
    lock.verify()
        .map_err(|e| io_error("revalidate the key creation lock", e))?;
    let published = publish_key(&tmp_path, path, key)?;
    lock.verify()
        .map_err(|e| io_error("verify the key creation lock", e))?;
    Ok(published)
}

fn admit_existing_store(namespace: &secure::Namespace, path: &Path) -> Result<Vec<File>> {
    let data_name =
        secure::file_name(path).map_err(|e| io_error("admit existing store name", e))?;
    let mut store_lock = data_name.as_os_str().to_os_string();
    store_lock.push(".lock");
    let mut ledger = data_name.as_os_str().to_os_string();
    ledger.push(".imports");
    let mut admitted = Vec::new();
    // If it exists, retain the cooperating store lock while admitting data and
    // ledger objects. A true empty target does not create this lock before key.
    for name in [Path::new(&store_lock), data_name, Path::new(&ledger)] {
        match namespace.open_file(name, name == Path::new(&store_lock)) {
            Ok(file) => {
                if name == Path::new(&store_lock) {
                    fs4::FileExt::lock(&file)
                        .map_err(|e| io_error("lock existing secret store", e))?;
                }
                admitted.push(file);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("admit existing secret store object", error)),
        }
    }
    namespace
        .verify()
        .map_err(|e| io_error("verify existing secret store namespace", e))?;
    Ok(admitted)
}

/// Publish the staged key at `tmp_path` as `path` unless `path` exists, in
/// which case the admitted existing key wins. Cleanup errors are reported.
fn publish_key(
    tmp_path: &Path,
    path: &Path,
    key: Zeroizing<[u8; KEY_LEN]>,
) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let source_namespace = open_parent(tmp_path, false)?;
    let namespace = match open_parent(path, false) {
        Ok(namespace) => namespace,
        Err(error) => {
            let temporary = secure::file_name(tmp_path)
                .map_err(|e| io_error("admit the staged key name", e))?;
            source_namespace
                .remove(temporary)
                .map_err(|e| io_error("remove the rejected staged key", e))?;
            #[cfg(unix)]
            source_namespace
                .sync()
                .map_err(|e| io_error("persist staged key cleanup", e))?;
            return Err(error);
        }
    };
    if source_namespace
        .identity()
        .map_err(|e| io_error("inspect the staged key namespace", e))?
        != namespace
            .identity()
            .map_err(|e| io_error("inspect the key namespace", e))?
    {
        return Err(StorageError::conflict(
            "key publication requires one admitted directory",
        ));
    }
    let temporary =
        secure::file_name(tmp_path).map_err(|e| io_error("admit the staged key name", e))?;
    let target = secure::file_name(path).map_err(|e| io_error("admit the key name", e))?;
    let staged = namespace
        .open_file(temporary, false)
        .map_err(|e| io_error("retain the staged key identity", e))?;
    let expected =
        secure::identity(&staged).map_err(|e| io_error("inspect the staged key identity", e))?;
    #[cfg(test)]
    tests::after_staged_key_identity();
    let linked = namespace.link(temporary, target);
    let cleanup = namespace.remove(temporary);
    if let Err(error) = cleanup {
        return Err(io_error("remove the staged key", error));
    }
    #[cfg(unix)]
    namespace
        .sync()
        .map_err(|e| io_error("persist key publication and staging cleanup", e))?;
    match linked {
        Ok(()) => {
            let mut published = namespace
                .open_file(target, true)
                .map_err(|e| io_error("open the published key", e))?;
            if secure::identity(&published)
                .map_err(|e| io_error("inspect the published key identity", e))?
                != expected
            {
                return Err(StorageError::conflict("published key object changed"));
            }
            published
                .sync_all()
                .map_err(|e| io_error("flush the published key", e))?;
            let mut hex = Zeroizing::new(String::new());
            published
                .read_to_string(&mut hex)
                .map_err(|e| io_error("read back the published key", e))?;
            let verified = key_from_hex(&hex)?;
            if *verified != *key {
                return Err(StorageError::conflict("published key readback differs"));
            }
            drop(key);
            Ok(verified)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            parse_key_file(read_key_file(path))
        }
        Err(error) => Err(io_error("publish the key file", error)),
    }
}

fn read_key_file(path: &Path) -> std::io::Result<Zeroizing<String>> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let namespace = secure::Namespace::open(parent, false)?;
    let mut file = namespace.open_file(secure::file_name(path)?, false)?;
    let mut text = Zeroizing::new(String::new());
    file.read_to_string(&mut text)?;
    namespace.verify()?;
    Ok(text)
}

fn parse_key_file(read: std::io::Result<Zeroizing<String>>) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let hex = read.map_err(|e| io_error("read the key file", e))?;
    key_from_hex(&hex)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
