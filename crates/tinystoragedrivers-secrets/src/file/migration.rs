//! Bounded source admission, immutable import intents and conditional cleanup.
//!
//! The encrypted intent ledger retains the old bytes across uncertain cleanup.
//! It is an independent auxiliary file; legacy secret/key formats do not change.

use super::{
    Arc, EncryptedFileSecrets, File, Inner, Path, PathBuf, Read, Result, SecretMap, StorageError,
    Zeroize, Zeroizing, crypto, fmt, io_error, open_parent, read_private, require_utf8,
    run_blocking, secure, validate_name, write_atomic_in,
};
use serde::{Deserialize, Serialize};
use sha2_11::{Digest, Sha256};

/// The native API boundary actually acknowledged by a file publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationBoundary {
    /// File content and the containing directory were successfully synced.
    /// This acknowledges those native APIs, not physical hardware survival.
    UnixDirectorySync,
    /// The visible published identity was verified, file flushed and read back.
    /// No ordinary-user durable namespace barrier is established.
    WindowsVisibleFileFlushed,
}

/// What conditional source cleanup actually acknowledged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupAcknowledgment {
    /// The verified source was removed and its directory synced.
    RemovedAndDirectorySynced,
    /// An exact operation already recorded completed cleanup.
    AlreadyCompleted,
    /// The platform cannot establish the required namespace persistence boundary.
    /// The source is retained.
    UnsupportedBoundary,
    /// Removal occurred, but persistence or its final ledger acknowledgment failed.
    /// The encrypted source backup remains available for recovery and retry.
    RemovedUnacknowledged,
}

struct Source {
    path: PathBuf,
    namespace: secure::Namespace,
    identity: secure::Identity,
    namespace_identity: secure::Identity,
    bytes: Zeroizing<Vec<u8>>,
    file: File,
    // Opaque randomness identifies this live source instance, not an inode
    // generation. A fresh open can never choose a persisted instance token.
    live_instance: String,
    #[cfg(unix)]
    coordination: std::sync::Mutex<()>,
}

/// A private regular source file admitted through its opened object.
///
/// Source selection, parsing and consent belong to the caller. Admission is
/// limited to 1 MiB, rejects aliases and unsafe namespaces, and retains pins.
pub struct FileImportSource(Arc<Source>);

impl FileImportSource {
    /// Read and pin a bounded private migration source without changing it.
    ///
    /// # Errors
    /// Returns an error for unsafe identity/access, unsupported paths, I/O or
    /// files exceeding the 1 MiB bound. No destination key is created here.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let namespace = open_parent(&path, false)?;
        let name =
            secure::file_name(&path).map_err(|e| io_error("admit the import source name", e))?;
        let mut file = namespace
            .open_file(name, false)
            .map_err(|e| io_error("admit the import source", e))?;
        if file
            .metadata()
            .map_err(|e| io_error("inspect the import source", e))?
            .len()
            > 1_048_576
        {
            return Err(StorageError::invalid_input(
                "import source exceeds the bounded file size",
            ));
        }
        let identity = secure::identity(&file)
            .map_err(|e| io_error("inspect the import source identity", e))?;
        let namespace_identity = namespace
            .identity()
            .map_err(|e| io_error("inspect the import namespace identity", e))?;
        // Allocate the whole bounded buffer before any plaintext arrives:
        // a growing Vec would free earlier copies of it without zeroizing.
        let mut bytes = Zeroizing::new(vec![0_u8; 1_048_577]);
        let mut used = 0;
        while used < bytes.len() {
            match file.read(&mut bytes[used..]) {
                Ok(0) => break,
                Ok(count) => used += count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(io_error("read the import source", error)),
            }
        }
        if used > 1_048_576 {
            return Err(StorageError::invalid_input(
                "import source exceeds the bounded file size",
            ));
        }
        bytes.truncate(used);
        namespace
            .verify()
            .map_err(|e| io_error("verify the import namespace", e))?;
        Ok(Self(Arc::new(Source {
            path,
            namespace,
            identity,
            namespace_identity,
            bytes,
            file,
            live_instance: crypto::hex_encode(crypto::generate_key()?.as_ref()),
            #[cfg(unix)]
            coordination: std::sync::Mutex::new(()),
        })))
    }

    /// The admitted bytes for caller-owned source parsing.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.0.bytes
    }
}

impl fmt::Debug for FileImportSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileImportSource([redacted])")
    }
}

/// Opaque exact-object acknowledgment for an immutable file import operation.
///
/// This is issued only after destination verification. A Windows acknowledgment
/// never authorizes durable source cleanup. No key, name, path or value appears
/// in its Debug representation. The retained handles make this a live receipt;
/// it cannot be reconstructed from a pathname or recycled file identity after
/// process restart. Restart recovery retrieves authenticated backup bytes.
pub struct VerifiedFileImport {
    inner: Arc<Inner>,
    #[cfg(unix)]
    source: Arc<Source>,
    #[cfg(windows)]
    _source: Arc<Source>,
    #[cfg(unix)]
    intent: Intent,
    #[cfg(unix)]
    destination: secure::Identity,
    _destination_file: File,
    #[cfg(unix)]
    namespace: secure::Identity,
    #[cfg(windows)]
    _namespace: secure::Identity,
    boundary: PublicationBoundary,
}

impl VerifiedFileImport {
    /// The precise native publication boundary this acknowledgment represents.
    #[must_use]
    pub fn boundary(&self) -> PublicationBoundary {
        self.boundary
    }
}

impl fmt::Debug for VerifiedFileImport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedFileImport")
            .field("boundary", &self.boundary)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
struct Intent {
    operation: String,
    name: String,
    #[serde(default)]
    live_instance: String,
    source_identity: [u64; 2],
    namespace_identity: [u64; 2],
    source_hash: String,
    value_hash: String,
    source_backup: String,
    cleaned: bool,
    #[serde(default)]
    removing: bool,
}

impl Drop for Intent {
    fn drop(&mut self) {
        self.source_backup.zeroize();
    }
}

impl EncryptedFileSecrets {
    /// Publish a caller-selected value backed by an immutable admitted source.
    ///
    /// An encrypted auxiliary intent ledger binds operation, source and value
    /// before destination publication. Exact retries reconcile uncertain writes;
    /// a different operation/value/source instance conflicts. A new open cannot
    /// reconstruct an old receipt from matching filesystem IDs and bytes.
    /// This never deletes source.
    ///
    /// # Errors
    /// Validation, admission, corruption, conflict and I/O failures are errors.
    /// An error after publication can mean visible-but-unacknowledged state;
    /// retry this exact operation. The source and encrypted backup are retained.
    pub async fn prepare_file_import(
        &self,
        operation: &str,
        name: &str,
        value: &[u8],
        source: &FileImportSource,
    ) -> Result<VerifiedFileImport> {
        validate_name(operation)?;
        validate_name(name)?;
        let value = Zeroizing::new(require_utf8(value, "file import")?.to_string());
        let operation = operation.to_string();
        let name = name.to_string();
        let source = Arc::clone(&source.0);
        let inner = Arc::clone(&self.inner);
        run_blocking(move || prepare(inner, source, &operation, &name, &value)).await
    }

    /// Conditionally remove the exact source of a verified acknowledged import.
    ///
    /// Unix captures the source under a quarantine named for the import intent before
    /// verifying identity again. Unsupported Windows persistence retains source.
    /// Source backup remains encrypted in the ledger after every outcome.
    ///
    /// Cleanup requires callers and cooperating writers to acquire the source
    /// file's exclusive advisory lock before changing its bytes or namespace
    /// links. Cleanup takes that lock nonblocking, and rechecks identity, safety
    /// and bytes after ledger publication. An uncoordinated same-user change
    /// between the final check and unlink is outside this guarantee.
    ///
    /// # Errors
    /// A mismatching source, destination, key, operation or namespace conflicts.
    /// Before removal, I/O errors retain the source or its quarantine. An error
    /// never authorizes deleting a different object occupying the original name.
    pub async fn cleanup_file_import(
        &self,
        receipt: &VerifiedFileImport,
    ) -> Result<CleanupAcknowledgment> {
        if self.inner.key.as_ref() != receipt.inner.key.as_ref()
            || self.inner.path != receipt.inner.path
        {
            return Err(StorageError::conflict(
                "import acknowledgment belongs to a different store",
            ));
        }
        #[cfg(windows)]
        {
            // No publicly issued Windows receipt grants namespace durability.
            // Keep its pins and authenticated backup without cleanup dispatch.
            std::future::ready(Ok(CleanupAcknowledgment::UnsupportedBoundary)).await
        }
        #[cfg(unix)]
        {
            if receipt.boundary == PublicationBoundary::WindowsVisibleFileFlushed {
                return Ok(CleanupAcknowledgment::UnsupportedBoundary);
            }
            let inner = Arc::clone(&self.inner);
            let source = Arc::clone(&receipt.source);
            let intent = receipt.intent.clone();
            let destination = receipt.destination;
            let namespace = receipt.namespace;
            run_blocking(move || cleanup(&inner, &source, &intent, destination, namespace)).await
        }
    }

    /// Recover admitted source bytes retained by an exact encrypted import intent.
    ///
    /// This works after process restart with the same key. It returns bytes,
    /// not a cleanup receipt, and never removes a current pathname. Caller
    /// policy decides how to use the recovered payload. A fresh source object
    /// cannot replace the exact source bound by an existing import operation.
    ///
    /// # Errors
    /// Returns an error for unknown/mismatching operations, corruption or I/O.
    pub async fn recover_file_import_source(
        &self,
        operation: &str,
        name: &str,
    ) -> Result<Zeroizing<Vec<u8>>> {
        validate_name(operation)?;
        validate_name(name)?;
        let operation = operation.to_string();
        let name = name.to_string();
        let inner = Arc::clone(&self.inner);
        run_blocking(move || {
            let lock = inner.lock()?;
            let map = read_ledger(&inner, &lock.namespace)?;
            let intent = get_intent(&map, &name)?
                .ok_or_else(|| StorageError::not_found("import intent is absent"))?;
            if intent.operation != operation {
                return Err(StorageError::conflict("import operation differs"));
            }
            crypto::hex_decode(&intent.source_backup).map(Zeroizing::new)
        })
        .await
    }
}

/// The cleanup quarantine for one import: scoped by its name and by the exact
/// source object, so imports that share a source directory never share one.
fn quarantine_name(intent: &Intent) -> PathBuf {
    let scope = format!(
        "{}:{}:{}",
        intent.name, intent.source_identity[0], intent.source_identity[1]
    );
    PathBuf::from(format!(".import-cleanup-{}", digest(scope.as_bytes())))
}

fn digest(bytes: &[u8]) -> String {
    crypto::hex_encode(&Sha256::digest(bytes))
}
fn ids(id: secure::Identity) -> [u64; 2] {
    [id.0, id.1]
}

fn ledger_path(inner: &Inner) -> PathBuf {
    let mut name = inner.path.file_name().unwrap_or_default().to_os_string();
    name.push(".imports");
    inner.path.with_file_name(name)
}

fn read_ledger(inner: &Inner, namespace: &secure::Namespace) -> Result<SecretMap> {
    let (blob, _) = match read_private(namespace, &ledger_path(inner)) {
        Ok(blob) => blob,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SecretMap::default());
        }
        Err(error) => return Err(io_error("read the import ledger", error)),
    };
    let bytes = crypto::decrypt(&inner.key, &blob)?;
    serde_json::from_slice(&bytes)
        .map(SecretMap)
        .map_err(|_| StorageError::serialization("import ledger is malformed"))
}

fn write_ledger(inner: &Inner, namespace: &secure::Namespace, map: &SecretMap) -> Result<()> {
    let bytes = Zeroizing::new(serde_json::to_vec(&map.0)?);
    let blob = crypto::encrypt(&inner.key, &bytes)?;
    write_atomic_in(namespace, &ledger_path(inner), &blob)
}

fn get_intent(map: &SecretMap, name: &str) -> Result<Option<Intent>> {
    map.0
        .get(name)
        .map(|encoded| {
            serde_json::from_str(encoded)
                .map_err(|_| StorageError::serialization("import intent is malformed"))
        })
        .transpose()
}

fn record_intent(map: &mut SecretMap, intent: &Intent) -> Result<()> {
    map.0
        .insert(intent.name.clone(), serde_json::to_string(intent)?);
    Ok(())
}

fn verify_source(source: &Source, name: &Path) -> Result<()> {
    let mut file = source
        .namespace
        .open_file(name, false)
        .map_err(|e| io_error("verify the import source", e))?;
    let id =
        secure::identity(&file).map_err(|e| io_error("verify the import source identity", e))?;
    let mut bytes = Zeroizing::new(Vec::new());
    (&mut file)
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .map_err(|e| io_error("verify the import source bytes", e))?;
    let retained = secure::identity(&source.file)
        .map_err(|e| io_error("verify retained import source identity", e))?;
    if id != source.identity
        || retained != source.identity
        || digest(&bytes) != digest(&source.bytes)
    {
        return Err(StorageError::conflict("import source changed"));
    }
    source
        .namespace
        .verify()
        .map_err(|e| io_error("verify the import source namespace", e))
}

fn reject_source_alias(
    inner: &Inner,
    namespace: &secure::Namespace,
    source: &Source,
    name: &Path,
) -> Result<()> {
    if namespace
        .identity()
        .map_err(|e| io_error("inspect the import namespace", e))?
        != source.namespace_identity
    {
        return Ok(());
    }
    let destination =
        secure::file_name(&inner.path).map_err(|e| io_error("admit the import destination", e))?;
    let mut lock = destination.as_os_str().to_os_string();
    lock.push(".lock");
    let ledger = ledger_path(inner);
    for reserved in [
        destination,
        Path::new(&lock),
        secure::file_name(&ledger).map_err(|e| io_error("admit the import ledger", e))?,
        Path::new(super::KEY_FILE_NAME),
        Path::new(".secret_key.lock"),
    ] {
        if name == reserved {
            return Err(StorageError::conflict(
                "import source aliases destination bookkeeping",
            ));
        }
        // Native aliases (case, Unicode normalization, filesystem spelling)
        // are properties of opened objects, never guessed by text folding.
        match namespace.open_file(reserved, false) {
            Ok(file) => {
                if secure::identity(&file)
                    .map_err(|e| io_error("inspect reserved import object", e))?
                    == source.identity
                {
                    return Err(StorageError::conflict(
                        "import source aliases destination bookkeeping",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("admit reserved import object", error)),
        }
    }
    Ok(())
}

fn prepare(
    inner: Arc<Inner>,
    source: Arc<Source>,
    operation: &str,
    name: &str,
    value: &str,
) -> Result<VerifiedFileImport> {
    let namespace = open_parent(&inner.path, true)?;
    let source_name =
        secure::file_name(&source.path).map_err(|e| io_error("admit the import source name", e))?;
    reject_source_alias(&inner, &namespace, &source, source_name)?;
    let lock = inner.lock()?;
    reject_source_alias(&inner, &lock.namespace, &source, source_name)?;
    let wanted = Intent {
        operation: operation.to_string(),
        name: name.to_string(),
        live_instance: source.live_instance.clone(),
        source_identity: ids(source.identity),
        namespace_identity: ids(source.namespace_identity),
        source_hash: digest(&source.bytes),
        value_hash: digest(value.as_bytes()),
        source_backup: crypto::hex_encode(&source.bytes),
        cleaned: false,
        removing: false,
    };
    let mut ledger = read_ledger(&inner, &lock.namespace)?;
    let mut map = inner.read_map_in(&lock.namespace)?;
    let intent = if let Some(mut previous) = get_intent(&ledger, name)? {
        let cleaned = previous.cleaned;
        let removing = previous.removing;
        previous.cleaned = false;
        previous.removing = false;
        if previous != wanted {
            return Err(StorageError::conflict(
                "import operation source or value differs",
            ));
        }
        previous.cleaned = cleaned;
        previous.removing = removing;
        if !cleaned {
            verify_source(&source, source_name)?;
        }
        previous
    } else {
        verify_source(&source, source_name)?;
        if map.0.contains_key(name) {
            return Err(StorageError::conflict(
                "import destination already exists without this intent",
            ));
        }
        record_intent(&mut ledger, &wanted)?;
        write_ledger(&inner, &lock.namespace, &ledger)?;
        wanted
    };
    if let Some(existing) = map.0.get(name) {
        if existing != value {
            return Err(StorageError::conflict("import destination value differs"));
        }
    } else {
        map.0.insert(name.to_string(), value.to_string());
        inner.write_map_in(&lock.namespace, &map)?;
    }
    lock.verify()
        .map_err(|e| io_error("revalidate the import lock", e))?;
    let destination = verify_destination(&inner, &lock.namespace, &intent, None)?;
    let destination_file = lock
        .namespace
        .open_file(
            secure::file_name(&inner.path)
                .map_err(|e| io_error("admit the destination receipt name", e))?,
            false,
        )
        .map_err(|e| io_error("retain the acknowledged destination", e))?;
    if secure::identity(&destination_file)
        .map_err(|e| io_error("inspect the retained destination", e))?
        != destination
    {
        return Err(StorageError::conflict(
            "destination changed before receipt creation",
        ));
    }
    let namespace = lock
        .namespace
        .identity()
        .map_err(|e| io_error("verify the destination namespace identity", e))?;
    #[cfg(unix)]
    let boundary = PublicationBoundary::UnixDirectorySync;
    #[cfg(windows)]
    let boundary = PublicationBoundary::WindowsVisibleFileFlushed;
    Ok(VerifiedFileImport {
        inner,
        #[cfg(unix)]
        source,
        #[cfg(windows)]
        _source: source,
        #[cfg(unix)]
        intent,
        #[cfg(unix)]
        destination,
        _destination_file: destination_file,
        #[cfg(unix)]
        namespace,
        #[cfg(windows)]
        _namespace: namespace,
        boundary,
    })
}

fn verify_destination(
    inner: &Inner,
    namespace: &secure::Namespace,
    intent: &Intent,
    #[cfg(unix)] expected: Option<secure::Identity>,
    #[cfg(windows)] _expected: Option<secure::Identity>,
) -> Result<secure::Identity> {
    let name = secure::file_name(&inner.path)
        .map_err(|e| io_error("admit the imported destination name", e))?;
    let file = namespace
        .open_file(name, true)
        .map_err(|e| io_error("verify the imported destination", e))?;
    let id = secure::identity(&file)
        .map_err(|e| io_error("verify the imported destination identity", e))?;
    #[cfg(unix)]
    if expected.is_some_and(|expected| expected != id) {
        return Err(StorageError::conflict(
            "imported destination object changed",
        ));
    }
    let map = inner.read_map_in(namespace)?;
    if map
        .0
        .get(&intent.name)
        .is_none_or(|value| digest(value.as_bytes()) != intent.value_hash)
    {
        return Err(StorageError::conflict("imported destination value changed"));
    }
    file.sync_all()
        .map_err(|e| io_error("flush the imported destination", e))?;
    #[cfg(unix)]
    namespace
        .sync()
        .map_err(|e| io_error("persist the imported destination namespace", e))?;
    Ok(id)
}

#[cfg(unix)]
struct SourceLock<'a>(&'a File);

#[cfg(unix)]
impl<'a> SourceLock<'a> {
    fn acquire(file: &'a File) -> Result<Self> {
        fs4::FileExt::try_lock(file)
            .map_err(|e| io_error("coordinate the import source", e.into()))?;
        Ok(Self(file))
    }
}

#[cfg(unix)]
impl Drop for SourceLock<'_> {
    fn drop(&mut self) {
        // An error cannot authorize unlink: this guard releases only after
        // cleanup's acknowledged/error result has already been determined.
        let _ = fs4::FileExt::unlock(self.0);
    }
}

#[cfg(unix)]
fn cleanup(
    inner: &Inner,
    source: &Source,
    receipt: &Intent,
    destination: secure::Identity,
    namespace: secure::Identity,
) -> Result<CleanupAcknowledgment> {
    cleanup_with_barrier(
        inner,
        source,
        receipt,
        destination,
        namespace,
        &mut secure::Namespace::sync,
    )
}

#[cfg(unix)]
fn cleanup_with_barrier(
    inner: &Inner,
    source: &Source,
    receipt: &Intent,
    destination: secure::Identity,
    namespace: secure::Identity,
    removal_barrier: &mut dyn FnMut(&secure::Namespace) -> std::io::Result<()>,
) -> Result<CleanupAcknowledgment> {
    let lock = inner.lock()?;
    // Serialize receipts sharing a Source, and cooperate with external writers
    // using this same opened-file advisory lock. Uncoordinated writers are not
    // excluded by an advisory lock; the caller owns namespace coordination.
    let _coordination = source
        .coordination
        .lock()
        .map_err(|_| StorageError::backend("import source coordination lock is poisoned"))?;
    let _source_lock = SourceLock::acquire(&source.file)?;
    if lock
        .namespace
        .identity()
        .map_err(|e| io_error("verify the cleanup namespace identity", e))?
        != namespace
    {
        return Err(StorageError::conflict(
            "import destination namespace changed",
        ));
    }
    let mut ledger = read_ledger(inner, &lock.namespace)?;
    let intent = get_intent(&ledger, &receipt.name)?
        .ok_or_else(|| StorageError::conflict("import intent disappeared"))?;
    let mut pending = intent.clone();
    pending.cleaned = false;
    pending.removing = false;
    let mut expected = receipt.clone();
    expected.cleaned = false;
    expected.removing = false;
    if pending != expected {
        return Err(StorageError::conflict("import intent changed"));
    }
    verify_destination(inner, &lock.namespace, receipt, Some(destination))?;
    if intent.cleaned {
        return Ok(CleanupAcknowledgment::AlreadyCompleted);
    }
    let original = secure::file_name(&source.path)
        .map_err(|e| io_error("admit the cleanup source name", e))?;
    let quarantine = quarantine_name(receipt);
    if intent.removing && absent(&source.namespace, &quarantine)? {
        if !absent(&source.namespace, original)? {
            return Err(StorageError::conflict(
                "source name changed during removal reconciliation",
            ));
        }
        if !secure::is_unlinked(&source.file)
            .map_err(|e| io_error("inspect retained source unlink state", e))?
        {
            return Err(StorageError::conflict(
                "captured source remains linked outside cleanup names",
            ));
        }
        return finish_removal(
            inner,
            &lock.namespace,
            &source.namespace,
            &mut ledger,
            &mut pending,
            removal_barrier,
        );
    }
    capture_source(source, original, &quarantine)?;
    source
        .namespace
        .sync()
        .map_err(|e| io_error("persist the captured cleanup source", e))?;
    verify_destination(inner, &lock.namespace, receipt, Some(destination))?;
    lock.verify()
        .map_err(|e| io_error("revalidate the cleanup lock", e))?;
    // Persist the exact verified capture phase before unlinking it. Only this
    // phase permits a later absent-name retry to acknowledge removal.
    pending.removing = true;
    record_intent(&mut ledger, &pending)?;
    write_ledger(inner, &lock.namespace, &ledger)?;
    #[cfg(test)]
    tests::after_removing_ledger();
    lock.verify()
        .map_err(|e| io_error("verify the committed cleanup lock", e))?;
    verify_destination(inner, &lock.namespace, receipt, Some(destination))?;
    // All fallible ledger/publication work precedes the final exact-object,
    // permissions and byte check. Retain mismatches without unlink or restore.
    verify_source(source, &quarantine)?;
    source
        .namespace
        .remove(&quarantine)
        .map_err(|e| io_error("remove the verified import source", e))?;
    finish_removal(
        inner,
        &lock.namespace,
        &source.namespace,
        &mut ledger,
        &mut pending,
        removal_barrier,
    )
}

#[cfg(unix)]
fn capture_source(source: &Source, original: &Path, quarantine: &Path) -> Result<()> {
    match source.namespace.create_file(quarantine) {
        Ok(file) => {
            if let Err(error) = verify_source(source, original) {
                // Unlink only the reservation this call created: a name that
                // now resolves to another object is left for its owner.
                let reserved = secure::identity(&file)
                    .map_err(|e| io_error("identify the cleanup reservation", e))?;
                drop(file);
                let still_reserved = source
                    .namespace
                    .open_file(quarantine, false)
                    .ok()
                    .and_then(|named| secure::identity(&named).ok())
                    == Some(reserved);
                if still_reserved {
                    source
                        .namespace
                        .remove(quarantine)
                        .map_err(|e| io_error("remove unused cleanup reservation", e))?;
                }
                return Err(error);
            }
            drop(file);
            source
                .namespace
                .replace(original, quarantine)
                .map_err(|e| io_error("capture the cleanup source", e))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io_error("reserve the cleanup source quarantine", error)),
    }
    // Verification after the atomic capture prevents deleting a substituted
    // object. A mismatching capture remains recoverable, never unlinked.
    if let Err(error) = verify_source(source, quarantine) {
        // Restore a mismatching capture only with no-clobber publication.
        // If another source appeared, retain the captured object in quarantine.
        if source.namespace.link(quarantine, original).is_ok() {
            source
                .namespace
                .remove(quarantine)
                .map_err(|e| io_error("remove restored capture link", e))?;
            source
                .namespace
                .sync()
                .map_err(|e| io_error("persist restored source capture", e))?;
        }
        return Err(error);
    }
    Ok(())
}

#[cfg(unix)]
fn finish_removal(
    inner: &Inner,
    destination: &secure::Namespace,
    source: &secure::Namespace,
    ledger: &mut SecretMap,
    intent: &mut Intent,
    barrier: &mut dyn FnMut(&secure::Namespace) -> std::io::Result<()>,
) -> Result<CleanupAcknowledgment> {
    if barrier(source).is_err() {
        return Ok(CleanupAcknowledgment::RemovedUnacknowledged);
    }
    intent.cleaned = true;
    intent.removing = true;
    record_intent(ledger, intent)?;
    if write_ledger(inner, destination, ledger).is_err() {
        return Ok(CleanupAcknowledgment::RemovedUnacknowledged);
    }
    Ok(CleanupAcknowledgment::RemovedAndDirectorySynced)
}

#[cfg(unix)]
fn absent(namespace: &secure::Namespace, name: &Path) -> Result<bool> {
    match namespace.open_file(name, false) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(io_error("inspect the cleanup name", error)),
    }
}

#[cfg(test)]
#[path = "migration_tests.rs"]
mod tests;
