//! Bounded read-only admission of a host-selected legacy HEX key file.

use super::{
    File, Path, PathBuf, Read, Result, StorageError, Zeroizing, crypto, fmt, open_parent, secure,
};
use async_trait::async_trait;
use tinystoragedrivers_types::ErrorKind;

/// Maximum original encoded bytes, including surrounding UTF-8 whitespace.
pub const MAX_HEX_KEY_FILE_BYTES: usize = 4096;

/// Read-only access to one trusted, explicitly registered historical key source.
///
/// This port grants no creation, repair, publication or cleanup authority.
/// Implementations return original encoding only after validating a 32-byte key.
#[async_trait]
pub trait HexKeyFileSource: Send + Sync + fmt::Debug {
    /// Read the validated original encoding, or confirmed admitted leaf absence.
    ///
    /// # Errors
    /// Malformed keys are `Crypto`; invalid UTF-8, size or binding is
    /// `InvalidInput`; unsafe/unsupported native admission is `Backend`;
    /// transient I/O is `Unavailable`. Missing parents are errors.
    async fn read_encoded(&self) -> Result<Option<Zeroizing<Vec<u8>>>>;
}

/// An absolute host-selected key file, read through the driver's native floor.
///
/// Each read admits existing ancestors and an owner-only, single-link regular
/// file, rejects symlinks/reparse points, then rechecks protection, named identity
/// and exact bytes. Windows read admission permits owner-only inherited DACLs;
/// it does not require the protected-DACL bit. No modes or ACLs are repaired.
///
/// These checks detect changes during the checked interval, not arbitrary
/// uncoordinated same-user writes after the final check. Reading establishes
/// neither durability nor source deletion authority. A canceled async operation
/// may finish its read-only blocking work; it cannot create, write or delete.
#[derive(Clone)]
pub struct ProtectedHexKeyFile {
    path: PathBuf,
}

impl ProtectedHexKeyFile {
    /// Bind an absolute explicit source without filesystem I/O.
    ///
    /// # Errors
    /// Returns `InvalidInput` for relative, ambiguous, null-containing or
    /// leafless paths. Unsupported native paths fail during read admission.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !path.is_absolute()
            || path.as_os_str().as_encoded_bytes().contains(&0)
            || path.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
            || secure::file_name(&path).is_err()
        {
            return Err(StorageError::invalid_input(
                "invalid protected key source binding",
            ));
        }
        Ok(Self { path })
    }
}

impl fmt::Debug for ProtectedHexKeyFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProtectedHexKeyFile([redacted])")
    }
}

#[async_trait]
impl HexKeyFileSource for ProtectedHexKeyFile {
    async fn read_encoded(&self) -> Result<Option<Zeroizing<Vec<u8>>>> {
        let path = self.path.clone();
        crate::task::run_blocking(move || read_encoded(&path))
            .await
            .map_err(redact)
    }
}

fn redact(error: StorageError) -> StorageError {
    let kind = error.kind();
    drop(error);
    StorageError::new(kind, "protected key source operation failed")
}

fn native_error(error: std::io::Error) -> StorageError {
    let kind = match error.kind() {
        std::io::ErrorKind::WouldBlock
        | std::io::ErrorKind::Interrupted
        | std::io::ErrorKind::TimedOut => ErrorKind::Unavailable,
        _ => ErrorKind::Backend,
    };
    drop(error);
    StorageError::new(kind, "protected key source admission or read failed")
}

fn read_encoded(path: &Path) -> Result<Option<Zeroizing<Vec<u8>>>> {
    read_checked(path, &mut |_| {})
}

#[derive(Clone, Copy)]
enum Checkpoint {
    Metadata,
    Read,
    Absence,
}

fn read_checked(
    path: &Path,
    observe: &mut impl FnMut(Checkpoint),
) -> Result<Option<Zeroizing<Vec<u8>>>> {
    let namespace = open_parent(path, false).map_err(redact)?;
    let name = secure::file_name(path).map_err(native_error)?;
    let mut file = match namespace.open_file(name, false) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            observe(Checkpoint::Absence);
            namespace.verify().map_err(native_error)?;
            // Confirm the leaf is still absent after namespace verification.
            return match namespace.open_file(name, false) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    namespace.verify().map_err(native_error)?;
                    Ok(None)
                }
                _ => Err(StorageError::backend(
                    "protected key source absence changed",
                )),
            };
        }
        Err(error) => return Err(native_error(error)),
    };
    let identity = secure::identity(&file).map_err(native_error)?;
    if file.metadata().map_err(native_error)?.len() > MAX_HEX_KEY_FILE_BYTES as u64 {
        return Err(StorageError::invalid_input(
            "protected key source exceeds size bound",
        ));
    }
    observe(Checkpoint::Metadata);
    let bytes = read_bounded(&mut file)?;
    validate(&bytes)?;
    observe(Checkpoint::Read);
    verify_file(&file)?;
    namespace.verify().map_err(native_error)?;
    let mut reopened = namespace.open_file(name, false).map_err(native_error)?;
    if secure::identity(&reopened).map_err(native_error)? != identity {
        return Err(StorageError::backend(
            "protected key source identity changed",
        ));
    }
    let readback = read_bounded(&mut reopened)?;
    if readback != bytes {
        return Err(StorageError::backend("protected key source bytes changed"));
    }
    verify_file(&file)?;
    verify_file(&reopened)?;
    let final_named = namespace.open_file(name, false).map_err(native_error)?;
    if secure::identity(&final_named).map_err(native_error)? != identity {
        return Err(StorageError::backend(
            "protected key source identity changed",
        ));
    }
    namespace.verify().map_err(native_error)?;
    Ok(Some(bytes))
}

fn verify_file(file: &File) -> Result<()> {
    #[cfg(unix)]
    super::unix::verify_file(file).map_err(native_error)?;
    #[cfg(windows)]
    super::windows::verify_file(file).map_err(native_error)?;
    Ok(())
}

fn read_bounded(reader: &mut impl Read) -> Result<Zeroizing<Vec<u8>>> {
    let limit = MAX_HEX_KEY_FILE_BYTES + 1;
    // Allocate and initialize before any plaintext arrives. Direct slice reads
    // avoid read_to_end's stack probe and Vec growth with retired key copies.
    let mut bytes = Zeroizing::new(vec![0; limit]);
    let mut used = 0;
    while used < limit {
        match reader.read(&mut bytes[used..limit]) {
            Ok(0) => break,
            Ok(count) => {
                if count > limit - used {
                    return Err(StorageError::backend(
                        "protected key source reader exceeded destination",
                    ));
                }
                used += count;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(native_error(error)),
        }
    }
    if used > MAX_HEX_KEY_FILE_BYTES {
        return Err(StorageError::invalid_input(
            "protected key source exceeds size bound",
        ));
    }
    // Truncation changes only the logical length; it never reallocates.
    bytes.truncate(used);
    Ok(bytes)
}

fn validate(bytes: &[u8]) -> Result<()> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| StorageError::invalid_input("protected key source is not utf-8"))?;
    let trimmed = text.trim();
    if trimmed.len() != 64 || !trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StorageError::crypto(
            "protected key source is not a 32-byte hex key",
        ));
    }
    // Entire borrowed input is prevalidated before crypto's ordinary hex decoder
    // can encounter an invalid partial decode. The decoded key zeroizes on drop.
    let _key = crypto::key_from_hex(trimmed).map_err(redact)?;
    Ok(())
}

#[cfg(test)]
#[path = "hex_key_source_tests.rs"]
mod tests;
