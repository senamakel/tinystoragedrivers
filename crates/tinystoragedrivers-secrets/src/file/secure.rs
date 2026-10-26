//! Platform-independent names and guarded object identity.

use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
pub(super) use super::unix::{Namespace, identity, is_unlinked};
#[cfg(windows)]
pub(super) use super::windows::{Namespace, identity};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Identity(pub(super) u64, pub(super) u64);

pub(super) fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

pub(super) fn leaf(path: &Path) -> io::Result<&Path> {
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(name)), None) if !name.is_empty() => Ok(path),
        _ => Err(denied("invalid secret file name")),
    }
}

pub(super) fn file_name(path: &Path) -> io::Result<&Path> {
    let name = Path::new(
        path.file_name()
            .ok_or_else(|| denied("missing secret file name"))?,
    );
    leaf(name)
}

/// An exclusive advisory file lock held until this guard is dropped.
pub struct Locked {
    pub(super) namespace: Namespace,
    // Both the descriptor and namespace pins live until the operation finishes.
    pub(super) file: File,
    pub(super) name: PathBuf,
}

impl std::fmt::Debug for Locked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Locked").finish_non_exhaustive()
    }
}

impl Locked {
    /// Confirm the named lock file still refers to the descriptor held by
    /// this guard.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory, lock name, or held file no longer
    /// matches the identities admitted when the lock was acquired.
    pub fn verify(&self) -> io::Result<()> {
        let reopened = self.namespace.open_file(leaf(&self.name)?, false)?;
        if identity(&reopened)? != identity(&self.file)? {
            return Err(denied("lock object identity changed"));
        }
        Ok(())
    }
}
