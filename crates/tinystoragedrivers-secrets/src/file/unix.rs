//! Descriptor-relative Unix namespace admission and private file operations.

use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path};

use rustix::fs::{self, AtFlags, Mode, OFlags, RenameFlags};

use super::secure::{Identity, denied};

pub(super) struct Namespace {
    pins: Vec<File>,
    names: Vec<std::ffi::OsString>,
}

impl Namespace {
    pub(super) fn open(path: &Path, create: bool) -> io::Result<Self> {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let mut pins = vec![File::from(fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?)];
        let mut names = Vec::new();
        for component in path.components() {
            let name = match component {
                Component::RootDir => continue,
                Component::Normal(name) => name,
                _ => return Err(denied("ambiguous namespace path")),
            };
            let parent = pins
                .last()
                .ok_or_else(|| denied("missing namespace authority"))?;
            verify_directory(parent, false)?;
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let opened = match fs::openat(parent, name, flags, Mode::empty()) {
                Ok(fd) => fd,
                Err(error) if error == rustix::io::Errno::NOENT && create => {
                    match fs::mkdirat(parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
                        Ok(()) => parent.sync_all()?,
                        Err(error) if error == rustix::io::Errno::EXIST => {}
                        Err(error) => return Err(error.into()),
                    }
                    fs::openat(parent, name, flags, Mode::empty())?
                }
                Err(error) => return Err(error.into()),
            };
            let file = File::from(opened);
            verify_directory(&file, false)?;
            pins.push(file);
            names.push(name.to_os_string());
        }
        verify_directory(
            pins.last().ok_or_else(|| denied("missing namespace"))?,
            true,
        )?;
        let namespace = Self { pins, names };
        namespace.verify()?;
        Ok(namespace)
    }

    fn directory(&self) -> io::Result<&File> {
        self.pins
            .last()
            .ok_or_else(|| denied("missing namespace authority"))
    }

    pub(super) fn identity(&self) -> io::Result<Identity> {
        identity(self.directory()?)
    }

    pub(super) fn verify(&self) -> io::Result<()> {
        for (index, pair) in self.pins.windows(2).enumerate() {
            verify_directory(&pair[0], false)?;
            let named = fs::statat(&pair[0], &self.names[index], AtFlags::SYMLINK_NOFOLLOW)?;
            let pinned = fs::fstat(&pair[1])?;
            if named.st_dev != pinned.st_dev || named.st_ino != pinned.st_ino {
                return Err(denied("namespace component identity changed"));
            }
        }
        verify_directory(self.directory()?, true)
    }

    pub(super) fn open_file(&self, name: &Path, write: bool) -> io::Result<File> {
        self.verify()?;
        let flags = if write { OFlags::RDWR } else { OFlags::RDONLY };
        let file = File::from(fs::openat(
            self.directory()?,
            name,
            flags | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        verify_file(&file)?;
        Ok(file)
    }

    pub(super) fn create_file(&self, name: &Path) -> io::Result<File> {
        self.verify()?;
        let file = File::from(fs::openat(
            self.directory()?,
            name,
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        verify_file(&file)?;
        Ok(file)
    }

    pub(super) fn replace(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.verify()?;
        fs::renameat(self.directory()?, source, self.directory()?, target)?;
        Ok(())
    }

    pub(super) fn move_noreplace(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.verify()?;
        fs::renameat_with(
            self.directory()?,
            source,
            self.directory()?,
            target,
            RenameFlags::NOREPLACE,
        )?;
        self.verify()
    }

    pub(super) fn link(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.verify()?;
        fs::linkat(
            self.directory()?,
            source,
            self.directory()?,
            target,
            AtFlags::empty(),
        )?;
        Ok(())
    }

    pub(super) fn remove(&self, name: &Path) -> io::Result<()> {
        self.verify()?;
        fs::unlinkat(self.directory()?, name, AtFlags::empty())?;
        Ok(())
    }

    pub(super) fn sync(&self) -> io::Result<()> {
        self.verify()?;
        self.directory()?.sync_all()
    }
}

pub(super) fn identity(file: &File) -> io::Result<Identity> {
    let metadata = file.metadata()?;
    Ok(Identity(metadata.dev(), metadata.ino()))
}

pub(super) fn is_unlinked(file: &File) -> io::Result<bool> {
    Ok(file.metadata()?.nlink() == 0)
}

pub(super) fn verify_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.nlink() != 1
        || metadata.mode() & 0o777 != 0o600
    {
        return Err(denied("secret file identity or permissions are unsafe"));
    }
    Ok(())
}

fn verify_directory(file: &File, leaf: bool) -> io::Result<()> {
    let metadata = file.metadata()?;
    let user = rustix::process::geteuid().as_raw();
    let trusted_owner = metadata.uid() == user || (!leaf && metadata.uid() == 0);
    // Root-owned sticky temporary roots may contain user-owned private children;
    // the acting namespace itself must never be foreign-writable.
    let sticky_root = !leaf && metadata.uid() == 0 && metadata.mode() & 0o1000 != 0;
    if !metadata.is_dir() || !trusted_owner || (metadata.mode() & 0o022 != 0 && !sticky_root) {
        return Err(denied("secret namespace permits foreign mutation"));
    }
    Ok(())
}
