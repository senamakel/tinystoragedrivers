//! Protected NTFS objects, retained namespace pins and safe native publication.
//!
//! Publication verifies the visible object and flushes its content explicitly.
//! Quarantine moves use Windows write-through semantics for durable directory
//! publication. Ordinary replacements preserve Rust's POSIX replacement behavior.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf, Prefix};
use std::thread;
use std::time::Duration;

use super::secure::{Identity, denied};
use windows_permissions::{
    LocalBox, SecurityDescriptor, Sid,
    constants::{AceFlags, AceType, SeObjectType, SecurityInformation},
    wrappers,
};

pub(super) struct Namespace {
    pins: Vec<(PathBuf, File, Identity)>,
    pub(super) path: PathBuf,
}

impl Namespace {
    pub(super) fn open(path: &Path, create: bool) -> io::Result<Self> {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let mut components = path.components();
        let Some(Component::Prefix(prefix)) = components.next() else {
            return Err(denied("missing local volume"));
        };
        let (Prefix::Disk(drive) | Prefix::VerbatimDisk(drive)) = prefix.kind() else {
            return Err(denied("unsupported namespace path"));
        };
        if components.next() != Some(Component::RootDir) {
            return Err(denied("ambiguous namespace path"));
        }
        let root = format!("{}:\\", char::from(drive));
        let mut fs_name = String::new();
        let mut flags = winsafe::co::FILE_VOL::default();
        winsafe::GetVolumeInformation(
            Some(&root),
            None,
            None,
            None,
            Some(&mut flags),
            Some(&mut fs_name),
        )
        .map_err(|_| denied("cannot admit namespace volume"))?;
        if fs_name != "NTFS" || !flags.has(winsafe::co::FILE_VOL::PERSISTENT_ACLS) {
            return Err(denied("unsupported namespace volume"));
        }
        let user = current_user()?;
        // Preserve verbatim local spelling for every native component pin.
        // Rebuilding an ordinary prefix would silently restore MAX_PATH limits.
        let mut current = PathBuf::from(prefix.as_os_str());
        current.push(std::path::MAIN_SEPARATOR.to_string());
        let mut pins = Vec::new();
        pin_directory(&mut pins, &current, &user)?;
        for component in components {
            let Component::Normal(name) = component else {
                return Err(denied("ambiguous namespace component"));
            };
            valid_leaf(Path::new(name))?;
            current.push(name);
            if create {
                match clack_private_fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            pin_directory(&mut pins, &current, &user)?;
        }
        let namespace = Self { pins, path };
        namespace.verify()?;
        Ok(namespace)
    }

    pub(super) fn identity(&self) -> io::Result<Identity> {
        self.pins
            .last()
            .map(|(_, _, id)| *id)
            .ok_or_else(|| denied("missing namespace authority"))
    }

    pub(super) fn verify(&self) -> io::Result<()> {
        let user = current_user()?;
        for (path, file, original) in &self.pins {
            verify_directory(file, &user)?;
            let reopened = directory_options().open(path)?;
            if identity(&reopened)? != *original {
                return Err(denied("namespace component identity changed"));
            }
        }
        Ok(())
    }

    pub(super) fn open_file(&self, name: &Path, write: bool) -> io::Result<File> {
        self.verify()?;
        valid_leaf(name)?;
        let file = OpenOptions::new()
            .read(true)
            .write(write)
            // Retained leaf handles preserve FileID without blocking a later
            // legitimate atomic replacement. Directory pins deny delete sharing.
            .share_mode(7)
            .custom_flags(0x0020_0000)
            .open(self.path.join(name))?;
        verify_file(&file)?;
        Ok(file)
    }

    pub(super) fn create_file(&self, name: &Path) -> io::Result<File> {
        self.verify()?;
        valid_leaf(name)?;
        let file = clack_private_fs::create_new_file(&self.path.join(name))?;
        verify_file(&file)?;
        Ok(file)
    }

    pub(super) fn replace(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.verify()?;
        valid_leaf(source)?;
        valid_leaf(target)?;
        // Rust's Windows rename fallback uses FileRenameInfoEx with
        // POSIX_SEMANTICS, preserving admitted old handles while replacing.
        // Keep this separate from the write-through quarantine move below.
        retry_av_transient(|| std::fs::rename(self.path.join(source), self.path.join(target)))?;
        self.verify()
    }

    pub(super) fn move_noreplace(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.verify()?;
        valid_leaf(source)?;
        valid_leaf(target)?;
        atomicwrites::move_atomic(&self.path.join(source), &self.path.join(target))?;
        self.verify()
    }

    pub(super) fn link(&self, source: &Path, target: &Path) -> io::Result<()> {
        self.verify()?;
        valid_leaf(source)?;
        valid_leaf(target)?;
        std::fs::hard_link(self.path.join(source), self.path.join(target))
    }

    pub(super) fn remove(&self, name: &Path) -> io::Result<()> {
        self.verify()?;
        valid_leaf(name)?;
        retry_av_transient(|| std::fs::remove_file(self.path.join(name)))
    }
}

fn retry_av_transient<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    const RETRIES: u32 = 5;
    let mut attempt = 0;
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if transient_av_error(&error) && attempt < RETRIES - 1 => {
                thread::sleep(Duration::from_millis(20 * (1 << attempt)));
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

fn transient_av_error(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(5 | 32 | 33))
}

#[cfg(test)]
#[path = "windows_tests.rs"]
mod tests;

fn directory_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    // FILE_LIST_DIRECTORY ensures no-delete sharing is a real replacement pin.
    options
        .access_mode(0x0002_0081)
        .share_mode(3)
        .custom_flags(0x0220_0000);
    options
}

fn pin_directory(
    pins: &mut Vec<(PathBuf, File, Identity)>,
    path: &Path,
    user: &str,
) -> io::Result<()> {
    let file = directory_options().open(path)?;
    verify_directory(&file, user)?;
    let id = identity(&file)?;
    pins.push((path.to_path_buf(), file, id));
    Ok(())
}

pub(super) fn identity(file: &File) -> io::Result<Identity> {
    let info = winapi_util::file::information(file)?;
    Ok(Identity(info.volume_serial_number(), info.file_index()))
}

pub(super) fn verify_file(file: &File) -> io::Result<()> {
    let info = winapi_util::file::information(file)?;
    if !file.metadata()?.is_file()
        || info.file_attributes() & 0x400 != 0
        || info.number_of_links() != 1
    {
        return Err(denied("secret file type or link count is unsafe"));
    }
    let user = current_user()?;
    let sd = descriptor(file)?;
    if sd.owner().map(sid_text).transpose()?.as_deref() != Some(&user) {
        return Err(denied("secret file owner is unsafe"));
    }
    let acl = sd
        .dacl()
        .ok_or_else(|| denied("secret file dacl is absent"))?;
    if acl.len() != 1 {
        return Err(denied("secret file dacl is unsafe"));
    }
    let ace = acl
        .get_ace(0)
        .ok_or_else(|| denied("secret file ace is absent"))?;
    if ace.ace_type() != AceType::ACCESS_ALLOWED_ACE_TYPE
        || ace.flags().contains(AceFlags::InheritOnly)
        || ace.mask().bits() != 0x001f_01ff
        || ace.sid().map(sid_text).transpose()?.as_deref() != Some(&user)
    {
        return Err(denied("secret file access is unsafe"));
    }
    // Existing owner-only inherited files may be admitted, but foreign grants
    // are never repaired. New files are separately protected at native birth.
    Ok(())
}

fn descriptor(file: &File) -> io::Result<LocalBox<SecurityDescriptor>> {
    wrappers::GetSecurityInfo(
        file,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
}

fn verify_directory(file: &File, user: &str) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & 0x400 != 0 {
        return Err(denied("namespace directory type is unsafe"));
    }
    let sd = descriptor(file)?;
    let owner = sd
        .owner()
        .map(sid_text)
        .transpose()?
        .ok_or_else(|| denied("namespace owner is absent"))?;
    if !trusted(&owner, user) {
        return Err(denied("namespace owner is unsafe"));
    }
    let acl = sd
        .dacl()
        .ok_or_else(|| denied("namespace dacl is absent"))?;
    if acl.len() == 0 {
        return Err(denied("namespace dacl is empty"));
    }
    for index in 0..acl.len() {
        let ace = acl
            .get_ace(index)
            .ok_or_else(|| denied("namespace ace is absent"))?;
        if ace.ace_type() != AceType::ACCESS_ALLOWED_ACE_TYPE {
            return Err(denied("unsupported namespace acl"));
        }
        if ace.flags().contains(AceFlags::InheritOnly) {
            continue;
        }
        let sid = ace
            .sid()
            .map(sid_text)
            .transpose()?
            .ok_or_else(|| denied("namespace ace identity is absent"))?;
        if !trusted(&sid, user) && ace.mask().bits() & 0x500d_0152 != 0 {
            return Err(denied("namespace permits foreign mutation"));
        }
    }
    Ok(())
}

fn trusted(sid: &str, user: &str) -> bool {
    sid == user
        || matches!(
            sid,
            "S-1-5-18"
                | "S-1-5-32-544"
                | "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464"
        )
}

fn current_user() -> io::Result<String> {
    match winsafe::HTHREAD::GetCurrentThread().OpenThreadToken(winsafe::co::TOKEN::QUERY, true) {
        Err(error) if error == winsafe::co::ERROR::NO_TOKEN => {}
        _ => return Err(denied("impersonated private creation is unsupported")),
    }
    let token = winsafe::HPROCESS::GetCurrentProcess()
        .OpenProcessToken(winsafe::co::TOKEN::QUERY)
        .map_err(|_| denied("cannot inspect effective user"))?;
    let winsafe::TokenInfo::User(user) = token
        .GetTokenInformation(winsafe::co::TOKEN_INFORMATION_CLASS::User)
        .map_err(|_| denied("cannot inspect effective user"))?
    else {
        return Err(denied("effective user is absent"));
    };
    winsafe::ConvertSidToStringSid(
        user.User
            .Sid()
            .ok_or_else(|| denied("effective user is absent"))?,
    )
    .map_err(|_| denied("cannot inspect effective user"))
}

fn sid_text(sid: &Sid) -> io::Result<String> {
    wrappers::ConvertSidToStringSid(sid)?
        .into_string()
        .map_err(|_| denied("invalid native identity"))
}

fn valid_leaf(name: &Path) -> io::Result<()> {
    super::secure::leaf(name)?;
    let value = name
        .to_str()
        .ok_or_else(|| denied("invalid native file name"))?;
    if value.contains([':', '\\', '/', '<', '>', '"', '|', '?', '*', '\0'])
        || value.ends_with(['.', ' '])
    {
        return Err(denied("invalid native file name"));
    }
    let stem = value
        .split('.')
        .next()
        .ok_or_else(|| denied("invalid native file name"))?
        .to_ascii_uppercase();
    if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && stem.as_bytes()[3].is_ascii_digit())
    {
        return Err(denied("reserved native file name"));
    }
    Ok(())
}
