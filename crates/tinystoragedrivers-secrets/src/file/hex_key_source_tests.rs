//! Protected encoded-key source behavior.
use super::*;
use std::path::PathBuf;

fn directory() -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let path = root.path().join("private");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
    }
    #[cfg(windows)]
    clack_private_fs::create_dir(&path).unwrap();
    (root, path)
}

fn inventory(path: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}

#[test]
fn admitted_missing_leaf_is_absence_without_mutation() {
    let (_root, dir) = directory();
    let before = inventory(&dir);
    let result = read_encoded(&dir.join(".secret_key")).unwrap();
    let after = inventory(&dir);
    assert_eq!(after, before, "read-only source created filesystem entries");
    assert!(result.is_none(), "missing source minted a replacement");
}

fn fixture(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap()
    };
    #[cfg(windows)]
    let mut file = clack_private_fs::create_new_file(path).unwrap();
    file.write_all(bytes).unwrap();
}

fn encoded() -> Zeroizing<Vec<u8>> {
    Zeroizing::new(b"Ab".repeat(32))
}

#[test]
fn legacy_master_loader_is_a_mutating_initializer() {
    let (_root, dir) = directory();
    super::super::load_or_create_key_file(&dir.join(".secret_key")).unwrap();
    assert_eq!(inventory(&dir), [".secret_key", ".secret_key.lock"]);
}

#[tokio::test]
async fn public_trait_preserves_original_encoding_without_sidecars() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    let mut bytes = Zeroizing::new("\u{2003}\t".as_bytes().to_vec());
    bytes.extend_from_slice(&encoded());
    bytes.extend_from_slice("\r\n\u{a0}".as_bytes());
    fixture(&path, &bytes);
    let before = inventory(&dir);
    let source: Box<dyn HexKeyFileSource> = Box::new(ProtectedHexKeyFile::new(&path).unwrap());
    let result = source.read_encoded().await.unwrap().unwrap();
    assert_eq!(result, bytes);
    assert_eq!(
        crypto::key_from_hex(std::str::from_utf8(&result).unwrap())
            .unwrap()
            .as_ref(),
        &[0xab; 32]
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes.as_slice());
    assert_eq!(inventory(&dir), before);
    assert!(!format!("{source:?}").contains("registered"));
    assert!(!format!("{source:?}").contains("AbAb"));
}

#[test]
fn exact_bound_is_accepted_and_bound_plus_one_is_rejected_without_repair() {
    for length in [MAX_HEX_KEY_FILE_BYTES, MAX_HEX_KEY_FILE_BYTES + 1] {
        let (_root, dir) = directory();
        let path = dir.join("registered");
        let mut bytes = Zeroizing::new(vec![b' '; length - 64]);
        bytes.extend_from_slice(&encoded());
        fixture(&path, &bytes);
        if length == MAX_HEX_KEY_FILE_BYTES {
            assert_eq!(read_encoded(&path).unwrap().unwrap(), bytes);
        } else {
            assert_eq!(
                read_encoded(&path).unwrap_err().kind(),
                ErrorKind::InvalidInput
            );
        }
        assert_eq!(std::fs::read(&path).unwrap(), bytes.as_slice());
        assert_eq!(inventory(&dir), ["registered"]);
    }
}

#[tokio::test]
async fn malformed_sources_fail_with_redacted_errors_and_remain_unchanged() {
    use std::error::Error;
    for (bytes, kind) in [
        (Zeroizing::new(vec![0xff; 64]), ErrorKind::InvalidInput),
        (Zeroizing::new(b"g".repeat(64)), ErrorKind::Crypto),
        (Zeroizing::new(vec![]), ErrorKind::Crypto),
        (Zeroizing::new(b"0".repeat(63)), ErrorKind::Crypto),
        (Zeroizing::new(b"0".repeat(65)), ErrorKind::Crypto),
        (Zeroizing::new(b"0".repeat(32)), ErrorKind::Crypto),
        (Zeroizing::new(vec![0; 64]), ErrorKind::Crypto),
        (Zeroizing::new(b"0 ".repeat(32)), ErrorKind::Crypto),
        (
            Zeroizing::new(
                "\u{feff}"
                    .as_bytes()
                    .iter()
                    .copied()
                    .chain(b"0".repeat(64))
                    .collect(),
            ),
            ErrorKind::Crypto,
        ),
    ] {
        let (_root, dir) = directory();
        let path = dir.join("private-synthetic-path");
        fixture(&path, &bytes);
        let error = ProtectedHexKeyFile::new(&path)
            .unwrap()
            .read_encoded()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert!(error.source().is_none());
        let diagnostics = format!("{error} {error:?}");
        assert!(!diagnostics.contains("private-synthetic-path"));
        assert!(!diagnostics.contains("0000000000000000"));
        assert_eq!(std::fs::read(&path).unwrap(), bytes.as_slice());
        assert_eq!(inventory(&dir), ["private-synthetic-path"]);
    }
}

#[tokio::test]
async fn missing_parent_is_error_and_is_never_created() {
    use std::error::Error;
    let (_root, dir) = directory();
    let path = dir.join("missing-parent").join("registered");
    let error = ProtectedHexKeyFile::new(&path)
        .unwrap()
        .read_encoded()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(error.source().is_none());
    assert!(!format!("{error:?}").contains("missing-parent"));
    assert_eq!(inventory(&dir).len(), 0);
}

#[test]
fn relative_ambiguous_null_and_leafless_bindings_are_rejected() {
    for path in [
        PathBuf::from("relative"),
        PathBuf::from("/"),
        PathBuf::from("/private/../registered"),
        PathBuf::from("/private/null\0"),
    ] {
        assert_eq!(
            ProtectedHexKeyFile::new(path).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[test]
fn nonregular_source_is_error_without_sidecars() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
    }
    #[cfg(windows)]
    clack_private_fs::create_dir(&path).unwrap();
    assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(inventory(&dir), ["registered"]);
}

#[test]
fn hardlinked_source_is_error_without_mutation() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    std::fs::hard_link(&path, dir.join("alias")).unwrap();
    assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(inventory(&dir), ["alias", "registered"]);
    assert_eq!(std::fs::read(&path).unwrap(), encoded().as_slice());
}

#[test]
fn source_growth_after_metadata_is_bounded() {
    use std::io::Write;
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Metadata) {
            let mut writer = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writer
                .write_all(&vec![b' '; MAX_HEX_KEY_FILE_BYTES])
                .unwrap();
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        MAX_HEX_KEY_FILE_BYTES as u64 + 64
    );
    assert_eq!(inventory(&dir), ["registered"]);
}

#[test]
fn stream_limit_reads_only_bound_plus_one() {
    let mut reader = std::io::Cursor::new(vec![b' '; MAX_HEX_KEY_FILE_BYTES * 2]);
    assert_eq!(
        read_bounded(&mut reader).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(reader.position(), MAX_HEX_KEY_FILE_BYTES as u64 + 1);
}

#[test]
fn retained_source_byte_change_is_rejected() {
    use std::io::Write;
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Read) {
            let mut writer = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            writer.write_all(&b"42".repeat(32)).unwrap();
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(std::fs::read(&path).unwrap(), b"42".repeat(32));
    assert_eq!(inventory(&dir), ["registered"]);
}

#[test]
fn a_leaf_appearing_during_absence_verification_is_error() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Absence) {
            fixture(&path, &encoded());
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(std::fs::read(&path).unwrap(), encoded().as_slice());
}

#[test]
fn named_source_replacement_after_read_is_rejected() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let replacement = dir.join("replacement");
    fixture(&replacement, &encoded());
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Read) {
            std::fs::rename(&replacement, &path).unwrap();
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(inventory(&dir), ["registered"]);
}

#[test]
fn same_byte_named_substitution_preserves_both_objects_and_fails_identity_check() {
    use std::error::Error;
    let (_root, dir) = directory();
    let path = dir.join("registered");
    let replacement = dir.join("replacement");
    let preserved = dir.join("preserved-original");
    fixture(&path, &encoded());
    fixture(&replacement, &encoded());
    let original_file = File::open(&path).unwrap();
    let replacement_file = File::open(&replacement).unwrap();
    let original_id = secure::identity(&original_file).unwrap();
    let replacement_id = secure::identity(&replacement_file).unwrap();
    assert_ne!(original_id, replacement_id);

    let error = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Read) {
            std::fs::rename(&path, &preserved).unwrap();
            std::fs::rename(&replacement, &path).unwrap();
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(error.message(), "protected key source identity changed");
    assert!(error.source().is_none());
    let diagnostics = format!("{error} {error:?}");
    for sensitive in ["registered", "preserved-original", "AbAb"] {
        assert!(!diagnostics.contains(sensitive));
    }
    for file in [&original_file, &replacement_file] {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(file.metadata().unwrap().nlink(), 1);
        }
        #[cfg(windows)]
        assert_eq!(
            winapi_util::file::information(file)
                .unwrap()
                .number_of_links(),
            1
        );
    }
    assert_eq!(secure::identity(&original_file).unwrap(), original_id);
    assert_eq!(secure::identity(&replacement_file).unwrap(), replacement_id);
    assert_eq!(
        secure::identity(&File::open(&preserved).unwrap()).unwrap(),
        original_id
    );
    assert_eq!(
        secure::identity(&File::open(&path).unwrap()).unwrap(),
        replacement_id
    );
    assert_eq!(std::fs::read(&preserved).unwrap(), encoded().as_slice());
    assert_eq!(std::fs::read(&path).unwrap(), encoded().as_slice());
    assert_eq!(inventory(&dir), ["preserved-original", "registered"]);
}

#[cfg(unix)]
#[test]
fn unsafe_modes_and_symlink_sources_are_rejected_without_repair() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for mode in [0o644, 0o400, 0o000] {
        let (_root, dir) = directory();
        let path = dir.join("registered");
        fixture(&path, &encoded());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            mode
        );
        assert_eq!(inventory(&dir), ["registered"]);
    }
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&dir.join("original"), &encoded());
    symlink(dir.join("original"), &path).unwrap();
    assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
    assert!(
        std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[cfg(unix)]
#[test]
fn unsafe_or_symlink_ancestor_cannot_be_confirmed_absence() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let (_root, dir) = directory();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(
        read_encoded(&dir.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(inventory(&dir).len(), 0);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alias = dir.with_file_name("alias");
    symlink(&dir, &alias).unwrap();
    assert_eq!(
        read_encoded(&alias.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(inventory(&dir).len(), 0);
}

#[cfg(unix)]
#[test]
fn source_protection_change_after_read_is_rejected_and_not_repaired() {
    use std::os::unix::fs::PermissionsExt;
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let error = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Read) {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );
}

#[cfg(unix)]
#[test]
fn replaced_namespace_cannot_be_confirmed_absence() {
    use std::os::unix::fs::DirBuilderExt;
    let (_root, dir) = directory();
    let moved = dir.with_file_name("moved");
    let error = read_checked(&dir.join("registered"), &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Absence) {
            std::fs::rename(&dir, &moved).unwrap();
            std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(inventory(&dir).len(), 0);
    assert_eq!(inventory(&moved).len(), 0);
}

#[test]
fn native_transient_errors_have_finite_content_free_categories() {
    use std::error::Error;
    for kind in [
        std::io::ErrorKind::WouldBlock,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::TimedOut,
    ] {
        let error = native_error(std::io::Error::new(kind, "synthetic-private-payload"));
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(error.source().is_none());
        assert!(!format!("{error:?}").contains("synthetic-private-payload"));
    }
}

#[test]
fn all_hex_cases_return_original_bytes_and_construction_is_io_free() {
    let (_root, dir) = directory();
    let missing = dir.join("absent-parent").join("registered");
    let _source = ProtectedHexKeyFile::new(missing).unwrap();
    assert_eq!(inventory(&dir).len(), 0);
    for (index, pair) in [b"ab", b"AB", b"Ab"].iter().enumerate() {
        let path = dir.join(format!("case-{index}"));
        let bytes = Zeroizing::new(pair.repeat(32));
        fixture(&path, &bytes);
        assert_eq!(read_encoded(&path).unwrap().unwrap(), bytes);
    }
}

#[tokio::test]
async fn canceled_blocking_read_can_finish_without_filesystem_mutation() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let before = inventory(&dir);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        crate::task::run_blocking(move || {
            let mut entered = Some(entered_tx);
            let result = read_checked(&path, &mut |checkpoint| {
                if matches!(checkpoint, Checkpoint::Read) {
                    entered.take().unwrap().send(()).unwrap();
                    release_rx.recv().unwrap();
                }
            });
            finished_tx.send(()).unwrap();
            result
        })
        .await
    });
    entered_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();
    finished_rx.await.unwrap();
    assert_eq!(inventory(&dir), before);
    assert_eq!(
        std::fs::read(dir.join("registered")).unwrap(),
        encoded().as_slice()
    );
}

#[cfg(windows)]
fn native_descriptor(
    file: &File,
) -> windows_permissions::LocalBox<windows_permissions::SecurityDescriptor> {
    use windows_permissions::{
        constants::{SeObjectType, SecurityInformation},
        wrappers,
    };
    wrappers::GetSecurityInfo(
        file,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
    .unwrap()
}

#[cfg(windows)]
fn native_owner(file: &File) -> String {
    windows_permissions::wrappers::ConvertSidToStringSid(native_descriptor(file).owner().unwrap())
        .unwrap()
        .into_string()
        .unwrap()
}

#[cfg(windows)]
fn native_effective_user() -> String {
    let token = match winsafe::HTHREAD::GetCurrentThread()
        .OpenThreadToken(winsafe::co::TOKEN::QUERY, true)
    {
        Ok(token) => token,
        Err(error) => {
            assert!(
                error == winsafe::co::ERROR::NO_TOKEN,
                "cannot inspect effective fixture identity"
            );
            winsafe::HPROCESS::GetCurrentProcess()
                .OpenProcessToken(winsafe::co::TOKEN::QUERY)
                .unwrap()
        }
    };
    let winsafe::TokenInfo::User(user) = token
        .GetTokenInformation(winsafe::co::TOKEN_INFORMATION_CLASS::User)
        .unwrap()
    else {
        panic!("native token did not return its user");
    };
    winsafe::ConvertSidToStringSid(user.User.Sid().unwrap()).unwrap()
}

#[cfg(windows)]
fn native_acl(path: &Path, sddl: &str) {
    use windows_permissions::{
        LocalBox, SecurityDescriptor,
        constants::{SeObjectType, SecurityInformation},
        wrappers,
    };
    let descriptor: LocalBox<SecurityDescriptor> = sddl.parse().unwrap();
    wrappers::SetNamedSecurityInfo(
        path.as_os_str(),
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        None,
        None,
        descriptor.dacl(),
        None,
    )
    .unwrap();
}

#[cfg(windows)]
#[test]
fn native_foreign_grant_and_read_denial_are_errors_without_acl_repair() {
    for denied in [false, true] {
        let (_root, dir) = directory();
        let path = dir.join("registered");
        fixture(&path, &encoded());
        let file = File::open(&path).unwrap();
        let user = native_owner(&file);
        let sddl = if denied {
            format!("D:P(D;;FR;;;{user})(A;;FA;;;{user})")
        } else {
            format!("D:P(A;;FA;;;{user})(A;;FR;;;WD)")
        };
        native_acl(&path, &sddl);
        let before = native_descriptor(&file)
            .as_sddl()
            .unwrap()
            .into_string()
            .unwrap();
        assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
        assert_eq!(
            native_descriptor(&file)
                .as_sddl()
                .unwrap()
                .into_string()
                .unwrap(),
            before
        );
        assert_eq!(inventory(&dir), ["registered"]);
        // Restore only the disposable fixture so its drop can remove it.
        native_acl(&path, &format!("D:P(A;;FA;;;{user})"));
        assert_eq!(std::fs::read(&path).unwrap(), encoded().as_slice());
    }
}

#[cfg(windows)]
#[test]
fn native_foreign_mutation_parent_cannot_be_confirmed_absence() {
    use std::os::windows::fs::OpenOptionsExt;
    let (_root, dir) = directory();
    // Open a directory using the native fixture-only backup-semantics flag.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x0200_0000)
        .open(&dir)
        .unwrap();
    let user = native_owner(&file);
    native_acl(&dir, &format!("D:P(A;OICI;FA;;;{user})(A;OICI;FA;;;WD)"));
    let before = native_descriptor(&file)
        .as_sddl()
        .unwrap()
        .into_string()
        .unwrap();
    assert_eq!(
        read_encoded(&dir.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        native_descriptor(&file)
            .as_sddl()
            .unwrap()
            .into_string()
            .unwrap(),
        before
    );
    assert_eq!(inventory(&dir).len(), 0);
    native_acl(&dir, &format!("D:P(A;OICI;FA;;;{user})"));
}

#[cfg(windows)]
#[test]
fn native_junction_ancestor_is_rejected_without_source_mutation() {
    let (_root, dir) = directory();
    fixture(&dir.join("registered"), &encoded());
    let alias = dir.with_file_name("junction");
    let result = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(&alias)
        .arg(&dir)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "native disposable junction creation failed"
    );
    let before = inventory(&dir);
    assert_eq!(
        read_encoded(&alias.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(inventory(&dir), before);
    assert_eq!(
        std::fs::read(dir.join("registered")).unwrap(),
        encoded().as_slice()
    );
    std::fs::remove_dir(alias).unwrap();
}

#[cfg(windows)]
fn set_fixture_owner_preserving_inherited_dacl(file: &File, path: &Path, user: &str) {
    use windows_permissions::{
        LocalBox, SecurityDescriptor,
        constants::{SeObjectType, SecurityInformation},
        wrappers,
    };
    let birth_identity = secure::identity(file).unwrap();
    let birth_descriptor = native_descriptor(file);
    let birth_sddl = birth_descriptor.as_sddl().unwrap().into_string().unwrap();
    let birth_dacl = birth_sddl.split("D:").nth(1).unwrap();
    // Ordinary native creation can choose a different token default owner.
    // Set only this disposable fixture's owner before any key bytes arrive;
    // preserve its genuinely inherited DACL and opened object identity.
    let Ok(owner_descriptor) = format!("O:{user}").parse::<LocalBox<SecurityDescriptor>>() else {
        panic!("fixture owner descriptor must be valid");
    };
    assert!(
        wrappers::SetNamedSecurityInfo(
            path.as_os_str(),
            SeObjectType::SE_FILE_OBJECT,
            SecurityInformation::Owner,
            owner_descriptor.owner(),
            None,
            None,
            None,
        )
        .is_ok(),
        "fixture owner setup must succeed"
    );
    let descriptor = native_descriptor(file);
    let owned_sddl = descriptor.as_sddl().unwrap().into_string().unwrap();
    let dacl_unchanged = owned_sddl.split("D:").nth(1) == Some(birth_dacl);
    assert!(dacl_unchanged, "fixture owner setup must preserve the DACL");
    assert!(
        birth_dacl
            .split('(')
            .next()
            .is_some_and(|flags| !flags.contains('P')),
        "fixture DACL must remain unprotected and inherited"
    );
    assert_eq!(secure::identity(file).unwrap(), birth_identity);
    let owner_matches = native_owner(file) == user;
    assert!(
        owner_matches,
        "fixture must have the current token user owner"
    );
}

#[cfg(windows)]
#[test]
fn native_owner_only_inherited_dacl_is_admitted_without_repair() {
    use std::io::Write;
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use windows_permissions::constants::{AceFlags, AceType};
    let (_root, dir) = directory();
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x0200_0000)
        .open(&dir)
        .unwrap();
    let user = native_owner(&directory);
    native_acl(&dir, &format!("D:P(A;OICI;FA;;;{user})"));
    let path = dir.join("registered");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let effective_user = native_effective_user();
    set_fixture_owner_preserving_inherited_dacl(&file, &path, &effective_user);
    let descriptor = native_descriptor(&file);
    let acl = descriptor.dacl().unwrap();
    let ace = acl.get_ace(0);
    let ace_user = ace
        .and_then(|ace| ace.sid())
        .and_then(|sid| windows_permissions::wrappers::ConvertSidToStringSid(sid).ok())
        .and_then(|sid| sid.into_string().ok());
    let metadata = file.metadata().unwrap();
    eprintln!(
        "NATIVE_INHERITED_FIXTURE owner_current_user_match={} regular={} no_reparse={} single_link={} acl_count_one={} ace_allowed={} ace_inherited={} ace_not_inherit_only={} ace_full_mask={} ace_current_user_match={} direct_file_floor_accepted={} namespace_accepted={}",
        native_owner(&file) == effective_user,
        metadata.is_file(),
        metadata.file_attributes() & 0x400 == 0,
        winapi_util::file::information(&file)
            .unwrap()
            .number_of_links()
            == 1,
        acl.len() == 1,
        ace.is_some_and(|ace| ace.ace_type() == AceType::ACCESS_ALLOWED_ACE_TYPE),
        ace.is_some_and(|ace| ace.flags().contains(AceFlags::Inherited)),
        ace.is_some_and(|ace| !ace.flags().contains(AceFlags::InheritOnly)),
        ace.is_some_and(|ace| ace.mask().bits() == 0x001f_01ff),
        ace_user.as_deref() == Some(effective_user.as_str()),
        super::super::windows::verify_file(&file).is_ok(),
        super::super::windows::Namespace::open(&dir, false).is_ok(),
    );
    assert_eq!(acl.len(), 1);
    assert!(
        acl.get_ace(0)
            .unwrap()
            .flags()
            .contains(AceFlags::Inherited)
    );
    assert!(ace.is_some_and(|ace| ace.mask().bits() == 0x001f_01ff));
    let ace_matches_user = ace_user.as_deref() == Some(effective_user.as_str());
    assert!(
        ace_matches_user,
        "inherited ACE must grant the current token user"
    );
    file.write_all(&encoded()).unwrap();
    let before = descriptor.as_sddl().unwrap().into_string().unwrap();
    assert_eq!(read_encoded(&path).unwrap().unwrap(), encoded());
    let reader_preserved_descriptor = native_descriptor(&file)
        .as_sddl()
        .unwrap()
        .into_string()
        .unwrap()
        == before;
    assert!(
        reader_preserved_descriptor,
        "reader must preserve the source descriptor"
    );
    assert_eq!(inventory(&dir), ["registered"]);
}

#[cfg(windows)]
#[test]
fn native_unsupported_unc_namespace_is_error_without_network_discovery() {
    use std::error::Error;
    // The existing floor rejects the prefix before any native volume/file call.
    let source = ProtectedHexKeyFile::new(r"\\synthetic-invalid\share\registered").unwrap();
    let error = read_encoded(&source.path).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(error.source().is_none());
    assert!(!format!("{error:?}").contains("synthetic-invalid"));
}

struct ObservedReader {
    bytes: Zeroizing<Vec<u8>>,
    used: usize,
    spans: Vec<(usize, usize, usize)>,
    chunk: usize,
    interrupted: bool,
    terminal_error_after: Option<usize>,
}

impl ObservedReader {
    fn new(length: usize, chunk: usize) -> Self {
        let mut bytes = Zeroizing::new(vec![b' '; length]);
        if length >= 64 {
            bytes[..64].copy_from_slice(&encoded());
        }
        Self {
            bytes,
            used: 0,
            spans: Vec::new(),
            chunk,
            interrupted: false,
            terminal_error_after: None,
        }
    }
}

impl Read for ObservedReader {
    fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
        self.spans
            .push((destination.as_ptr() as usize, destination.len(), self.used));
        if self.interrupted {
            self.interrupted = false;
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        if self
            .terminal_error_after
            .is_some_and(|limit| self.used >= limit)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "synthetic-private-payload",
            ));
        }
        let count = destination
            .len()
            .min(self.chunk)
            .min(self.bytes.len() - self.used);
        destination[..count].copy_from_slice(&self.bytes[self.used..self.used + count]);
        self.used += count;
        Ok(count)
    }
}

#[test]
fn encoded_reads_use_one_full_zeroizing_allocation_from_the_first_read() {
    // Exercise the production helper used by both initial read and readback.
    // Only compare addresses recorded during live reads; never dereference them.
    for length in [64, MAX_HEX_KEY_FILE_BYTES] {
        for _pass in 0..2 {
            let mut reader = ObservedReader::new(length, 17);
            reader.interrupted = true;
            let bytes = read_bounded(&mut reader).unwrap();
            let original_bytes_retained = bytes.as_slice() == reader.bytes.as_slice();
            assert!(original_bytes_retained);
            validate(&bytes).unwrap();
            let limit = MAX_HEX_KEY_FILE_BYTES + 1;
            assert!(
                reader.spans[0].1 == limit,
                "first read must receive the full bounded allocation"
            );
            let base = bytes.as_ptr() as usize;
            assert!(
                reader
                    .spans
                    .iter()
                    .all(|&(address, span, used)| address == base + used && span == limit - used),
                "every read must stay inside the original allocation"
            );
            assert!(bytes.capacity() >= limit);
            assert_eq!(reader.used, length);
        }
    }
}

#[test]
fn bounded_short_reads_stop_at_eof_or_the_single_overflow_byte() {
    for length in [
        0,
        64,
        MAX_HEX_KEY_FILE_BYTES,
        MAX_HEX_KEY_FILE_BYTES + 1,
        MAX_HEX_KEY_FILE_BYTES * 2,
    ] {
        let mut reader = ObservedReader::new(length, 13);
        let result = read_bounded(&mut reader);
        assert_eq!(reader.used, length.min(MAX_HEX_KEY_FILE_BYTES + 1));
        if length <= MAX_HEX_KEY_FILE_BYTES {
            let original_bytes_retained = result.unwrap().as_slice() == reader.bytes.as_slice();
            assert!(original_bytes_retained);
        } else {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
        }
    }
}

#[test]
fn interrupted_short_reads_followed_by_timeout_never_return_a_partial_key() {
    use std::error::Error;
    let mut reader = ObservedReader::new(64, 17);
    reader.interrupted = true;
    reader.terminal_error_after = Some(51);
    let error = read_bounded(&mut reader).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(error.source().is_none());
    let diagnostics = format!("{error} {error:?}");
    assert!(!diagnostics.contains("synthetic-private-payload"));
    assert!(!diagnostics.contains("AbAb"));
    assert_eq!(reader.used, 51);
    assert_eq!(reader.spans.len(), 5);
    let limit = MAX_HEX_KEY_FILE_BYTES + 1;
    let base = reader.spans[0].0;
    assert_eq!(reader.spans[0].1, limit);
    // Addresses describe only live read destinations; never inspect freed data.
    assert!(
        reader
            .spans
            .iter()
            .all(|&(address, span, used)| address == base + used && span == limit - used)
    );
}

struct ErroredRead {
    first: bool,
    panic_after_read: bool,
}

impl Read for ErroredRead {
    fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
        if self.first {
            self.first = false;
            let count = destination.len().min(64);
            destination[..count].copy_from_slice(&encoded()[..count]);
            return Ok(count);
        }
        assert!(!self.panic_after_read, "synthetic reader panic");
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "synthetic-private-payload",
        ))
    }
}

#[test]
fn reader_errors_after_partial_key_are_content_free_and_panic_unwinds() {
    use std::error::Error;
    // Error and unwind paths drop the helper's owned Zeroizing allocation;
    // inspecting freed storage would be invalid and is deliberately avoided.
    let mut reader = ErroredRead {
        first: true,
        panic_after_read: false,
    };
    let error = read_bounded(&mut reader).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(error.source().is_none());
    assert!(!format!("{error} {error:?}").contains("synthetic-private-payload"));
    let mut reader = ErroredRead {
        first: true,
        panic_after_read: true,
    };
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read_bounded(&mut reader)))
            .is_err()
    );
}

#[test]
fn reader_reporting_more_than_its_destination_is_rejected() {
    struct InvalidCount;
    impl Read for InvalidCount {
        fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
            Ok(destination.len() + 1)
        }
    }
    assert_eq!(
        read_bounded(&mut InvalidCount).unwrap_err().kind(),
        ErrorKind::Backend
    );
}
