//! Filesystem helpers: atomic writes, optional reads, listings, error kinds.

use super::*;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn maps_transient_io_errors_to_unavailable() {
    for kind in [
        io::ErrorKind::WouldBlock,
        io::ErrorKind::TimedOut,
        io::ErrorKind::Interrupted,
    ] {
        let error = io_error("test")(io::Error::from(kind));
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(std::error::Error::source(&error).is_some());
    }
    let error = io_error("do a thing")(io::Error::from(io::ErrorKind::PermissionDenied));
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(error.message(), "file storage could not do a thing");
}

#[test]
fn reads_and_writes_json_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("v.json");
    assert_eq!(read_json::<u32>(&path).unwrap(), None);
    write_json(&path, &7_u32).unwrap();
    assert_eq!(read_json::<u32>(&path).unwrap(), Some(7));
    write_json(&path, &8_u32).unwrap();
    assert_eq!(read_json::<u32>(&path).unwrap(), Some(8));
    std::fs::write(&path, b"{").unwrap();
    assert_eq!(
        read_json::<u32>(&path).unwrap_err().kind(),
        ErrorKind::Serialization
    );
    let leftovers = files_with_suffix(path.parent().unwrap(), "").unwrap();
    assert_eq!(leftovers, [path], "no temporary file is left behind");
}

#[test]
fn reading_a_directory_is_a_backend_error() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        read_optional(dir.path()).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn a_failed_write_cleans_up_its_temporary_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("taken");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("child"), b"x").unwrap();
    let error = write_atomic(&target, b"data").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    let names: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(names, ["taken"]);
}

#[test]
fn a_path_without_a_parent_is_rejected() {
    assert_eq!(
        write_atomic(Path::new("/"), b"x").unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        open_append(Path::new("/")).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn creating_a_directory_under_a_file_fails() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    let below = file.join("sub").join("x.json");
    assert_eq!(
        write_atomic(&below, b"x").unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(open_append(&below).unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(
        files_with_suffix(&file, ".json").unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn opening_a_directory_for_append_fails() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    assert_eq!(open_append(&sub).unwrap_err().kind(), ErrorKind::Backend);
}

#[test]
fn removes_optionally() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x");
    assert!(!remove_optional(&path).unwrap());
    std::fs::write(&path, b"x").unwrap();
    assert!(remove_optional(&path).unwrap());
    let sub = dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    assert_eq!(
        remove_optional(&sub).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn lists_only_finished_files_with_the_suffix() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        files_with_suffix(&dir.path().join("missing"), ".json")
            .unwrap()
            .len(),
        0
    );
    for name in ["b.json", "a.json", ".tmp-1-2", "c.txt"] {
        std::fs::write(dir.path().join(name), b"{}").unwrap();
    }
    std::fs::create_dir(dir.path().join("d.json")).unwrap();
    let listed = files_with_suffix(dir.path(), ".json").unwrap();
    assert_eq!(
        listed,
        [dir.path().join("a.json"), dir.path().join("b.json")]
    );
    sync_dir(dir.path()).unwrap();
    #[cfg(unix)]
    assert!(sync_dir(&dir.path().join("missing")).is_err());
}

// The native macOS filesystem rejects this fixture with EILSEQ before the
// driver can inspect it. Other Unix platforms retain the non-UTF8 case.
#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn listing_ignores_non_utf8_filenames() {
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir().unwrap();
    let raw = std::ffi::OsStr::from_bytes(b"\xff.json");
    std::fs::write(dir.path().join(raw), b"{}").unwrap();
    std::fs::write(dir.path().join("valid.json"), b"{}").unwrap();
    assert_eq!(
        files_with_suffix(dir.path(), ".json").unwrap(),
        [dir.path().join("valid.json")]
    );
}

#[test]
fn a_bare_file_name_lives_in_the_current_directory() {
    assert_eq!(parent(Path::new("state.json")).unwrap(), Path::new("."));
    assert_eq!(parent(Path::new("a/state.json")).unwrap(), Path::new("a"));
    assert!(parent(Path::new("/")).is_err());
}

#[cfg(unix)]
#[test]
fn a_planted_temporary_symlink_is_never_followed() {
    let dir = tempfile::tempdir().unwrap();
    let victim = dir.path().join("victim");
    std::fs::write(&victim, b"keep").unwrap();
    // Plant a symlink on every name the next few attempts could pick.
    let before = temp_path(dir.path());
    let pid = std::process::id();
    let first: u64 = before
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .rsplit('-')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    for n in first + 1..first + 4 {
        std::os::unix::fs::symlink(&victim, dir.path().join(format!(".tmp-{pid}-{n}"))).unwrap();
    }
    let target = dir.path().join("out.json");
    write_atomic(&target, b"new").unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"new");
    assert_eq!(std::fs::read(&victim).unwrap(), b"keep");
}

#[cfg(unix)]
#[test]
fn a_symlink_is_refused_for_reads_and_appends() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    std::fs::write(&real, b"data").unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    assert!(read_optional(&link).is_err());
    assert!(open_read(&link).is_err());
    assert!(open_append(&link).is_err());
    assert_eq!(std::fs::read(&real).unwrap(), b"data");
}

#[test]
fn creating_a_nested_hierarchy_is_durable_and_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("a/b/c");
    create_dir_durable(&nested).unwrap();
    assert!(nested.is_dir());
    create_dir_durable(&nested).unwrap();
    write_atomic(&dir.path().join("x/y/file.json"), b"{}").unwrap();
    assert!(dir.path().join("x/y/file.json").is_file());
}
