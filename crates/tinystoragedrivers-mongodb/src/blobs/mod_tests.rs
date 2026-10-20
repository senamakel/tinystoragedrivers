//! File metadata and chunk arithmetic.

use super::*;
use mongodb::bson::DateTime;

#[test]
fn metadata_carries_scope_and_content_type() {
    let scope = Scope::new("alice").unwrap();
    assert_eq!(
        file_metadata(&scope, Some("text/plain"), None),
        doc! {"_scope": "alice", "content_type": "text/plain"}
    );
    assert_eq!(file_metadata(&scope, None, None), doc! {"_scope": "alice"});
    assert_eq!(
        file_metadata(&scope, None, Some(42)),
        doc! {"_scope": "alice", "expires_at_ms": 42_i64}
    );
}

fn file(id: i32, key: &str, expires_at_ms: Option<i64>) -> FilesCollectionDocument {
    let mut metadata = doc! {"_scope": "s"};
    if let Some(at) = expires_at_ms {
        metadata.insert("expires_at_ms", at);
    }
    mongodb::bson::from_document(doc! {
        "_id": id,
        "length": 1_i64,
        "chunkSize": 4,
        "uploadDate": DateTime::now(),
        "filename": key,
        "metadata": metadata,
    })
    .unwrap()
}

#[test]
fn a_file_lapses_at_its_expiry_instant() {
    assert!(!lapsed(&file(1, "a", None), 1_000));
    assert!(!lapsed(&file(1, "a", Some(1_001)), 1_000));
    assert!(lapsed(&file(1, "a", Some(1_000)), 1_000));
    assert!(lapsed(&file(1, "a", Some(0)), 1_000));
}

#[test]
fn source_expiry_is_absent_only_for_known_permanent_files() {
    assert_eq!(blob_expiry(&file(1, "permanent", None)).unwrap(), None);
    assert_eq!(
        blob_expiry(&file(2, "expiring", Some(6_000))).unwrap(),
        Some(6_000)
    );
    let malformed: FilesCollectionDocument = mongodb::bson::from_document(doc! {
        "_id": 3,
        "length": 1_i64,
        "chunkSize": 4,
        "uploadDate": DateTime::now(),
        "filename": "malformed",
        "metadata": {"_scope": "s", "expires_at_ms": "unknown"},
    })
    .unwrap();
    assert_eq!(
        blob_expiry(&malformed).unwrap_err().kind(),
        tinystoragedrivers_core::ErrorKind::Serialization
    );
}

#[test]
fn a_lapsed_newest_file_hides_its_key_and_older_versions() {
    // `files` orders by key, then newest first.
    let files = vec![
        file(9, "a", Some(10)),
        file(3, "a", None),
        file(5, "b", None),
        file(4, "b", Some(10)),
        file(7, "c", Some(10)),
    ];
    let (live, dead) = live_newest(files, 100);
    let keys: Vec<_> = live.iter().map(|f| f.filename.clone().unwrap()).collect();
    assert_eq!(keys, ["b"]);
    assert_eq!(live[0].id, Bson::Int32(5));
    // Older versions of a lapsed key are reclaimed too, so they cannot resurface.
    assert_eq!(
        dead,
        [
            Bson::Int32(9),
            Bson::Int32(3),
            Bson::Int32(4),
            Bson::Int32(7)
        ]
    );
}

#[test]
fn files_become_blob_meta() {
    let file: FilesCollectionDocument = mongodb::bson::from_document(doc! {
        "_id": 1,
        "length": 11_i64,
        "chunkSize": 4,
        "uploadDate": DateTime::now(),
        "filename": "a/b",
        "metadata": {"_scope": "s", "content_type": "text/plain"},
    })
    .unwrap();
    assert_eq!(
        blob_meta(&file),
        BlobMeta {
            key: "a/b".into(),
            len: 11,
            content_type: Some("text/plain".into())
        }
    );
    let bare: FilesCollectionDocument = mongodb::bson::from_document(doc! {
        "_id": 1, "length": 0_i64, "chunkSize": 4, "uploadDate": DateTime::now(),
    })
    .unwrap();
    assert_eq!(blob_meta(&bare).content_type, None);
}

#[test]
fn ranges_map_to_the_chunks_they_touch() {
    assert_eq!(chunk_span(&(0..1), 4), (0, 0, 0));
    assert_eq!(chunk_span(&(3..9), 4), (0, 2, 3));
    assert_eq!(chunk_span(&(8..12), 4), (2, 2, 0));
    assert_eq!(chunk_span(&(5..6), 0), (5, 5, 0));
}

#[test]
fn the_blob_indexes_cover_the_lookups_gridfs_makes() {
    assert_eq!(
        standard_file_index().keys,
        doc! {"filename": 1, "uploadDate": 1}
    );
    let scoped = file_index();
    assert_eq!(scoped.keys, doc! {FILE_SCOPE: 1, "filename": 1, "_id": -1});
    assert_eq!(
        scoped.options.and_then(|options| options.name).as_deref(),
        Some(BLOB_KEY_INDEX)
    );
    let chunks = chunk_index();
    assert_eq!(chunks.keys, doc! {"files_id": 1, "n": 1});
    assert_eq!(
        chunks.options.and_then(|options| options.unique),
        Some(true)
    );
}
