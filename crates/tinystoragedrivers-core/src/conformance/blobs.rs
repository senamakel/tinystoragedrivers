//! Blob port checks.

use super::{empty, fails, ok, unique};
use crate::backend::ScopedStorage;
use crate::blob::{TreeEntry, TreeFile};
use crate::error::ErrorKind;
use crate::{BlobReader, Capability};
use std::time::Duration;

pub(super) async fn run(storage: &ScopedStorage) {
    let blobs = storage.blobs();
    let base = unique("blob");
    let key = format!("{base}/a.txt");

    assert!(ok(blobs.get(&key).await, "get missing").is_none());
    assert!(ok(blobs.head(&key).await, "head missing").is_none());
    assert!(ok(blobs.get_range(&key, 0..1).await, "range missing").is_none());

    let meta = ok(
        blobs
            .put(&key, b"hello world".to_vec(), Some("text/plain"))
            .await,
        "put",
    );
    assert_eq!(meta.len, 11);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
    let blob = ok(blobs.get(&key).await, "get").expect("blob exists");
    assert_eq!(blob.bytes, b"hello world");
    assert_eq!(blob.meta, meta);
    assert_eq!(ok(blobs.head(&key).await, "head"), Some(meta));

    assert_eq!(
        ok(blobs.get_range(&key, 6..11).await, "range").as_deref(),
        Some(&b"world"[..])
    );
    assert_eq!(
        ok(blobs.get_range(&key, 6..999).await, "range clamps").as_deref(),
        Some(&b"world"[..])
    );
    assert_eq!(
        ok(blobs.get_range(&key, 50..60).await, "range past the end").as_deref(),
        Some(&b""[..])
    );
    #[allow(
        clippy::reversed_empty_ranges,
        reason = "the inverted range is the input under test"
    )]
    let inverted = 5..1;
    fails(
        blobs.get_range(&key, inverted).await,
        ErrorKind::InvalidInput,
        "inverted range",
    );

    let replaced = ok(blobs.put(&key, vec![1, 2], None).await, "replace");
    assert_eq!(replaced.len, 2);
    assert_eq!(replaced.content_type, None);

    ok(
        blobs.put(&format!("{base}/b.bin"), vec![0], None).await,
        "second",
    );
    let listed: Vec<_> = ok(blobs.list(&format!("{base}/")).await, "list")
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(listed, [key.clone(), format!("{base}/b.bin")]);

    assert!(ok(blobs.delete(&key).await, "delete"));
    assert!(!ok(blobs.delete(&key).await, "delete missing"));

    key_limits(storage).await;
    sealed_prefix_is_reserved(storage).await;
    tree(storage).await;
    expiry(storage).await;

    for bad in ["", "/abs", "a//b", "a/../b", "a/./b"] {
        fails(
            blobs.put(bad, vec![], None).await,
            ErrorKind::InvalidInput,
            "invalid key",
        );
    }
}

/// Build a key of exactly the announced byte limit with small path segments,
/// so local filesystems do not reject one overly long filename component.
pub(super) fn boundary_key(limit: usize, utf8: bool) -> String {
    // Boundary checks share a blob namespace when conformance runs overlap.
    // Keep their writes and cleanup independent, just like ordinary blob keys.
    let mut key = unique("boundary");
    key.truncate(limit);
    let mut segment = key.len();
    while key.len() < limit {
        let remaining = limit - key.len();
        if segment >= 60 && remaining > 1 {
            key.push('/');
            segment = 0;
        } else if utf8 && remaining >= 2 {
            key.push('é');
            segment += 2;
        } else {
            key.push('x');
            segment += 1;
        }
    }
    key
}

/// The prefix sealed streams keep their entries under belongs to the drivers:
/// no caller operation reads, writes, lists or deletes beneath it.
async fn sealed_prefix_is_reserved(storage: &ScopedStorage) {
    let blobs = storage.blobs();
    for key in ["_tsd/sealed/x", "_tsd/sealed/a/b", "_tsd/sealed/run%2F1"] {
        fails(
            blobs.put(key, vec![1], None).await,
            ErrorKind::InvalidInput,
            "put under the sealed prefix",
        );
        fails(
            blobs.get(key).await,
            ErrorKind::InvalidInput,
            "get under the sealed prefix",
        );
        fails(
            blobs.get_range(key, 0..1).await,
            ErrorKind::InvalidInput,
            "get_range under the sealed prefix",
        );
        fails(
            blobs.head(key).await,
            ErrorKind::InvalidInput,
            "head under the sealed prefix",
        );
        fails(
            blobs.delete(key).await,
            ErrorKind::InvalidInput,
            "delete under the sealed prefix",
        );
    }
    fails(
        blobs.list("_tsd/sealed/").await,
        ErrorKind::InvalidInput,
        "list under the sealed prefix",
    );
    fails(
        blobs
            .put_tree("_tsd/sealed/tree", vec![TreeFile::new("f", b"x".to_vec())])
            .await,
        ErrorKind::InvalidInput,
        "put_tree under the sealed prefix",
    );
    // Neighbouring names are ordinary keys.
    let sibling = format!("_tsd/sealed-{}", unique("sibling"));
    ok(blobs.put(&sibling, vec![1], None).await, "put a sibling");
    assert!(ok(blobs.delete(&sibling).await, "delete the sibling"));
}

async fn key_limits(storage: &ScopedStorage) {
    let blobs = storage.blobs();
    let limit = blobs.max_key_len();
    assert!(limit > 0 && limit <= crate::MAX_BLOB_KEY_LEN);
    for utf8 in [false, true] {
        let key = boundary_key(limit, utf8);
        assert_eq!(key.len(), limit);
        ok(
            blobs.put(&key, vec![1], None).await,
            "announced key boundary",
        );
        assert_eq!(
            ok(blobs.get(&key).await, "read boundary")
                .expect("exists")
                .bytes,
            [1]
        );
        fails(
            blobs.put(&(key.clone() + "x"), vec![], None).await,
            ErrorKind::InvalidInput,
            "key beyond announced boundary",
        );
        ok(blobs.delete(&key).await, "delete boundary");
    }
}

/// The directory view: listing one level, writing and removing a bundle, and
/// the `a` versus `ab` prefix boundary.
async fn tree(storage: &ScopedStorage) {
    let blobs = storage.blobs();
    let root = unique("tree");
    let sibling = format!("{root}b");
    let files = vec![
        TreeFile::new("SKILL.md", "# skill"),
        TreeFile::new("scripts/run.sh", "echo hi"),
        TreeFile::new("scripts/lib/util.sh", "util"),
        TreeFile {
            path: "refs/notes.md".to_owned(),
            bytes: b"notes".to_vec(),
            content_type: Some("text/markdown".to_owned()),
        },
    ];
    let written = ok(blobs.put_tree(&root, files).await, "put_tree");
    assert_eq!(written.len(), 4);
    assert_eq!(written[0].key, format!("{root}/SKILL.md"));
    ok(
        blobs
            .put(&format!("{sibling}/other.txt"), vec![1], None)
            .await,
        "sibling prefix",
    );

    let top = ok(blobs.list_dir(&root).await, "list_dir");
    let shape: Vec<_> = top
        .iter()
        .map(|entry| (entry.name(), matches!(entry, TreeEntry::Dir { .. })))
        .collect();
    assert_eq!(
        shape,
        [("SKILL.md", false), ("refs", true), ("scripts", true)],
        "one level, sorted, sub-directories folded"
    );
    assert!(matches!(
        &top[2],
        TreeEntry::Dir { prefix, .. } if *prefix == format!("{root}/scripts")
    ));
    let scripts = ok(blobs.list_dir(&format!("{root}/scripts")).await, "nested");
    let names: Vec<_> = scripts.iter().map(TreeEntry::name).collect();
    assert_eq!(names, ["lib", "run.sh"]);
    if let TreeEntry::Blob { meta, .. } =
        &ok(blobs.list_dir(&format!("{root}/refs")).await, "refs")[0]
    {
        assert_eq!(meta.content_type.as_deref(), Some("text/markdown"));
    } else {
        panic!("refs/notes.md should list as a blob");
    }

    fails(
        blobs
            .put_tree(
                &root,
                vec![TreeFile::new("ok.md", ""), TreeFile::new("../escape", "")],
            )
            .await,
        ErrorKind::InvalidInput,
        "a bad path fails the whole tree",
    );
    assert!(
        ok(
            blobs.head(&format!("{root}/ok.md")).await,
            "validated first"
        )
        .is_none(),
        "nothing is written when a path is invalid"
    );
    fails(
        blobs.delete_tree("").await,
        ErrorKind::InvalidInput,
        "delete_tree refuses the root",
    );

    assert_eq!(ok(blobs.delete_tree(&root).await, "delete_tree"), 4);
    assert_eq!(ok(blobs.list_dir(&root).await, "emptied").len(), 0);
    assert_eq!(
        ok(blobs.list_dir(&sibling).await, "sibling survives").len(),
        1,
        "deleting `a` leaves `ab` alone"
    );
    assert_eq!(ok(blobs.delete_tree(&sibling).await, "cleanup"), 1);
}

fn reader(bytes: &[u8]) -> BlobReader {
    Box::pin(std::io::Cursor::new(bytes.to_vec()))
}

/// Every read of `key` finds nothing, and `list` omits it.
async fn assert_absent(storage: &ScopedStorage, key: &str, check: &str) {
    let blobs = storage.blobs();
    assert!(ok(blobs.get(key).await, check).is_none(), "{check}: get");
    assert!(ok(blobs.head(key).await, check).is_none(), "{check}: head");
    assert!(
        ok(blobs.get_range(key, 0..1).await, check).is_none(),
        "{check}: get_range"
    );
    assert!(
        ok(blobs.get_stream(key).await, check).is_none(),
        "{check}: get_stream"
    );
    let listed = ok(blobs.list(key).await, check);
    empty(&listed, &format!("{check}: list"));
}

/// Blob lifetimes: an expired blob reads absent everywhere, a plain write
/// clears a lifetime, and a driver without the capability refuses.
async fn expiry(storage: &ScopedStorage) {
    let blobs = storage.blobs();
    let key = format!("{}/ttl.bin", unique("ttl"));
    let day = Duration::from_secs(86_400);
    if !blobs.capabilities().contains(Capability::BlobTtl) {
        fails(
            blobs.put_expiring(&key, vec![1], None, day).await,
            ErrorKind::Unsupported(Capability::BlobTtl),
            "put_expiring without the capability",
        );
        fails(
            blobs
                .put_stream_expiring(&key, reader(&[1]), 1, None, day)
                .await,
            ErrorKind::Unsupported(Capability::BlobTtl),
            "put_stream_expiring without the capability",
        );
        fails(
            blobs.get_with_expiry(&key).await,
            ErrorKind::Unsupported(Capability::BlobTtl),
            "get_with_expiry without support",
        );
        fails(
            blobs.put_until(&key, vec![1], None, None).await,
            ErrorKind::Unsupported(Capability::BlobTtl),
            "put_until without support",
        );
        fails(
            blobs
                .put_stream_until(&key, reader(&[1]), 1, None, None)
                .await,
            ErrorKind::Unsupported(Capability::BlobTtl),
            "put_stream_until without support",
        );
        assert_absent(storage, &key, "refused write stored nothing").await;
        return;
    }

    expiry_absolute(storage).await;
    expiry_relative(storage, &key, day).await;
}

async fn expiry_absolute(storage: &ScopedStorage) {
    let blobs = storage.blobs();
    let deadline = crate::memory::system_clock().saturating_add(86_400_000);
    let absolute = format!("{}/absolute.bin", unique("ttl"));
    ok(
        blobs
            .put_until(&absolute, b"absolute".to_vec(), Some("x/y"), Some(deadline))
            .await,
        "put_until",
    );
    let source = ok(blobs.get_with_expiry(&absolute).await, "get_with_expiry")
        .expect("absolute source is live");
    assert_eq!(source.blob.bytes, b"absolute");
    assert_eq!(source.expires_at_ms, Some(deadline));
    let mut streamed_source = ok(
        blobs.get_stream_with_expiry(&absolute).await,
        "get_stream_with_expiry",
    )
    .expect("absolute stream source is live");
    assert_eq!(streamed_source.expires_at_ms, Some(deadline));
    let mut streamed_bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut streamed_source.reader, &mut streamed_bytes)
        .await
        .expect("absolute stream reads");
    assert_eq!(streamed_bytes, b"absolute");

    let absolute_stream = format!("{}/absolute-stream.bin", unique("ttl"));
    ok(
        blobs
            .put_stream_until(
                &absolute_stream,
                reader(b"streamed"),
                8,
                Some("x/y"),
                Some(deadline),
            )
            .await,
        "put_stream_until",
    );
    assert_eq!(
        ok(
            blobs.get_with_expiry(&absolute_stream).await,
            "get stream target expiry"
        )
        .expect("stream target exists")
        .expires_at_ms,
        Some(deadline)
    );

    let permanent = format!("{}/permanent.bin", unique("ttl"));
    ok(
        blobs
            .put_until(&permanent, b"forever".to_vec(), None, None)
            .await,
        "put_until permanent",
    );
    let source = ok(
        blobs.get_with_expiry(&permanent).await,
        "get permanent source with expiry",
    )
    .expect("permanent source is live");
    assert_eq!(source.expires_at_ms, None);
}

async fn expiry_relative(storage: &ScopedStorage, key: &str, day: Duration) {
    let blobs = storage.blobs();
    // A lifetime far away leaves the blob readable everywhere.
    let meta = ok(
        blobs
            .put_expiring(key, b"hello world".to_vec(), Some("text/plain"), day)
            .await,
        "put_expiring",
    );
    assert_eq!(
        (meta.len, meta.content_type.as_deref()),
        (11, Some("text/plain"))
    );
    assert_eq!(ok(blobs.head(key).await, "head live"), Some(meta.clone()));
    assert_eq!(
        ok(blobs.get(key).await, "get live").expect("live").bytes,
        b"hello world"
    );
    assert_eq!(
        ok(blobs.get_range(key, 6..11).await, "range live").as_deref(),
        Some(&b"world"[..])
    );
    assert!(ok(blobs.get_stream(key).await, "stream live").is_some());
    assert_eq!(ok(blobs.list(key).await, "list live"), [meta]);

    // A zero lifetime is expired on arrival, and replaces what was there.
    let meta = ok(
        blobs.put_expiring(key, vec![9], None, Duration::ZERO).await,
        "zero lifetime",
    );
    assert_eq!(meta.len, 1, "the write still reports what it stored");
    assert_absent(storage, key, "zero lifetime").await;
    assert!(
        !ok(blobs.delete(key).await, "delete expired"),
        "an expired blob does not exist"
    );

    // A plain write after an expiring one is permanent.
    ok(
        blobs.put_expiring(key, vec![1], None, day).await,
        "expiring again",
    );
    ok(blobs.put(key, vec![2, 2], None).await, "plain overwrite");
    assert_eq!(
        ok(blobs.get(key).await, "plain after expiring")
            .expect("permanent")
            .bytes,
        [2, 2]
    );
    // And an expired key can be written again.
    ok(
        blobs.put_expiring(key, vec![3], None, Duration::ZERO).await,
        "expire the permanent blob",
    );
    assert_absent(storage, key, "replaced by an expired write").await;
    ok(blobs.put(key, vec![4], None).await, "reuse expired key");
    assert_eq!(ok(blobs.get(key).await, "reused").expect("live").bytes, [4]);

    expiry_streamed(storage, key).await;
}

/// The streamed form and a lapsing lifetime; removes `key` when done.
async fn expiry_streamed(storage: &ScopedStorage, key: &str) {
    let blobs = storage.blobs();
    let day = Duration::from_secs(86_400);
    // The streamed form honours the same lifetimes.
    let streamed = format!("{}/stream.bin", unique("ttl"));
    let meta = ok(
        blobs
            .put_stream_expiring(&streamed, reader(b"abc"), 3, Some("x/y"), day)
            .await,
        "put_stream_expiring",
    );
    assert_eq!((meta.len, meta.content_type.as_deref()), (3, Some("x/y")));
    assert_eq!(
        ok(blobs.get(&streamed).await, "streamed live")
            .expect("live")
            .bytes,
        b"abc"
    );
    fails(
        blobs
            .put_stream_expiring(&streamed, reader(b"ab"), 3, None, day)
            .await,
        ErrorKind::InvalidInput,
        "a short body fails before publishing",
    );
    ok(
        blobs
            .put_stream_expiring(&streamed, reader(b"abc"), 3, None, Duration::ZERO)
            .await,
        "streamed zero lifetime",
    );
    assert_absent(storage, &streamed, "streamed zero lifetime").await;

    // A short real lifetime lapses. Only the direction is asserted: once the
    // lifetime has elapsed the blob is gone, however late the read.
    let brief = format!("{}/brief.bin", unique("ttl"));
    ok(
        blobs
            .put_expiring(&brief, vec![1], None, Duration::from_millis(50))
            .await,
        "brief lifetime",
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_absent(storage, &brief, "lapsed lifetime").await;

    // The sweep hook reclaims lapsed blobs and never touches a live one. The
    // count is not asserted: a read may already have reclaimed the lapsed blob,
    // and a bus client leaves reclaiming to the host that owns the data.
    ok(blobs.put(&brief, vec![9], None).await, "revive after lapse");
    ok(blobs.sweep_expired().await, "sweep expired");
    assert!(
        ok(blobs.get(&brief).await, "get after sweep").is_some(),
        "a live blob survives the sweep"
    );
    ok(blobs.delete(&brief).await, "cleanup revived");

    for left in [key, streamed.as_str()] {
        ok(blobs.delete(left).await, "cleanup");
    }
}
