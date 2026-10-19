//! Scoped blob conformance and multipart publication guarantees.

use super::*;
use tinystoragedrivers_core::{MemoryStorage, ScopeGeneration, ScopedStorage, StorageBackend};

#[derive(Debug)]
struct Composite {
    documents: Arc<dyn StorageBackend>,
    objects: Arc<object_store::memory::InMemory>,
}

impl StorageBackend for Composite {
    fn driver(&self) -> &'static str {
        "test-composite"
    }
    fn document_driver(&self) -> &'static str {
        self.documents.document_driver()
    }
    fn capabilities(&self) -> Capabilities {
        self.documents
            .capabilities()
            .iter()
            .filter(|capability| {
                matches!(
                    capability,
                    tinystoragedrivers_core::Capability::SharedAcrossProcesses
                        | tinystoragedrivers_core::Capability::CrossProcessCas
                )
            })
            .fold(Capabilities::none(), Capabilities::with)
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        let local = self.documents.for_scope(scope)?;
        Ok(ScopedStorage::new(
            scope.clone(),
            self.driver(),
            local.documents().clone(),
            local.streams().clone(),
            Arc::new(S3Blobs::with_store(
                self.objects.clone(),
                None,
                "test",
                scope,
            )?),
        ))
    }
}

fn handle(objects: Arc<object_store::memory::InMemory>, scope: &str) -> S3Blobs {
    S3Blobs::with_store(objects, None, "test", &Scope::new(scope).unwrap()).unwrap()
}

#[tokio::test]
async fn blob_contract_and_scope_isolation() {
    let backend = Composite {
        documents: Arc::new(MemoryStorage::new()),
        objects: Arc::new(object_store::memory::InMemory::new()),
    };
    tinystoragedrivers_core::conformance::run_with_process_capabilities(
        &backend, false, false, false,
    )
    .await;
}

#[tokio::test]
async fn tenant_path_characters_cannot_alias_another_scope() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    let a = handle(objects.clone(), "alice/bob");
    let b = handle(objects.clone(), "alice%2Fbob");
    let c = handle(objects, "alice");
    a.put("x", vec![1], None).await.unwrap();
    b.put("x", vec![2], None).await.unwrap();
    c.put("bob/x", vec![3], None).await.unwrap();
    assert_eq!(a.get("x").await.unwrap().unwrap().bytes, [1]);
    assert_eq!(b.get("x").await.unwrap().unwrap().bytes, [2]);
    assert_eq!(c.get("bob/x").await.unwrap().unwrap().bytes, [3]);
    assert_eq!(a.list("").await.unwrap().len(), 1);
}

#[tokio::test]
async fn unsigned_object_store_keeps_enumeration_but_withholds_physical_purge() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    let backend = S3Storage::with_store(objects, None, "test").unwrap();
    let target = Scope::new("u:purge-target").unwrap();
    let sibling = Scope::new("u:purge-sibling").unwrap();
    backend
        .for_scope(&target)
        .unwrap()
        .put("one", vec![1], None)
        .await
        .unwrap();
    backend
        .for_scope(&target)
        .unwrap()
        .put("two", vec![2], None)
        .await
        .unwrap();
    backend
        .for_scope(&sibling)
        .unwrap()
        .put("one", vec![3], None)
        .await
        .unwrap();
    assert_eq!(
        backend.scopes("u:purge-").await.unwrap(),
        [sibling.clone(), target.clone()]
    );
    assert!(!backend.capabilities().contains(Capability::ScopePurge));
    assert_eq!(
        backend.purge_scope(&target).await.unwrap_err().kind(),
        tinystoragedrivers_core::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    let target_list = backend.for_scope(&target).unwrap().list("").await.unwrap();
    assert_eq!(target_list.len(), 2);
    assert!(
        backend
            .for_scope(&target)
            .unwrap()
            .head("one")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        backend
            .for_scope(&sibling)
            .unwrap()
            .list("")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn unsigned_object_store_does_not_advertise_generation_purge() {
    let backend = S3Storage::with_store(
        Arc::new(object_store::memory::InMemory::new()),
        None,
        "test",
    )
    .unwrap();
    assert!(!backend.supports_generation_purge());
    assert!(!backend.capabilities().contains(Capability::ScopePurge));
}

#[tokio::test]
async fn purge_removes_only_one_physical_generation_and_enumerates_both_roots() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    let backend = S3Storage::with_store(objects, None, "test").unwrap();
    let scope = Scope::new("u:generation-isolation").unwrap();
    let legacy = backend
        .for_scope_generation(&scope, ScopeGeneration(0))
        .unwrap();
    legacy.put("g/1/foo", vec![1], None).await.unwrap();
    let next = backend
        .for_scope_generation(&scope, ScopeGeneration(1))
        .unwrap();
    next.put("g/1/foo", vec![2], None).await.unwrap();
    assert_eq!(
        backend.scopes("u:generation-").await.unwrap(),
        std::slice::from_ref(&scope)
    );

    assert_eq!(
        backend
            .purge_scope_generation(&scope, ScopeGeneration(0))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Unsupported(Capability::ScopePurge)
    );

    assert_eq!(legacy.list("").await.unwrap().len(), 1);
    assert_eq!(next.get("g/1/foo").await.unwrap().unwrap().bytes, [2]);
    assert_eq!(backend.scopes("u:generation-").await.unwrap(), [scope]);
}

#[tokio::test]
async fn scope_enumeration_ignores_malformed_utf8_hex_segments() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    objects
        .put(
            &object_store::path::Path::from("test/scopes/a-é-a/blob"),
            bytes::Bytes::from_static(b"x").into(),
        )
        .await
        .unwrap();
    let backend = S3Storage::with_store(objects, None, "test").unwrap();
    assert_eq!(
        backend.scopes("").await.unwrap(),
        [] as [tinystoragedrivers_core::Scope; 0]
    );
}

#[tokio::test]
async fn multipart_roundtrip_and_length_mismatch_preserve_previous_object() {
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    let data = vec![42; PART_SIZE * 2 + 123];
    store
        .put_stream(
            "large",
            Box::pin(std::io::Cursor::new(data.clone())),
            data.len() as u64,
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    let mut result = store.get_stream("large").await.unwrap().unwrap();
    assert_eq!(result.meta.len, data.len() as u64);
    let mut got = Vec::new();
    result.reader.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, data);

    for len in [data.len() as u64 - 1, data.len() as u64 + 1] {
        let err = store
            .put_stream(
                "large",
                Box::pin(std::io::Cursor::new(data.clone())),
                len,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(store.get("large").await.unwrap().unwrap().bytes, data);
    }
}

#[tokio::test]
async fn buffered_length_mismatch_does_not_publish() {
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    for len in [1, 3] {
        assert!(
            store
                .put_stream(
                    "small",
                    Box::pin(std::io::Cursor::new(vec![1, 2])),
                    len,
                    None
                )
                .await
                .is_err()
        );
        assert!(store.head("small").await.unwrap().is_none());
    }
    assert!(!store.capabilities().contains(Capability::PresignedUrls));
    assert_eq!(
        store
            .presign_get("x", Duration::from_secs(60))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
}

#[test]
fn parses_compatible_endpoint_and_rejects_embedded_credentials() {
    let config =
        S3Config::parse("s3://bucket/prefix?region=local&endpoint=http%3A%2F%2Flocalhost%3A9000")
            .unwrap();
    assert_eq!(config.bucket, "bucket");
    assert_eq!(config.prefix, "prefix");
    assert_eq!(config.region, "local");
    assert_eq!(config.endpoint.as_deref(), Some("http://localhost:9000"));
    for bad in [
        "s3://user:secret@bucket",
        "s3://bucket?secret=x",
        "s3://bucket?endpoint=https%3A%2F%2Fuser%3Asecret%40host",
    ] {
        let error = S3Config::parse(bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(!error.to_string().contains("secret"));
    }
}

#[tokio::test]
async fn presigning_binds_scope_method_and_expiry_without_network() {
    let client = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("test-access")
            .with_secret_access_key("test-secret")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .build()
            .unwrap(),
    );
    let blobs = S3Blobs::with_store(
        client.clone(),
        Some(client),
        "prefix",
        &Scope::new("alice/bob").unwrap(),
    )
    .unwrap();
    assert!(blobs.capabilities().contains(Capability::StreamingBlobs));
    assert!(blobs.capabilities().contains(Capability::PresignedUrls));
    let get = blobs
        .presign_get("hello world.txt", Duration::from_secs(60))
        .await
        .unwrap();
    let put = blobs
        .presign_put("hello world.txt", Duration::from_secs(60))
        .await
        .unwrap();
    let get = url::Url::parse(&get).unwrap();
    let put = url::Url::parse(&put).unwrap();
    assert_eq!(
        get.path(),
        "/bucket/prefix/scopes/616c6963652f626f62/hello%20world.txt"
    );
    let query: std::collections::HashMap<_, _> = get.query_pairs().collect();
    assert_eq!(query.get("X-Amz-Expires").unwrap(), "60");
    assert_ne!(get.query(), put.query());
    assert!(blobs.presign_get("x", Duration::ZERO).await.is_err());
    assert!(
        blobs
            .presign_get("x", Duration::from_secs(604_801))
            .await
            .is_err()
    );
    assert!(
        blobs
            .presign_get("../x", Duration::from_secs(60))
            .await
            .is_err()
    );
}

#[path = "bounded_tests.rs"]
mod bounded;

#[tokio::test]
async fn scoped_key_limit_counts_utf8_and_rejects_overflow_without_network() {
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    let limit = store.max_key_len();
    let ascii = "x".repeat(limit);
    store.put(&ascii, vec![1], None).await.unwrap();
    let mut utf8 = "é".repeat(limit / 2);
    if !limit.is_multiple_of(2) {
        utf8.push('x');
    }
    store.put(&utf8, vec![2], None).await.unwrap();
    assert_eq!(store.get(&utf8).await.unwrap().unwrap().bytes, [2]);
    assert_eq!(
        store
            .put(&(ascii + "x"), vec![], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(
        S3Blobs::with_store(
            Arc::new(object_store::memory::InMemory::new()),
            None,
            &"x".repeat(1020),
            &Scope::new("alice").unwrap(),
        )
        .is_err()
    );
}

#[derive(Debug)]
struct FailedReader;

impl tokio::io::AsyncRead for FailedReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )))
    }
}

#[tokio::test]
async fn multipart_source_io_failure_is_a_backend_error_with_its_source() {
    use std::error::Error;
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    let error = store
        .put_stream("failed", Box::pin(FailedReader), PART_SIZE as u64 + 1, None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .is_some()
    );
    assert!(store.head("failed").await.unwrap().is_none());
}

#[test]
fn object_store_error_source_describes_the_category_without_transport_secrets() {
    use std::error::Error;
    let error = map_error(object_store::Error::Generic {
        store: "s3",
        source: Box::new(std::io::Error::other(
            "https://secret@example.test?credential=hidden",
        )),
    });
    let source = error
        .source()
        .expect("backend failures retain a sanitized source");
    assert!(source.to_string().contains("backend"));
    assert!(!format!("{error:?} {source}").contains("hidden"));
    assert!(source.source().is_none());
}

#[tokio::test]
async fn listing_skips_oversized_keys_written_by_other_clients() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    let store = handle(objects.clone(), "alice");
    store.put("valid", vec![1], None).await.unwrap();
    for key in ["x".repeat(store.max_key_len() + 1)] {
        let path = Path::from(format!("{}{key}", store.prefix));
        objects
            .put(&path, Bytes::from_static(b"external").into())
            .await
            .unwrap();
    }
    let listed = store.list("").await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        ["valid"]
    );
}

#[test]
fn scoped_s3_clients_are_constructed_without_network() {
    let config = S3Config::parse(
        "s3://test-bucket/root?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A1",
    )
    .unwrap();
    let scope = Scope::new("tenant").unwrap();
    let blobs = config.blobs(&scope).unwrap();
    assert!(blobs.capabilities().contains(Capability::PresignedUrls));
    let backend = S3Storage::open(&config).unwrap();
    assert!(backend.capabilities().contains(Capability::StreamingBlobs));
    assert!(
        backend
            .for_scope(&scope)
            .unwrap()
            .capabilities()
            .contains(Capability::PresignedUrls)
    );
    assert!(
        S3Storage::with_store(Arc::new(object_store::memory::InMemory::new()), None, "../")
            .is_err()
    );
    assert!(format!("{blobs:?} {backend:?}").contains("S3"));
}

#[derive(Debug)]
struct FailsAfter(u64);
impl tokio::io::AsyncRead for FailsAfter {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.0 == 0 {
            return std::task::Poll::Ready(Err(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset,
            )));
        }
        let count = usize::try_from(self.0.min(buffer.remaining() as u64)).unwrap();
        buffer.initialize_unfilled()[..count].fill(0);
        buffer.advance(count);
        self.0 -= count as u64;
        std::task::Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn multipart_extra_byte_probe_preserves_source_failure_and_does_not_publish() {
    use std::error::Error;
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    let len = PART_SIZE as u64 + 1;
    let error = store
        .put_stream("probe", Box::pin(FailsAfter(len)), len, None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .is_some()
    );
    assert!(store.head("probe").await.unwrap().is_none());
}

#[tokio::test]
async fn small_upload_reader_failure_preserves_its_source() {
    use std::error::Error;
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    let error = store
        .put_stream("failed", Box::pin(FailedReader), 1, None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .is_some()
    );
    assert!(store.head("failed").await.unwrap().is_none());
}

#[test]
fn explicit_endpoint_overrides_inherited_service_endpoint() {
    use object_store::aws::AmazonS3ConfigKey;
    let config = S3Config::parse("s3://bucket?endpoint=http%3A%2F%2Flocalhost%3A9000").unwrap();
    let inherited = AmazonS3Builder::new()
        .with_config(AmazonS3ConfigKey::S3Endpoint, "https://ambient.example.com");
    let builder = config.configure(inherited);
    assert_eq!(
        builder
            .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
            .as_deref(),
        config.endpoint.as_deref()
    );
}

static DRIVEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn driven_clock() -> u64 {
    DRIVEN.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn blob_lifetimes_follow_the_clock_and_are_stored_as_object_metadata() {
    use std::sync::atomic::Ordering;
    let objects = Arc::new(object_store::memory::InMemory::new());
    let blobs = handle(objects.clone(), "ttl").with_clock(driven_clock);
    DRIVEN.store(5_000, Ordering::SeqCst);

    blobs
        .put_expiring("k", vec![1, 2, 3], Some("a/b"), Duration::from_millis(100))
        .await
        .unwrap();
    let stored = objects
        .get_opts(
            &blobs.path("k").unwrap(),
            object_store::GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        stored
            .attributes
            .get(&Attribute::Metadata(EXPIRES_AT.into()))
            .map(|value| value.as_ref().to_owned()),
        Some("5100".to_owned())
    );

    DRIVEN.store(5_099, Ordering::SeqCst);
    assert_eq!(blobs.get("k").await.unwrap().unwrap().bytes, [1, 2, 3]);
    assert_eq!(blobs.list("").await.unwrap().len(), 1);

    DRIVEN.store(5_100, Ordering::SeqCst);
    assert!(blobs.get("k").await.unwrap().is_none());
    assert!(blobs.head("k").await.unwrap().is_none());
    assert!(blobs.get_range("k", 0..1).await.unwrap().is_none());
    assert!(blobs.get_stream("k").await.unwrap().is_none());
    assert_eq!(blobs.list("").await.unwrap().len(), 0);
    assert!(!blobs.delete("k").await.unwrap());
    assert!(
        objects.head(&blobs.path("k").unwrap()).await.is_err(),
        "delete removes the lapsed object"
    );
}

static DRIVEN_B: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn driven_clock_b() -> u64 {
    DRIVEN_B.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn a_plain_put_replaces_a_lifetime_and_a_multipart_blob_can_expire() {
    use std::sync::atomic::Ordering;
    let objects = Arc::new(object_store::memory::InMemory::new());
    let blobs = handle(objects, "ttl-b").with_clock(driven_clock_b);
    DRIVEN_B.store(1_000, Ordering::SeqCst);
    blobs
        .put_expiring("k", vec![1], None, Duration::from_millis(10))
        .await
        .unwrap();
    blobs.put("k", vec![2], None).await.unwrap();
    DRIVEN_B.store(1_000_000, Ordering::SeqCst);
    assert_eq!(blobs.get("k").await.unwrap().unwrap().bytes, [2]);

    let big = vec![7u8; PART_SIZE + 1];
    let len = big.len() as u64;
    blobs
        .put_stream_expiring(
            "big",
            Box::pin(std::io::Cursor::new(big)),
            len,
            None,
            Duration::from_millis(10),
        )
        .await
        .unwrap();
    assert!(blobs.head("big").await.unwrap().is_some());
    DRIVEN_B.store(1_000_011, Ordering::SeqCst);
    assert!(blobs.head("big").await.unwrap().is_none());
}

static DRIVEN_C: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn driven_clock_c() -> u64 {
    DRIVEN_C.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn the_sweeper_deletes_only_lapsed_objects_of_its_scope() {
    use std::sync::atomic::Ordering;
    let objects = Arc::new(object_store::memory::InMemory::new());
    let blobs = handle(objects.clone(), "sweep").with_clock(driven_clock_c);
    let other = handle(objects.clone(), "sweep-other").with_clock(driven_clock_c);
    DRIVEN_C.store(1_000, Ordering::SeqCst);
    for store in [&blobs, &other] {
        store
            .put_expiring("old", vec![1], None, Duration::from_millis(10))
            .await
            .unwrap();
    }
    blobs
        .put_expiring("young", vec![2], None, Duration::from_secs(1))
        .await
        .unwrap();
    blobs.put("forever", vec![3], None).await.unwrap();

    DRIVEN_C.store(1_500, Ordering::SeqCst);
    assert_eq!(blobs.sweep_expired().await.unwrap(), 1);
    assert!(
        objects.head(&blobs.path("old").unwrap()).await.is_err(),
        "the lapsed object is physically gone"
    );
    assert!(
        objects.head(&other.path("old").unwrap()).await.is_ok(),
        "another scope's lapsed object is left to its own sweep"
    );
    assert_eq!(blobs.list("").await.unwrap().len(), 2);
    assert_eq!(blobs.sweep_expired().await.unwrap(), 0);
}
