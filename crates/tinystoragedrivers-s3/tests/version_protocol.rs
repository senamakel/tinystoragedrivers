//! Loopback S3 protocol tests for version-aware deletion and generation isolation.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::TryStreamExt;
use http::Method;
use object_store::{
    ObjectStore, ObjectStoreExt,
    memory::InMemory,
    path::Path,
    signer::{SignedUrlOptions, Signer},
};
use tinystoragedrivers_core::{
    BlobBackend, CompositeStorage, MemoryStorage, PurgeMode, PurgeRequest, Scope, ScopeGeneration,
    StorageBackend, async_trait,
};
use tinystoragedrivers_s3::S3Storage;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use url::Url;

#[derive(Debug)]
struct LoopbackSigner(String);

#[derive(Debug)]
struct LeakyFailureSigner;

#[derive(Debug)]
struct DeleteFailureSigner(LoopbackSigner);

#[async_trait]
impl Signer for LoopbackSigner {
    async fn signed_url(
        &self,
        _method: Method,
        path: &Path,
        _expires_in: Duration,
    ) -> object_store::Result<Url> {
        Url::parse(&format!("{}{path}", self.0)).map_err(|error| object_store::Error::Generic {
            store: "loopback signer",
            source: Box::new(error),
        })
    }

    async fn signed_url_opts(
        &self,
        _method: Method,
        path: &Path,
        _expires_in: Duration,
        options: &SignedUrlOptions,
    ) -> object_store::Result<Url> {
        let mut url = self
            .signed_url(Method::GET, path, Duration::from_secs(1))
            .await?;
        url.query_pairs_mut().extend_pairs(&options.extra_query);
        Ok(url)
    }
}

#[async_trait]
impl Signer for LeakyFailureSigner {
    async fn signed_url(
        &self,
        _method: Method,
        _path: &Path,
        _expires_in: Duration,
    ) -> object_store::Result<Url> {
        Err(leaky_signing_error())
    }

    async fn signed_url_opts(
        &self,
        _method: Method,
        _path: &Path,
        _expires_in: Duration,
        _options: &SignedUrlOptions,
    ) -> object_store::Result<Url> {
        Err(leaky_signing_error())
    }
}

#[async_trait]
impl Signer for DeleteFailureSigner {
    async fn signed_url(
        &self,
        method: Method,
        path: &Path,
        expires_in: Duration,
    ) -> object_store::Result<Url> {
        self.0.signed_url(method, path, expires_in).await
    }

    async fn signed_url_opts(
        &self,
        method: Method,
        path: &Path,
        expires_in: Duration,
        options: &SignedUrlOptions,
    ) -> object_store::Result<Url> {
        if method == Method::DELETE {
            return Err(leaky_signing_error());
        }
        self.0
            .signed_url_opts(method, path, expires_in, options)
            .await
    }
}

fn leaky_signing_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "test signer",
        source: Box::new(std::io::Error::other(
            "https://user:secret@example.invalid/?X-Amz-Signature=secret",
        )),
    }
}

#[derive(Default)]
struct FixtureState {
    objects: Option<Arc<InMemory>>,
    pages: Mutex<VecDeque<String>>,
    get_requests: Mutex<Vec<String>>,
    delete_requests: Mutex<Vec<String>>,
    reject_get: AtomicBool,
    stall_get: AtomicBool,
    stalled_get_entered: Notify,
    release_stalled_get: Notify,
    reject_delete: AtomicBool,
    pause_first_delete: AtomicBool,
    delete_entered: Notify,
    release_delete: Notify,
}

struct Fixture {
    base: String,
    state: Arc<FixtureState>,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(state: FixtureState) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let state = Arc::new(state);
        let task_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(handle_request(stream, Arc::clone(&task_state)));
            }
        });
        Ok(Self {
            base: format!("http://{address}/"),
            state,
            task,
        })
    }

    fn signer(&self) -> Arc<dyn Signer> {
        Arc::new(LoopbackSigner(self.base.clone()))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_request(mut stream: TcpStream, state: Arc<FixtureState>) {
    let mut request = Vec::new();
    let mut chunk = [0; 2048];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    let Ok(text) = std::str::from_utf8(&request) else {
        return;
    };
    let Some(line) = text.lines().next() else {
        return;
    };
    let mut parts = line.split_whitespace();
    let Some(method) = parts.next() else { return };
    let Some(target) = parts.next() else { return };
    let Ok(url) = Url::parse(&format!("http://fixture{target}")) else {
        return;
    };
    let path = url.path().trim_start_matches('/').to_owned();
    let query: Vec<_> = url.query_pairs().into_owned().collect();

    let (status, body) = match method {
        "GET" => get_response(&state, target, &query).await,
        "DELETE" => {
            state
                .delete_requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(target.to_owned());
            if state.pause_first_delete.swap(false, Ordering::SeqCst) {
                state.delete_entered.notify_one();
                state.release_delete.notified().await;
            }
            if state.reject_delete.load(Ordering::SeqCst) {
                return write_response(&mut stream, "500 Internal Server Error", "").await;
            }
            if let Some(objects) = &state.objects
                && let Ok(path) = Path::parse(&path)
            {
                let _ = objects.delete(&path).await;
            }
            ("204 No Content", String::new())
        }
        _ => ("405 Method Not Allowed", String::new()),
    };
    write_response(&mut stream, status, &body).await;
}

async fn get_response(
    state: &FixtureState,
    target: &str,
    query: &[(String, String)],
) -> (&'static str, String) {
    state
        .get_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(target.to_owned());
    if state.stall_get.load(Ordering::SeqCst) {
        state.stalled_get_entered.notify_one();
        state.release_stalled_get.notified().await;
    }
    let page = state
        .pages
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .pop_front();
    if state.reject_get.load(Ordering::SeqCst) {
        return ("500 Internal Server Error", String::new());
    }
    if let Some(page) = page {
        return ("200 OK", page);
    }
    let Some(objects) = &state.objects else {
        return ("500 Internal Server Error", String::new());
    };
    let prefix = query
        .iter()
        .find(|(key, _)| key == "prefix")
        .map_or("", |(_, value)| value.as_str());
    let path = Path::parse(prefix.trim_end_matches('/')).ok();
    let mut entries = Vec::new();
    if let Some(path) = path.as_ref() {
        let mut listing = objects.list(Some(path));
        while let Ok(Some(meta)) = listing.try_next().await {
            entries.push(meta.location.to_string());
        }
    }
    let mut versions = String::new();
    for key in entries {
        use std::fmt::Write as _;
        let _ = write!(
            versions,
            "<Version><Key>{key}</Key><VersionId>current</VersionId></Version>"
        );
    }
    (
        "200 OK",
        format!(
            "<ListVersionsResult><IsTruncated>false</IsTruncated>{versions}</ListVersionsResult>"
        ),
    )
}

async fn write_response(stream: &mut TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

fn page(prefix: &str, truncated: bool, marker: Option<(&str, &str)>, versions: &str) -> String {
    let markers = marker.map_or_else(String::new, |(key, version)| {
        format!("<NextKeyMarker>{key}</NextKeyMarker><NextVersionIdMarker>{version}</NextVersionIdMarker>")
    });
    format!(
        "<ListVersionsResult><IsTruncated>{truncated}</IsTruncated>{markers}{versions}</ListVersionsResult>",
        versions = versions.replace("{{prefix}}", prefix)
    )
}

#[tokio::test]
async fn version_purge_forwards_markers_and_deletes_versions_and_markers()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new("protocol-pagination")?;
    let prefix = format!("test/scopes/{}/", scope_hex(&scope));
    let mut pages = VecDeque::new();
    pages.push_back(page(
        &prefix,
        true,
        Some(("marker-key", "marker-version")),
        "<Version><Key>{{prefix}}one</Key><VersionId>v1</VersionId></Version>",
    ));
    pages.push_back(page(
        &prefix,
        false,
        None,
        "<Version><Key>{{prefix}}one</Key><VersionId>null</VersionId></Version><DeleteMarker><Key>{{prefix}}two</Key><VersionId>delete-2</VersionId></DeleteMarker>",
    ));
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(pages),
        ..FixtureState::default()
    })
    .await?;
    let objects = Arc::new(InMemory::new());
    let storage = S3Storage::with_store(objects, Some(fixture.signer()), "test")?;
    storage
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await?;

    let requests = fixture
        .state
        .get_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains("key-marker=marker-key"));
    assert!(requests[1].contains("version-id-marker=marker-version"));
    let deletes = fixture
        .state
        .delete_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(deletes.len(), 3);
    assert!(deletes[0].contains("versionId=v1"));
    assert!(deletes[1].contains("versionId=null"));
    assert!(deletes[2].contains("versionId=delete-2"));
    Ok(())
}

#[tokio::test]
async fn repeated_version_marker_cycle_fails_before_an_extra_request()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new("protocol-marker-cycle")?;
    let prefix = format!("test/scopes/{}/", scope_hex(&scope));
    let pages = VecDeque::from([
        page(&prefix, true, Some(("key-a", "v1")), ""),
        page(&prefix, true, Some(("key-b", "v2")), ""),
        page(&prefix, true, Some(("key-a", "v1")), ""),
    ]);
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(pages),
        ..FixtureState::default()
    })
    .await?;
    let storage = S3Storage::with_store(Arc::new(InMemory::new()), Some(fixture.signer()), "test")?;
    let result = storage
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await;
    let error = match result {
        Ok(()) => return Err(std::io::Error::other("expected marker-cycle failure").into()),
        Err(error) => error,
    };
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    let requests = fixture
        .state
        .get_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 3);
    Ok(())
}

#[tokio::test]
async fn signed_url_failure_does_not_expose_credentials_or_query_values()
-> Result<(), Box<dyn std::error::Error>> {
    let storage = S3Storage::with_store(
        Arc::new(InMemory::new()),
        Some(Arc::new(LeakyFailureSigner)),
        "test",
    )?;
    let result = storage
        .purge_scope_generation(&Scope::new("signing-error-scope")?, ScopeGeneration(0))
        .await;
    let error = match result {
        Ok(()) => return Err(std::io::Error::other("expected signer failure").into()),
        Err(error) => error,
    };
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    assert!(!format!("{error:?}").contains("secret"));
    Ok(())
}

#[tokio::test]
async fn version_listing_refuses_http_errors_and_unreachable_signed_urls()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new(FixtureState {
        reject_get: AtomicBool::new(true),
        ..FixtureState::default()
    })
    .await?;
    let scope = Scope::new("protocol-list-http-error")?;
    let storage = S3Storage::with_store(Arc::new(InMemory::new()), Some(fixture.signer()), "test")?;
    let error = storage
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    assert!(format!("{error}").contains("version-list request was rejected"));

    let unreachable = S3Storage::with_store(
        Arc::new(InMemory::new()),
        Some(Arc::new(LoopbackSigner("http://127.0.0.1:1/".into()))),
        "test",
    )?;
    let error = unreachable
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await
        .unwrap_err();
    assert!(format!("{error}").contains("version-list request failed"));
    Ok(())
}

#[tokio::test]
async fn stalled_version_listing_returns_a_bounded_backend_error()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new(FixtureState {
        stall_get: AtomicBool::new(true),
        ..FixtureState::default()
    })
    .await?;
    let scope = Scope::new("protocol-stalled-version-list")?;
    let storage = S3Storage::with_store(Arc::new(InMemory::new()), Some(fixture.signer()), "test")?;
    let request = tokio::spawn(async move {
        storage
            .purge_scope_generation(&scope, ScopeGeneration(0))
            .await
    });
    fixture.state.stalled_get_entered.notified().await;

    let completion = tokio::time::timeout(Duration::from_secs(12), request).await;
    fixture.state.release_stalled_get.notify_waiters();
    let result = match completion {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => {
            return Err(std::io::Error::other(
                "stalled version-list request did not reach its configured timeout",
            )
            .into());
        }
    };
    let error = match result {
        Ok(()) => {
            return Err(std::io::Error::other("stalled listing unexpectedly succeeded").into());
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    assert!(format!("{error}").contains("version-list request failed"));
    Ok(())
}

#[tokio::test]
async fn version_delete_http_failure_keeps_the_purge_incomplete()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new("protocol-delete-http-error")?;
    let prefix = format!("test/scopes/{}/", scope_hex(&scope));
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(VecDeque::from([page(
            &prefix,
            false,
            None,
            "<Version><Key>{{prefix}}one</Key><VersionId>v1</VersionId></Version>",
        )])),
        reject_delete: AtomicBool::new(true),
        ..FixtureState::default()
    })
    .await?;
    let storage = S3Storage::with_store(Arc::new(InMemory::new()), Some(fixture.signer()), "test")?;
    let error = storage
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await
        .unwrap_err();
    assert!(format!("{error}").contains("version-delete request was rejected"));
    assert_eq!(
        fixture
            .state
            .delete_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn version_delete_signing_failure_is_sanitized() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new("protocol-delete-signing-error")?;
    let prefix = format!("test/scopes/{}/", scope_hex(&scope));
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(VecDeque::from([page(
            &prefix,
            false,
            None,
            "<Version><Key>{{prefix}}one</Key><VersionId>v1</VersionId></Version>",
        )])),
        ..FixtureState::default()
    })
    .await?;
    let signer = Arc::new(DeleteFailureSigner(LoopbackSigner(fixture.base.clone())));
    let storage = S3Storage::with_store(Arc::new(InMemory::new()), Some(signer), "test")?;
    let error = storage
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    assert!(!format!("{error:?}").contains("secret"));
    Ok(())
}

#[tokio::test]
async fn purge_rejects_a_version_listing_that_crosses_the_scope_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new("protocol-prefix-isolation")?;
    let prefix = format!("test/scopes/{}/", scope_hex(&scope));
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(VecDeque::from([page(
            &prefix,
            false,
            None,
            "<Version><Key>test/scopes/another-tenant/key</Key><VersionId>v1</VersionId></Version>",
        )])),
        ..FixtureState::default()
    })
    .await?;
    let storage = S3Storage::with_store(Arc::new(InMemory::new()), Some(fixture.signer()), "test")?;
    let error = storage
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    assert_eq!(
        fixture
            .state
            .delete_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn scope_enumeration_decodes_legacy_and_generation_roots_across_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new("protocol-enumeration")?;
    let encoded = scope_hex(&scope);
    let pages = VecDeque::from([
        page("test/scopes/", true, Some(("cursor", "version")), ""),
        page(
            "test/scopes/",
            false,
            None,
            &format!(
                "<Version><Key>test/scopes/{encoded}/legacy</Key><VersionId>v1</VersionId></Version>"
            ),
        ),
        page(
            "test/scope-generations/",
            false,
            None,
            &format!(
                "<Version><Key>test/scope-generations/{encoded}/2/current</Key><VersionId>v2</VersionId></Version>"
            ),
        ),
    ]);
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(pages),
        ..FixtureState::default()
    })
    .await?;
    let storage = S3Storage::with_store(Arc::new(InMemory::new()), Some(fixture.signer()), "test")?;
    assert_eq!(storage.scopes("").await?, vec![scope]);
    let requests = fixture
        .state
        .get_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 3);
    assert!(requests[1].contains("key-marker=cursor"));
    assert!(requests[1].contains("version-id-marker=version"));
    Ok(())
}

#[tokio::test]
async fn delayed_old_generation_delete_cannot_remove_replacement_blob()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = Arc::new(InMemory::new());
    let fixture = Fixture::new(FixtureState {
        objects: Some(Arc::clone(&objects)),
        pause_first_delete: AtomicBool::new(true),
        ..FixtureState::default()
    })
    .await?;
    let blobs = Arc::new(S3Storage::with_store(
        objects,
        Some(fixture.signer()),
        "test",
    )?);
    let records = Arc::new(MemoryStorage::new());
    let first = CompositeStorage::new(records.clone(), blobs.clone());
    let second = CompositeStorage::new(records, blobs);
    let scope = Scope::new("delayed-s3-generation-delete")?;
    first
        .for_scope(&scope)?
        .blobs()
        .put("same-key", vec![1], None)
        .await?;
    let request = PurgeRequest {
        operation_id: "delayed-generation-purge".into(),
        mode: PurgeMode::Quiescent,
    };
    let stale_runner = {
        let first = first;
        let scope = scope.clone();
        let request = request.clone();
        tokio::spawn(async move { first.for_scope(&scope)?.purge(&request).await })
    };
    fixture.state.delete_entered.notified().await;

    let receipt = second.for_scope(&scope)?.purge(&request).await?;
    assert_eq!(
        receipt.status,
        tinystoragedrivers_core::PurgeStatus::Complete
    );
    let replacement = second
        .for_scope_generation(&scope, receipt.generation)?
        .blobs()
        .clone();
    replacement.put("same-key", vec![9], None).await?;

    fixture.state.release_delete.notify_one();
    stale_runner.await??;
    assert_eq!(
        replacement
            .get("same-key")
            .await?
            .ok_or_else(|| std::io::Error::other(
                "replacement blob disappeared after stale delete"
            ))?
            .bytes,
        [9]
    );
    let deletes = fixture
        .state
        .delete_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        deletes
            .iter()
            .all(|request| !request.contains("scope-generations"))
    );
    Ok(())
}

fn scope_hex(scope: &Scope) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(scope.as_str().len() * 2);
    for byte in scope.as_str().bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
