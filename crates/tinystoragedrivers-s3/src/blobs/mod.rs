//! Scoped S3 blobs, with bounded multipart writes and incremental reads.

use std::{
    ops::Range,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::TryStreamExt;
use http::Method;
use object_store::{
    Attribute, Attributes, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    aws::AmazonS3Builder, path::Path, signer::Signer,
};
use tinystoragedrivers_core::{
    Blob, BlobBackend, BlobMeta, BlobReader, BlobStore, BlobStream, Capabilities, Capability,
    ErrorKind, Result, Scope, ScopeGeneration, StorageError, async_trait, validate_blob_key,
    validate_user_blob_key, validate_user_blob_prefix,
};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

/// Fixed multipart chunk size (8 MiB); at most one part is in flight.
const PART_SIZE: usize = 8 * 1024 * 1024;
const HEX: &[u8; 16] = b"0123456789abcdef";
/// User metadata holding a blob's expiry as epoch milliseconds. S3 prefixes it
/// with `x-amz-meta-` on the wire.
const EXPIRES_AT: &str = "tsd-expires-at-ms";
mod versions;

fn hex_scope(scope: &Scope) -> String {
    scope
        .as_str()
        .bytes()
        .flat_map(|byte| {
            [
                char::from(HEX[usize::from(byte >> 4)]),
                char::from(HEX[usize::from(byte & 15)]),
            ]
        })
        .collect()
}

/// Milliseconds since the Unix epoch.
fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// S3 bucket, endpoint and object-key prefix. Credentials come from the
/// standard AWS environment/credential provider chain, never from the URL.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Bucket name.
    pub bucket: String,
    /// Prefix inside the bucket.
    pub prefix: String,
    /// AWS region (also supplied for compatible endpoints).
    pub region: String,
    /// Optional S3-compatible endpoint; HTTP is accepted for local `MinIO`.
    pub endpoint: Option<String>,
}

impl S3Config {
    /// Parse an S3 URL with optional region and endpoint query parameters.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for malformed URLs, credentials, or unknown parameters.
    pub fn parse(value: &str) -> Result<Self> {
        let url = url::Url::parse(value)
            .map_err(|_| StorageError::invalid_input("invalid s3 storage URL"))?;
        if url.scheme() != "s3"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.fragment().is_some()
        {
            return Err(StorageError::invalid_input(
                "s3 URL must name a bucket without credentials",
            ));
        }
        let bucket = url
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| StorageError::invalid_input("s3 URL requires a bucket"))?
            .to_owned();
        let prefix = url.path().trim_matches('/').to_owned();
        if !prefix.is_empty() {
            validate_blob_key(&prefix)?;
        }
        let mut region = "us-east-1".to_owned();
        let mut endpoint = None;
        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                "region" if !value.is_empty() => region = value.into_owned(),
                "endpoint" if !value.is_empty() => {
                    let parsed = url::Url::parse(&value)
                        .map_err(|_| StorageError::invalid_input("invalid s3 endpoint"))?;
                    if !matches!(parsed.scheme(), "http" | "https")
                        || parsed.host_str().is_none()
                        || !parsed.username().is_empty()
                        || parsed.password().is_some()
                        || parsed.query().is_some()
                        || parsed.fragment().is_some()
                    {
                        return Err(StorageError::invalid_input("invalid s3 endpoint"));
                    }
                    endpoint = Some(value.into_owned());
                }
                _ => {
                    return Err(StorageError::invalid_input(
                        "unknown or empty s3 URL parameter",
                    ));
                }
            }
        }
        Ok(Self {
            bucket,
            prefix,
            region,
            endpoint,
        })
    }

    fn configure(&self, builder: AmazonS3Builder) -> AmazonS3Builder {
        let mut builder = builder
            .with_bucket_name(&self.bucket)
            .with_region(&self.region);
        if let Some(endpoint) = &self.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_config(object_store::aws::AmazonS3ConfigKey::S3Endpoint, endpoint)
                .with_allow_http(endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false);
        }
        builder
    }

    /// Construct a scoped blob handle, using path-style requests for compatible
    /// endpoints. Construction performs no network request.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an invalid prefix or client configuration.
    pub fn blobs(&self, scope: &Scope) -> Result<S3Blobs> {
        let builder = self.configure(AmazonS3Builder::from_env());
        let client = Arc::new(builder.build().map_err(map_error)?);
        S3Blobs::with_store(client.clone(), Some(client), &self.prefix, scope)
    }
}

/// One connected S3 client shared by all tenant blob handles.
///
/// Generation zero stays in the legacy `scopes/<hex-scope>/` root. Purged
/// generations use the sibling `scope-generations/<hex-scope>/<generation>/`
/// root, so a valid legacy blob key cannot overlap a later generation. Scope
/// enumeration covers both roots; a write already in flight during purge can
/// leave an inaccessible old-generation object for a later orphan sweep.
#[derive(Debug)]
pub struct S3Storage {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
    prefix: String,
}

impl S3Storage {
    /// Build a client using the configuration's standard credential providers.
    ///
    /// # Errors
    ///
    /// Invalid configuration or client construction failure.
    pub fn open(config: &S3Config) -> Result<Self> {
        let builder = config.configure(AmazonS3Builder::from_env());
        let client = Arc::new(builder.build().map_err(map_error)?);
        Self::with_store(client.clone(), Some(client), &config.prefix)
    }

    /// Inject an object-store client for local conformance or custom providers.
    ///
    /// # Errors
    ///
    /// Invalid object prefix.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        prefix: &str,
    ) -> Result<Self> {
        if !prefix.is_empty() {
            validate_blob_key(prefix.trim_matches('/'))?;
        }
        Ok(Self {
            store,
            signer,
            prefix: prefix.trim_matches('/').to_owned(),
        })
    }

    fn scoped_prefix(&self, scope: &Scope, generation: ScopeGeneration) -> String {
        let tenant = hex_scope(scope);
        let root = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        // Keep generation roots beside the legacy root. Nesting them below
        // `scopes/<scope>/` would collide with legal legacy keys such as g/1/x.
        if generation == ScopeGeneration(0) {
            format!("{root}scopes/{tenant}/")
        } else {
            format!("{root}scope-generations/{tenant}/{}/", generation.0)
        }
    }

    async fn purge_prefix(&self, prefix: &str) -> Result<()> {
        versions::purge_prefix(self.signer.as_ref(), prefix).await
    }
}

#[async_trait]
impl BlobBackend for S3Storage {
    fn capabilities(&self) -> Capabilities {
        let mut caps = Capabilities::none()
            .with(Capability::StreamingBlobs)
            .with(Capability::BlobTtl);
        if self.signer.is_some() {
            caps = caps
                .with(Capability::PresignedUrls)
                .with(Capability::ScopePurge);
        }
        caps.with(Capability::ScopeEnumeration)
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.for_scope_generation(scope, ScopeGeneration(0))
    }
    fn supports_generation_purge(&self) -> bool {
        self.signer.is_some()
    }
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<Arc<dyn BlobStore>> {
        Ok(Arc::new(S3Blobs::with_prefix(
            self.store.clone(),
            self.signer.clone(),
            self.scoped_prefix(scope, generation),
        )?))
    }
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        let tenant = hex_scope(scope);
        let root = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        self.purge_prefix(&format!("{root}scopes/{tenant}/"))
            .await?;
        self.purge_prefix(&format!("{root}scope-generations/{tenant}/"))
            .await
    }
    async fn purge_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<()> {
        let prefix = self.scoped_prefix(scope, generation);
        self.purge_prefix(&prefix).await
    }
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let mut found = std::collections::BTreeSet::new();
        let base = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        for root in [
            format!("{base}scopes/"),
            format!("{base}scope-generations/"),
        ] {
            let keys = if self.signer.is_some() {
                versions::list_keys(self.signer.as_ref(), &root).await?
            } else {
                let path = Path::parse(root.trim_end_matches('/'))
                    .map_err(|_| StorageError::invalid_input("invalid scope prefix"))?;
                let mut objects = self.store.list(Some(&path));
                let mut keys = Vec::new();
                while let Some(object) = objects.try_next().await.map_err(map_error)? {
                    keys.push(object.location.to_string());
                }
                keys
            };
            for key in keys {
                let Some(rest) = key.strip_prefix(&root) else {
                    continue;
                };
                let Some((encoded, _)) = rest.split_once('/') else {
                    continue;
                };
                if encoded.len() % 2 != 0 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    continue;
                }
                let Ok(bytes) = (0..encoded.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&encoded[i..i + 2], 16))
                    .collect::<std::result::Result<Vec<_>, _>>()
                else {
                    continue;
                };
                let Ok(text) = String::from_utf8(bytes) else {
                    continue;
                };
                if text.starts_with(prefix)
                    && let Ok(scope) = Scope::new(text)
                {
                    found.insert(scope);
                }
            }
        }
        Ok(found.into_iter().collect())
    }
}

/// Blob port under an unambiguous tenant prefix.
///
/// Writes are not fenced with documents. Upload immutable blobs first, then
/// fence the document referencing them, and garbage-collect unreferenced blobs.
#[derive(Debug)]
pub struct S3Blobs {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
    prefix: String,
    clock: fn() -> u64,
}

impl S3Blobs {
    /// Inject an S3-compatible object store and optional signer. This also enables
    /// deterministic conformance tests against a local object store.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an invalid prefix.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        prefix: &str,
        scope: &Scope,
    ) -> Result<Self> {
        let root = prefix.trim_matches('/');
        if !root.is_empty() {
            validate_blob_key(root)?;
        }
        let prefix = format!(
            "{}scopes/{}/",
            if root.is_empty() {
                String::new()
            } else {
                format!("{root}/")
            },
            hex_scope(scope)
        );
        Self::with_prefix(store, signer, prefix)
    }

    fn with_prefix(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        prefix: String,
    ) -> Result<Self> {
        if prefix.len() >= 1024 {
            return Err(StorageError::invalid_input(
                "s3 tenant prefix leaves no room for a blob key",
            ));
        }
        Ok(Self {
            store,
            signer,
            prefix,
            clock: system_clock,
        })
    }

    /// Read blob lifetimes against `clock` (milliseconds since the Unix
    /// epoch) instead of the system clock, so tests can drive expiry.
    #[must_use]
    pub fn with_clock(mut self, clock: fn() -> u64) -> Self {
        self.clock = clock;
        self
    }

    /// Delete every object of this scope whose lifetime has passed and return
    /// how many went, for a periodic sweeper.
    ///
    /// Reads already treat a lapsed blob as absent, but object storage cannot
    /// delete conditionally, so a read does not reclaim: a writer replacing the
    /// key between the read and the delete would lose its new object. The sweep
    /// re-checks each object immediately before deleting it, which narrows but
    /// cannot close that window; run it from one sweeper and keep a bucket
    /// lifecycle rule as the backstop.
    ///
    /// # Errors
    ///
    /// A backend error.
    pub async fn sweep_expired(&self) -> Result<u64> {
        let root = Path::parse(self.prefix.trim_end_matches('/'))
            .map_err(|_| StorageError::invalid_input("invalid scope prefix"))?;
        let mut objects = self.store.list(Some(&root));
        let mut swept = 0;
        while let Some(object) = objects.try_next().await.map_err(map_error)? {
            let found = match self.head_object(&object.location).await {
                Ok(found) => found,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(error) => return Err(map_error(error)),
            };
            if !lapsed(&found.attributes, (self.clock)()) {
                continue;
            }
            // Re-check right before the delete: a concurrent put may have
            // replaced the object with a live one since the listing.
            match self.head_object(&object.location).await {
                Ok(again) if lapsed(&again.attributes, (self.clock)()) => {}
                Ok(_) | Err(object_store::Error::NotFound { .. }) => continue,
                Err(error) => return Err(map_error(error)),
            }
            match self.store.delete(&object.location).await {
                Ok(()) => swept += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(map_error(error)),
            }
        }
        Ok(swept)
    }

    async fn head_object(
        &self,
        path: &Path,
    ) -> std::result::Result<object_store::GetResult, object_store::Error> {
        self.store
            .get_opts(
                path,
                object_store::GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
    }

    fn path(&self, key: &str) -> Result<Path> {
        validate_user_blob_key(key)?;
        if key.len() > self.max_key_len() {
            return Err(StorageError::invalid_input(
                "blob key exceeds the scoped s3 key limit",
            ));
        }
        Path::parse(format!("{}{key}", self.prefix))
            .map_err(|_| StorageError::invalid_input("invalid object key"))
    }

    async fn signed(&self, key: &str, ttl: Duration, method: Method) -> Result<String> {
        let path = self.path(key)?;
        if ttl.as_secs() == 0 || ttl > Duration::from_secs(7 * 24 * 60 * 60) {
            return Err(StorageError::invalid_input(
                "presigned URL ttl must be between one second and seven days",
            ));
        }
        let signer = self.signer.as_ref().ok_or_else(|| {
            StorageError::unsupported(Capability::PresignedUrls, "this blob store has no signer")
        })?;
        signer
            .signed_url(method, &path, ttl)
            .await
            .map(|url| url.to_string())
            .map_err(map_error)
    }
}

fn attributes(content_type: Option<&str>, expires_at: Option<u64>) -> Attributes {
    let mut attributes = Attributes::default();
    if let Some(value) = content_type {
        attributes.insert(Attribute::ContentType, value.to_owned().into());
    }
    if let Some(at) = expires_at {
        attributes.insert(
            Attribute::Metadata(EXPIRES_AT.into()),
            at.to_string().into(),
        );
    }
    attributes
}

/// Whether the object these attributes came from has outlived its lifetime.
/// Unreadable metadata counts as no lifetime: the writer always emits digits.
fn lapsed(attributes: &Attributes, now: u64) -> bool {
    attributes
        .get(&Attribute::Metadata(EXPIRES_AT.into()))
        .and_then(|value| value.as_ref().parse::<u64>().ok())
        .is_some_and(|at| at <= now)
}

fn metadata(key: &str, len: u64, attributes: &Attributes) -> BlobMeta {
    BlobMeta {
        key: key.to_owned(),
        len,
        content_type: attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_owned()),
    }
}

fn map_error(error: object_store::Error) -> StorageError {
    let kind = match &error {
        object_store::Error::NotFound { .. } => ErrorKind::NotFound,
        object_store::Error::AlreadyExists { .. } => ErrorKind::AlreadyExists,
        object_store::Error::Precondition { .. } => ErrorKind::Conflict,
        _ => ErrorKind::Backend,
    };
    // Transport messages and paths can contain credentials and signed URLs.
    // Retain the error category through a source with no unsanitized chain.
    let category = match &error {
        object_store::Error::Generic { .. } => "object store backend failure",
        object_store::Error::NotFound { .. } => "object store object not found",
        object_store::Error::AlreadyExists { .. } => "object store object already exists",
        object_store::Error::Precondition { .. } => "object store precondition failed",
        object_store::Error::PermissionDenied { .. } => "object store permission denied",
        object_store::Error::Unauthenticated { .. } => "object store authentication failed",
        object_store::Error::InvalidPath { .. } => "object store invalid path",
        object_store::Error::NotSupported { .. } => "object store operation unsupported",
        object_store::Error::NotImplemented { .. } => "object store operation unimplemented",
        object_store::Error::NotModified { .. } => "object store object not modified",
        object_store::Error::UnknownConfigurationKey { .. } => "object store configuration invalid",
        _ => "object store task failed",
    };
    drop(error);
    StorageError::new(kind, "object store operation failed")
        .with_source(SanitizedObjectStoreError(category))
}

#[derive(Debug)]
struct SanitizedObjectStoreError(&'static str);

impl std::fmt::Display for SanitizedObjectStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for SanitizedObjectStoreError {}

fn multipart_reader_error(error: std::io::Error) -> StorageError {
    let kind = if error.kind() == std::io::ErrorKind::UnexpectedEof {
        ErrorKind::InvalidInput
    } else {
        ErrorKind::Backend
    };
    StorageError::new(kind, "blob reader failed before declared length").with_source(error)
}

impl S3Blobs {
    /// Upload exactly `len` bytes, bounded by [`PART_SIZE`], optionally with a
    /// lifetime counted from now.
    async fn store(
        &self,
        key: &str,
        mut reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Option<Duration>,
    ) -> Result<BlobMeta> {
        let path = self.path(key)?;
        let expires_at = ttl.map(|ttl| {
            (self.clock)().saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
        });
        let attrs = attributes(content_type, expires_at);
        if len <= PART_SIZE as u64 {
            let mut data = Vec::new();
            (&mut reader)
                .take(len + 1)
                .read_to_end(&mut data)
                .await
                .map_err(|error| StorageError::backend("blob reader failed").with_source(error))?;
            if data.len() as u64 != len {
                return Err(StorageError::invalid_input(
                    "blob reader length differs from declared length",
                ));
            }
            self.store
                .put_opts(
                    &path,
                    Bytes::from(data).into(),
                    PutOptions {
                        attributes: attrs.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(map_error)?;
        } else {
            if len > PART_SIZE as u64 * 10_000 {
                return Err(StorageError::invalid_input(
                    "blob exceeds multipart part limit",
                ));
            }
            let mut upload = self
                .store
                .put_multipart_opts(
                    &path,
                    PutMultipartOptions {
                        attributes: attrs.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(map_error)?;
            let result: Result<()> = async {
                let mut remaining = len;
                while remaining > 0 {
                    let size = usize::try_from(remaining.min(PART_SIZE as u64))
                        .map_err(|_| StorageError::invalid_input("blob part size overflow"))?;
                    let mut part = vec![0; size];
                    reader
                        .read_exact(&mut part)
                        .await
                        .map_err(multipart_reader_error)?;
                    upload
                        .put_part(Bytes::from(part).into())
                        .await
                        .map_err(map_error)?;
                    remaining -= size as u64;
                }
                let mut extra = [0];
                if reader.read(&mut extra).await.map_err(|error| {
                    StorageError::backend("blob reader failed").with_source(error)
                })? != 0
                {
                    return Err(StorageError::invalid_input(
                        "blob reader exceeds declared length",
                    ));
                }
                upload.complete().await.map_err(map_error)?;
                Ok(())
            }
            .await;
            if let Err(error) = result {
                // Lifecycle expiry is still required for abrupt process death or
                // cancellation, where asynchronous cleanup cannot run.
                let _ = upload.abort().await;
                return Err(error);
            }
        }
        Ok(metadata(key, len, &attrs))
    }
}

#[async_trait]
impl BlobStore for S3Blobs {
    async fn sweep_expired(&self) -> Result<u64> {
        S3Blobs::sweep_expired(self).await
    }

    fn max_key_len(&self) -> usize {
        1024 - self.prefix.len()
    }
    fn capabilities(&self) -> Capabilities {
        let caps = Capabilities::none()
            .with(Capability::StreamingBlobs)
            .with(Capability::BlobTtl);
        if self.signer.is_some() {
            caps.with(Capability::PresignedUrls)
        } else {
            caps
        }
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        let len = bytes.len() as u64;
        self.store(
            key,
            Box::pin(std::io::Cursor::new(bytes)),
            len,
            content_type,
            None,
        )
        .await
    }

    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        let len = bytes.len() as u64;
        self.store(
            key,
            Box::pin(std::io::Cursor::new(bytes)),
            len,
            content_type,
            Some(ttl),
        )
        .await
    }

    async fn put_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.store(key, reader, len, content_type, None).await
    }

    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, reader, len, content_type, Some(ttl)).await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        let path = self.path(key)?;
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(map_error(error)),
        };
        if lapsed(&result.attributes, (self.clock)()) {
            return Ok(None);
        }
        let meta = metadata(key, result.meta.size, &result.attributes);
        let stream = result
            .into_stream()
            .map_err(|_| std::io::Error::other("object store read failed"));
        Ok(Some(BlobStream {
            meta,
            reader: Box::pin(StreamReader::new(stream)),
        }))
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        let Some(mut stream) = self.get_stream(key).await? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        stream
            .reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| StorageError::backend("object store read failed"))?;
        Ok(Some(Blob {
            meta: stream.meta,
            bytes,
        }))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        let result = match self
            .store
            .get_opts(
                &self.path(key)?,
                object_store::GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(map_error(error)),
        };
        if lapsed(&result.attributes, (self.clock)()) {
            return Ok(None);
        }
        Ok(Some(metadata(key, result.meta.size, &result.attributes)))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        if range.start > range.end {
            return Err(StorageError::invalid_input(
                "blob range start is after its end",
            ));
        }
        let Some(meta) = self.head(key).await? else {
            return Ok(None);
        };
        let range = range.start.min(meta.len)..range.end.min(meta.len);
        if range.is_empty() {
            return Ok(Some(Vec::new()));
        }
        self.store
            .get_range(&path, range)
            .await
            .map(|b| Some(b.to_vec()))
            .map_err(map_error)
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let path = self.path(key)?;
        let found = match self
            .store
            .get_opts(
                &path,
                object_store::GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(found) => found,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(map_error(error)),
        };
        self.store.delete(&path).await.map_err(map_error)?;
        // A lapsed blob is not there to delete, but its object goes too.
        Ok(!lapsed(&found.attributes, (self.clock)()))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_user_blob_prefix(prefix)?;
        // A partial key prefix need not itself be a valid full key.
        // ObjectStore list prefixes match complete path segments. Push the
        // requested directory portion down; filter its partial last segment below.
        let directory = prefix
            .rsplit_once('/')
            .map_or("", |(directory, _)| directory);
        let query_prefix = format!("{}{directory}", self.prefix);
        let root = Path::parse(query_prefix.trim_end_matches('/'))
            .map_err(|_| StorageError::invalid_input("invalid scope prefix"))?;
        let mut objects = self.store.list(Some(&root));
        let mut metas = Vec::new();
        while let Some(object) = objects.try_next().await.map_err(map_error)? {
            if let Some(key) = object.location.as_ref().strip_prefix(&self.prefix)
                && key.starts_with(prefix)
                && validate_user_blob_key(key).is_ok()
                && key.len() <= self.max_key_len()
            {
                // S3 ListObjects omits content-type, so fetch HEAD for the
                // port's complete metadata contract.
                if let Some(meta) = self.head(key).await? {
                    metas.push(meta);
                }
            }
        }
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(metas)
    }

    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.signed(key, ttl, Method::GET).await
    }

    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        self.signed(key, ttl, Method::PUT).await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
