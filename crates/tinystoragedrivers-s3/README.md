# S3 blob storage

This driver supplies a scoped BlobStore over S3 and S3-compatible endpoints.
It uses object_store's AWS protocol client and does not depend on aws-sdk-s3.
It supplies blobs only; the host supplies documents and streams separately.

Parse s3://bucket/prefix?region=us-east-1 or add an endpoint parameter, for
example s3://bucket/prefix?region=local&endpoint=http%3A%2F%2Flocalhost%3A9000.
Credentials come from the standard AWS environment and credential providers.
Do not put credentials in either URL. An explicit endpoint uses path-style
requests. Plain HTTP is supported for local MinIO; production endpoints
should use HTTPS.

S3Config::blobs(scope) binds one handle to
prefix/scopes/hex-encoded-UTF8-scope/. Hex encoding prevents scope
names containing slash, percent, colon or Unicode from crossing tenant
boundaries. Every operation, including presigning and listing, uses this
prefix. max_key_len reports 1024 minus the prefix's UTF-8 byte length;
keys exceeding that scoped limit fail before a network call. List consumes all result pages and returns complete metadata sorted
by logical key.

## Scope purge

Scope purge is advertised only when a signer is configured. Generation zero
uses the legacy `scopes/<scope-hex>/` prefix; later generations use the sibling
`scope-generations/<scope-hex>/<generation>/` prefix so a legal legacy object
key cannot collide with the generation namespace. Purge lists every object
version and delete marker beneath the exact prefix, follows S3 version markers
across pages, and deletes each entry by VersionId. This handles versioned and
versioning-suspended buckets, including the `null` version. It does not claim
atomicity with document or stream deletion.

The signing identity needs `s3:ListBucketVersions` on the bucket and
`s3:DeleteObjectVersion` on the scoped object resources. Signer-less stores
withhold ScopePurge, and CompositeStorage refuses the purge capability before
mutation if its blob backend cannot provide generation-safe purge. Hosts must
stop and drain writers before quiescent reset. An upload that finishes late in
an old physical generation can leave an unreachable orphan; its reclamation is
separate maintenance work.

## Streaming and publication

put_stream reads at most 8 MiB for a small upload. Larger objects use sequential
8 MiB multipart parts with one request in flight; memory use stays bounded as
the object grows. A short or long reader fails without publishing its body.
A failed multipart write aborts its upload, leaving the previous object intact.
Configure bucket lifecycle rules to remove incomplete uploads after process
death or task cancellation, where asynchronous abort cannot run.

get_stream returns the HTTP response incrementally as an AsyncRead. put/get
remain convenient buffered APIs for callers with small values. The blob
handle advertises StreamingBlobs; older drivers' compatibility methods buffer
the whole object and do not advertise it.

presign_get and presign_put issue bearer URLs scoped to the same object key.
They advertise PresignedUrls only when a signer is installed, and accept
durations from one second through seven days. Protect returned URLs as
credentials. Presigning does not establish that the object exists.

## Fencing and garbage collection

S3 writes cannot share a transaction with document writes and do not enforce
document lease fences. Write an immutable blob first, then write its reference
through the fenced document handle. If that reference fails, the blob is an
orphan; garbage collection must enumerate unreferenced objects after a grace
period. Direct presigned PUTs also bypass document fences.

## Validation

Unit tests use the local object_store memory implementation and injected
credentials for offline signing. They cover the blob contract, tenant and
scope isolation, multipart publication and length errors, and GET/PUT URL
signatures. They do not prove compatibility with a particular live service.
Run .github/scripts/test-minio.sh for live local MinIO conformance, direct
presigned transfers and a 500 MiB streaming roundtrip. The fixture verifies a
pinned upstream release checksum, stores everything under target/, and binds
only loopback. CI runs this same suite. Composite facade URL selection is
available through BlobConfig and open_composite behind the facade's s3 feature.
Host feature forwarding and desktop data import remain host integration work.
