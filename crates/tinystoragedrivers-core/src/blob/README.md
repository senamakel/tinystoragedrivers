# Blob port

BlobStore stores opaque bytes under validated relative keys within the scope
chosen by the backend. BlobMeta contains the logical key, byte length, and
optional content type. max_key_len reports this scoped handle's UTF-8 byte limit; object stores
subtract tenant prefixes from their physical limit. Ranges are half open and clamp to the object size.

put_stream accepts an owned BlobReader and an exact byte length. A mismatch
fails before publishing the replacement. get_stream returns metadata and a
reader, so errors after response headers arrive are I/O errors on that reader.
The compatibility implementations use put/get and buffer the complete body.
Callers handling large values should require StreamingBlobs on the blob
handle; this capability promises bounded driver buffers.

PresignedUrls permits presign_get and presign_put on the blob handle. The
default reports Unsupported. These URLs carry authorization and must not be
logged or exposed beyond the intended recipient.

Fencing support is separate. A driver advertising streaming or presigned
URLs does not imply it can fence blob writes atomically with document writes.
An object-store adapter should publish blobs first, fence the document
reference afterward, and collect orphaned objects.

## Lifetimes

put_expiring and put_stream_expiring write a blob that reads as absent once
its lifetime, counted from the write, has elapsed. Every read (get, get_range,
head, get_stream, list) honours it, a zero lifetime is expired on arrival, and a
plain put of the key clears any lifetime. Only handles reporting BlobTtl accept
a lifetime; the default refuses with Unsupported(BlobTtl). Reclaiming an expired
blob's storage is the driver's choice, so callers rely only on its being
invisible.

## Orphan cleanup

`sweep_unreferenced` pages through physical blob metadata below a key prefix,
then asks the host whether each old key is referenced before deleting it.
`older_than` is an exclusive epoch-millisecond cutoff: a blob is eligible
only when its trustworthy `written_at_ms` is strictly less than the cutoff.
Unknown legacy timestamps are retained. Pages are bounded and key ordered;
listing, reference-check, or delete errors stop the sweep and return an error.
Earlier successful deletions remain deleted, so an error can mean partial
progress and callers can safely retry after repairing the failure.

The cleanup listing includes physical blobs hidden from ordinary reads by an
expired lifetime. Drivers that cannot provide bounded pages and trustworthy
write times must not advertise `BlobSweep`. A sweep does not atomically bind a
reference check to deletion, and it cannot detect a concurrent overwrite of an
immutable key. Hosts must coordinate writers and sweepers so keys cannot be
replaced or referenced between the callback and deletion. The grace period
must also exceed the maximum delay between a blob write and its document
reference becoming durable; it cannot protect an indefinitely interrupted
write.
