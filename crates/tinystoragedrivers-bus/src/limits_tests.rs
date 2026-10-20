use super::*;

const _: () = {
    assert!(MAX_INLINE_BYTES < MAX_FRAME_BYTES);
    // A JSON byte array costs up to four bytes per byte ("255,").
    assert!(MAX_INLINE_BLOB_BYTES * 4 <= MAX_INLINE_BYTES);
    assert!(BLOB_STREAM_CHUNK_BYTES <= MAX_INLINE_BLOB_BYTES);
    // A Receive reply always fits at least one maximal message.
    assert!(MAX_QUEUE_MESSAGE_BYTES * 2 <= MAX_INLINE_BYTES);
};

#[test]
fn limits_are_pinned() {
    assert_eq!(CONTRACT_VERSION, 7);
    assert_eq!(MAX_QUEUE_MESSAGE_BYTES, 256 * 1024);
    assert_eq!(MAX_FRAME_BYTES, 16 * 1024 * 1024);
    assert_eq!(MAX_INLINE_BYTES, 8 * 1024 * 1024);
    assert_eq!(MAX_PAGE_ITEMS, 1_000);
    assert_eq!(MAX_BATCH_OPS, 1_000);
    assert_eq!(MAX_PRESIGN_TTL_SECS, 3_600);
}
