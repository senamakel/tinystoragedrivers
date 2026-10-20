//! Capability set arithmetic, requirement checks and rendering.

use super::*;
use crate::ErrorKind;

#[test]
fn starts_empty_and_accumulates() {
    let caps = Capabilities::none();
    assert_eq!(caps, Capabilities::default());
    assert!(!caps.contains(Capability::FullText));

    let caps = caps.with(Capability::FullText).with(Capability::Ttl);
    assert!(caps.contains(Capability::FullText));
    assert!(caps.contains(Capability::Ttl));
    assert!(!caps.contains(Capability::Transactions));

    let caps = caps.without(Capability::FullText);
    assert!(!caps.contains(Capability::FullText));
}

#[test]
fn all_holds_every_capability() {
    let all = Capabilities::all();
    assert_eq!(
        all.iter().collect::<Vec<_>>(),
        vec![
            Capability::ExistingSchema,
            Capability::CollectionValidation,
            Capability::FullText,
            Capability::Transactions,
            Capability::AtomicWriteOptions,
            Capability::Ttl,
            Capability::Fencing,
            Capability::PresignedUrls,
            Capability::StreamingBlobs,
            Capability::CollectionEnumeration,
            Capability::ConditionalStreamAppend,
            Capability::ValidatedBatch,
            Capability::ScopePurge,
            Capability::AtomicScopePurge,
            Capability::Watch,
            Capability::BlobTtl,
            Capability::RecordMetadata,
            Capability::Maintenance,
            Capability::ScopeEnumeration,
            Capability::StreamSeal,
            Capability::SharedAcrossProcesses,
            Capability::CrossProcessCas,
        ]
    );
}

#[test]
fn require_names_the_missing_capability() {
    let error = Capabilities::none()
        .require(Capability::Transactions)
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::Transactions)
    );
    assert!(error.message().contains("transactions"));
    assert!(
        Capabilities::all()
            .require(Capability::Transactions)
            .is_ok()
    );
}

#[test]
fn renders_names() {
    for (capability, expected) in [
        (Capability::FullText, "full_text"),
        (Capability::Transactions, "transactions"),
        (Capability::Ttl, "ttl"),
        (Capability::Fencing, "fencing"),
        (Capability::PresignedUrls, "presigned_urls"),
        (Capability::StreamingBlobs, "streaming_blobs"),
        (Capability::CollectionEnumeration, "collection_enumeration"),
        (
            Capability::ConditionalStreamAppend,
            "conditional_stream_append",
        ),
        (Capability::ValidatedBatch, "validated_batch"),
        (Capability::ExistingSchema, "existing_schema"),
        (Capability::CollectionValidation, "collection_validation"),
        (Capability::ScopePurge, "scope_purge"),
        (Capability::AtomicScopePurge, "atomic_scope_purge"),
        (Capability::Watch, "watch"),
        (Capability::RecordMetadata, "record_metadata"),
        (Capability::Maintenance, "maintenance"),
        (Capability::ScopeEnumeration, "scope_enumeration"),
        (Capability::StreamSeal, "stream_seal"),
        (Capability::SharedAcrossProcesses, "shared_across_processes"),
        (Capability::CrossProcessCas, "cross_process_cas"),
    ] {
        assert_eq!(capability.to_string(), expected);
    }
    assert_eq!(
        format!("{:?}", Capabilities::none().with(Capability::Ttl)),
        "{Ttl}"
    );
}

#[test]
fn each_capability_is_independent_of_every_other_capability() {
    for capability in Capabilities::all().iter() {
        let singleton = Capabilities::none().with(capability);
        assert_eq!(singleton.iter().collect::<Vec<_>>(), vec![capability]);
        assert_eq!(singleton.without(capability), Capabilities::none());
    }
}
