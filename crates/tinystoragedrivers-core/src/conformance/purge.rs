//! Optional complete native scope purge, generation admission and fencing.
use super::{fails, ok, unique};
use crate::{
    Capability, CollectionSpec, ErrorKind, Fence, Precondition, PurgeMode, PurgeRequest,
    PurgeStatus, Scope, ScopeGeneration, ScopedStorage, StorageBackend,
};
use serde_json::json;

pub(super) async fn run(backend: &dyn StorageBackend) {
    let owner = Scope::new(unique("purge_owner")).expect("valid scope");
    let ports = ok(backend.for_scope(&owner), "purge handle");
    if backend.capabilities().contains(Capability::ScopePurge) {
        sequential_generations(backend).await;
    }
    if !backend
        .capabilities()
        .contains(Capability::AtomicScopePurge)
    {
        fails(
            ports.purge(&request()).await,
            ErrorKind::Unsupported(if backend.capabilities().contains(Capability::ScopePurge) {
                Capability::AtomicScopePurge
            } else {
                Capability::ScopePurge
            }),
            "unsupported atomic purge",
        );
        return;
    }
    let guard_scope = Scope::new(unique("purge_guard")).expect("valid scope");
    let guard = ok(backend.for_scope(&guard_scope), "purge guard scope");
    let leases = unique("purge_leases");
    ok(
        guard
            .documents()
            .put(&leases, "lease", json!({"epoch":1}), Precondition::None)
            .await,
        "seed purge guard",
    );
    let fence = Fence::epoch(guard_scope.clone(), &leases, "lease", "epoch", 1);
    let target = ok(
        backend.for_scope_fenced(&owner, &fence),
        "native purge handle",
    );
    let keep = ok(backend.for_scope(&guard_scope), "other scoped data");
    seed(&target).await;
    seed(&keep).await;
    let request = request();
    let receipt = ok(target.purge(&request).await, "complete native purge");
    assert_eq!(receipt.status, PurgeStatus::Complete);
    assert_eq!(receipt.generation, ScopeGeneration(1));
    assert_eq!(
        ok(
            target.purge_status(&request.operation_id).await,
            "durable native receipt"
        ),
        Some(receipt.clone())
    );
    assert!(
        ok(
            target.purge_status("missing_reset").await,
            "unknown native receipt"
        )
        .is_none()
    );
    let verified = ok(
        backend.for_scope_generation(&owner, receipt.generation),
        "verify issued purge generation",
    );
    empty(&verified).await;
    assert!(
        ok(
            keep.documents().get("records", "one").await,
            "other scoped documents"
        )
        .is_some()
    );
    assert_eq!(
        ok(keep.streams().len("events").await, "other scoped stream"),
        1
    );
    assert!(ok(keep.blobs().get("blob").await, "other scoped blob").is_some());
    stale(&target).await;
    assert_eq!(
        ok(target.purge(&request).await, "idempotent purge retry"),
        receipt
    );
    resumption(backend, &owner, &receipt, &fence, &guard, &leases, &request).await;
}

async fn sequential_generations(backend: &dyn StorageBackend) {
    let scope = Scope::new(unique("purge_generation")).expect("valid scope");
    let initial = ok(backend.for_scope(&scope), "generation-zero purge handle");
    let first_request = PurgeRequest {
        operation_id: unique("generation_one"),
        mode: PurgeMode::Quiescent,
    };
    let first = ok(initial.purge(&first_request).await, "purge generation zero");
    assert_eq!(first.generation, ScopeGeneration(1));

    let generation_one = ok(
        backend.for_scope_generation(&scope, first.generation),
        "generation-one purge handle",
    );
    let second_request = PurgeRequest {
        operation_id: unique("generation_two"),
        mode: PurgeMode::Quiescent,
    };
    let second = ok(
        generation_one.purge(&second_request).await,
        "purge generation one",
    );
    assert_eq!(second.generation, ScopeGeneration(2));
    assert_eq!(
        ok(
            initial.purge(&first_request).await,
            "idempotent generation-zero retry"
        ),
        first
    );
    assert_eq!(
        ok(
            generation_one.purge(&second_request).await,
            "idempotent generation-one retry"
        ),
        second
    );
}

async fn resumption(
    backend: &dyn StorageBackend,
    owner: &Scope,
    receipt: &crate::PurgeReceipt,
    fence: &Fence,
    guard: &ScopedStorage,
    leases: &str,
    request: &PurgeRequest,
) {
    let resumed = ok(
        backend.for_scope_fenced_generation(owner, receipt.generation, fence),
        "resume with generation",
    );
    // Deleting tombstones permits a new lifetime starting at version/offset 1/0.
    assert_eq!(
        ok(
            resumed
                .documents()
                .put("records", "removed", json!({}), Precondition::Absent)
                .await,
            "tombstone erased"
        ),
        crate::Version::FIRST
    );
    assert_eq!(
        ok(
            resumed.streams().append("events", json!({})).await,
            "cursor lifetime erased"
        ),
        0
    );
    // Shared declarations are retained and reject an incompatible merge.
    fails(
        resumed
            .documents()
            .ensure_collection(&CollectionSpec::new("records").ttl("different_expiry"))
            .await,
        ErrorKind::InvalidInput,
        "shared declaration retained",
    );
    ok(
        guard
            .documents()
            .put(leases, "lease", json!({"epoch":2}), Precondition::None)
            .await,
        "take over purge guard",
    );
    fails(
        resumed
            .purge(&PurgeRequest {
                operation_id: unique("stale_reset"),
                mode: PurgeMode::Atomic,
            })
            .await,
        ErrorKind::Fenced,
        "stale native purge",
    );
    assert_eq!(
        ok(
            resumed.streams().len("events").await,
            "stale purge preserved data"
        ),
        1
    );
    let self_guard = Fence::epoch(owner.clone(), "records", "removed", "epoch", 1);
    let refused = ok(
        backend.for_scope_fenced_generation(owner, receipt.generation, &self_guard),
        "same-scope purge binding",
    );
    fails(
        refused.purge(request).await,
        ErrorKind::InvalidInput,
        "guard outside erased scope",
    );
}

fn request() -> PurgeRequest {
    PurgeRequest {
        operation_id: unique("native_reset"),
        mode: PurgeMode::Atomic,
    }
}

async fn seed(ports: &ScopedStorage) {
    ok(
        ports
            .documents()
            .ensure_collection(&CollectionSpec::new("records").ttl("expires"))
            .await,
        "purge declaration",
    );
    ok(
        ports
            .documents()
            .put("records", "one", json!({}), Precondition::None)
            .await,
        "purge document",
    );
    ok(
        ports
            .documents()
            .put("records", "removed", json!({}), Precondition::None)
            .await,
        "purge tombstone seed",
    );
    ok(
        ports
            .documents()
            .delete("records", "removed", Precondition::None)
            .await,
        "purge tombstone",
    );
    ok(
        ports
            .documents()
            .put(
                "secret_keys",
                "data-key-v1",
                json!({"wrapped":"opaque"}),
                Precondition::None,
            )
            .await,
        "purge envelope",
    );
    ok(
        ports
            .documents()
            .put("imports", "receipt", json!({}), Precondition::None)
            .await,
        "purge import receipt",
    );
    ok(
        ports.streams().append("events", json!({})).await,
        "purge stream",
    );
    ok(ports.blobs().put("blob", vec![1], None).await, "purge blob");
}

async fn empty(ports: &ScopedStorage) {
    for (collection, id) in [
        ("records", "one"),
        ("secret_keys", "data-key-v1"),
        ("imports", "receipt"),
    ] {
        assert!(
            ok(
                ports.documents().get(collection, id).await,
                "purged document absent"
            )
            .is_none()
        );
    }
    assert_eq!(
        ok(ports.streams().len("events").await, "purged stream absent"),
        0
    );
    assert!(ok(ports.blobs().get("blob").await, "purged blob absent").is_none());
}

async fn stale(ports: &ScopedStorage) {
    stale_reads(ports).await;
    let docs = ports.documents();
    fails(
        docs.put("records", "late", json!({}), Precondition::None)
            .await,
        ErrorKind::ScopeGenerationRejected,
        "late document writer",
    );
    fails(
        docs.delete("records", "missing", Precondition::None).await,
        ErrorKind::ScopeGenerationRejected,
        "late delete",
    );
    fails(
        docs.delete_where("records", &crate::Filter::All).await,
        ErrorKind::ScopeGenerationRejected,
        "late delete filter",
    );
    fails(
        docs.claim("records", &crate::Filter::All, &[], &json!({}))
            .await,
        ErrorKind::ScopeGenerationRejected,
        "late claim",
    );
    fails(
        docs.atomic_batch(vec![]).await,
        ErrorKind::ScopeGenerationRejected,
        "late empty batch",
    );
    fails(
        docs.atomic_batch_checked(vec![], vec![]).await,
        ErrorKind::ScopeGenerationRejected,
        "late checked batch",
    );
    fails(
        docs.drop_collection("records").await,
        ErrorKind::ScopeGenerationRejected,
        "late collection drop",
    );
    fails(
        docs.ensure_collection(&CollectionSpec::new("late_declaration"))
            .await,
        ErrorKind::ScopeGenerationRejected,
        "late declaration",
    );
    fails(
        ports.streams().append("events", json!({})).await,
        ErrorKind::ScopeGenerationRejected,
        "late stream writer",
    );
    fails(
        ports.streams().append_batch("events", vec![]).await,
        ErrorKind::ScopeGenerationRejected,
        "late empty append",
    );
    fails(
        ports.streams().truncate_before("events", 0).await,
        ErrorKind::ScopeGenerationRejected,
        "late truncate",
    );
    fails(
        ports.streams().delete_stream("events").await,
        ErrorKind::ScopeGenerationRejected,
        "late stream delete",
    );
    fails(
        ports.blobs().put("late", vec![], None).await,
        ErrorKind::ScopeGenerationRejected,
        "late blob writer",
    );
    fails(
        ports.blobs().delete("missing").await,
        ErrorKind::ScopeGenerationRejected,
        "late blob delete",
    );
}

async fn stale_reads(ports: &ScopedStorage) {
    let docs = ports.documents();
    fails(
        docs.get("records", "one").await,
        ErrorKind::ScopeGenerationRejected,
        "retired document read",
    );
    fails(
        docs.query("records", &crate::Query::default()).await,
        ErrorKind::ScopeGenerationRejected,
        "retired document query",
    );
    fails(
        docs.count("records", &crate::Filter::All).await,
        ErrorKind::ScopeGenerationRejected,
        "retired document count",
    );
    fails(
        docs.search("records", "one", 0).await,
        ErrorKind::ScopeGenerationRejected,
        "retired empty search",
    );
    fails(
        docs.collections("").await,
        ErrorKind::ScopeGenerationRejected,
        "retired collection enumeration",
    );
    fails(
        ports.streams().len("events").await,
        ErrorKind::ScopeGenerationRejected,
        "retired stream length",
    );
    fails(
        ports.streams().read_window("events", 0, 0).await,
        ErrorKind::ScopeGenerationRejected,
        "retired empty stream window",
    );
    fails(
        ports.streams().streams("").await,
        ErrorKind::ScopeGenerationRejected,
        "retired stream enumeration",
    );
    fails(
        ports.blobs().get("blob").await,
        ErrorKind::ScopeGenerationRejected,
        "retired blob read",
    );
    fails(
        ports.blobs().get_range("blob", 0..0).await,
        ErrorKind::ScopeGenerationRejected,
        "retired blob range",
    );
    fails(
        ports.blobs().head("blob").await,
        ErrorKind::ScopeGenerationRejected,
        "retired blob metadata",
    );
    fails(
        ports.blobs().list("").await,
        ErrorKind::ScopeGenerationRejected,
        "retired blob enumeration",
    );
}
