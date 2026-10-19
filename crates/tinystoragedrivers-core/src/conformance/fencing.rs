//! Fencing checks: handles from
//! [`StorageBackend::for_scope_fenced`] write only while their fence holds.

use serde_json::json;

use super::{fails, ok, unique};
use crate::backend::{ScopedStorage, StorageBackend};
use crate::capabilities::Capability;
use crate::document::{CollectionSpec, Precondition, Query, WriteOp};
use crate::error::{ErrorKind, Result};
use crate::fence::Fence;
use crate::filter::Filter;
use crate::primitives::{self, Lease, Leases, RetryPolicy};
use crate::scope::Scope;

/// The names one run writes under.
struct Names {
    coll: String,
    stream: String,
    blob: String,
}

/// Assert a blob write was fenced, or refused as unfenceable by a driver
/// whose blobs cannot join the fence's atomic step.
fn blob_refused<T: std::fmt::Debug>(result: Result<T>, check: &str) {
    match result {
        Ok(value) => panic!("{check}: expected fenced, got Ok({value:?})"),
        Err(error) => assert!(
            matches!(
                error.kind(),
                ErrorKind::Fenced | ErrorKind::Unsupported(Capability::Fencing)
            ),
            "{check}: {error}"
        ),
    }
}

/// Whether a blob write landed; `false` when the driver refuses fenced blob
/// writes as unfenceable.
fn blob_landed<T>(result: Result<T>, check: &str) -> bool {
    match result {
        Ok(_) => true,
        Err(error) if error.kind() == ErrorKind::Unsupported(Capability::Fencing) => false,
        Err(error) => panic!("{check}: unexpected error {error}"),
    }
}

/// Run the fencing checks. The guard lives in `conformance-fence`, or in
/// [`Scope::local`] beside the fenced data on a single-scope backend.
pub(super) async fn run(backend: &dyn StorageBackend, single_scope: bool) {
    let guard_scope = if single_scope {
        Scope::local()
    } else {
        Scope::new("conformance-fence").expect("valid scope")
    };
    let leases = unique("fence_leases");
    let fence = Fence::epoch(guard_scope.clone(), &leases, "lease", "epoch", 1);
    if !backend.capabilities().contains(Capability::Fencing) {
        fails(
            backend.for_scope_fenced(&Scope::local(), &fence),
            ErrorKind::Unsupported(Capability::Fencing),
            "fencing without the capability",
        );
        return;
    }
    fails(
        backend.for_scope_fenced(
            &Scope::local(),
            &Fence::new(Scope::local(), "_tsd_guard", "lease", Filter::All),
        ),
        ErrorKind::InvalidInput,
        "a fence on a reserved collection",
    );

    let guards = ok(backend.for_scope(&guard_scope), "guard scope");
    let plain = ok(backend.for_scope(&Scope::local()), "plain scope");
    let fenced = ok(
        backend.for_scope_fenced(&Scope::local(), &fence),
        "bind a fenced scope",
    );
    assert_eq!(fenced.scope(), &Scope::local(), "fenced scope");
    let names = Names {
        coll: unique("fenced"),
        stream: unique("fenced_stream"),
        blob: format!("{}/b", unique("fenced_blob")),
    };

    // No guard document yet: nothing lands.
    refused(&fenced, &plain, &names, "absent guard").await;

    ok(
        guards
            .documents()
            .put(&leases, "lease", json!({"epoch": 1}), Precondition::Absent)
            .await,
        "grant epoch 1",
    );
    holding(&fenced, &plain, &names).await;

    // A takeover moves the guard on: the handle bound at epoch 1 is cut off.
    ok(
        guards
            .documents()
            .put(&leases, "lease", json!({"epoch": 2}), Precondition::None)
            .await,
        "take over at epoch 2",
    );
    refused(&fenced, &plain, &names, "moved guard").await;

    // The new holder's fence writes again.
    let current = ok(
        backend.for_scope_fenced(
            &Scope::local(),
            &Fence::epoch(guard_scope.clone(), &leases, "lease", "epoch", 2),
        ),
        "bind at epoch 2",
    );
    ok(
        current
            .documents()
            .put(&names.coll, "d2", json!({"n": 2}), Precondition::None)
            .await,
        "the current holder writes",
    );

    if backend.capabilities().contains(Capability::Ttl) {
        expired_guard(backend, &guard_scope, &guards).await;
    }
    holder(backend, single_scope).await;
}

/// Every fenced write is refused and changes nothing; reads and declarations
/// still work.
async fn refused(fenced: &ScopedStorage, plain: &ScopedStorage, names: &Names, check: &str) {
    let before = ok(
        plain.documents().query(&names.coll, &Query::all()).await,
        check,
    )
    .items;
    let stream_len = ok(plain.streams().len(&names.stream).await, check);
    let blob = ok(plain.blobs().get(&names.blob).await, check);

    refused_documents(fenced, names, check).await;
    refused_streams_and_blobs(fenced, names, check).await;

    let after = ok(
        plain.documents().query(&names.coll, &Query::all()).await,
        check,
    )
    .items;
    assert_eq!(after, before, "{check}: a refused write changed documents");
    assert_eq!(
        ok(plain.streams().len(&names.stream).await, check),
        stream_len,
        "{check}: a refused write changed the stream"
    );
    assert_eq!(
        ok(plain.blobs().get(&names.blob).await, check),
        blob,
        "{check}: a refused write changed the blob"
    );
}

/// Every fenced document write is refused; reads and declarations work.
async fn refused_documents(fenced: &ScopedStorage, names: &Names, check: &str) {
    let docs = fenced.documents();
    let fenced_kind = |what: &str| format!("{check}: {what}");
    fails(
        docs.put(&names.coll, "d1", json!({"n": 9}), Precondition::None)
            .await,
        ErrorKind::Fenced,
        &fenced_kind("put"),
    );
    fails(
        docs.put(&names.coll, "seed", json!({"n": 9}), Precondition::Absent)
            .await,
        ErrorKind::Fenced,
        &fenced_kind("the fence is checked before the precondition"),
    );
    fails(
        docs.delete(&names.coll, "seed", Precondition::None).await,
        ErrorKind::Fenced,
        &fenced_kind("delete"),
    );
    fails(
        docs.delete_where(&names.coll, &Filter::All).await,
        ErrorKind::Fenced,
        &fenced_kind("delete_where"),
    );
    fails(
        docs.claim(&names.coll, &Filter::All, &[], &json!({"claimed": true}))
            .await,
        ErrorKind::Fenced,
        &fenced_kind("claim"),
    );
    fails(
        docs.drop_collection(&names.coll).await,
        ErrorKind::Fenced,
        &fenced_kind("drop_collection"),
    );
    if docs.capabilities().contains(Capability::ValidatedBatch) {
        fails(
            docs.atomic_batch_checked(vec![], vec![]).await,
            ErrorKind::Fenced,
            &fenced_kind("empty checked batch"),
        );
    }
    let batch = docs
        .atomic_batch(vec![WriteOp::Put {
            collection: names.coll.clone(),
            id: "b1".to_owned(),
            doc: json!({}),
            precondition: Precondition::None,
        }])
        .await;
    if docs.capabilities().contains(Capability::Transactions) {
        fails(batch, ErrorKind::Fenced, &fenced_kind("atomic_batch"));
    } else {
        fails(
            batch,
            ErrorKind::Unsupported(Capability::Transactions),
            &fenced_kind("atomic_batch without transactions"),
        );
    }
    ok(
        docs.ensure_collection(&CollectionSpec::new(names.coll.clone()))
            .await,
        &fenced_kind("ensure_collection"),
    );
    ok(docs.get(&names.coll, "seed").await, &fenced_kind("get"));
}

/// Every fenced stream and blob write is refused; reads work.
async fn refused_streams_and_blobs(fenced: &ScopedStorage, names: &Names, check: &str) {
    let fenced_kind = |what: &str| format!("{check}: {what}");
    let streams = fenced.streams();
    fails(
        streams.append(&names.stream, json!(9)).await,
        ErrorKind::Fenced,
        &fenced_kind("append"),
    );
    fails(
        streams
            .append_batch(&names.stream, vec![json!(9), json!(10)])
            .await,
        ErrorKind::Fenced,
        &fenced_kind("append_batch"),
    );
    if super::has(fenced, Capability::ConditionalStreamAppend) {
        let next = ok(streams.len(&names.stream).await, "fenced stream length");
        for values in [vec![], vec![json!(11)]] {
            fails(
                streams
                    .append_batch_if_offset(&names.stream, next, values)
                    .await,
                ErrorKind::Fenced,
                &fenced_kind("conditional append"),
            );
        }
    }
    fails(
        streams.truncate_before(&names.stream, u64::MAX).await,
        ErrorKind::Fenced,
        &fenced_kind("truncate_before"),
    );
    fails(
        streams.delete_stream(&names.stream).await,
        ErrorKind::Fenced,
        &fenced_kind("delete_stream"),
    );
    if super::has(fenced, Capability::StreamSeal) {
        fails(
            streams.seal(&names.stream).await,
            ErrorKind::Fenced,
            &fenced_kind("seal"),
        );
    }

    blob_refused(
        fenced.blobs().put(&names.blob, vec![9, 9], None).await,
        &fenced_kind("blob put"),
    );
    blob_refused(
        fenced.blobs().delete(&names.blob).await,
        &fenced_kind("blob delete"),
    );

    ok(
        streams.read_window(&names.stream, 0, 10).await,
        &fenced_kind("read_window"),
    );
    ok(fenced.blobs().head(&names.blob).await, &fenced_kind("head"));
}

/// While the fence holds, every write lands as it would unfenced.
async fn holding(fenced: &ScopedStorage, plain: &ScopedStorage, names: &Names) {
    holding_documents(fenced, plain, names).await;
    holding_streams(fenced, plain, names).await;
    holding_blobs(fenced, plain, names).await;
}

/// Fenced document writes land while the fence holds.
async fn holding_documents(fenced: &ScopedStorage, plain: &ScopedStorage, names: &Names) {
    let docs = fenced.documents();
    if docs.capabilities().contains(Capability::ValidatedBatch) {
        ok(
            docs.atomic_batch_checked(vec![], vec![]).await,
            "holding checked batch",
        );
    }

    let v1 = ok(
        docs.put(&names.coll, "seed", json!({"n": 1}), Precondition::Absent)
            .await,
        "holding: put",
    );
    fails(
        docs.put(&names.coll, "seed", json!({"n": 1}), Precondition::Absent)
            .await,
        ErrorKind::Conflict,
        "holding: the precondition still applies",
    );
    ok(
        docs.put(&names.coll, "gone", json!({"n": 2}), Precondition::None)
            .await,
        "holding: put another",
    );
    assert!(
        ok(
            docs.delete(&names.coll, "gone", Precondition::None).await,
            "holding: delete"
        ),
        "holding: delete removed the document"
    );
    ok(
        docs.put(&names.coll, "doomed", json!({"n": 3}), Precondition::None)
            .await,
        "holding: put a doomed one",
    );
    assert_eq!(
        ok(
            docs.delete_where(&names.coll, &Filter::eq("n", 3)).await,
            "holding: delete_where"
        ),
        1
    );
    let claimed = ok(
        docs.claim(
            &names.coll,
            &Filter::eq("n", 1),
            &[],
            &json!({"claimed": true}),
        )
        .await,
        "holding: claim",
    )
    .expect("holding: claim found the seed");
    assert!(claimed.version > v1, "holding: claim wrote a new version");
    if docs.capabilities().contains(Capability::Transactions) {
        ok(
            docs.atomic_batch(vec![WriteOp::Put {
                collection: names.coll.clone(),
                id: "batched".to_owned(),
                doc: json!({"n": 4}),
                precondition: Precondition::Absent,
            }])
            .await,
            "holding: atomic_batch",
        );
    }
    let scratch = unique("fenced_scratch");
    ok(
        docs.put(&scratch, "x", json!({}), Precondition::None).await,
        "holding: put scratch",
    );
    ok(docs.drop_collection(&scratch).await, "holding: drop");
    assert_eq!(
        ok(
            plain.documents().count(&scratch, &Filter::All).await,
            "holding: scratch count"
        ),
        0,
        "holding: drop_collection landed"
    );
    let seed = ok(
        plain.documents().get(&names.coll, "seed").await,
        "holding: read back",
    )
    .expect("holding: the fenced write landed");
    assert_eq!(seed.doc["claimed"], true);
}

/// Fenced stream writes land while the fence holds.
async fn holding_streams(fenced: &ScopedStorage, plain: &ScopedStorage, names: &Names) {
    let streams = fenced.streams();
    let scratch_stream = unique("fenced_scratch_stream");
    ok(
        streams.append(&scratch_stream, json!(1)).await,
        "holding: scratch append",
    );
    if super::has(fenced, Capability::StreamSeal) {
        ok(streams.seal(&scratch_stream).await, "holding: seal");
    }
    assert!(
        ok(
            streams.delete_stream(&scratch_stream).await,
            "holding: delete_stream"
        ),
        "holding: delete_stream removed the stream"
    );
    assert_eq!(
        ok(
            streams.append(&names.stream, json!(1)).await,
            "holding: append"
        ),
        0
    );
    assert_eq!(
        ok(
            streams
                .append_batch(&names.stream, vec![json!(2), json!(3)])
                .await,
            "holding: append_batch"
        ),
        1
    );
    assert_eq!(
        ok(
            streams.truncate_before(&names.stream, 1).await,
            "holding: truncate"
        ),
        1
    );
    assert_eq!(
        ok(plain.streams().len(&names.stream).await, "holding: len"),
        3
    );
}

/// Fenced blob writes land while the fence holds, unless the driver refuses
/// them as unfenceable.
async fn holding_blobs(fenced: &ScopedStorage, plain: &ScopedStorage, names: &Names) {
    let blobs = fenced.blobs();
    let scratch_blob = format!("{}/b", unique("fenced_scratch_blob"));
    if blob_landed(
        blobs.put(&scratch_blob, vec![1], None).await,
        "holding: blob put",
    ) {
        assert!(
            blob_landed(blobs.delete(&scratch_blob).await, "holding: blob delete"),
            "a driver that fences blob puts fences blob deletes"
        );
        assert!(
            ok(plain.blobs().head(&scratch_blob).await, "holding: head").is_none(),
            "holding: blob delete landed"
        );
        ok(
            blobs.put(&names.blob, vec![1, 2], None).await,
            "holding: blob put",
        );
    }
}

/// A guard document past its collection's expiry fences like an absent one.
async fn expired_guard(backend: &dyn StorageBackend, guard_scope: &Scope, guards: &ScopedStorage) {
    let leases = unique("fence_ttl");
    ok(
        guards
            .documents()
            .ensure_collection(&CollectionSpec::new(leases.clone()).ttl("expires_at"))
            .await,
        "declare an expiring guard collection",
    );
    ok(
        guards
            .documents()
            .put(
                &leases,
                "lease",
                json!({"epoch": 1, "expires_at": 1}),
                Precondition::None,
            )
            .await,
        "write an expired guard",
    );
    let fenced = ok(
        backend.for_scope_fenced(
            &Scope::local(),
            &Fence::epoch(guard_scope.clone(), &leases, "lease", "epoch", 1),
        ),
        "bind to an expired guard",
    );
    fails(
        fenced
            .documents()
            .put(&unique("fenced_ttl"), "x", json!({}), Precondition::None)
            .await,
        ErrorKind::Fenced,
        "an expired guard",
    );
}

/// What the holder-fence checks share: the leases, and the data they fence.
struct HolderRig<'a> {
    backend: &'a dyn StorageBackend,
    guard_scope: Scope,
    guards: ScopedStorage,
    leases: Leases,
    owner: String,
    coll: String,
    stream: String,
}

/// The outcome of one fenced document put and stream append.
type Writes = (Result<crate::Version>, Result<u64>);

impl HolderRig<'_> {
    /// A handle fenced by `lease`'s holder fence.
    fn bind(&self, lease: &Lease) -> ScopedStorage {
        ok(
            self.backend
                .for_scope_fenced(&Scope::local(), &lease.holder_fence(&self.guard_scope)),
            "bind a holder fence",
        )
    }

    /// A document put and a stream append through `fenced`.
    async fn writes(&self, fenced: &ScopedStorage, id: &str) -> Writes {
        let put = fenced
            .documents()
            .put(&self.coll, id, json!({"id": id}), Precondition::None)
            .await;
        let append = fenced.streams().append(&self.stream, json!(id)).await;
        (put, append)
    }

    /// Acquire `key` for `holder`, which must be free.
    async fn acquire(&self, key: &str, holder: &str) -> Lease {
        ok(
            self.leases.acquire(&self.owner, key, holder, 60_000).await,
            "holder acquire",
        )
        .expect("a free lease is acquirable")
    }

    /// Release `key` held by `holder` at `lease`'s epoch.
    async fn release(&self, key: &str, holder: &str, lease: &Lease) {
        assert!(
            ok(
                self.leases
                    .release(&self.owner, key, holder, lease.epoch)
                    .await,
                "holder release"
            ),
            "the holder releases its own tenure"
        );
    }

    /// Overwrite the lease record `lease` fences on, as another actor would.
    async fn rewrite_record(&self, lease: &Lease, holder: &str) {
        let record = lease.holder_fence(&self.guard_scope);
        ok(
            self.guards
                .documents()
                .put(
                    record.collection(),
                    record.id(),
                    json!({"holder": holder, "epoch": lease.epoch, "expires_ms": i64::MAX}),
                    Precondition::None,
                )
                .await,
            "rewrite the lease record",
        );
    }
}

/// Both writes of `outcome` were refused as fenced.
fn writes_refused(outcome: Writes, check: &str) {
    fails(outcome.0, ErrorKind::Fenced, &format!("{check}: put"));
    fails(outcome.1, ErrorKind::Fenced, &format!("{check}: append"));
}

/// Both writes of `outcome` landed.
fn writes_landed(outcome: Writes, check: &str) {
    ok(outcome.0, &format!("{check}: put"));
    ok(outcome.1, &format!("{check}: append"));
}

/// The compound fence of [`Lease::holder_fence`]: `epoch == e AND holder == h
/// AND released absent`. A write fenced by a tenure that was taken over,
/// released or compacted away is refused, which the epoch-only fence cannot
/// say for a release.
async fn holder(backend: &dyn StorageBackend, single_scope: bool) {
    if !backend.capabilities().contains(Capability::Fencing) {
        return;
    }
    let guard_scope = if single_scope {
        Scope::local()
    } else {
        Scope::new("conformance-fence").expect("valid scope")
    };
    let guards = ok(backend.for_scope(&guard_scope), "holder guard scope");
    ok(primitives::ensure(&guards).await, "holder primitives");
    let rig = HolderRig {
        backend,
        guard_scope,
        leases: Leases::new(guards.documents()).with_retry(RetryPolicy::default()),
        guards,
        owner: unique("holder_fence"),
        coll: unique("holder_fenced"),
        stream: unique("holder_fenced_stream"),
    };
    holder_takeover(&rig).await;
    holder_release(&rig).await;
    holder_compaction(&rig).await;
    let plain = ok(backend.for_scope(&Scope::local()), "holder plain scope");
    let landed = ok(
        plain.documents().count(&rig.coll, &Filter::All).await,
        "holder count",
    );
    assert_eq!(landed, 5, "only the permitted holder writes landed");
}

/// Another holder under the same epoch, then a takeover, cut the old holder off.
async fn holder_takeover(rig: &HolderRig<'_>) {
    let a = rig.acquire("takeover", "a").await;
    let fenced_a = rig.bind(&a);
    writes_landed(rig.writes(&fenced_a, "a1").await, "the holder writes");

    // A record carrying the same epoch under another holder does not match.
    rig.rewrite_record(&a, "z").await;
    writes_refused(
        rig.writes(&fenced_a, "a2").await,
        "another holder at the same epoch",
    );
    rig.rewrite_record(&a, "a").await;
    writes_landed(
        rig.writes(&fenced_a, "a3").await,
        "the restored holder writes",
    );

    rig.release("takeover", "a", &a).await;
    writes_refused(rig.writes(&fenced_a, "a4").await, "a released tenure");
    let b = ok(
        rig.leases
            .acquire(&rig.owner, "takeover", "b", 60_000)
            .await,
        "acquire b",
    )
    .expect("a released lease is acquirable");
    assert!(b.epoch > a.epoch, "a takeover raises the epoch");
    writes_refused(rig.writes(&rig.bind(&a), "a5").await, "a taken-over tenure");
    writes_landed(
        rig.writes(&rig.bind(&b), "b1").await,
        "the new holder writes",
    );
}

/// A release alone, with no successor: the epoch is unchanged, so the
/// epoch-only fence would still hold, and the holder fence does not.
async fn holder_release(rig: &HolderRig<'_>) {
    let c = rig.acquire("release", "c").await;
    let fenced_c = rig.bind(&c);
    let epoch_only = ok(
        rig.backend
            .for_scope_fenced(&Scope::local(), &c.fence(&rig.guard_scope)),
        "bind an epoch fence",
    );
    rig.release("release", "c", &c).await;
    writes_refused(
        rig.writes(&fenced_c, "c1").await,
        "a released tenure with no successor",
    );
    ok(
        rig.writes(&epoch_only, "c2").await.0,
        "the epoch-only fence cannot see a release",
    );
}

/// Compaction removes the record: the fence has nothing to match.
async fn holder_compaction(rig: &HolderRig<'_>) {
    let d = rig.acquire("compact", "d").await;
    let fenced_d = rig.bind(&d);
    ok(
        rig.writes(&fenced_d, "d1").await.0,
        "the holder writes before compaction",
    );
    assert!(
        ok(
            rig.leases
                .compact(&rig.owner, "compact", "d", d.epoch)
                .await,
            "compact d"
        ),
        "compaction removes the holder's own tenure"
    );
    writes_refused(rig.writes(&fenced_d, "d2").await, "a compacted tenure");
}
