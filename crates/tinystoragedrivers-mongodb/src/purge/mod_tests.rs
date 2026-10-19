//! Native scope purge authority and replay behavior.
use super::*;
use crate::MongoStorage;
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, ErrorKind, Precondition, StorageBackend};

async fn live(label: &str) -> Option<(MongoStorage, Scope)> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return None;
    };
    let rest = url.split_once("://").map_or(url.as_str(), |(_, rest)| rest);
    let path = rest.split_once('/').map_or("", |(_, path)| path);
    let database = path
        .split(['?', '/'])
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("tsd_test");
    let scope = Scope::new(format!(
        "purge_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .unwrap();
    Some((MongoStorage::connect(&url, database).await.unwrap(), scope))
}

fn port(backend: &MongoStorage, scope: &Scope) -> MongoPurge {
    MongoPurge {
        shared: Arc::clone(&backend.shared),
        scope: scope.clone(),
        generation: ScopeGeneration::default(),
        fenced: false,
        fail_after_chunks: false,
    }
}

#[test]
fn admission_and_receipts_fail_closed_on_malformed_authority() {
    assert!(verify(None, ScopeGeneration::default()).is_ok());
    assert_eq!(
        verify(
            Some(&doc! {"generation": 0_i64, "pending": "false"}),
            ScopeGeneration::default()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Serialization
    );

    assert_eq!(
        verify(None, ScopeGeneration(1)).unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        verify(
            Some(&doc! {"generation": -1_i64}),
            ScopeGeneration::default()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        verify(
            Some(
                &doc! {"generation": 0_i64, "pending": true, "operation": "r", "stage": "records"}
            ),
            ScopeGeneration::default()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    for record in [
        doc! {},
        doc! {"generation": -1_i64},
        doc! {"generation": 1_i64, "pending": false, "operation": "r", "stage": "records"},
        doc! {"generation": 1_i64, "pending": true, "operation": "r", "stage": "unknown"},
    ] {
        assert_eq!(
            decode(&record).unwrap_err().kind(),
            ErrorKind::Serialization
        );
    }
}

#[tokio::test]
async fn missing_reset_authority_fields_never_reopen_retired_or_pending_ports() {
    for missing in ["generation", "pending"] {
        let Some((backend, scope)) = live(missing).await else {
            return;
        };
        let original = backend.for_scope(&scope).unwrap();
        let request = PurgeRequest {
            operation_id: "authority_reset".into(),
            mode: PurgeMode::Quiescent,
        };
        let reset = port(&backend, &scope);
        let receipt = if missing == "generation" {
            reset.purge(&request).await.unwrap()
        } else {
            reset.begin(&request).await.unwrap()
        };
        let current = backend
            .for_scope_generation(&scope, receipt.generation)
            .unwrap();
        if missing == "generation" {
            current
                .documents()
                .put(
                    "records",
                    "replacement",
                    json!({"keep":true}),
                    Precondition::Absent,
                )
                .await
                .unwrap();
            current.streams().append("events", json!(1)).await.unwrap();
            current
                .blobs()
                .put("replacement", vec![7], None)
                .await
                .unwrap();
        }
        let mut unset = Document::new();
        unset.insert(missing, "");
        backend
            .shared
            .raw(ADMISSIONS)
            .update_one(doc! {"_id": scope_key(&scope)}, doc! {"$unset": unset})
            .await
            .unwrap();
        let rejected = if missing == "generation" {
            &original
        } else {
            &current
        };
        let kinds = [
            rejected
                .documents()
                .get("records", "replacement")
                .await
                .err()
                .map(|e| e.kind()),
            rejected
                .streams()
                .len("events")
                .await
                .err()
                .map(|e| e.kind()),
            rejected
                .blobs()
                .get("replacement")
                .await
                .err()
                .map(|e| e.kind()),
        ];
        assert_eq!(
            kinds,
            [Some(ErrorKind::Serialization); 3],
            "missing {missing} must not reopen ports"
        );
    }
}

#[tokio::test]
async fn delayed_overlapping_reset_stages_never_delete_replacement_blobs() {
    let Some((backend, scope)) = live("overlapping_resume").await else {
        return;
    };
    let reset = port(&backend, &scope);
    let request = PurgeRequest {
        operation_id: "overlapping_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    reset.begin(&request).await.unwrap();
    reset.records(&request.operation_id).await.unwrap();
    // A planner has selected this stage but is delayed before it enumerates.
    assert_eq!(
        reset.begin(&request).await.unwrap().stage,
        PurgeStage::BlobManifest
    );
    reset.plan_blobs(&request.operation_id).await.unwrap();
    // Another caller selects erasure and is delayed before reading its manifest.
    assert_eq!(
        reset.begin(&request).await.unwrap().stage,
        PurgeStage::Blobs
    );
    let receipt = reset.purge(&request).await.unwrap();
    let current = backend
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current
        .blobs()
        .put("replacement", vec![9; 600_000], None)
        .await
        .unwrap();
    let planner = reset.plan_blobs(&request.operation_id).await;
    let manifest_count = backend
        .shared
        .raw(MANIFEST)
        .count_documents(doc! {"scope":scope.as_str(),"operation":&request.operation_id})
        .await
        .unwrap();
    let eraser = reset.erase_blobs(&request.operation_id).await;
    let replacement = current.blobs().get("replacement").await.unwrap();
    assert!(planner.is_err() && eraser.is_err());
    assert!(
        manifest_count == 0 && replacement.is_some(),
        "stale stages recorded {manifest_count} replacement entries; replacement survived={}",
        replacement.is_some()
    );
    assert_eq!(replacement.unwrap().bytes, vec![9; 600_000]);
}

#[tokio::test]
async fn pending_reset_blocks_all_generations_and_forced_off_independent_handles() {
    let Some((backend, scope)) = live("pending").await else {
        return;
    };
    let reset = port(&backend, &scope);
    let request = PurgeRequest {
        operation_id: "pending_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = reset.begin(&request).await.unwrap();
    assert_eq!(receipt.stage, PurgeStage::Records);
    let independent = live("independent").await.unwrap().0.without_transactions();
    for source in [&backend, &independent] {
        for generation in [ScopeGeneration(0), receipt.generation] {
            let bound = source.for_scope_generation(&scope, generation).unwrap();
            for error in [
                bound.documents().get("records", "k").await.unwrap_err(),
                bound
                    .documents()
                    .put("records", "k", json!({}), Precondition::None)
                    .await
                    .unwrap_err(),
                bound
                    .documents()
                    .ensure_collection(&CollectionSpec::new("records"))
                    .await
                    .unwrap_err(),
                bound
                    .streams()
                    .read_window("events", 0, 0)
                    .await
                    .unwrap_err(),
                bound
                    .streams()
                    .append_batch("events", vec![])
                    .await
                    .unwrap_err(),
                bound.blobs().get("blob").await.unwrap_err(),
                bound.blobs().put("blob", vec![1], None).await.unwrap_err(),
            ] {
                assert_eq!(error.kind(), ErrorKind::ScopeGenerationRejected);
            }
        }
    }
    assert_eq!(
        backend
            .sweep_expired(&scope, "records")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    let other = PurgeRequest {
        operation_id: "another".into(),
        ..request.clone()
    };
    assert_eq!(
        reset.purge(&other).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    reset.purge(&request).await.unwrap();
    assert_eq!(
        backend
            .for_scope(&scope)
            .unwrap()
            .documents()
            .get("records", "k")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
}

#[test]
fn namespace_ownership_excludes_metadata_and_legacy_prefixed_collections() {
    assert_eq!(scope_key(&Scope::local()), "scope:local");
    assert!(owns_records("records"));
    assert!(owns_records("_tsd_stream_heads"));
    assert!(!owns_records("other:records"));
    for metadata in [
        META,
        ADMISSIONS,
        MANIFEST,
        RECEIPTS,
        "_tsd_blobs.files",
        "_tsd_blobs.chunks",
        "system.views",
    ] {
        assert!(!owns_records(metadata));
    }
}

#[tokio::test]
async fn historical_receipts_backfill_and_survive_an_independent_reconnection() {
    let Some((backend, scope)) = live("history_restart").await else {
        return;
    };
    let name = format!("history_{}", scope.as_str());
    let target = backend.fixture_named(&name).unwrap();
    let first = PurgeRequest {
        operation_id: "first".into(),
        mode: PurgeMode::Quiescent,
    };
    let second = PurgeRequest {
        operation_id: "second".into(),
        mode: PurgeMode::Quiescent,
    };
    let original = target.for_scope(&scope).unwrap();
    let receipt = original.purge(&first).await.unwrap();
    // Simulate a completed reset written before the historical receipt schema.
    target
        .shared
        .raw(RECEIPTS)
        .delete_one(doc! {"_id": {"s": scope.as_str(), "o": &first.operation_id}})
        .await
        .unwrap();
    let next = target
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    let second_receipt = next.purge(&second).await.unwrap();
    let current = target
        .for_scope_generation(&scope, second_receipt.generation)
        .unwrap();
    current
        .documents()
        .put(
            "records",
            "replacement",
            json!({"keep":true}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    current
        .streams()
        .append("events", json!({"keep":true}))
        .await
        .unwrap();
    current
        .blobs()
        .put("replacement", vec![7], None)
        .await
        .unwrap();
    drop((original, next, current, target, backend));

    let reconnected = live("history_reconnect")
        .await
        .unwrap()
        .0
        .fixture_named(&name)
        .unwrap();
    let current = reconnected
        .for_scope_generation(&scope, second_receipt.generation)
        .unwrap();
    assert_eq!(
        current.purge_status(&first.operation_id).await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(current.purge(&first).await.unwrap(), receipt);
    assert!(
        current
            .documents()
            .get("records", "replacement")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(current.streams().len("events").await.unwrap(), 1);
    assert_eq!(
        current
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![7]
    );
}

#[tokio::test]
async fn chunk_stage_failure_keeps_manifest_and_blocks_admission_until_resume() {
    let Some((backend, scope)) = live("partial").await else {
        return;
    };
    let bound = backend.for_scope(&scope).unwrap();
    bound
        .blobs()
        .put("blob", vec![5; 600_000], None)
        .await
        .unwrap();
    let files = backend.shared.raw(&format!("{BLOBS}.files"));
    let file = files
        .find_one(doc! {"metadata._scope": scope.as_str()})
        .await
        .unwrap()
        .unwrap();
    let id = file.get("_id").unwrap();
    let mut reset = port(&backend, &scope);
    reset.fail_after_chunks = true;
    let request = PurgeRequest {
        operation_id: "partial_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let error = reset.purge(&request).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    let receipt = reset.status(&request.operation_id).await.unwrap().unwrap();
    assert_eq!(receipt.status, PurgeStatus::Pending);
    assert_eq!(receipt.stage, PurgeStage::Blobs);
    assert_eq!(files.count_documents(doc! {"_id": id}).await.unwrap(), 1);
    assert_eq!(
        backend
            .shared
            .raw(&format!("{BLOBS}.chunks"))
            .count_documents(doc! {"files_id": id})
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        backend
            .shared
            .raw(MANIFEST)
            .count_documents(doc! {"scope": scope.as_str()})
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        backend
            .for_scope_generation(&scope, receipt.generation)
            .unwrap()
            .blobs()
            .get("blob")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    let independent = live("partial_resume").await.unwrap().0;
    let resumed = port(&independent, &scope).purge(&request).await.unwrap();
    assert_eq!(resumed.status, PurgeStatus::Complete);
    assert_eq!(files.count_documents(doc! {"_id": id}).await.unwrap(), 0);
    assert_eq!(
        backend
            .shared
            .raw(MANIFEST)
            .count_documents(doc! {"scope": scope.as_str()})
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn stale_snapshot_writer_cannot_commit_after_reset_admission_closes() {
    let Some((backend, scope)) = live("snapshot").await else {
        return;
    };
    backend
        .shared
        .prepare_validation(Some(&scope))
        .await
        .unwrap();
    let docs =
        crate::documents::MongoDocuments::new(Arc::clone(&backend.shared), scope.clone(), None);
    let mut session = backend.shared.client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    // The admission precheck already passed in this snapshot before reset.
    check(
        &backend.shared,
        &scope,
        ScopeGeneration::default(),
        Some(&mut session),
    )
    .await
    .unwrap();
    let request = PurgeRequest {
        operation_id: "snapshot_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = port(&backend, &scope).begin(&request).await.unwrap();
    let result = docs.hold_validation(&mut session).await;
    session.abort_transaction().await.unwrap();
    assert!(result.unwrap_err().is_retryable());
    let fresh = backend
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    assert_eq!(
        fresh
            .documents()
            .get("records", "late")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    port(&backend, &scope).purge(&request).await.unwrap();
    assert!(
        fresh
            .documents()
            .get("records", "late")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn unsupported_guarantees_never_close_admission_or_erase_data() {
    let Some((backend, scope)) = live("unsupported").await else {
        return;
    };
    let bound = backend.for_scope(&scope).unwrap();
    bound.streams().append("events", json!(1)).await.unwrap();
    let request = PurgeRequest {
        operation_id: "unsupported_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let guard = Scope::new(format!("guard_{}", scope.as_str())).unwrap();
    backend
        .for_scope(&guard)
        .unwrap()
        .documents()
        .put(
            "reset_guards",
            "lease",
            json!({"epoch": 1}),
            Precondition::None,
        )
        .await
        .unwrap();
    let fence = tinystoragedrivers_core::Fence::epoch(guard, "reset_guards", "lease", "epoch", 1);
    let native = backend.for_scope_fenced(&scope, &fence).unwrap();
    native.validate_purge_generation().await.unwrap();
    assert_eq!(
        native.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    assert_eq!(
        native
            .purge_status(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    let mut reset = port(&backend, &scope);
    assert!(reset.status("missing").await.unwrap().is_none());
    reset.fenced = true;
    assert_eq!(
        reset.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    assert_eq!(
        reset
            .status(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    let disabled = backend.without_transactions();
    let unsupported = port(&disabled, &scope);
    assert!(!unsupported.supports_generation_validation());
    assert_eq!(
        unsupported.validate_generation().await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        unsupported.validate_binding().await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        unsupported.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        unsupported
            .status(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert!(!disabled.capabilities().contains(Capability::ScopePurge));
    assert_eq!(bound.streams().len("events").await.unwrap(), 1);
}

#[tokio::test]
async fn unexpected_late_file_remains_pending_and_replans_after_writer_drain() {
    let Some((backend, scope)) = live("late_blob").await else {
        return;
    };
    let reset = port(&backend, &scope);
    let request = PurgeRequest {
        operation_id: "late_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    reset.begin(&request).await.unwrap();
    reset.records(&request.operation_id).await.unwrap();
    reset.plan_blobs(&request.operation_id).await.unwrap();
    // Simulate an upload whose owner failed to drain before asserting quiescence.
    let files = backend.shared.raw(&format!("{BLOBS}.files"));
    let id = mongodb::bson::oid::ObjectId::new();
    files.insert_one(doc! {"_id": id, "metadata": {"_scope": scope.as_str()},
        "filename": "late", "length": 0_i64, "chunkSize": 255_000, "uploadDate": mongodb::bson::DateTime::now()})
        .await.unwrap();
    assert_eq!(
        reset
            .erase_blobs(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unavailable
    );
    let pending = reset.status(&request.operation_id).await.unwrap().unwrap();
    assert_eq!(pending.status, PurgeStatus::Pending);
    assert_eq!(pending.stage, PurgeStage::BlobManifest);
    assert_eq!(
        reset.purge(&request).await.unwrap().status,
        PurgeStatus::Complete
    );
    assert_eq!(files.count_documents(doc! {"_id": id}).await.unwrap(), 0);
}

async fn stale_operations_with_missing_admission(
    old: &tinystoragedrivers_core::ScopedStorage,
    operation_id: &str,
) -> [Option<tinystoragedrivers_core::ErrorKind>; 5] {
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest};
    [
        old.documents()
            .get("records", "replacement")
            .await
            .err()
            .map(|e| e.kind()),
        old.streams()
            .read_window("records", 0, 10)
            .await
            .err()
            .map(|e| e.kind()),
        old.blobs().get("replacement").await.err().map(|e| e.kind()),
        old.documents()
            .put("records", "stale", json!({"old": true}), Precondition::None)
            .await
            .err()
            .map(|e| e.kind()),
        old.purge(&PurgeRequest {
            operation_id: operation_id.into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .err()
        .map(|e| e.kind()),
    ]
}

async fn assert_replacements_survive_authority_loss(
    current: &tinystoragedrivers_core::ScopedStorage,
) {
    assert!(
        current
            .documents()
            .get("records", "replacement")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        current
            .documents()
            .get("records", "stale")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        current
            .streams()
            .read_window("records", 0, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        current
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![1, 2]
    );
}

#[tokio::test]
async fn missing_admission_with_receipt_history_never_revives_legacy_ports() {
    let Some((backend, scope)) = live("missing_admission").await else {
        return;
    };
    let old = backend.for_scope(&scope).unwrap();
    let receipt = old
        .purge(&PurgeRequest {
            operation_id: "missing_admission_first".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    let current = backend
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current
        .documents()
        .put(
            "records",
            "replacement",
            json!({"new": true}),
            Precondition::None,
        )
        .await
        .unwrap();
    current
        .streams()
        .append("records", json!({"new": true}))
        .await
        .unwrap();
    current
        .blobs()
        .put("replacement", vec![1, 2], None)
        .await
        .unwrap();
    let original_authority = backend
        .shared
        .raw(ADMISSIONS)
        .find_one(doc! {"_id": scope_key(&scope)})
        .await
        .unwrap()
        .unwrap();
    backend
        .shared
        .raw(ADMISSIONS)
        .delete_one(doc! {"_id": scope_key(&scope)})
        .await
        .unwrap();
    for recreated_registration in [false, true] {
        if recreated_registration {
            let mut registration = original_authority.clone();
            for field in ["generation", "pending", "operation", "stage"] {
                registration.remove(field);
            }
            backend
                .shared
                .raw(ADMISSIONS)
                .insert_one(registration)
                .await
                .unwrap();
        }
        let errors =
            stale_operations_with_missing_admission(&old, "missing_admission_second").await;
        assert_eq!(&errors[..3], &[Some(ErrorKind::Serialization); 3]);
        // Native validation already refuses writes/reset when the registration
        // itself is missing. Preserve that native failure instead of remapping it.
        assert!(errors[3..].iter().all(Option::is_some));
    }
    backend
        .shared
        .raw(ADMISSIONS)
        .replace_one(doc! {"_id": scope_key(&scope)}, original_authority)
        .await
        .unwrap();
    assert_replacements_survive_authority_loss(&current).await;
}

#[tokio::test]
async fn first_pending_reset_with_lost_authority_never_reopens_admission() {
    let Some((backend, scope)) = live("pending_missing_admission").await else {
        return;
    };
    let old = backend.for_scope(&scope).unwrap();
    old.documents()
        .put(
            "records",
            "existing",
            json!({"old": true}),
            Precondition::None,
        )
        .await
        .unwrap();
    old.streams()
        .append("records", json!({"old": true}))
        .await
        .unwrap();
    old.blobs().put("existing", vec![1, 2], None).await.unwrap();
    let reset = port(&backend, &scope);
    reset
        .begin(&PurgeRequest {
            operation_id: "first_pending".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    let authority = backend
        .shared
        .raw(ADMISSIONS)
        .find_one(doc! {"_id": scope_key(&scope)})
        .await
        .unwrap()
        .unwrap();
    backend
        .shared
        .raw(ADMISSIONS)
        .delete_one(doc! {"_id": scope_key(&scope)})
        .await
        .unwrap();
    let mut outcomes = Vec::new();
    for recreated_registration in [false, true] {
        if recreated_registration {
            let mut registration = authority.clone();
            for field in ["generation", "pending", "operation", "stage"] {
                registration.remove(field);
            }
            backend
                .shared
                .raw(ADMISSIONS)
                .insert_one(registration)
                .await
                .unwrap();
        }
        outcomes.push(stale_operations_with_missing_admission(&old, "different_pending").await);
    }
    for errors in outcomes {
        assert_eq!(&errors[..3], &[Some(ErrorKind::Serialization); 3]);
        assert!(errors[3..].iter().all(Option::is_some));
    }
}

#[tokio::test]
async fn reset_between_read_admission_and_payload_rejects_replacement_data() {
    let Some((backend, scope)) = live("read_race").await else {
        return;
    };
    let reader = port(&backend, &scope);
    let request = PurgeRequest {
        operation_id: "read_race_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let outcome: Result<Document> = async {
        admitted!(reader, {
            let receipt = reader.purge(&request).await?;
            backend
                .for_scope_generation(&scope, receipt.generation)?
                .documents()
                .put(
                    "records",
                    "new",
                    json!({"replacement": true}),
                    Precondition::None,
                )
                .await?;
            let record = backend
                .shared
                .raw("records")
                .find_one(doc! {"_scope": scope.as_str(), "_key": "new"})
                .await
                .map_err(errors::failed("test payload read"))?
                .unwrap();
            Ok(record)
        })
    }
    .await;
    assert_eq!(
        outcome.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
}

#[tokio::test]
async fn issued_generation_keeps_the_native_guard() {
    use tinystoragedrivers_core::Fence;
    let Some((backend, scope)) = live("guard_generation").await else {
        return;
    };
    let target = backend;
    let guard_scope = Scope::new(format!("guard_{}", scope.as_str())).unwrap();
    let request = PurgeRequest {
        operation_id: "guard_generation_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = target
        .for_scope(&scope)
        .unwrap()
        .purge(&request)
        .await
        .unwrap();
    {
        let index = 0;
        let guard = target.for_scope(&guard_scope).unwrap();
        let id = format!("lease_{index}");
        guard
            .documents()
            .put(
                "generation_guards",
                &id,
                json!({"epoch":1}),
                Precondition::None,
            )
            .await
            .unwrap();
        let fence = Fence::epoch(guard_scope.clone(), "generation_guards", &id, "epoch", 1);
        let fenced = target
            .for_scope_fenced_generation(&scope, receipt.generation, &fence)
            .unwrap();
        fenced
            .documents()
            .put("records", &id, json!({"live":true}), Precondition::None)
            .await
            .unwrap();
        fenced.streams().append(&id, json!(1)).await.unwrap();
        assert_eq!(
            fenced
                .blobs()
                .put("blob", vec![1], None)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported(Capability::Fencing)
        );
        guard
            .documents()
            .put(
                "generation_guards",
                &id,
                json!({"epoch":2}),
                Precondition::None,
            )
            .await
            .unwrap();
        assert_eq!(
            fenced
                .documents()
                .put("records", &id, json!({"late":true}), Precondition::None)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Fenced
        );
        assert_eq!(
            fenced
                .streams()
                .append(&id, json!(2))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Fenced
        );
    }
}

#[test]
fn generation_and_pending_reset_refusals_do_not_claim_native_lease_loss() {
    let retired = doc! {
        "generation": 1_i64, "pending": false, "operation": "reset", "stage": "complete"
    };
    let pending = doc! {
        "generation": 1_i64, "pending": true, "operation": "reset", "stage": "records"
    };
    assert!(verify(Some(&retired), ScopeGeneration(1)).is_ok());
    for (record, expected) in [
        (Some(&retired), ScopeGeneration(0)),
        (Some(&pending), ScopeGeneration(1)),
        (None, ScopeGeneration(1)),
    ] {
        let refusal = verify(record, expected).unwrap_err();
        assert_eq!(refusal.kind(), ErrorKind::ScopeGenerationRejected);
        assert_ne!(
            refusal.kind(),
            ErrorKind::Fenced,
            "generation/reset admission cannot revoke an unrelated native lease"
        );
    }
}
