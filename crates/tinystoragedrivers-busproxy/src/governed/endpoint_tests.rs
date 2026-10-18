use super::*;

#[test]
fn a_newer_async_binding_resolution_fences_a_late_completion() {
    let gate = ResolutionGate::new();
    let slow_old_resolution = gate.begin();
    let fast_new_resolution = gate.begin();

    assert!(!gate.is_latest(slow_old_resolution));
    assert!(gate.is_latest(fast_new_resolution));
    // Finishing the old future later has no operation that can roll the gate
    // back or make its server eligible for publication.
    assert!(!gate.is_latest(slow_old_resolution));
}

#[tokio::test]
async fn a_backend_panic_becomes_a_content_free_failure() {
    let result = guarded(async { panic!("private backend detail") }).await;
    assert_eq!(
        result.unwrap_err(),
        HostFailure::Storage(StorageFailure {
            kind: StorageKind::Backend,
        })
    );
}
