use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn opaque_context_and_drop_guard_debug_redact_values_and_run_cleanup_once() {
    let context = HostStorageContext::new("private-profile-marker");
    let rendered_context = format!("{context:?}");
    assert_eq!(rendered_context, "HostStorageContext(..)");
    assert!(!rendered_context.contains("private-profile-marker"));

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_cleanup = Arc::clone(&calls);
    let guard = Arc::new(LeaseDropGuard::new(move || {
        calls_for_cleanup.fetch_add(1, Ordering::AcqRel);
    }));
    let rendered_guard = format!("{guard:?}");
    assert_eq!(rendered_guard, "LeaseDropGuard(..)");
    assert!(!rendered_guard.contains("private-profile-marker"));

    let retained = Arc::clone(&guard);
    drop(guard);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    drop(retained);
    assert_eq!(calls.load(Ordering::Acquire), 1);
}
