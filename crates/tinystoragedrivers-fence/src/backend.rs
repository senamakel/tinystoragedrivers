//! Helpers for binding a backend-native atomic write fence.

use tinystoragedrivers_core::{Fence, Result, Scope, ScopedStorage, StorageBackend};

/// Bind a fenced view using the backend's native atomic fencing support.
///
/// This delegates to [`StorageBackend::for_scope_fenced`]. A backend that
/// cannot check the guard atomically with protected writes returns its normal
/// unsupported-capability error; this helper never substitutes a racy
/// check-then-write wrapper.
///
/// # Errors
///
/// Returns the backend's native validation, unsupported-fencing, or storage
/// error.
pub fn for_scope_fenced(
    backend: &dyn StorageBackend,
    scope: &Scope,
    fence: &Fence,
) -> Result<ScopedStorage> {
    backend.for_scope_fenced(scope, fence)
}

#[cfg(test)]
#[path = "backend_tests.rs"]
mod tests;
