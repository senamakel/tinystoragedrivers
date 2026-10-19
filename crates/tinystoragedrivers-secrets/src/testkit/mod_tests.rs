//! The suite catches a store that breaks the contract.

use async_trait::async_trait;
use tinystoragedrivers_core::{Result, StorageError};
use zeroize::Zeroizing;

use super::*;
use crate::MemorySecrets;

/// A memory store that claims it cannot enumerate, to drive the
/// non-enumerable branch.
#[derive(Debug, Default)]
struct Opaque(MemorySecrets);

#[async_trait]
impl SecretStore for Opaque {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.0.get(name).await
    }
    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        self.0.set(name, value).await
    }
    async fn delete(&self, name: &str) -> Result<bool> {
        self.0.delete(name).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        crate::store::validate_prefix(prefix)?;
        Err(StorageError::backend("cannot enumerate"))
    }
    fn enumerable(&self) -> bool {
        false
    }
    fn backend_name(&self) -> &'static str {
        "opaque"
    }
}

#[tokio::test]
async fn a_non_enumerable_store_passes_when_list_fails_with_backend() {
    secrets_conformance(&Opaque::default()).await;
}

#[tokio::test]
#[should_panic(expected = "a store that cannot enumerate must say so")]
async fn a_non_enumerable_store_with_the_wrong_list_error_fails() {
    #[derive(Debug, Default)]
    struct WrongKind(Opaque);
    #[async_trait]
    impl SecretStore for WrongKind {
        async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
            self.0.get(name).await
        }
        async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
            self.0.set(name, value).await
        }
        async fn delete(&self, name: &str) -> Result<bool> {
            self.0.delete(name).await
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Err(StorageError::invalid_input("wrong kind"))
        }
        fn enumerable(&self) -> bool {
            false
        }
        fn backend_name(&self) -> &'static str {
            "wrong"
        }
    }
    secrets_conformance(&WrongKind::default()).await;
}

/// A store that drops every overwrite, so the suite fails part way.
#[derive(Debug, Default)]
struct ForgetsOverwrites(MemorySecrets);

#[async_trait]
impl SecretStore for ForgetsOverwrites {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.0.get(name).await
    }
    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        if self.0.get(name).await?.is_none() {
            self.0.set(name, value).await?;
        }
        Ok(())
    }
    async fn delete(&self, name: &str) -> Result<bool> {
        self.0.delete(name).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.0.list(prefix).await
    }
    fn backend_name(&self) -> &'static str {
        "forgetful"
    }
}

#[tokio::test]
async fn a_failed_check_still_removes_what_the_suite_wrote() {
    let store = ForgetsOverwrites::default();
    let outcome = CatchUnwind(Box::pin(secrets_conformance(&store))).await;
    assert!(outcome.is_err(), "the overwrite check must fail");
    assert_eq!(store.0.list("").await.unwrap(), Vec::<String>::new());
}

#[test]
fn prefixes_are_unique_per_call() {
    assert_ne!(unique_prefix(), unique_prefix());
}

#[tokio::test]
async fn document_secrets_pass_on_the_memory_backend() {
    use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    document_secrets_conformance(&storage).await;
}
