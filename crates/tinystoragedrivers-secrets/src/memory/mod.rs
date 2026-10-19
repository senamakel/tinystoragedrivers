//! [`MemorySecrets`]: secrets in process memory, for tests.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use tinystoragedrivers_core::{Result, StorageError};
use zeroize::Zeroizing;

use crate::store::{
    Precondition, SecretStore, SecretVersion, validate_name, validate_prefix, version_from_parts,
};

static NEXT_MEMORY_STORE_ID: AtomicU64 = AtomicU64::new(1);

/// Secrets in a process-local map. Nothing survives the process, and values
/// are wiped from memory when replaced, deleted, or dropped with the store.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use tinystoragedrivers_secrets::{MemorySecrets, SecretStore};
///
/// let secrets = MemorySecrets::new();
/// secrets.set("token", b"abc").await?;
/// assert_eq!(secrets.get("token").await?.unwrap().as_slice(), b"abc");
/// assert!(secrets.delete("token").await?);
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// # }).unwrap();
/// ```
pub struct MemorySecrets {
    entries: Mutex<Entries>,
    identity: u64,
}

#[derive(Default)]
struct Entries {
    values: BTreeMap<String, Zeroizing<Vec<u8>>>,
    generations: BTreeMap<String, u64>,
}

impl Default for MemorySecrets {
    fn default() -> Self {
        Self {
            entries: Mutex::new(Entries::default()),
            identity: NEXT_MEMORY_STORE_ID.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl MemorySecrets {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The map. A panic in another holder cannot leave it half-written (every
    /// mutation is one map call), so a poisoned lock is still safe to use.
    fn entries(&self) -> MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn version(&self, name: &str, generation: u64) -> SecretVersion {
        version_from_parts(&[
            b"memory",
            &self.identity.to_be_bytes(),
            name.as_bytes(),
            &generation.to_be_bytes(),
        ])
    }
}

impl fmt::Debug for MemorySecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySecrets")
            .field("len", &self.entries().values.len())
            .finish()
    }
}

#[async_trait]
impl SecretStore for MemorySecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        validate_name(name)?;
        Ok(self.entries().values.get(name).cloned())
    }

    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        validate_name(name)?;
        let entries = self.entries();
        let Some(value) = entries.values.get(name) else {
            return Ok(None);
        };
        let generation = entries
            .generations
            .get(name)
            .copied()
            .ok_or_else(|| StorageError::backend("memory secret revision is missing"))?;
        Ok(Some((value.clone(), self.version(name, generation))))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        let mut entries = self.entries();
        let generation = next_generation(&entries, name)?;
        entries.generations.insert(name.to_string(), generation);
        entries
            .values
            .insert(name.to_string(), Zeroizing::new(value.to_vec()));
        Ok(())
    }

    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: Precondition,
    ) -> Result<SecretVersion> {
        validate_name(name)?;
        let mut entries = self.entries();
        match condition {
            Precondition::Absent if entries.values.contains_key(name) => {
                return Err(StorageError::conflict("secret changed since it was read"));
            }
            Precondition::Version(expected) => {
                let Some(generation) = entries.generations.get(name).copied() else {
                    return Err(StorageError::conflict("secret changed since it was read"));
                };
                if !entries.values.contains_key(name) || self.version(name, generation) != expected
                {
                    return Err(StorageError::conflict("secret changed since it was read"));
                }
            }
            Precondition::Absent => {}
        }
        let generation = next_generation(&entries, name)?;
        entries.generations.insert(name.to_string(), generation);
        entries
            .values
            .insert(name.to_string(), Zeroizing::new(value.to_vec()));
        Ok(self.version(name, generation))
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        let mut entries = self.entries();
        if !entries.values.contains_key(name) {
            return Ok(false);
        }
        let generation = next_generation(&entries, name)?;
        entries.values.remove(name);
        entries.generations.insert(name.to_string(), generation);
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        Ok(self
            .entries()
            .values
            .keys()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn backend_name(&self) -> &'static str {
        "memory"
    }
}

fn next_generation(entries: &Entries, name: &str) -> Result<u64> {
    entries
        .generations
        .get(name)
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| StorageError::backend("memory secret revision space is exhausted"))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
