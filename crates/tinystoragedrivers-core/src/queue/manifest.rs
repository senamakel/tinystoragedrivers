//! [`QueueManifest`]: queues declared in a JSON file instead of in code.
//!
//! The queues a host or module uses, and how each treats its messages, live in
//! one editable file that people and agents change without touching Rust:
//!
//! ```json
//! {
//!   "$schema": "https://raw.githubusercontent.com/tinyhumansai/tinystoragedrivers/main/schemas/queues.schema.json",
//!   "version": 1,
//!   "queues": [
//!     {
//!       "name": "inbound.ingress",
//!       "description": "Raw inbound events, persisted before the platform is acknowledged.",
//!       "owner": "tinyeventqueues",
//!       "max_attempts": 8,
//!       "dedupe_window_ms": 2592000000,
//!       "visibility_ms": 30000
//!     }
//!   ]
//! }
//! ```
//!
//! [`QueueManifest::apply`] declares every queue on a [`QueueStore`], so the
//! file is the single source of truth for what exists. Parsing is strict:
//! unknown fields, duplicate names and invalid values are rejected with an
//! error naming the offending entry, so a typo fails loudly instead of being
//! ignored. `schemas/queues.schema.json` in this repository describes the same
//! format for editors and agents.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::QueueStore;
use super::types::{QueueSpec, validate_queue_name};
use crate::error::{Result, StorageError};

/// The manifest format version this crate reads.
pub const QUEUE_MANIFEST_VERSION: u32 = 1;

/// A file of queue declarations. See the [module](crate::queue) docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueManifest {
    /// The JSON Schema the file declares for editors. Not interpreted.
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Must equal [`QUEUE_MANIFEST_VERSION`].
    pub version: u32,
    /// The queues, each name at most once.
    pub queues: Vec<QueueDeclaration>,
}

/// One declared queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDeclaration {
    /// The queue name; see [`validate_queue_name`].
    pub name: String,
    /// What the queue is for, for the next person or agent who edits it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// The component that consumes the queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// [`QueueSpec::max_attempts`]; defaults to the [`QueueSpec`] default.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// [`QueueSpec::dedupe_window_ms`]; defaults to `0`.
    #[serde(default)]
    pub dedupe_window_ms: u64,
    /// The lease a consumer should ask for when it receives, in milliseconds.
    /// A hint for consumers; the queue itself takes the window per receive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility_ms: Option<u64>,
}

fn default_max_attempts() -> u32 {
    QueueSpec::default().max_attempts
}

impl QueueDeclaration {
    /// The [`QueueSpec`] this declaration asks for.
    #[must_use]
    pub fn spec(&self) -> QueueSpec {
        QueueSpec {
            max_attempts: self.max_attempts,
            dedupe_window_ms: self.dedupe_window_ms,
        }
    }

    fn validate(&self) -> Result<()> {
        validate_queue_name(&self.name)?;
        self.spec().validate()?;
        if self.visibility_ms == Some(0) {
            return Err(StorageError::invalid_input(
                "visibility_ms must be positive",
            ));
        }
        Ok(())
    }
}

impl QueueManifest {
    /// Parse and [validate](Self::validate) a manifest.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for malformed JSON, an unknown field, or any failure of
    /// [`validate`](Self::validate). The message names the line and column or
    /// the offending queue.
    pub fn from_json(text: &str) -> Result<Self> {
        let manifest: Self = serde_json::from_str(text)
            .map_err(|error| StorageError::invalid_input(format!("queue manifest: {error}")))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Read, parse and validate the manifest at `path`.
    ///
    /// # Errors
    ///
    /// `NotFound` when the file is missing, `Backend` when it cannot be read,
    /// otherwise as [`from_json`](Self::from_json), prefixed with the path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|error| {
            let message = format!("queue manifest {}: {error}", path.display());
            if error.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(message)
            } else {
                StorageError::backend(message).with_source(error)
            }
        })?;
        Self::from_json(&text).map_err(|error| {
            StorageError::new(
                error.kind(),
                format!("{}: {}", path.display(), error.message()),
            )
        })
    }

    /// Check the version, every declaration, and that names are unique.
    ///
    /// # Errors
    ///
    /// `InvalidInput` naming the first offending queue by index and name.
    pub fn validate(&self) -> Result<()> {
        if self.version != QUEUE_MANIFEST_VERSION {
            return Err(StorageError::invalid_input(format!(
                "queue manifest version {} is not supported, expected {QUEUE_MANIFEST_VERSION}",
                self.version
            )));
        }
        let mut seen = std::collections::HashSet::new();
        for (index, queue) in self.queues.iter().enumerate() {
            let at = |error: StorageError| {
                StorageError::invalid_input(format!(
                    "queues[{index}] ({:?}): {}",
                    queue.name,
                    error.message()
                ))
            };
            queue.validate().map_err(at)?;
            if !seen.insert(queue.name.as_str()) {
                return Err(at(StorageError::invalid_input("declared more than once")));
            }
        }
        Ok(())
    }

    /// The declaration of `name`, if the manifest has one.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&QueueDeclaration> {
        self.queues.iter().find(|queue| queue.name == name)
    }

    /// Declare every queue on `store`, in file order.
    ///
    /// # Errors
    ///
    /// The first failing [`ensure_queue`](QueueStore::ensure_queue).
    pub async fn apply(&self, store: &dyn QueueStore) -> Result<()> {
        for queue in &self.queues {
            store.ensure_queue(&queue.name, &queue.spec()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
