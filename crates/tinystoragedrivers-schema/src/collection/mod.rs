//! Collection specifications and pure validation.

use crate::{Result, StorageError};
use serde::{Deserialize, Serialize};

/// Longest accepted collection name, in bytes.
pub const MAX_COLLECTION_LEN: usize = 120;

/// Prefix reserved for driver bookkeeping collections.
pub const RESERVED_PREFIX: &str = "_tsd";

/// A secondary index declaration.
///
/// Every driver must answer filters on any field, indexed or not; an index is a
/// performance hint plus, when `unique`, a constraint every driver enforces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSpec {
    /// Index name, unique within the collection.
    pub name: String,
    /// Dotted field paths, in key order.
    pub fields: Vec<String>,
    /// Reject a write that would give two documents equal values for every
    /// field. Documents missing any of the fields are not constrained.
    #[serde(default)]
    pub unique: bool,
}

impl IndexSpec {
    /// A non-unique index over `fields`.
    #[must_use]
    pub fn new<S: Into<String>>(
        name: impl Into<String>,
        fields: impl IntoIterator<Item = S>,
    ) -> Self {
        Self {
            name: name.into(),
            fields: fields.into_iter().map(Into::into).collect(),
            unique: false,
        }
    }

    /// Make the index unique.
    #[must_use]
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Which string fields `DocumentStore::search`
/// covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchSpec {
    /// Dotted paths of the searchable fields.
    pub fields: Vec<String>,
}

/// A collection's declared shape: indexes, expiry and search.
///
/// `DocumentStore::ensure_collection`
/// is idempotent, and a collection that was never declared behaves as one
/// declared with [`CollectionSpec::new`] alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionSpec {
    /// Collection name.
    pub name: String,
    /// Secondary indexes.
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    /// A field holding an expiry time in Unix epoch milliseconds. Once the time
    /// has passed the document reads as absent and may be removed. Requires
    /// `Capability::Ttl`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_field: Option<String>,
    /// Full-text search fields. Requires
    /// `Capability::FullText` to be searched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchSpec>,
}

impl CollectionSpec {
    /// A collection with no indexes, expiry or search.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            indexes: Vec::new(),
            ttl_field: None,
            search: None,
        }
    }

    /// Add an index.
    #[must_use]
    pub fn index(mut self, index: IndexSpec) -> Self {
        self.indexes.push(index);
        self
    }

    /// Expire documents on `field`.
    #[must_use]
    pub fn ttl(mut self, field: impl Into<String>) -> Self {
        self.ttl_field = Some(field.into());
        self
    }

    /// Make `fields` searchable.
    #[must_use]
    pub fn searchable<S: Into<String>>(mut self, fields: impl IntoIterator<Item = S>) -> Self {
        self.search = Some(SearchSpec {
            fields: fields.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Combine an existing declaration with a newer one for the same
    /// collection: indexes and search fields accumulate, and the expiry field
    /// is kept or added.
    ///
    /// # Errors
    ///
    /// `ErrorKind::InvalidInput` when the
    /// names differ, when an index name is redeclared with different fields or
    /// uniqueness, or when the expiry field changes.
    pub fn merge(&self, newer: &CollectionSpec) -> Result<CollectionSpec> {
        if self.name != newer.name {
            return Err(StorageError::invalid_input(
                "cannot merge declarations of different collections",
            ));
        }
        let mut merged = self.clone();
        for index in &newer.indexes {
            match merged.indexes.iter().find(|have| have.name == index.name) {
                Some(have) if have == index => {}
                Some(_) => {
                    return Err(StorageError::invalid_input(format!(
                        "index `{}` is already declared differently",
                        index.name
                    )));
                }
                None => merged.indexes.push(index.clone()),
            }
        }
        match (&merged.ttl_field, &newer.ttl_field) {
            (Some(have), Some(want)) if have != want => {
                return Err(StorageError::invalid_input(
                    "the expiry field of a collection cannot change",
                ));
            }
            (None, Some(want)) => merged.ttl_field = Some(want.clone()),
            _ => {}
        }
        if let Some(newer_search) = &newer.search {
            let search = merged
                .search
                .get_or_insert_with(|| SearchSpec { fields: Vec::new() });
            for field in &newer_search.fields {
                if !search.fields.contains(field) {
                    search.fields.push(field.clone());
                }
            }
        }
        Ok(merged)
    }

    /// Check the name, index names and field paths.
    ///
    /// # Errors
    ///
    /// `ErrorKind::InvalidInput` for an
    /// invalid collection name, an index with no fields, or a duplicated index
    /// name.
    pub fn validate(&self) -> Result<()> {
        validate_collection(&self.name)?;
        let mut names = std::collections::BTreeSet::new();
        for index in &self.indexes {
            if index.name.is_empty() || index.fields.is_empty() {
                return Err(StorageError::invalid_input(format!(
                    "index `{}` on `{}` needs a name and at least one field",
                    index.name, self.name
                )));
            }
            if !names.insert(index.name.as_str()) {
                return Err(StorageError::invalid_input(format!(
                    "index `{}` is declared twice on `{}`",
                    index.name, self.name
                )));
            }
            for field in &index.fields {
                validate_field_path(field)?;
            }
        }
        if let Some(field) = &self.ttl_field {
            validate_field_path(field)?;
        }
        if let Some(search) = &self.search {
            for field in &search.fields {
                validate_field_path(field)?;
            }
        }
        Ok(())
    }
}

/// Validate a collection name: 1 to [`MAX_COLLECTION_LEN`] ASCII letters,
/// digits, `_`, `-` or `.`, not starting with [`RESERVED_PREFIX`].
///
/// # Errors
///
/// `ErrorKind::InvalidInput` describing the
/// problem.
pub fn validate_collection(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_COLLECTION_LEN {
        return Err(StorageError::invalid_input(format!(
            "collection name must be 1 to {MAX_COLLECTION_LEN} bytes"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(StorageError::invalid_input(
            "collection names may only contain ASCII letters, digits, `_`, `-` and `.`",
        ));
    }
    if name.starts_with(RESERVED_PREFIX) {
        return Err(StorageError::invalid_input(format!(
            "collection names starting with `{RESERVED_PREFIX}` are reserved"
        )));
    }
    Ok(())
}

/// Validate a dotted field path without consulting storage.
///
/// # Errors
/// Returns invalid input when the path or a dotted segment is empty.
pub fn validate_field_path(path: &str) -> Result<()> {
    if path.is_empty() || path.split('.').any(str::is_empty) {
        return Err(StorageError::invalid_input(format!(
            "field path `{path}` is empty or has an empty segment"
        )));
    }
    Ok(())
}
