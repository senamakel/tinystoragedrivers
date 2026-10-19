//! Values the document port takes and returns.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Meta;
use crate::error::{Result, StorageError};
use crate::filter::{Filter, Sort};

/// The longest document id a driver must accept, in bytes.
pub const MAX_ID_LEN: usize = 512;

/// A document's revision. Every successful write of a document produces a
/// strictly greater version; a newly created document starts at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Version(pub u64);

impl Version {
    /// The version of a freshly created document.
    pub const FIRST: Self = Self(1);

    /// The version after this one, or `None` once the version space is
    /// exhausted. A driver must fail the write rather than reuse a version.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// What must be true of the stored document for a write to proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", content = "version", rename_all = "snake_case")]
pub enum Precondition {
    /// Write unconditionally (upsert).
    #[default]
    None,
    /// The document must not exist (insert).
    Absent,
    /// The document must exist at exactly this version (compare-and-swap).
    Version(Version),
}

/// A stored document with its id and revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Versioned<T> {
    /// The document id.
    pub id: String,
    /// The revision this copy was read at.
    pub version: Version,
    /// The document body.
    pub doc: T,
    /// What the driver recorded about the document: [`Meta::unknown`] when the
    /// driver lacks [`Capability::RecordMetadata`](crate::Capability::RecordMetadata).
    #[serde(default)]
    pub meta: Meta,
}

impl<T> Versioned<T> {
    /// The precondition that succeeds only if nobody wrote the document since
    /// this copy was read.
    #[must_use]
    pub fn unchanged(&self) -> Precondition {
        Precondition::Version(self.version)
    }

    /// Transform the body, keeping id and version.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Versioned<U> {
        Versioned {
            id: self.id,
            version: self.version,
            doc: f(self.doc),
            meta: self.meta,
        }
    }
}

pub use tinystoragedrivers_schema::{
    CollectionSpec, IndexSpec, MAX_COLLECTION_LEN, RESERVED_PREFIX, SearchSpec, validate_collection,
};

/// Validate a document id: 1 to [`MAX_ID_LEN`] bytes without NUL.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) describing the
/// problem.
pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_ID_LEN || id.contains('\0') {
        return Err(StorageError::invalid_input(format!(
            "document id must be 1 to {MAX_ID_LEN} bytes without NUL"
        )));
    }
    Ok(())
}

/// Require a document body to be a JSON object, the shape every backend can
/// store and index.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for any other
/// JSON value.
pub fn validate_doc(doc: &Value) -> Result<()> {
    if doc.is_object() {
        Ok(())
    } else {
        Err(StorageError::invalid_input(
            "document body must be a JSON object",
        ))
    }
}

/// An opaque position in a query's result order, returned by one page and
/// passed back to fetch the next.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(pub String);

impl Cursor {
    /// Encode a keyset position for a driver-owned query fingerprint.
    ///
    /// The payload is deliberately versioned so legacy driver cursors that
    /// represented offsets are rejected instead of silently changing meaning.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the position cannot be encoded.
    pub fn keyset(driver: &str, fingerprint: u64, position: &CursorPosition) -> Result<Self> {
        let payload = serde_json::to_string(position)?;
        Ok(Self(format!(
            "{driver}:keyset1:{fingerprint:016x}:{payload}"
        )))
    }

    /// Decode a keyset position issued for the expected driver and query.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput)
    /// when the cursor is legacy, malformed, or belongs to another query.
    pub fn decode_keyset(&self, driver: &str, fingerprint: u64) -> Result<CursorPosition> {
        let prefix = format!("{driver}:keyset1:{fingerprint:016x}:");
        let payload = self
            .0
            .strip_prefix(&prefix)
            .ok_or_else(|| StorageError::invalid_input("cursor was not issued for this query"))?;
        serde_json::from_str(payload)
            .map_err(|_| StorageError::invalid_input("cursor was not issued for this query"))
    }
}

/// The last document's total-order position in a keyset-paged query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorPosition {
    /// The document id, used after all explicit sort keys tie.
    pub id: String,
    /// Sort values in query sort-key order; absent fields are represented as null.
    pub values: Vec<Value>,
}

impl CursorPosition {
    /// Capture a document's sort values and id for a continuation cursor,
    /// with [`Meta::unknown`] as its metadata.
    #[must_use]
    pub fn for_document(id: &str, document: &Value, sort: &[Sort]) -> Self {
        Self::for_record(id, document, &Meta::unknown(), sort)
    }

    /// As [`for_document`](Self::for_document), reading the metadata sort
    /// paths from the document's own [`Meta`].
    #[must_use]
    pub fn for_record(id: &str, document: &Value, meta: &Meta, sort: &[Sort]) -> Self {
        let values = sort
            .iter()
            .map(|key| {
                crate::filter::field_value(id, document, meta, &key.field).unwrap_or(Value::Null)
            })
            .collect();
        Self {
            id: id.to_owned(),
            values,
        }
    }

    /// Compare two positions in the total order defined by `sort` and id.
    #[must_use]
    pub fn compare(&self, other: &Self, sort: &[Sort]) -> std::cmp::Ordering {
        for (index, key) in sort.iter().enumerate() {
            let left = self.values.get(index).unwrap_or(&Value::Null);
            let right = other.values.get(index).unwrap_or(&Value::Null);
            let ordering = crate::value::compare(left, right);
            let ordering = match key.direction {
                crate::Direction::Asc => ordering,
                crate::Direction::Desc => ordering.reverse(),
            };
            if !ordering.is_eq() {
                return ordering;
            }
        }
        self.id.cmp(&other.id)
    }

    /// Validate a decoded keyset position against its query sort.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) if
    /// the id or number of stored values cannot represent this query order.
    pub fn validate(&self, sort: &[Sort]) -> Result<()> {
        crate::document::validate_id(&self.id)?;
        if self.values.len() != sort.len()
            || sort.iter().zip(&self.values).any(|(key, value)| {
                key.field == crate::ID_FIELD && value != &Value::String(self.id.clone())
            })
        {
            return Err(StorageError::invalid_input(
                "cursor was not issued for this query",
            ));
        }
        Ok(())
    }
}

/// A filtered, ordered, paginated read.
///
/// ```
/// use tinystoragedrivers_core::{Filter, Query, Sort};
///
/// let query = Query::filter(Filter::eq("state", "queued"))
///     .sort(Sort::asc("run_at"))
///     .limit(10);
/// assert_eq!(query.limit, Some(10));
/// ```
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Query {
    /// Which documents.
    #[serde(default)]
    pub filter: Filter,
    /// Ordering. Ties (and an empty list) order by id ascending.
    #[serde(default)]
    pub sort: Vec<Sort>,
    /// At most this many documents per page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Continue after this position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

impl Query {
    /// Every document, ordered by id.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Documents matching `filter`.
    #[must_use]
    pub fn filter(filter: Filter) -> Self {
        Self {
            filter,
            ..Self::default()
        }
    }

    /// Add a sort key.
    #[must_use]
    pub fn sort(mut self, sort: Sort) -> Self {
        self.sort.push(sort);
        self
    }

    /// Cap the page size.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Reject a query no driver can page through: an untranslatable filter or
    /// a zero page size (which would never advance).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
    pub fn validate(&self) -> Result<()> {
        if self.limit == Some(0) {
            return Err(StorageError::invalid_input(
                "query limit must be at least 1",
            ));
        }
        for sort in &self.sort {
            Filter::exists(sort.field.clone(), true).validate()?;
        }
        self.filter.validate()
    }

    /// Continue from a previous page.
    #[must_use]
    pub fn after(mut self, cursor: Cursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
}

/// One page of query results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    /// The documents, in query order.
    pub items: Vec<T>,
    /// Where the next page starts, or `None` when this is the last page.
    pub next: Option<Cursor>,
}

/// Expected live state of a read-only batch dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "version", rename_all = "snake_case")]
pub enum ExpectedDocument {
    /// The dependency must be absent, including documents expired by TTL.
    Absent,
    /// The dependency must exist at this exact logical revision.
    Version(Version),
}

/// A read-only dependency in the document handle's database and scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadCheck {
    /// Collection containing the dependency.
    pub collection: String,
    /// Dependency document id.
    pub id: String,
    /// Required live state at the batch's atomic validation point.
    pub expected: ExpectedDocument,
}

impl ReadCheck {
    /// Validate this address without consulting persistent state.
    ///
    /// # Errors
    /// Invalid collection or document id.
    pub fn validate(&self) -> Result<()> {
        validate_collection(&self.collection)?;
        validate_id(&self.id)
    }

    /// Check the dependency's live revision, without rewriting it.
    ///
    /// # Errors
    /// Conflict if the live state differs, or an invalid address.
    pub fn verify(&self, current: Option<Version>) -> Result<()> {
        self.validate()?;
        match (self.expected, current) {
            (ExpectedDocument::Absent, None) => Ok(()),
            (ExpectedDocument::Version(expected), Some(actual)) if expected == actual => Ok(()),
            _ => Err(StorageError::conflict("a batch read dependency changed")),
        }
    }
}

/// One write inside an atomic batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WriteOp {
    /// [`DocumentStore::put`](crate::DocumentStore::put).
    Put {
        /// Collection.
        collection: String,
        /// Document id.
        id: String,
        /// Document body.
        doc: Value,
        /// Precondition.
        #[serde(default)]
        precondition: Precondition,
    },
    /// [`DocumentStore::delete`](crate::DocumentStore::delete).
    Delete {
        /// Collection.
        collection: String,
        /// Document id.
        id: String,
        /// Precondition.
        #[serde(default)]
        precondition: Precondition,
    },
}

/// The outcome of one [`WriteOp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WriteResult {
    /// The document's new version.
    Put {
        /// New version.
        version: Version,
    },
    /// Whether a document was removed.
    Delete {
        /// Whether something was removed.
        removed: bool,
    },
}

/// One full-text match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    /// The matching document's id.
    pub id: String,
    /// Driver-specific relevance; larger is better. Only comparable within one
    /// result list.
    pub score: f64,
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
