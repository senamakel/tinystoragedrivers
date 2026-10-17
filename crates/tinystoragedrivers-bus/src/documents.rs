//! Synchronous serde wire shapes for the Documents object.
//!
//! These types describe data only. A server binds scope, collection
//! authority, schema grants and caller rights out of band.
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Precondition, Version, Versioned};

/// Root storage interface named by the storage bus contract.
pub const STORAGE_INTERFACE: &str = "ai.tinyhumans.tinystorage.Storage";
/// Documents object name under [`STORAGE_INTERFACE`].
pub const DOCUMENTS_OBJECT: &str = "Documents";
/// Finite selector set; servers must reject every other selector.
pub const DOCUMENTS_SELECTORS: [&str; 17] = [
    "Get",
    "Put",
    "Delete",
    "Query",
    "Count",
    "DeleteWhere",
    "Claim",
    "AtomicBatch",
    "AtomicBatchChecked",
    "Search",
    "EnsureCollection",
    "ValidateCollectionSpec",
    "DropCollection",
    "Collections",
    "WatchOpen",
    "WatchClose",
    "Capabilities",
];

/// Return whether `selector` belongs to the fixed Documents selector set.
#[must_use]
pub fn is_documents_selector(selector: &str) -> bool {
    DOCUMENTS_SELECTORS.contains(&selector)
}

/// A loadable module's requested storage resources.
///
/// These borrowed declarations are input to host policy; they do not grant
/// authority. Hosts must bind the resulting grant to the authenticated caller.
/// Secret prefixes are deliberately omitted from debug output.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageNeeds<'a> {
    /// Collection schemas the module requests.
    pub collections: &'a [CollectionSpec],
    /// Blob key prefixes the module requests.
    pub blob_prefixes: &'a [&'a str],
    /// Stream name prefixes the module requests.
    pub stream_prefixes: &'a [&'a str],
    /// Secret name prefixes the module requests.
    pub secret_prefixes: &'a [&'a str],
}

impl std::fmt::Debug for StorageNeeds<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageNeeds")
            .field("collections", &self.collections.len())
            .field("blob_prefixes", &self.blob_prefixes.len())
            .field("stream_prefixes", &self.stream_prefixes.len())
            .field("secret_prefixes", &"[REDACTED]")
            .finish()
    }
}

impl<'a> StorageNeeds<'a> {
    /// Add exact secret names while preserving this declaration's existing
    /// secret-prefix semantics.
    #[must_use]
    pub const fn with_exact_secret_names(
        self,
        exact_secret_names: &'a [&'a str],
    ) -> StorageNeedsWithExactSecrets<'a> {
        StorageNeedsWithExactSecrets {
            needs: self,
            exact_secret_names,
        }
    }
}

/// A storage declaration with additional exact secret-name needs.
///
/// This wrapper keeps [`StorageNeeds`] struct literals and their prefix
/// semantics unchanged. Exact names are separate declarations: they can
/// authorize individual CRUD operations but do not authorize secret listing.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageNeedsWithExactSecrets<'a> {
    /// The complete existing collection, blob, stream, and secret-prefix needs.
    pub needs: StorageNeeds<'a>,
    /// Secret names requested individually. These are not prefixes.
    pub exact_secret_names: &'a [&'a str],
}

impl std::fmt::Debug for StorageNeedsWithExactSecrets<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageNeedsWithExactSecrets")
            .field("needs", &self.needs)
            .field("exact_secret_names", &"[REDACTED]")
            .finish()
    }
}

/// A collection schema grant requested from the host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionSpec {
    /// Collection name within the host grant.
    pub name: String,
    /// Index declarations; index rights are separately granted by the host.
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    /// Optional expiry field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_field: Option<String>,
    /// Optional full-text field declaration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchSpec>,
}
/// Wire index declaration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexSpec {
    /// Stable index name.
    pub name: String,
    /// Ordered dotted fields.
    pub fields: Vec<String>,
    /// Enforce uniqueness when true.
    #[serde(default)]
    pub unique: bool,
}
/// Fields covered by full-text search.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchSpec {
    /// Dotted searchable fields.
    pub fields: Vec<String>,
}
/// Query filter expression; shape follows native Filter.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "wire Filter mirrors the native public shape; boxing changes the serialized Rust contract without reducing wire size"
)]
pub enum Filter {
    /// Match all records.
    #[default]
    All,
    /// Equality predicate.
    Eq {
        /// Dotted field path or `_id`.
        field: String,
        /// Required value.
        value: Value,
    },
    /// Inequality predicate; absent fields match.
    Ne {
        /// Dotted field path or `_id`.
        field: String,
        /// Excluded value.
        value: Value,
    },
    /// Membership predicate.
    In {
        /// Dotted field path or `_id`.
        field: String,
        /// Accepted values.
        values: Vec<Value>,
    },
    /// Range predicate.
    Range {
        /// Dotted field path or `_id`.
        field: String,
        /// Exclusive lower bound.
        #[serde(default)]
        gt: Option<Value>,
        /// Inclusive lower bound.
        #[serde(default)]
        gte: Option<Value>,
        /// Exclusive upper bound.
        #[serde(default)]
        lt: Option<Value>,
        /// Inclusive upper bound.
        #[serde(default)]
        lte: Option<Value>,
    },
    /// Field presence predicate.
    Exists {
        /// Dotted field path or `_id`.
        field: String,
        /// True when the field must be present.
        exists: bool,
    },
    /// All child filters match.
    And {
        /// Child predicates; all must match.
        filters: Vec<Filter>,
    },
    /// Any child filter matches.
    Or {
        /// Child predicates; at least one must match.
        filters: Vec<Filter>,
    },
    /// Child filter does not match.
    Not {
        /// Predicate that must not match.
        filter: Box<Filter>,
    },
}
/// Sort direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Smallest first.
    #[default]
    Asc,
    /// Largest first.
    Desc,
}
/// Sort key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sort {
    /// Dotted field or `_id`.
    pub field: String,
    /// Sort direction.
    #[serde(default)]
    pub direction: Direction,
}
/// Filtered, ordered keyset page request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// Predicate.
    #[serde(default)]
    pub filter: Filter,
    /// Sort order; ties resolve by id ascending.
    #[serde(default)]
    pub sort: Vec<Sort>,
    /// Maximum returned records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Opaque cursor from the same grant/query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}
/// Query request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Query definition.
    pub query: Query,
}
/// Query result page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryResponse {
    /// Page records.
    pub items: Vec<Versioned>,
    /// Next opaque cursor.
    #[serde(default)]
    pub next: Option<String>,
}
/// Read one document from a host-granted collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetDocumentRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Document id.
    #[serde(rename = "document_id")]
    pub id: String,
}
/// Write one document in a host-granted collection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutDocumentRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Document id.
    #[serde(rename = "document_id")]
    pub id: String,
    /// JSON object encoded as JSON text on this interface.
    #[serde(rename = "document_json", with = "crate::inline_json")]
    pub doc: Value,
    /// Atomic native condition.
    pub precondition: Precondition,
    /// Body version the writer declares; `0` (omitted) means undeclared.
    /// Needs `record_metadata`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub schema: u32,
    /// Unix epoch milliseconds from which the document reads as absent.
    /// Needs `record_metadata`. There is deliberately no `created_ms`: that
    /// is a host-only migration option and a request naming it fails to decode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
}
/// Serde `skip_serializing_if` predicate: omit a zero `schema`.
#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde passes a reference"
)]
fn is_zero(value: &u32) -> bool {
    *value == 0
}
/// Delete one document from a host-granted collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteDocumentRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Document id.
    #[serde(rename = "document_id")]
    pub id: String,
    /// Atomic native condition.
    pub precondition: Precondition,
}
/// Count request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CountRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Predicate.
    #[serde(default)]
    pub filter: Filter,
}
/// Delete matching documents request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteWhereRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Predicate.
    pub filter: Filter,
}
/// Atomic claim request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Predicate.
    pub filter: Filter,
    /// Ordered claim preference.
    #[serde(default)]
    pub sort: Vec<Sort>,
    /// RFC 7396 merge patch.
    #[serde(with = "crate::inline_json")]
    pub patch: Value,
}
/// Expected state for a read dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "version",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExpectedDocument {
    /// Must be absent.
    Absent,
    /// Must have this version.
    Version(Version),
}
/// Read-only dependency of an atomic checked batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadCheck {
    /// Collection, constrained by host grants.
    pub collection: String,
    /// Record id.
    pub id: String,
    /// Required pre-write state.
    pub expected: ExpectedDocument,
}
/// One atomic write operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteOp {
    /// Conditional write.
    Put {
        /// Collection constrained by host grants.
        collection: String,
        /// Record id.
        #[serde(rename = "document_id")]
        id: String,
        /// Replacement object.
        #[serde(rename = "document_json", with = "crate::inline_json")]
        doc: Value,
        /// Atomic precondition.
        #[serde(default)]
        precondition: Precondition,
    },
    /// Conditional delete.
    Delete {
        /// Collection constrained by host grants.
        collection: String,
        /// Record id.
        #[serde(rename = "document_id")]
        id: String,
        /// Atomic precondition.
        #[serde(default)]
        precondition: Precondition,
    },
}
/// Batch request. The host enforces count and encoded-size limits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRequest {
    /// Read dependencies; empty for `AtomicBatch`.
    #[serde(default)]
    pub checks: Vec<ReadCheck>,
    /// Ordered writes.
    pub ops: Vec<WriteOp>,
}
/// Result of one write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteResult {
    /// Written version.
    Put {
        /// Resulting revision.
        version: Version,
    },
    /// Whether delete removed a record.
    Delete {
        /// Whether a live record was removed.
        removed: bool,
    },
}
/// Batch response in input order.
pub type BatchResponse = Vec<WriteResult>;
/// Full-text search request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Search text.
    pub text: String,
    /// Maximum hits.
    pub limit: usize,
}
/// Ranked hit; score is comparable only within the returned list.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchHit {
    /// Document id.
    pub id: String,
    /// Driver relevance score.
    pub score: f64,
}
/// Host-owned schema declaration request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsureCollectionRequest {
    /// Collection spec, still checked against grant rights.
    pub spec: CollectionSpec,
}
/// Validate an existing declaration without modifying it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidateCollectionSpecRequest {
    /// Required declaration.
    pub spec: CollectionSpec,
}
/// Collection operation request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionRequest {
    /// Collection name inside the captured grant.
    pub collection: String,
}
/// List collection names from the captured scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionsRequest {
    /// Literal name prefix.
    #[serde(default)]
    pub prefix: String,
}

/// Open a filtered document watch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchOpenRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Subscription filter.
    pub filter: Filter,
    /// Resume cursor bound to the same query and captured authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<u8>>,
    /// Maximum quiet wait in milliseconds, between 1 and 60,000 inclusive.
    pub interval_ms: u64,
}
/// Reply to `WatchOpen`: the handle that later `WatchChanged` signals carry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchOpenResponse {
    /// Host-issued watch handle, scoped to this caller binding.
    pub watch_id: String,
}
/// Reply to `Capabilities`: the optional port capabilities the admitted
/// backend provides to this caller, by their stable lowercase names
/// (`full_text`, `transactions`, `watch`, ...). Host-only capabilities such as
/// fencing and scope purge are never listed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesReply {
    /// Capability names.
    pub capabilities: Vec<String>,
}

/// Close a host-owned watch subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchCloseRequest {
    /// Host-issued watch handle, scoped to this caller binding.
    pub watch_id: String,
}
/// Content-free watch notice; callers reread state through Documents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatchNotice {
    /// Relevant state may have changed.
    Invalidated {
        /// Opaque resume cursor bytes, if available.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<Vec<u8>>,
    },
    /// The subscription can no longer preserve its invalidation boundary.
    ResetRequired {
        /// Sanitized reset category only.
        reason: WatchReset,
    },
}
/// Sanitized watch reset category; backend details remain in the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchReset {
    /// The source cannot resume from the provided cursor.
    CursorExpired,
    /// The host fence changed.
    Fenced,
    /// The native watch encountered an opaque failure.
    Backend,
}
/// Content-free `TinyBus` signal emitted when a watch is invalidated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchChanged {
    /// Host-issued subscription handle.
    pub watch_id: String,
    /// Invalidation or terminal reset, with no record payload.
    pub notice: WatchNotice,
}

#[cfg(test)]
#[path = "documents_tests.rs"]
mod tests;
