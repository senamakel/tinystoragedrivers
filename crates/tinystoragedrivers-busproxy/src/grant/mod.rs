//! Host-side, immutable storage grants derived from module needs and policy.
//!
//! See this module's `README.md` for host admission and binding requirements.

use std::collections::BTreeSet;

use serde_json::Value;
use tinystoragedrivers_bus::{
    CollectionSpec, StorageNeeds, StorageNeedsWithExactSecrets, is_blobs_selector,
    is_documents_selector, is_secrets_selector, is_streams_selector,
};
use tinystoragedrivers_client::Object;
use tinystoragedrivers_core::{
    MAX_BLOB_KEY_LEN, MAX_ID_LEN, SEALED_BLOB_PREFIX, validate_secret_prefix,
    validate_user_blob_key,
};

mod types;

pub use types::{Denied, GrantError, GrantPolicy, Plan, ReplyFilter, StorageGrant};

impl StorageGrant {
    /// Validate and own needs the host has approved for this caller.
    ///
    /// The caller must select needs through its module admission and
    /// resource-mapping policy first. `GrantPolicy` applies generic write,
    /// secret, and reserved-collection rules; it is not a per-module
    /// resource allow-list.
    ///
    /// # Errors
    ///
    /// Returns a content-free [`GrantError`] when a declaration is malformed
    /// or host policy refuses requested secret access.
    pub fn from_needs(
        needs: StorageNeeds<'_>,
        policy: &GrantPolicy<'_>,
    ) -> Result<Self, GrantError> {
        Self::from_declaration(needs, &[], policy)
    }

    /// Validate and own host-approved needs that include exact secret names.
    ///
    /// Exact names authorize individual Get, Set, and Delete calls. They do
    /// not authorize list operations; listing remains bounded by declared
    /// secret prefixes.
    ///
    /// # Errors
    ///
    /// Returns a content-free [`GrantError`] when the declaration is malformed
    /// or host policy refuses secret access.
    pub fn from_needs_with_exact_secrets(
        needs: StorageNeedsWithExactSecrets<'_>,
        policy: &GrantPolicy<'_>,
    ) -> Result<Self, GrantError> {
        Self::from_declaration(needs.needs, needs.exact_secret_names, policy)
    }

    fn from_declaration(
        needs: StorageNeeds<'_>,
        exact_secret_names: &[&str],
        policy: &GrantPolicy<'_>,
    ) -> Result<Self, GrantError> {
        if (!needs.secret_prefixes.is_empty() || !exact_secret_names.is_empty())
            && !policy.allow_secrets
        {
            return Err(GrantError::SecretsDenied);
        }

        let mut seen_collections = BTreeSet::new();
        let mut collections = Vec::with_capacity(needs.collections.len());
        for spec in needs.collections {
            let native = native_spec(spec);
            if native.validate().is_err()
                || spec.name.starts_with("_tsd")
                || spec.name.starts_with("tsd.")
                || policy.reserved_collections.contains(&spec.name.as_str())
                || !seen_collections.insert(spec.name.as_str())
            {
                return Err(GrantError::Collection);
            }
            collections.push(spec.clone());
        }

        let stream_prefixes = own_prefixes(needs.stream_prefixes, MAX_ID_LEN)?;
        let blob_prefixes = own_blob_prefixes(needs.blob_prefixes)?;
        let secret_prefixes = own_secret_prefixes(needs.secret_prefixes)?;
        let exact_secret_names = own_exact_secret_names(exact_secret_names)?;

        Ok(Self {
            collections,
            stream_prefixes,
            blob_prefixes,
            secret_prefixes,
            exact_secret_names,
            writable: policy.writable,
        })
    }

    /// The collection schemas this grant holds.
    #[must_use]
    pub fn collections(&self) -> &[CollectionSpec] {
        &self.collections
    }

    /// Whether this grant permits writes.
    #[must_use]
    pub const fn writable(&self) -> bool {
        self.writable
    }

    /// Decide whether a request is allowed and how to carry it out.
    ///
    /// # Errors
    ///
    /// Returns a content-free [`Denied`] when the selector, request shape,
    /// target, schema operation, or write right is not permitted.
    pub fn authorize(
        &self,
        object: Object,
        selector: &str,
        request: &Value,
    ) -> Result<Plan, Denied> {
        if !is_selector(object, selector) {
            return Err(Denied::Malformed);
        }
        validate_request(object, selector, request)?;
        if selector == "Capabilities" && !self.has_object(object) {
            if !request.is_null() {
                return Err(Denied::Malformed);
            }
            return match object {
                Object::Documents => Ok(Plan::Answer(serde_json::json!({"capabilities": []}))),
                Object::Blobs => Ok(Plan::Answer(serde_json::json!({
                    "presigned_urls": false,
                    "ttl": false,
                    "max_key_len": 0
                }))),
                Object::Streams | Object::Secrets => Err(Denied::Object),
            };
        }
        if !self.has_object(object) {
            return Err(Denied::Object);
        }
        if object == Object::Documents && selector == "DropCollection" {
            return Err(Denied::Schema);
        }
        let write = is_write(object, selector)?;
        if write && !self.writable {
            return Err(Denied::ReadOnly);
        }
        let plan = match object {
            Object::Documents => self.documents(selector, request)?,
            Object::Streams => self.streams(selector, request)?,
            Object::Blobs => self.blobs(selector, request)?,
            Object::Secrets => self.secrets(selector, request)?,
        };
        Ok(match plan {
            Plan::Forward { target, filter, .. } => Plan::Forward {
                target,
                filter,
                write,
            },
            answer @ Plan::Answer(_) => answer,
        })
    }

    /// Filter a Documents.Collections reply to this grant's collection names.
    ///
    /// # Errors
    ///
    /// Returns [`Denied::Malformed`] when a filtered reply is not an array of
    /// strings.
    pub fn filter_reply(&self, filter: ReplyFilter, reply: Value) -> Result<Value, Denied> {
        match filter {
            ReplyFilter::None => Ok(reply),
            ReplyFilter::GrantedCollections => {
                let names = reply.as_array().ok_or(Denied::Malformed)?;
                let mut filtered = Vec::new();
                for value in names {
                    let name = value.as_str().ok_or(Denied::Malformed)?;
                    if self.collection(name).is_ok() {
                        filtered.push(value.clone());
                    }
                }
                Ok(Value::Array(filtered))
            }
        }
    }

    fn has_object(&self, object: Object) -> bool {
        match object {
            Object::Documents => !self.collections.is_empty(),
            Object::Streams => !self.stream_prefixes.is_empty(),
            Object::Blobs => !self.blob_prefixes.is_empty(),
            Object::Secrets => {
                !self.secret_prefixes.is_empty() || !self.exact_secret_names.is_empty()
            }
        }
    }

    fn collection(&self, name: &str) -> Result<&CollectionSpec, Denied> {
        self.collections
            .iter()
            .find(|spec| spec.name == name)
            .ok_or(Denied::Target)
    }

    fn prefix<'a>(prefixes: &'a [String], value: &str) -> Result<&'a str, Denied> {
        prefixes
            .iter()
            .find(|prefix| value.starts_with(prefix.as_str()))
            .map(String::as_str)
            .ok_or(Denied::Target)
    }

    fn documents(&self, selector: &str, request: &Value) -> Result<Plan, Denied> {
        match selector {
            "Get" | "Query" | "Count" | "Search" | "Put" | "Delete" | "DeleteWhere" | "Claim"
            | "WatchOpen" => {
                let collection = text(request, "collection")?;
                self.collection(collection)?;
                Ok(forward(collection))
            }
            "AtomicBatch" | "AtomicBatchChecked" => self.batch(request, selector == "AtomicBatch"),
            "EnsureCollection" | "ValidateCollectionSpec" => {
                let spec: CollectionSpec = request
                    .get("spec")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok())
                    .ok_or(Denied::Malformed)?;
                if self.collection(&spec.name)? == &spec {
                    Ok(Plan::Answer(Value::Null))
                } else {
                    Err(Denied::Schema)
                }
            }
            "Collections" => {
                let _: tinystoragedrivers_bus::CollectionsRequest = decode(request)?;
                Ok(Plan::Forward {
                    target: String::new(),
                    filter: ReplyFilter::GrantedCollections,
                    write: false,
                })
            }
            "WatchClose" | "Capabilities" => Ok(forward("")),
            _ => Err(Denied::Malformed),
        }
    }

    fn batch(&self, request: &Value, reject_checks: bool) -> Result<Plan, Denied> {
        let batch: tinystoragedrivers_bus::BatchRequest = decode(request)?;
        let mut first = None;
        for check in &batch.checks {
            self.collection(&check.collection)?;
            first.get_or_insert(check.collection.as_str());
        }
        for operation in &batch.ops {
            let collection = match operation {
                tinystoragedrivers_bus::WriteOp::Put { collection, .. }
                | tinystoragedrivers_bus::WriteOp::Delete { collection, .. } => collection,
            };
            self.collection(collection)?;
            first.get_or_insert(collection.as_str());
        }
        if reject_checks && !batch.checks.is_empty() {
            return Err(Denied::Malformed);
        }
        Ok(forward(first.unwrap_or_default()))
    }

    fn streams(&self, selector: &str, request: &Value) -> Result<Plan, Denied> {
        match selector {
            "ReadWindow"
            | "Len"
            | "Append"
            | "AppendBatch"
            | "AppendBatchIfOffset"
            | "TruncateBefore"
            | "DeleteStream"
            | "Seal"
            | "Info" => {
                let stream = text(request, "stream")?;
                Self::prefix(&self.stream_prefixes, stream)?;
                Ok(forward(stream))
            }
            "Streams" => {
                let request: tinystoragedrivers_bus::ListStreamsRequest = decode(request)?;
                let prefix = request.prefix.as_str();
                let granted = Self::prefix(&self.stream_prefixes, prefix)?;
                Ok(forward(granted))
            }
            _ => Err(Denied::Malformed),
        }
    }

    fn blobs(&self, selector: &str, request: &Value) -> Result<Plan, Denied> {
        match selector {
            "Get" | "GetStream" | "GetRange" | "Head" | "PresignGet" | "Put" | "PutStream"
            | "Delete" | "PresignPut" => {
                let key = text(request, "key")?;
                validate_user_blob_key(key).map_err(|_| Denied::Malformed)?;
                Self::prefix(&self.blob_prefixes, key)?;
                Ok(forward(key))
            }
            "List" => {
                let request: tinystoragedrivers_bus::ListBlobsRequest = decode(request)?;
                let prefix = request.prefix.as_str();
                let granted = Self::prefix(&self.blob_prefixes, prefix)?;
                Ok(forward(granted))
            }
            "Capabilities" => Ok(forward("")),
            _ => Err(Denied::Malformed),
        }
    }

    fn secrets(&self, selector: &str, request: &Value) -> Result<Plan, Denied> {
        match selector {
            "Get" | "Set" | "Delete" => {
                let name = text(request, "name")?;
                if !self.exact_secret_names.iter().any(|exact| exact == name) {
                    Self::prefix(&self.secret_prefixes, name)?;
                }
                Ok(forward("[REDACTED]"))
            }
            "List" => {
                let request: tinystoragedrivers_bus::ListSecretsRequest = decode(request)?;
                let prefix = request.prefix.as_str();
                Self::prefix(&self.secret_prefixes, prefix)?;
                Ok(forward("[REDACTED]"))
            }
            _ => Err(Denied::Malformed),
        }
    }
}

fn native_spec(spec: &CollectionSpec) -> tinystoragedrivers_core::CollectionSpec {
    tinystoragedrivers_core::CollectionSpec {
        name: spec.name.clone(),
        indexes: spec
            .indexes
            .iter()
            .map(|index| tinystoragedrivers_core::IndexSpec {
                name: index.name.clone(),
                fields: index.fields.clone(),
                unique: index.unique,
            })
            .collect(),
        ttl_field: spec.ttl_field.clone(),
        search: spec
            .search
            .as_ref()
            .map(|search| tinystoragedrivers_core::SearchSpec {
                fields: search.fields.clone(),
            }),
    }
}

fn own_prefixes(prefixes: &[&str], max_len: usize) -> Result<Vec<String>, GrantError> {
    let mut seen = BTreeSet::new();
    let mut owned = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if prefix.is_empty()
            || prefix.len() > max_len
            || prefix.contains('\0')
            || !seen.insert(*prefix)
        {
            return Err(GrantError::Prefix);
        }
        owned.push((*prefix).to_owned());
    }
    Ok(owned)
}

fn own_blob_prefixes(prefixes: &[&str]) -> Result<Vec<String>, GrantError> {
    let mut seen = BTreeSet::new();
    let mut owned = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if prefix.is_empty()
            || prefix.len() > MAX_BLOB_KEY_LEN
            || prefix.contains('\0')
            || !valid_blob_prefix(prefix)
            || prefix.starts_with(SEALED_BLOB_PREFIX)
            || SEALED_BLOB_PREFIX.starts_with(prefix)
            || !seen.insert(*prefix)
        {
            return Err(GrantError::Prefix);
        }
        owned.push((*prefix).to_owned());
    }
    Ok(owned)
}

fn own_secret_prefixes(prefixes: &[&str]) -> Result<Vec<String>, GrantError> {
    let mut seen = BTreeSet::new();
    let mut owned = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if prefix.is_empty() || validate_secret_prefix(prefix).is_err() || !seen.insert(*prefix) {
            return Err(GrantError::Prefix);
        }
        owned.push((*prefix).to_owned());
    }
    Ok(owned)
}

fn own_exact_secret_names(names: &[&str]) -> Result<Vec<String>, GrantError> {
    let mut seen = BTreeSet::new();
    let mut owned = Vec::with_capacity(names.len());
    for name in names {
        if tinystoragedrivers_core::validate_secret_name(name).is_err() || !seen.insert(*name) {
            return Err(GrantError::Prefix);
        }
        owned.push((*name).to_owned());
    }
    Ok(owned)
}

fn valid_blob_prefix(prefix: &str) -> bool {
    if prefix.starts_with('/') {
        return false;
    }
    let mut segments = prefix.split('/').peekable();
    while let Some(segment) = segments.next() {
        if segment.is_empty() {
            if segments.peek().is_some() {
                return false;
            }
        } else if segment == "." || segment == ".." {
            return false;
        }
    }
    true
}

fn is_selector(object: Object, selector: &str) -> bool {
    match object {
        Object::Documents => is_documents_selector(selector),
        Object::Streams => is_streams_selector(selector),
        Object::Blobs => is_blobs_selector(selector),
        Object::Secrets => is_secrets_selector(selector),
    }
}

fn validate_request(object: Object, selector: &str, request: &Value) -> Result<(), Denied> {
    use tinystoragedrivers_bus as wire;
    macro_rules! check {
        ($request:ty) => {
            decode::<$request>(request).map(|_| ())
        };
    }
    match (object, selector) {
        (Object::Documents, "Get") => check!(wire::GetDocumentRequest),
        (Object::Documents, "Put") => check!(wire::PutDocumentRequest),
        (Object::Documents, "Delete") => check!(wire::DeleteDocumentRequest),
        (Object::Documents, "Query") => check!(wire::QueryRequest),
        (Object::Documents, "Count") => check!(wire::CountRequest),
        (Object::Documents, "DeleteWhere") => check!(wire::DeleteWhereRequest),
        (Object::Documents, "Claim") => check!(wire::ClaimRequest),
        (Object::Documents, "AtomicBatch" | "AtomicBatchChecked") => check!(wire::BatchRequest),
        (Object::Documents, "Search") => check!(wire::SearchRequest),
        (Object::Documents, "EnsureCollection") => check!(wire::EnsureCollectionRequest),
        (Object::Documents, "ValidateCollectionSpec") => {
            check!(wire::ValidateCollectionSpecRequest)
        }
        (Object::Documents, "DropCollection") => check!(wire::CollectionRequest),
        (Object::Documents, "Collections") => check!(wire::CollectionsRequest),
        (Object::Documents, "WatchOpen") => check!(wire::WatchOpenRequest),
        (Object::Documents, "WatchClose") => check!(wire::WatchCloseRequest),
        (Object::Documents | Object::Blobs, "Capabilities") if request.is_null() => Ok(()),
        (Object::Streams, "Append") => check!(wire::AppendRequest),
        (Object::Streams, "AppendBatch") => check!(wire::AppendBatchRequest),
        (Object::Streams, "AppendBatchIfOffset") => check!(wire::AppendBatchIfOffsetRequest),
        (Object::Streams, "ReadWindow") => check!(wire::ReadWindowRequest),
        (Object::Streams, "Len" | "DeleteStream" | "Seal" | "Info") => check!(wire::StreamRequest),
        (Object::Streams, "TruncateBefore") => check!(wire::TruncateBeforeRequest),
        (Object::Streams, "Streams") => check!(wire::ListStreamsRequest),
        (Object::Blobs, "Put") => check!(wire::PutBlobRequest),
        (Object::Blobs, "PutStream") => check!(wire::PutBlobStreamRequest),
        (Object::Blobs, "Get" | "GetStream" | "Head" | "Delete") => check!(wire::BlobKeyRequest),
        (Object::Blobs, "GetRange") => check!(wire::GetRangeRequest),
        (Object::Blobs, "List") => check!(wire::ListBlobsRequest),
        (Object::Blobs, "PresignGet" | "PresignPut") => check!(wire::PresignRequest),
        (Object::Secrets, "Get" | "Delete") => check!(wire::SecretNameRequest),
        (Object::Secrets, "Set") => check!(wire::SetSecretRequest),
        (Object::Secrets, "List") => check!(wire::ListSecretsRequest),
        _ => Err(Denied::Malformed),
    }
}

fn is_write(object: Object, selector: &str) -> Result<bool, Denied> {
    Ok(match (object, selector) {
        (
            Object::Documents,
            "Put" | "Delete" | "DeleteWhere" | "Claim" | "AtomicBatch" | "AtomicBatchChecked"
            | "DropCollection",
        )
        | (
            Object::Streams,
            "Append"
            | "AppendBatch"
            | "AppendBatchIfOffset"
            | "TruncateBefore"
            | "DeleteStream"
            | "Seal",
        )
        | (Object::Blobs, "Put" | "PutStream" | "Delete" | "PresignPut")
        | (Object::Secrets, "Set" | "Delete") => true,
        (
            Object::Documents,
            "Get"
            | "Query"
            | "Count"
            | "Search"
            | "EnsureCollection"
            | "ValidateCollectionSpec"
            | "Collections"
            | "WatchOpen"
            | "WatchClose"
            | "Capabilities",
        )
        | (Object::Streams, "ReadWindow" | "Len" | "Streams" | "Info")
        | (
            Object::Blobs,
            "Get" | "GetStream" | "GetRange" | "Head" | "List" | "PresignGet" | "Capabilities",
        )
        | (Object::Secrets, "Get" | "List") => false,
        _ => return Err(Denied::Malformed),
    })
}

fn text<'a>(request: &'a Value, field: &str) -> Result<&'a str, Denied> {
    request
        .get(field)
        .and_then(Value::as_str)
        .ok_or(Denied::Malformed)
}

fn decode<T: serde::de::DeserializeOwned>(request: &Value) -> Result<T, Denied> {
    serde_json::from_value(request.clone()).map_err(|_| Denied::Malformed)
}

fn forward(target: impl Into<String>) -> Plan {
    Plan::Forward {
        target: target.into(),
        filter: ReplyFilter::None,
        write: false,
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "exact_secret_tests.rs"]
mod exact_secret_tests;
