//! Document metadata and transform records.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What a driver records about one document, outside its body.
///
/// Drivers without `Capability::RecordMetadata`
/// return [`Meta::unknown`] for every document.
///
/// ```
/// use tinystoragedrivers_schema::Meta;
///
/// let meta = Meta { schema: 2, created_ms: 10, updated_ms: 20, expires_ms: Some(30) };
/// assert!(!meta.is_expired(29));
/// assert!(meta.is_expired(30));
/// assert_eq!(Meta::unknown().schema, 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Meta {
    /// The body version the writer declared with
    /// `WriteOptions::schema`. `0` means undeclared.
    #[serde(default)]
    pub schema: u32,
    /// Unix epoch milliseconds of the document's first write. An update keeps
    /// it; creating the id again after a delete (or after the document
    /// expired) starts it afresh.
    #[serde(default)]
    pub created_ms: i64,
    /// Unix epoch milliseconds of the document's latest write. It never goes
    /// backwards, even if the driver's clock does.
    #[serde(default)]
    pub updated_ms: i64,
    /// Unix epoch milliseconds from which the document reads as absent, or
    /// `None` when it never expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
}

impl Meta {
    /// The metadata of a document whose driver keeps none: schema `0`, both
    /// times `0` and no expiry.
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            schema: 0,
            created_ms: 0,
            updated_ms: 0,
            expires_ms: None,
        }
    }

    /// Whether the document has expired at `now_ms`: its expiry time is
    /// reached (`expires_ms <= now_ms`).
    #[must_use]
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_ms.is_some_and(|expires| expires <= now_ms)
    }

    /// The value of a reserved path (`_schema`, `_created_ms`, `_updated_ms`,
    /// `_expires_ms`), or `None` for any other path and for an absent expiry.
    #[must_use]
    pub fn field(&self, path: &str) -> Option<Value> {
        match path {
            "_schema" => Some(Value::from(self.schema)),
            "_created_ms" => Some(Value::from(self.created_ms)),
            "_updated_ms" => Some(Value::from(self.updated_ms)),
            "_expires_ms" => self.expires_ms.map(Value::from),
            _ => None,
        }
    }
}
