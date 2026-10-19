//! Record metadata: what the driver keeps about a document outside its body.

use serde::{Deserialize, Serialize};

pub use tinystoragedrivers_schema::Meta;

/// What a write declares about the document beyond its body.
///
/// A write replaces these values: a plain [`put`](crate::DocumentStore::put)
/// declares [`WriteOptions::default`], so an upsert without options clears an
/// earlier expiry. The creation and update times are never part of a write;
/// the driver sets them from its own clock.
///
/// ```
/// use tinystoragedrivers_core::WriteOptions;
///
/// let options = WriteOptions::new().schema(3).expires_ms(1_700_000_000_000);
/// assert_eq!(options.schema, 3);
/// assert!(WriteOptions::new().is_default());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WriteOptions {
    /// The body version the writer declares; `0` means undeclared.
    #[serde(default)]
    pub schema: u32,
    /// Unix epoch milliseconds from which the document reads as absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
    /// Unix epoch milliseconds to record as the document's creation time
    /// instead of the driver's clock. **Migration only**: a step that rewrites
    /// a record uses it so history keeps its original times. Honoured only by
    /// drivers with [`Capability::RecordMetadata`](crate::Capability::RecordMetadata);
    /// the others refuse a write that sets it. The update time still comes from
    /// the driver clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_ms: Option<i64>,
}

impl WriteOptions {
    /// Undeclared schema, no expiry, driver-clock creation time.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            schema: 0,
            expires_ms: None,
            created_ms: None,
        }
    }

    /// Record `created_ms` as the creation time (migration only; see the
    /// field).
    #[must_use]
    pub const fn created_ms(mut self, created_ms: i64) -> Self {
        self.created_ms = Some(created_ms);
        self
    }

    /// Declare the body version.
    #[must_use]
    pub const fn schema(mut self, schema: u32) -> Self {
        self.schema = schema;
        self
    }

    /// Expire the document at `expires_ms` (Unix epoch milliseconds).
    #[must_use]
    pub const fn expires_ms(mut self, expires_ms: i64) -> Self {
        self.expires_ms = Some(expires_ms);
        self
    }

    /// Whether these are the options of a plain write.
    #[must_use]
    pub const fn is_default(&self) -> bool {
        self.schema == 0 && self.expires_ms.is_none() && self.created_ms.is_none()
    }
}

#[cfg(test)]
#[path = "meta_tests.rs"]
mod tests;
