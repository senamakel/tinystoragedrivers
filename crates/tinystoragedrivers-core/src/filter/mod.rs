//! Backend-neutral predicates and orderings over documents.
//!
//! A [`Filter`] names fields by dotted path (see [`value::lookup`]); the
//! reserved path [`ID_FIELD`] addresses the document id rather than a field of
//! its body, and [`SCHEMA_FIELD`], [`CREATED_FIELD`], [`UPDATED_FIELD`] and
//! [`EXPIRES_FIELD`] address its [`Meta`]. A reserved path never reads the
//! body, even when the body has a field of that name. Equality and ranges use [`value::compare`], so `1` matches `1.0`
//! and a range over strings never matches a number. Drivers translate filters
//! into their query language where they can and must agree with
//! [`Filter::matches`] where they do.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_schema::validate_field_path as validate_path;

use crate::document::Meta;
use crate::error::{Result, StorageError};
use crate::value;

/// The path that addresses a document's id instead of a body field.
pub const ID_FIELD: &str = "_id";

/// The path that addresses a document's declared [`Meta::schema`].
pub const SCHEMA_FIELD: &str = "_schema";

/// The path that addresses a document's [`Meta::created_ms`].
pub const CREATED_FIELD: &str = "_created_ms";

/// The path that addresses a document's [`Meta::updated_ms`].
pub const UPDATED_FIELD: &str = "_updated_ms";

/// The path that addresses a document's [`Meta::expires_ms`]; absent when the
/// document never expires.
pub const EXPIRES_FIELD: &str = "_expires_ms";

/// A predicate over `(id, document)` pairs.
///
/// ```
/// use serde_json::json;
/// use tinystoragedrivers_core::Filter;
///
/// let due = Filter::eq("state", "queued").and(Filter::lt("run_at", 100));
/// assert!(due.matches("job-1", &json!({"state": "queued", "run_at": 50})));
/// assert!(!due.matches("job-2", &json!({"state": "queued", "run_at": 500})));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "op", rename_all = "snake_case")]
#[non_exhaustive]
#[allow(
    clippy::large_enum_variant,
    reason = "serde_json's `preserve_order`, which bson turns on wherever the MongoDB driver is linked, widens `Value` so `Range` outgrows the rest; boxing its bounds would change the public filter shape"
)]
pub enum Filter {
    /// Every document.
    #[default]
    All,
    /// The field equals the value.
    Eq {
        /// Dotted field path.
        field: String,
        /// Value to compare with.
        value: Value,
    },
    /// The field is absent or differs from the value.
    Ne {
        /// Dotted field path.
        field: String,
        /// Value to compare with.
        value: Value,
    },
    /// The field equals one of the values.
    In {
        /// Dotted field path.
        field: String,
        /// Accepted values.
        values: Vec<Value>,
    },
    /// The field lies within the bounds. A bound of a different JSON type than
    /// the field never matches.
    Range {
        /// Dotted field path.
        field: String,
        /// Exclusive lower bound.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gt: Option<Value>,
        /// Inclusive lower bound.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gte: Option<Value>,
        /// Exclusive upper bound.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lt: Option<Value>,
        /// Inclusive upper bound.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lte: Option<Value>,
    },
    /// The field is present (`true`) or absent (`false`). A present `null`
    /// counts as present.
    Exists {
        /// Dotted field path.
        field: String,
        /// Whether the field must be present.
        exists: bool,
    },
    /// Every sub-filter matches.
    And {
        /// Sub-filters.
        filters: Vec<Filter>,
    },
    /// At least one sub-filter matches.
    Or {
        /// Sub-filters.
        filters: Vec<Filter>,
    },
    /// The sub-filter does not match.
    Not {
        /// Sub-filter.
        filter: Box<Filter>,
    },
}

impl Filter {
    /// [`Filter::Eq`].
    #[must_use]
    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.into(),
        }
    }

    /// [`Filter::Ne`].
    #[must_use]
    pub fn ne(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Ne {
            field: field.into(),
            value: value.into(),
        }
    }

    /// [`Filter::In`].
    #[must_use]
    pub fn one_of<V: Into<Value>>(
        field: impl Into<String>,
        values: impl IntoIterator<Item = V>,
    ) -> Self {
        Self::In {
            field: field.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    fn range(field: impl Into<String>) -> Self {
        Self::Range {
            field: field.into(),
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        }
    }

    /// The field is greater than `value`.
    #[must_use]
    pub fn gt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        let mut filter = Self::range(field);
        if let Self::Range { gt, .. } = &mut filter {
            *gt = Some(value.into());
        }
        filter
    }

    /// The field is greater than or equal to `value`.
    #[must_use]
    pub fn gte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        let mut filter = Self::range(field);
        if let Self::Range { gte, .. } = &mut filter {
            *gte = Some(value.into());
        }
        filter
    }

    /// The field is less than `value`.
    #[must_use]
    pub fn lt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        let mut filter = Self::range(field);
        if let Self::Range { lt, .. } = &mut filter {
            *lt = Some(value.into());
        }
        filter
    }

    /// The field is less than or equal to `value`.
    #[must_use]
    pub fn lte(field: impl Into<String>, value: impl Into<Value>) -> Self {
        let mut filter = Self::range(field);
        if let Self::Range { lte, .. } = &mut filter {
            *lte = Some(value.into());
        }
        filter
    }

    /// [`Filter::Exists`].
    #[must_use]
    pub fn exists(field: impl Into<String>, exists: bool) -> Self {
        Self::Exists {
            field: field.into(),
            exists,
        }
    }

    /// Both this filter and `other` match. Flattens nested conjunctions.
    #[must_use]
    pub fn and(self, other: Filter) -> Self {
        match (self, other) {
            (Self::All, other) => other,
            (this, Self::All) => this,
            (Self::And { mut filters }, Self::And { filters: more }) => {
                filters.extend(more);
                Self::And { filters }
            }
            (Self::And { mut filters }, other) => {
                filters.push(other);
                Self::And { filters }
            }
            (this, other) => Self::And {
                filters: vec![this, other],
            },
        }
    }

    /// This filter or `other` matches.
    #[must_use]
    pub fn or(self, other: Filter) -> Self {
        match (self, other) {
            (Self::Or { mut filters }, other) => {
                filters.push(other);
                Self::Or { filters }
            }
            (this, other) => Self::Or {
                filters: vec![this, other],
            },
        }
    }

    /// This filter does not match.
    #[must_use]
    pub fn negate(self) -> Self {
        Self::Not {
            filter: Box::new(self),
        }
    }

    /// Reject filters a driver could not translate: empty field paths, empty
    /// path segments and ranges without any bound.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) describing
    /// the first offending clause.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::All => Ok(()),
            Self::Eq { field, .. }
            | Self::Ne { field, .. }
            | Self::In { field, .. }
            | Self::Exists { field, .. } => validate_path(field),
            Self::Range {
                field,
                gt,
                gte,
                lt,
                lte,
            } => {
                validate_path(field)?;
                if gt.is_none() && gte.is_none() && lt.is_none() && lte.is_none() {
                    return Err(StorageError::invalid_input(format!(
                        "range on `{field}` has no bound"
                    )));
                }
                Ok(())
            }
            Self::And { filters } | Self::Or { filters } => {
                filters.iter().try_for_each(Self::validate)
            }
            Self::Not { filter } => filter.validate(),
        }
    }

    /// Evaluate the filter against a document and its id, with
    /// [`Meta::unknown`] as its metadata.
    #[must_use]
    pub fn matches(&self, id: &str, doc: &Value) -> bool {
        self.matches_with(id, doc, &Meta::unknown())
    }

    /// Evaluate the filter against a document, its id and its metadata.
    #[must_use]
    pub fn matches_with(&self, id: &str, doc: &Value, meta: &Meta) -> bool {
        match self {
            Self::All => true,
            Self::Eq { field, value } => {
                field_value(id, doc, meta, field).is_some_and(|found| value::equal(&found, value))
            }
            Self::Ne { field, value } => {
                !field_value(id, doc, meta, field).is_some_and(|found| value::equal(&found, value))
            }
            Self::In { field, values } => field_value(id, doc, meta, field)
                .is_some_and(|found| values.iter().any(|v| value::equal(&found, v))),
            Self::Range {
                field,
                gt,
                gte,
                lt,
                lte,
            } => field_value(id, doc, meta, field).is_some_and(|found| {
                bound(&found, gt.as_ref(), |o| o == Ordering::Greater)
                    && bound(&found, gte.as_ref(), Ordering::is_ge)
                    && bound(&found, lt.as_ref(), |o| o == Ordering::Less)
                    && bound(&found, lte.as_ref(), Ordering::is_le)
            }),
            Self::Exists { field, exists } => {
                field_value(id, doc, meta, field).is_some() == *exists
            }
            Self::And { filters } => filters.iter().all(|f| f.matches_with(id, doc, meta)),
            Self::Or { filters } => filters.iter().any(|f| f.matches_with(id, doc, meta)),
            Self::Not { filter } => !filter.matches_with(id, doc, meta),
        }
    }
}

/// The value at `path`, treating [`ID_FIELD`] as the document id and the
/// metadata paths as the document's [`Meta`].
pub(crate) fn field_value(id: &str, doc: &Value, meta: &Meta, path: &str) -> Option<Value> {
    match path {
        ID_FIELD => Some(Value::String(id.to_owned())),
        SCHEMA_FIELD | CREATED_FIELD | UPDATED_FIELD | EXPIRES_FIELD => meta.field(path),
        _ => value::lookup(doc, path).cloned(),
    }
}

/// Whether `found` satisfies one optional bound. Bounds of a different JSON
/// type than the field never match, the way a typed SQL column or `MongoDB`
/// range behaves.
fn bound(found: &Value, limit: Option<&Value>, accept: impl Fn(Ordering) -> bool) -> bool {
    limit.is_none_or(|limit| same_type(found, limit) && accept(value::compare(found, limit)))
}

fn same_type(a: &Value, b: &Value) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Smallest first.
    #[default]
    Asc,
    /// Largest first.
    Desc,
}

/// One sort key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sort {
    /// Dotted field path, [`ID_FIELD`], or a metadata path such as
    /// [`CREATED_FIELD`].
    pub field: String,
    /// Direction.
    #[serde(default)]
    pub direction: Direction,
}

impl Sort {
    /// Ascending on `field`.
    #[must_use]
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: Direction::Asc,
        }
    }

    /// Descending on `field`.
    #[must_use]
    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: Direction::Desc,
        }
    }
}

/// Order `(id, doc)` pairs by `sorts`, then by id so the order is total.
///
/// A missing field sorts before every present value (as `null` would). The
/// metadata paths read [`Meta::unknown`]; use [`sort_documents_with_meta`] for
/// documents that carry their own.
pub fn sort_documents<T>(items: &mut [T], sorts: &[Sort], key: impl Fn(&T) -> (&str, &Value)) {
    static UNKNOWN: Meta = Meta::unknown();
    sort_documents_with_meta(items, sorts, |item| {
        let (id, doc) = key(item);
        (id, doc, &UNKNOWN)
    });
}

/// As [`sort_documents`], with each item's [`Meta`] so the reserved metadata
/// paths (`_created_ms` and the rest) order by the real values.
pub fn sort_documents_with_meta<T>(
    items: &mut [T],
    sorts: &[Sort],
    key: impl Fn(&T) -> (&str, &Value, &Meta),
) {
    items.sort_by(|a, b| {
        let (id_a, doc_a, meta_a) = key(a);
        let (id_b, doc_b, meta_b) = key(b);
        sorts
            .iter()
            .map(|sort| {
                let left = field_value(id_a, doc_a, meta_a, &sort.field).unwrap_or(Value::Null);
                let right = field_value(id_b, doc_b, meta_b, &sort.field).unwrap_or(Value::Null);
                let ordering = value::compare(&left, &right);
                match sort.direction {
                    Direction::Asc => ordering,
                    Direction::Desc => ordering.reverse(),
                }
            })
            .find(|ordering| ordering.is_ne())
            .unwrap_or_else(|| id_a.cmp(id_b))
    });
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
