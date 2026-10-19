//! Choosing and opening a backend from a URL.
//!
//! A host reads one string from its configuration (`memory`,
//! `sqlite:/var/lib/app`, `mongodb://db.internal/openhuman`) and hands it to
//! [`StorageConfig::parse`] then [`open`]. A URL for a driver this build did
//! not compile in is rejected with the Cargo feature to enable, so a
//! misconfigured deployment fails at boot with an actionable message.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use tinystoragedrivers_core::{MemoryStorage, Result, StorageBackend, StorageError};

/// A parsed storage location.
///
/// ```
/// use tinystoragedrivers::StorageConfig;
///
/// assert_eq!(StorageConfig::parse("memory")?, StorageConfig::Memory);
/// let mongo = StorageConfig::parse("mongodb://app:hunter2@db.internal/openhuman")?;
/// assert_eq!(mongo.to_string(), "mongodb://app:***@db.internal/openhuman");
/// # Ok::<(), tinystoragedrivers::StorageError>(())
/// ```
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageConfig {
    /// Process memory; nothing survives a restart.
    Memory,
    /// SQLite. A path ending in `.db` or `.sqlite` is one database file; any
    /// other path is a directory holding the one file `storage.db`.
    Sqlite {
        /// File or directory.
        path: PathBuf,
    },
    /// `MongoDB`, one database shared by every scope.
    MongoDb {
        /// The connection string, credentials included.
        uri: String,
        /// The database name taken from the URI path.
        database: String,
    },
    /// JSON and JSONL files under a directory.
    File {
        /// Root directory.
        dir: PathBuf,
    },
    /// Documents and streams on one backend with blobs on another.
    Composite {
        /// The records backend configuration.
        records: Box<StorageConfig>,
        /// The independently configured blob backend.
        blobs: Box<crate::BlobConfig>,
    },
}

impl StorageConfig {
    /// Parse a storage URL.
    ///
    /// Accepted forms: `memory`; `sqlite:<path>`; a bare path (absolute, or
    /// starting with `./` or `../`), read as SQLite;
    /// `mongodb://…/<database>` and `mongodb+srv://…/<database>`;
    /// `file:<dir>`. Add one percent-encoded `blobs=<URL>` query option to
    /// select an independent blob backend; nested composite blob URLs are not
    /// supported.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// empty or unrecognized URL, an empty path, a malformed or duplicate
    /// `blobs` option, or a `MongoDB` URI without a database name.
    pub fn parse(url: &str) -> Result<Self> {
        let url = url.trim();
        if url.is_empty() {
            return Err(StorageError::invalid_input("storage URL is empty"));
        }
        if let Some((records_url, blobs_url)) = split_blob_option(url)? {
            let records = Self::parse(&records_url)?;
            let blobs = crate::BlobConfig::parse(&blobs_url)?;
            return Ok(Self::Composite {
                records: Box::new(records),
                blobs: Box::new(blobs),
            });
        }
        if url == "memory" || url == "memory:" {
            return Ok(Self::Memory);
        }
        if url.starts_with("mongodb://") || url.starts_with("mongodb+srv://") {
            return Self::parse_mongo(url);
        }
        if let Some(path) = url.strip_prefix("sqlite:") {
            return Ok(Self::Sqlite {
                path: non_empty_path(path.strip_prefix("//").unwrap_or(path), "sqlite")?,
            });
        }
        if let Some(dir) = url.strip_prefix("file:") {
            return Ok(Self::File {
                dir: non_empty_path(dir.strip_prefix("//").unwrap_or(dir), "file")?,
            });
        }
        if url.starts_with('/') || url.starts_with("./") || url.starts_with("../") {
            return Ok(Self::Sqlite {
                path: PathBuf::from(url),
            });
        }
        Err(StorageError::invalid_input(format!(
            "unrecognized storage URL `{}`; expected memory, sqlite:<path>, mongodb://…/<db> or file:<dir>",
            redact_url(url)
        )))
    }

    fn parse_mongo(url: &str) -> Result<Self> {
        let path = split_uri(url).map_or("", |parts| parts.path);
        let database = path.split('/').next().unwrap_or_default();
        if database.is_empty() {
            return Err(StorageError::invalid_input(format!(
                "MongoDB URL `{}` names no database; add one as the path, e.g. mongodb://host/openhuman",
                redact_url(url)
            )));
        }
        Ok(Self::MongoDb {
            uri: url.to_owned(),
            database: database.to_owned(),
        })
    }

    /// The driver this configuration selects.
    #[must_use]
    pub fn driver(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite { .. } => "sqlite",
            Self::MongoDb { .. } => "mongodb",
            Self::File { .. } => "file",
            Self::Composite { .. } => "composite",
        }
    }
}

/// Remove and decode the one optional `blobs=` query component while retaining
/// every other records URL query component verbatim.
fn split_blob_option(url: &str) -> Result<Option<(String, String)>> {
    let Some(query_start) = url.find('?') else {
        return Ok(None);
    };
    let (query, fragment) = url[query_start + 1..]
        .split_once('#')
        .map_or((&url[query_start + 1..], ""), |(query, fragment)| {
            (query, fragment)
        });
    let mut blob_value = None;
    let mut kept = Vec::new();
    for option in query.split('&') {
        let (key, value) = option.split_once('=').unwrap_or((option, ""));
        if key == "blobs" {
            if blob_value.is_some() {
                return Err(StorageError::invalid_input(
                    "storage URL has more than one blobs option",
                ));
            }
            blob_value = Some(decode_query_component(value)?);
        } else {
            kept.push(option);
        }
    }
    let Some(blob_value) = blob_value else {
        return Ok(None);
    };
    if blob_value.is_empty() {
        return Err(StorageError::invalid_input(
            "storage URL blobs option is empty",
        ));
    }
    let prefix = &url[..query_start];
    let records_url = if kept.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix}?{}", kept.join("&"))
    };
    let records_url = if fragment.is_empty() {
        records_url
    } else {
        format!("{records_url}#{fragment}")
    };
    Ok(Some((records_url, blob_value)))
}

fn decode_query_component(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = bytes.get(index + 1).and_then(|byte| hex_nibble(*byte));
            let low = bytes.get(index + 2).and_then(|byte| hex_nibble(*byte));
            let (Some(high), Some(low)) = (high, low) else {
                return Err(StorageError::invalid_input(
                    "storage URL blobs option has invalid percent encoding",
                ));
            };
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| StorageError::invalid_input("storage URL blobs option is not valid UTF-8"))
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn encode_query_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}

fn non_empty_path(path: &str, driver: &str) -> Result<PathBuf> {
    if path.is_empty() {
        Err(StorageError::invalid_input(format!(
            "{driver} storage URL has no path"
        )))
    } else {
        Ok(PathBuf::from(path))
    }
}

/// The pieces of `scheme://[userinfo@]hosts[/path][?query]`.
struct UriParts<'a> {
    path: &'a str,
}

/// Split a connection URI without trusting the credentials to be
/// percent-encoded: the userinfo ends at the last `@` before the query, so a
/// stray `/` or `@` inside a password cannot move the boundary.
fn split_uri(url: &str) -> Option<UriParts<'_>> {
    let (_, rest) = url.split_once("://")?;
    let at = rest.split('?').next().and_then(|head| head.rfind('@'));
    let tail = match at {
        Some(at) => rest.get(at + 1..).unwrap_or_default(),
        None => rest,
    };
    let before_query = tail.split('?').next().unwrap_or(tail);
    let (_, path) = before_query.split_once('/').unwrap_or((before_query, ""));
    Some(UriParts { path })
}

/// Query options whose values are credentials (AWS session tokens ride in
/// `authMechanismProperties`).
const SECRET_OPTIONS: [&str; 4] = [
    "authmechanismproperties",
    "proxypassword",
    "tlscertificatekeyfilepassword",
    "password",
];

/// Hide credentials in a storage connection URI before displaying it.
///
/// The password and credential-valued query options are replaced with `***`.
/// When userinfo and query parsing overlap, both sensitive spans are found in
/// the original input and masked together, preserving conservative redaction
/// for unescaped reserved characters.
#[must_use]
pub fn redact_url(url: &str) -> String {
    let mut sensitive = legacy_userinfo_password_span(url)
        .into_iter()
        .collect::<Vec<_>>();
    sensitive.extend(secret_query_value_spans(url));
    preserve_legacy_query_path_separator(&redact_spans(url, sensitive))
}

/// The legacy renderer inserts `/` when a URI has a query but no path.
fn preserve_legacy_query_path_separator(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let query_end = rest.find('#').unwrap_or(rest.len());
    let Some(query_marker) = rest[..query_end].find('?') else {
        return url.to_owned();
    };
    if rest[..query_marker].contains('/') {
        return url.to_owned();
    }
    format!(
        "{scheme}://{}/{}",
        &rest[..query_marker],
        &rest[query_marker..]
    )
}

/// Locate the legacy userinfo password interval on the original URI. `@`
/// always means the last `@` after the scheme, even when reserved characters
/// make the input look like it has already entered its path or query.
fn legacy_userinfo_password_span(url: &str) -> Option<Range<usize>> {
    let (_, rest) = url.split_once("://")?;
    let at = rest.rfind('@')?;
    let colon = rest[..at].find(':')?;
    let base = url.len() - rest.len();
    Some(base + colon + 1..base + at)
}

/// Locate values for known secret query options using only original input
/// boundaries: `?` starts an option list and `&` separates options. Match
/// legacy behavior through the URI end, including `#` and any query-looking
/// text after it.
fn secret_query_value_spans(url: &str) -> Vec<Range<usize>> {
    scan_secret_query_value_spans(url, || {})
}

/// Each URI byte is visited once by the outer scan. Every query boundary can
/// inspect only the finite, bounded set of secret option names, so query work
/// is linear in URI length even when `?` appears in values or malformed input.
fn scan_secret_query_value_spans(
    url: &str,
    mut visit_input_byte: impl FnMut(),
) -> Vec<Range<usize>> {
    let bytes = url.as_bytes();
    let mut spans = Vec::new();
    let mut secret_starts = Vec::new();
    let mut query_started = false;

    for index in 0..=bytes.len() {
        let byte = bytes.get(index).copied();
        if byte == Some(b'?') {
            query_started = true;
            if let Some(value_start) =
                secret_query_value_start(bytes, index + 1, &mut visit_input_byte)
            {
                secret_starts.push(value_start);
            }
        } else if query_started && (byte == Some(b'&') || byte.is_none()) {
            spans.extend(secret_starts.drain(..).map(|start| start..index));
            if byte == Some(b'&')
                && let Some(value_start) =
                    secret_query_value_start(bytes, index + 1, &mut visit_input_byte)
            {
                secret_starts.push(value_start);
            }
        }
        if byte.is_some() {
            visit_input_byte();
        }
    }
    spans
}

fn secret_query_value_start(
    bytes: &[u8],
    start: usize,
    mut visit_input_byte: impl FnMut(),
) -> Option<usize> {
    for option in SECRET_OPTIONS {
        let mut cursor = start;
        let mut matched = true;
        for expected in option.bytes() {
            let Some(&byte) = bytes.get(cursor) else {
                matched = false;
                break;
            };
            visit_input_byte();
            let (decoded, width) = if byte == b'%'
                && let Some((&high, &low)) = bytes.get(cursor + 1).zip(bytes.get(cursor + 2))
                && let (Some(high), Some(low)) = (hex_value(high), hex_value(low))
            {
                visit_input_byte();
                visit_input_byte();
                (high << 4 | low, 3)
            } else {
                (byte, 1)
            };
            if !decoded.eq_ignore_ascii_case(&expected) {
                matched = false;
                break;
            }
            cursor += width;
        }
        if matched && bytes.get(cursor) == Some(&b'=') {
            visit_input_byte();
            return Some(cursor + 1);
        }
    }
    None
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Replace overlapping sensitive intervals together so processing one kind
/// of credential cannot move or erase boundaries needed to find another.
fn redact_spans(input: &str, mut spans: Vec<Range<usize>>) -> String {
    if spans.is_empty() {
        return input.to_owned();
    }
    spans.sort_by_key(|span| span.start);
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(spans.len());
    for span in spans {
        if let Some(previous) = merged.last_mut()
            && span.start <= previous.end
        {
            previous.end = previous.end.max(span.end);
            continue;
        }
        merged.push(span);
    }

    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    for span in merged {
        output.push_str(&input[cursor..span.start]);
        output.push_str("***");
        cursor = span.end;
    }
    output.push_str(&input[cursor..]);
    output
}

impl fmt::Display for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("memory"),
            Self::Sqlite { path } => write!(f, "sqlite:{}", path.display()),
            Self::MongoDb { uri, .. } => f.write_str(&redact_url(uri)),
            Self::File { dir } => write!(f, "file:{}", dir.display()),
            Self::Composite { records, blobs } => {
                let separator = if records.to_string().contains('?') {
                    '&'
                } else {
                    '?'
                };
                write!(
                    f,
                    "{records}{separator}blobs={}",
                    encode_query_component(&blobs.to_string())
                )
            }
        }
    }
}

impl fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageConfig({self})")
    }
}

/// Open the backend `config` names.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) naming the
/// Cargo feature to enable when the driver is not compiled in; otherwise the
/// driver's own connection error.
#[allow(
    clippy::unused_async,
    reason = "the network drivers this dispatches to connect asynchronously"
)]
pub async fn open(config: &StorageConfig) -> Result<Arc<dyn StorageBackend>> {
    match config {
        StorageConfig::Memory => Ok(Arc::new(MemoryStorage::new())),
        #[cfg(feature = "file")]
        StorageConfig::File { dir } => Ok(Arc::new(tinystoragedrivers_file::FileStorage::open(
            dir.clone(),
        )?)),
        #[cfg(feature = "sqlite")]
        StorageConfig::Sqlite { path } => Ok(Arc::new(
            tinystoragedrivers_sqlite::SqliteStorage::open(path)?,
        )),
        #[cfg(feature = "mongodb")]
        StorageConfig::MongoDb { uri, database } => Ok(Arc::new(
            tinystoragedrivers_mongodb::MongoStorage::connect(uri, database).await?,
        )),
        StorageConfig::Composite { records, blobs } => crate::open_composite(records, blobs).await,
        #[allow(
            unreachable_patterns,
            reason = "reachable unless every driver feature is enabled"
        )]
        other => Err(StorageError::invalid_input(format!(
            "storage URL `{other}` needs the `{}` driver, which is not available in this build of tinystoragedrivers",
            other.driver()
        ))),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
