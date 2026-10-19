//! Choosing and opening a backend from a URL.
//!
//! A host reads one string from its configuration (`memory`,
//! `sqlite:/var/lib/app`, `mongodb://db.internal/openhuman`) and hands it to
//! [`StorageConfig::parse`] then [`open`]. A URL for a driver this build did
//! not compile in is rejected with the Cargo feature to enable, so a
//! misconfigured deployment fails at boot with an actionable message.

use std::fmt;
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
            redact(url)
        )))
    }

    fn parse_mongo(url: &str) -> Result<Self> {
        let path = split_uri(url, Boundary::BeforeQuery).map_or("", |parts| parts.path);
        let database = path.split('/').next().unwrap_or_default();
        if database.is_empty() {
            return Err(StorageError::invalid_input(format!(
                "MongoDB URL `{}` names no database; add one as the path, e.g. mongodb://host/openhuman",
                redact(url)
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
    scheme: &'a str,
    userinfo: Option<&'a str>,
    hosts: &'a str,
    path: &'a str,
    query: Option<&'a str>,
}

/// Split a connection URI without trusting the credentials to be
/// percent-encoded: the userinfo ends at the *last* `@` (before the query, or
/// anywhere, per `boundary`), so a stray `/` or `@` inside a password cannot
/// move the boundary.
fn split_uri(url: &str, boundary: Boundary) -> Option<UriParts<'_>> {
    let (scheme, rest) = url.split_once("://")?;
    let at = match boundary {
        Boundary::BeforeQuery => rest.split('?').next().and_then(|head| head.rfind('@')),
        Boundary::Anywhere => rest.rfind('@'),
    };
    let (userinfo, tail) = match at {
        Some(at) => (rest.get(..at), rest.get(at + 1..).unwrap_or_default()),
        None => (None, rest),
    };
    let (before_query, query) = match tail.split_once('?') {
        Some((head, query)) => (head, Some(query)),
        None => (tail, None),
    };
    let (hosts, path) = before_query.split_once('/').unwrap_or((before_query, ""));
    Some(UriParts {
        scheme,
        userinfo,
        hosts,
        path,
        query,
    })
}

/// Where the userinfo of a connection URI may end.
#[derive(Clone, Copy)]
enum Boundary {
    /// At the last `@` before the query: the parse of a well-formed URI,
    /// where an `@` in a query value (`appName=a@b`) stays in the query.
    BeforeQuery,
    /// At the last `@` anywhere: for redaction, so an unencoded `?`, `/` or
    /// `@` in a password can only make more text hidden, never less.
    Anywhere,
}

/// Query options whose values are credentials (AWS session tokens ride in
/// `authMechanismProperties`).
const SECRET_OPTIONS: [&str; 4] = [
    "authmechanismproperties",
    "proxypassword",
    "tlscertificatekeyfilepassword",
    "password",
];

/// Hide every credential in a connection URI: the password, and the value of
/// any option in [`SECRET_OPTIONS`].
fn redact(url: &str) -> String {
    let Some(parts) = split_uri(url, Boundary::Anywhere) else {
        let Some((prefix, query)) = url.split_once('?') else {
            return url.to_owned();
        };
        return format!("{prefix}?{}", redact_query(query));
    };
    let mut out = format!("{}://", parts.scheme);
    if let Some(userinfo) = parts.userinfo {
        let user = userinfo.split_once(':').map_or(userinfo, |(user, _)| user);
        out.push_str(user);
        if userinfo.contains(':') {
            out.push_str(":***");
        }
        out.push('@');
    }
    out.push_str(parts.hosts);
    if !parts.path.is_empty() || parts.query.is_some() {
        out.push('/');
        out.push_str(parts.path);
    }
    if let Some(query) = parts.query {
        out.push('?');
        out.push_str(&redact_query(query));
    }
    out
}

fn redact_query(query: &str) -> String {
    query
        .split('&')
        .map(|option| match option.split_once('=') {
            Some((key, _)) if SECRET_OPTIONS.contains(&key.to_ascii_lowercase().as_str()) => {
                format!("{key}=***")
            }
            _ => option.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("&")
}

impl fmt::Display for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("memory"),
            Self::Sqlite { path } => write!(f, "sqlite:{}", path.display()),
            Self::MongoDb { uri, .. } => f.write_str(&redact(uri)),
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
