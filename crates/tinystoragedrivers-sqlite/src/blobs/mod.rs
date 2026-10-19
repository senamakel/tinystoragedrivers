//! [`BlobStore`] on SQLite: one row per blob, ranged reads with `substr`.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobStore, BlobSweepEntry, BlobSweepPage, Capabilities, Capability, Clock,
    Result, Scope, StorageError, async_trait, clamp_range, is_reserved_blob_key,
    validate_sweep_page_request, validate_user_blob_key, validate_user_blob_prefix,
};

use crate::connection::Db;
use crate::error::during;
use crate::fence::{self, Fencing};
use crate::sql::{Tables, ident};

/// SQLite blobs bound to one scope.
#[derive(Clone)]
pub struct SqliteBlobs {
    db: Arc<Db>,
    tables: Arc<Tables>,
    scope: Scope,
    generation: tinystoragedrivers_core::ScopeGeneration,
    /// Milliseconds since the Unix epoch; every expiry check and sweep reads it.
    clock: Clock,
    /// Checked inside every write's transaction, when set.
    fencing: Option<Arc<Fencing>>,
    sweep_supported: bool,
}

impl fmt::Debug for SqliteBlobs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteBlobs")
            .field("scope", &self.scope)
            .field("fenced", &self.fencing.is_some())
            .finish_non_exhaustive()
    }
}

impl SqliteBlobs {
    pub(crate) fn new(
        db: Arc<Db>,
        tables: Arc<Tables>,
        scope: Scope,
        clock: Clock,
        fencing: Option<Arc<Fencing>>,
        sweep_supported: bool,
    ) -> Self {
        Self {
            db,
            tables,
            scope,
            generation: tinystoragedrivers_core::ScopeGeneration::default(),
            clock,
            fencing,
            sweep_supported,
        }
    }

    pub(crate) fn with_generation(
        mut self,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Self {
        self.generation = generation;
        self
    }

    /// Run a write. An unfenced write is one statement and runs as it is; a
    /// fenced one runs in an immediate transaction that checks the fence
    /// first.
    async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let generation = self.generation;
        let fencing = self.fencing.clone();
        self.db
            .run(move |conn| {
                fence::write(conn, fencing.as_deref(), &tables, |tx| {
                    crate::purge::check(tx, &tables, scope.as_str(), generation)?;
                    f(tx)
                })
            })
            .await
    }

    /// Read generation and data in one native snapshot.
    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let generation = self.generation;
        self.db
            .run(move |conn| {
                let tx = conn
                    .transaction()
                    .map_err(during("start blob read transaction"))?;
                crate::purge::check(&tx, &tables, scope.as_str(), generation)?;
                let value = f(&tx)?;
                tx.commit()
                    .map_err(during("finish blob read transaction"))?;
                Ok(value)
            })
            .await
    }

    /// The injected clock's reading, as the signed value SQLite stores.
    fn now_ms(&self) -> i64 {
        i64::try_from((self.clock)()).unwrap_or(i64::MAX)
    }

    fn table(&self) -> String {
        ident(&self.tables.blobs)
    }

    /// Write a blob, optionally with a lifetime counted from now.
    async fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
    ) -> Result<BlobMeta> {
        validate_user_blob_key(key)?;
        let sql = format!(
            "INSERT INTO {} (scope, key, content_type, size, sha256, bytes, expires_ms, written_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT (scope, key) DO UPDATE SET content_type = excluded.content_type, size = excluded.size,
                 sha256 = excluded.sha256, bytes = excluded.bytes, expires_ms = excluded.expires_ms, written_ms = excluded.written_ms",
            self.table()
        );
        let sweep = format!(
            "DELETE FROM {} WHERE scope = ?1 AND expires_ms IS NOT NULL AND expires_ms <= ?2",
            self.table()
        );
        let described = BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        };
        // A plain write clears any lifetime the key had.
        let now = self.now_ms();
        let expires_at =
            ttl.map(|ttl| now.saturating_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX)));
        let (scope, row) = (self.scope.clone(), described.clone());
        self.write(move |conn| {
            if expires_at.is_some() {
                // Lapsed blobs are invisible; reclaim them where a lifetime
                // write already pays for the clock.
                conn.execute(&sweep, params![scope.as_str(), now])
                    .map_err(during("sweep expired blobs"))?;
            }
            conn.execute(
                &sql,
                params![
                    scope.as_str(),
                    row.key,
                    row.content_type,
                    i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                    digest(&bytes),
                    bytes,
                    expires_at,
                    now
                ],
            )
            .map_err(during("put blob"))
        })
        .await?;
        Ok(described)
    }
}

/// The SHA-256 of a blob's bytes, as the raw 32-byte digest the `sha256`
/// column stores.
pub(crate) fn digest(bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).to_vec()
}

/// Visible rows: no lifetime, or one still in the future.
const LIVE: &str = "(expires_ms IS NULL OR expires_ms > ?";

fn meta(key: String, len: i64, content_type: Option<String>) -> BlobMeta {
    BlobMeta {
        key,
        len: u64::try_from(len).unwrap_or(0),
        content_type,
    }
}

#[async_trait]
impl BlobStore for SqliteBlobs {
    async fn sweep_expired(&self) -> Result<u64> {
        let sweep = format!(
            "DELETE FROM {} WHERE scope = ?1 AND expires_ms IS NOT NULL AND expires_ms <= ?2",
            self.table()
        );
        let (scope, now) = (self.scope.clone(), self.now_ms());
        let removed = self
            .write(move |conn| {
                conn.execute(&sweep, params![scope.as_str(), now])
                    .map_err(during("sweep expired blobs"))
            })
            .await?;
        Ok(removed as u64)
    }

    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none().with(Capability::BlobTtl);
        if self.sweep_supported {
            capabilities.with(Capability::BlobSweep)
        } else {
            capabilities
        }
    }

    async fn list_sweep_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobSweepPage> {
        validate_sweep_page_request(prefix, after, limit)?;
        if !self.sweep_supported {
            return Err(StorageError::unsupported(
                Capability::BlobSweep,
                "legacy schema has no blob write times",
            ));
        }
        let sql = format!(
            "SELECT key, size, content_type, written_ms FROM {} WHERE scope = ?1 AND substr(key, 1, length(?2)) = ?2 AND (?3 IS NULL OR key > ?3) AND substr(key, 1, length(?4)) <> ?4 ORDER BY key LIMIT ?5",
            self.table()
        );
        let page_limit = limit;
        let (scope, prefix, after, sql_limit) = (
            self.scope.clone(),
            prefix.to_owned(),
            after.map(str::to_owned),
            i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX),
        );
        let reserved = tinystoragedrivers_core::SEALED_BLOB_PREFIX;
        let rows = self
            .read(move |conn| {
                let mut statement = conn
                    .prepare(&sql)
                    .map_err(during("prepare blob sweep page"))?;
                let rows = statement
                    .query_map(
                        params![scope.as_str(), prefix, after, reserved, sql_limit],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, Option<String>>(2)?,
                                row.get::<_, Option<i64>>(3)?,
                            ))
                        },
                    )
                    .map_err(during("query blob sweep page"))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(during("read blob sweep page"))
            })
            .await?;
        let has_more = rows.len() > page_limit;
        let items: Vec<_> = rows
            .into_iter()
            .take(page_limit)
            .map(|(key, size, content_type, written)| BlobSweepEntry {
                meta: meta(key, size, content_type),
                written_at_ms: written.and_then(|value| u64::try_from(value).ok()),
            })
            .collect();
        let next_after = if has_more {
            items.last().map(|item| item.meta.key.clone())
        } else {
            None
        };
        Ok(BlobSweepPage { items, next_after })
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None).await
    }

    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, Some(ttl)).await
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_user_blob_key(key)?;
        let sql = format!(
            "SELECT content_type, bytes FROM {} WHERE scope = ?1 AND key = ?2 AND {LIVE}3)",
            self.table()
        );
        let (scope, key, now) = (self.scope.clone(), key.to_owned(), self.now_ms());
        self.read(move |conn| {
            let row: Option<(Option<String>, Vec<u8>)> = conn
                .query_row(&sql, params![scope.as_str(), key, now], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .optional()
                .map_err(during("get blob"))?;
            Ok(row.map(|(content_type, bytes)| Blob {
                meta: BlobMeta {
                    key,
                    len: bytes.len() as u64,
                    content_type,
                },
                bytes,
            }))
        })
        .await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_user_blob_key(key)?;
        // One statement reads and clamps (`substr` stops at the end of the
        // blob), so a concurrent replace cannot mix two versions of it.
        clamp_range(&range, 0)?;
        let sql = format!(
            "SELECT substr(bytes, ?3, ?4) FROM {} WHERE scope = ?1 AND key = ?2 AND {LIVE}5)",
            self.table()
        );
        let start = i64::try_from(range.start.saturating_add(1)).unwrap_or(i64::MAX);
        let count = i64::try_from(range.end - range.start).unwrap_or(i64::MAX);
        let (scope, key, now) = (self.scope.clone(), key.to_owned(), self.now_ms());
        self.read(move |conn| {
            conn.query_row(
                &sql,
                params![scope.as_str(), key, start, count, now],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(during("read blob range"))
        })
        .await
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_user_blob_key(key)?;
        let sql = format!(
            "SELECT content_type, length(bytes) FROM {} WHERE scope = ?1 AND key = ?2 AND {LIVE}3)",
            self.table()
        );
        let (scope, key, now) = (self.scope.clone(), key.to_owned(), self.now_ms());
        self.read(move |conn| {
            let row: Option<(Option<String>, i64)> = conn
                .query_row(&sql, params![scope.as_str(), key, now], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .optional()
                .map_err(during("head blob"))?;
            Ok(row.map(|(content_type, len)| meta(key, len, content_type)))
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        validate_user_blob_key(key)?;
        let sql = format!(
            "DELETE FROM {} WHERE scope = ?1 AND key = ?2 AND {LIVE}3)",
            self.table()
        );
        let expired = format!("DELETE FROM {} WHERE scope = ?1 AND key = ?2", self.table());
        let (scope, key, now) = (self.scope.clone(), key.to_owned(), self.now_ms());
        self.write(move |conn| {
            let live = conn
                .execute(&sql, params![scope.as_str(), key, now])
                .map_err(during("delete blob"))?;
            // A lapsed row is not a blob, but its storage can go with the call.
            conn.execute(&expired, params![scope.as_str(), key])
                .map_err(during("delete expired blob"))?;
            Ok(live > 0)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_user_blob_prefix(prefix)?;
        let sql = format!(
            "SELECT key, length(bytes), content_type FROM {} WHERE scope = ?1 AND {LIVE}2) ORDER BY key",
            self.table()
        );
        let (scope, prefix, now) = (self.scope.clone(), prefix.to_owned(), self.now_ms());
        self.read(move |conn| {
            let mut statement = conn.prepare(&sql).map_err(during("list blobs"))?;
            let rows: Vec<(String, i64, Option<String>)> = statement
                .query_map(params![scope.as_str(), now], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(during("list blobs"))?
                .collect::<rusqlite::Result<_>>()
                .map_err(during("list blobs"))?;
            Ok(rows
                .into_iter()
                .filter(|(key, _, _)| key.starts_with(&prefix) && !is_reserved_blob_key(key))
                .map(|(key, len, content_type)| meta(key, len, content_type))
                .collect())
        })
        .await
    }
}
