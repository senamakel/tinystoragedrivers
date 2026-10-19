//! SQL text the driver builds: quoting, JSON paths, and the generic schema.
//!
//! # Layout 2
//!
//! One database file holds these tables (the three `_tsd_` tables keep the
//! names they had before layout 2):
//!
//! ```text
//! docs(scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
//! doc_fields(scope, coll, id, path, kind, value)    declared fields, see crate::fields
//! streams(scope, name, seq, body)            stream_heads(scope, name, base, len, sealed)
//! blobs(scope, key, content_type, size, sha256, expires_ms, written_ms, bytes)
//! fts                                         FTS5, searchable collections only
//! meta(key, value)                            `layout` = 2
//! _tsd_collections, _tsd_scope_admissions, _tsd_purge_receipts
//! ```
//!
//! A removed document keeps its row with `deleted = 1` and the body `{}`, so
//! its version continues if the id is written again. Every read skips deleted
//! rows. [`upgrade`] converts a v0.x file (`_tsd_docs` and friends) in place.
//!
//! `docs.body` holds JSON text, or a compressed `BLOB` for a body over 4 KiB
//! ([`crate::body`]); the declared index and search fields live in
//! `doc_fields` ([`crate::fields`]), which is what indexes and SQL filters
//! read.
//!
//! Every name that reaches SQL text is either validated by the core (collection
//! names are `[A-Za-z0-9_.-]`) or quoted here; values are always
//! bound as parameters. The one place a string is inlined is the correlated
//! `doc_fields` sub-select of a keyset cursor, and it goes through [`literal`].

use rusqlite::{Connection, OptionalExtension};
use tinystoragedrivers_core::{Result, StorageError};

use crate::error::during;
use crate::fields;

mod upgrade;

/// Quote an SQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote an SQL string literal.
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The `meta.layout` value this driver writes and understands.
pub(crate) const LAYOUT: &str = "2";

/// The table names of the database.
#[derive(Debug, Clone)]
pub(crate) struct Tables {
    pub(crate) docs: String,
    pub(crate) fields: String,
    pub(crate) admissions: String,
    pub(crate) purge_receipts: String,
    pub(crate) specs: String,
    pub(crate) fts: String,
    pub(crate) streams: String,
    pub(crate) stream_heads: String,
    pub(crate) blobs: String,
    pub(crate) meta: String,
}

impl Tables {
    /// The table names.
    pub(crate) fn new() -> Self {
        let internal = |table: &str| format!("_tsd_{table}");
        let plain = |table: &str| table.to_owned();
        Self {
            docs: plain("docs"),
            fields: plain("doc_fields"),
            admissions: internal("scope_admissions"),
            purge_receipts: internal("purge_receipts"),
            specs: internal("collections"),
            fts: plain("fts"),
            streams: plain("streams"),
            stream_heads: plain("stream_heads"),
            blobs: plain("blobs"),
            meta: plain("meta"),
        }
    }

    /// Every table with its `CREATE` statement, in creation order.
    fn declarations(&self) -> Vec<(&str, String)> {
        let mut all = self.document_declarations();
        all.extend(self.stream_declarations());
        all
    }

    /// The internal, document and search tables.
    fn document_declarations(&self) -> Vec<(&str, String)> {
        let (docs, fields, admissions, purge_receipts, specs, fts) = (
            ident(&self.docs),
            ident(&self.fields),
            ident(&self.admissions),
            ident(&self.purge_receipts),
            ident(&self.specs),
            ident(&self.fts),
        );
        vec![
            (
                &self.admissions,
                format!(
                    "CREATE TABLE IF NOT EXISTS {admissions} (
                 scope TEXT PRIMARY KEY,
                 generation INTEGER NOT NULL,
                 operation TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.purge_receipts,
                format!(
                    "CREATE TABLE IF NOT EXISTS {purge_receipts} (
                 scope TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 PRIMARY KEY (scope, operation)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.docs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {docs} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 schema INTEGER NOT NULL DEFAULT 0,
                 created_ms INTEGER NOT NULL,
                 updated_ms INTEGER NOT NULL,
                 expires_ms INTEGER,
                 deleted INTEGER NOT NULL DEFAULT 0,
                 body TEXT NOT NULL,
                 PRIMARY KEY (scope, coll, id)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.fields,
                format!(
                    "CREATE TABLE IF NOT EXISTS {fields} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 path TEXT NOT NULL,
                 kind INTEGER NOT NULL,
                 value,
                 PRIMARY KEY (scope, coll, id, path)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.specs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {specs} (
                 coll TEXT PRIMARY KEY,
                 spec TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.fts,
                format!(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS {fts} USING fts5(
                 scope UNINDEXED, coll UNINDEXED, id UNINDEXED, body,
                 tokenize = 'unicode61'
             )"
                ),
            ),
        ]
    }

    /// The stream, blob and meta tables.
    fn stream_declarations(&self) -> Vec<(&str, String)> {
        let (streams, heads, blobs, meta) = (
            ident(&self.streams),
            ident(&self.stream_heads),
            ident(&self.blobs),
            ident(&self.meta),
        );
        vec![
            (
                &self.streams,
                format!(
                    "CREATE TABLE IF NOT EXISTS {streams} (
                 scope TEXT NOT NULL,
                 name TEXT NOT NULL,
                 seq INTEGER NOT NULL,
                 body TEXT NOT NULL,
                 PRIMARY KEY (scope, name, seq)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.stream_heads,
                format!(
                    "CREATE TABLE IF NOT EXISTS {heads} (
                 scope TEXT NOT NULL,
                 name TEXT NOT NULL,
                 base INTEGER NOT NULL,
                 len INTEGER NOT NULL,
                 sealed INTEGER NOT NULL DEFAULT 0,
                 sealed_ms INTEGER,
                 last_append_ms INTEGER,
                 PRIMARY KEY (scope, name)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.blobs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {blobs} (
                 scope TEXT NOT NULL,
                 key TEXT NOT NULL,
                 content_type TEXT,
                 size INTEGER NOT NULL,
                 sha256 BLOB NOT NULL,
                 expires_ms INTEGER,
                 written_ms INTEGER,
                 bytes BLOB NOT NULL,
                 PRIMARY KEY (scope, key)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.meta,
                format!(
                    "CREATE TABLE IF NOT EXISTS {meta} (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
        ]
    }

    /// Create every table if missing, upgrading a v0.x file first.
    ///
    /// - A file with the v0.x `_tsd_docs` table and no layout-2 `meta` table
    ///   is upgraded in one transaction (see [`upgrade`]).
    /// - A file that is already layout 2 is left alone.
    /// - A layout-2 file that still has v0.x tables is corrupt (an upgrade
    ///   commits atomically, so it cannot be left half done) and is refused.
    pub(crate) fn ensure(&self, conn: &Connection) -> Result<()> {
        let has_v0 = table_exists(conn, &upgrade::V0::new().docs)?;
        if has_v0 {
            if table_exists(conn, &self.meta)? && self.layout(conn)?.is_some() {
                return Err(StorageError::backend(
                    "corrupt sqlite storage: layout 2 file still has v0 tables",
                ));
            }
            upgrade::run(conn, self)?;
        } else {
            // A layout-2 file written before the `doc_fields` side table has
            // plain-text bodies and expression indexes over `body`.
            let had_fields = table_exists(conn, &self.fields)?;
            self.create(conn)?;
            if !had_fields {
                self.adopt_projection(conn)?;
            }
        }
        self.validate(conn)
    }

    /// Build the side table for data written without it, and drop the
    /// body-expression indexes it replaces. One transaction, idempotent: a
    /// file that already has the table never gets here.
    fn adopt_projection(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("BEGIN IMMEDIATE")
            .map_err(during("begin projection"))?;
        let built = self
            .drop_body_indexes(conn)
            .and_then(|()| fields::rebuild_all(conn, self));
        match built {
            Ok(()) => conn
                .execute_batch("COMMIT")
                .map_err(during("commit projection")),
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Drop the per-index expression indexes over `docs.body` that layout 2
    /// first created (`tsd_ix_<collection>_<index>`).
    fn drop_body_indexes(&self, conn: &Connection) -> Result<()> {
        let marker = "tsd_ix_";
        let mut statement = conn
            .prepare(
                "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = ?1
                 AND substr(name, 1, length(?2)) = ?2",
            )
            .map_err(during("find body indexes"))?;
        let names: Vec<String> = statement
            .query_map([self.docs.as_str(), marker], |row| row.get(0))
            .map_err(during("find body indexes"))?
            .collect::<rusqlite::Result<_>>()
            .map_err(during("find body indexes"))?;
        for name in names {
            conn.execute_batch(&format!("DROP INDEX {}", ident(&name)))
                .map_err(during("drop body index"))?;
        }
        Ok(())
    }

    /// Create the layout-2 tables, the expiry index and the layout marker.
    fn create(&self, conn: &Connection) -> Result<()> {
        let ddl: Vec<String> = self
            .declarations()
            .into_iter()
            .map(|(_, ddl)| ddl)
            .collect();
        conn.execute_batch(&ddl.join(";\n"))
            .map_err(during("create tables"))?;
        self.finish(conn)?;
        conn.execute(
            &format!(
                "INSERT OR IGNORE INTO {} (key, value) VALUES ('layout', ?1)",
                ident(&self.meta)
            ),
            [LAYOUT],
        )
        .map_err(during("record layout"))?;
        conn.execute_batch(&format!(
            "INSERT OR IGNORE INTO {} (scope, operation, generation) SELECT scope, operation, generation FROM {}",
            ident(&self.purge_receipts),
            ident(&self.admissions),
        ))
        .map_err(during("migrate purge receipts"))
    }

    /// Add the head time columns to a layout-2 file created without them.
    /// Idempotent; the layout stays 2.
    fn add_head_times(&self, conn: &Connection) -> Result<()> {
        for column in ["sealed_ms", "last_append_ms"] {
            if !upgrade::has_column(conn, &self.stream_heads, column)? {
                conn.execute_batch(&format!(
                    "ALTER TABLE {} ADD COLUMN {column} INTEGER",
                    ident(&self.stream_heads)
                ))
                .map_err(during("add stream head times"))?;
            }
        }
        Ok(())
    }

    /// Indexes that live beside the tables rather than in a collection spec.
    fn finish(&self, conn: &Connection) -> Result<()> {
        self.add_head_times(conn)?;
        if !upgrade::has_column(conn, &self.blobs, "written_ms")? {
            conn.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN written_ms INTEGER",
                ident(&self.blobs)
            ))
            .map_err(during("add blob write times"))?;
        }
        // Lets a lifetime write find lapsed blobs without reading every one.
        conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (expires_ms) WHERE expires_ms IS NOT NULL",
            ident(&format!("{}_expiry", self.blobs)),
            ident(&self.blobs)
        ))
        .map_err(during("index blob expiry"))?;
        // Serves equality, membership and keyset seeks on a projected field.
        conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (scope, coll, path, value, id)",
            ident(&format!("{}_lookup", self.fields)),
            ident(&self.fields)
        ))
        .map_err(during("index projected fields"))
    }

    /// The recorded layout, `None` when the file has no `layout` row.
    fn layout(&self, conn: &Connection) -> Result<Option<String>> {
        conn.query_row(
            &format!(
                "SELECT value FROM {} WHERE key = 'layout'",
                ident(&self.meta)
            ),
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(during("read layout"))
    }

    /// Inspect the exact driver layout without creating or migrating tables.
    pub(crate) fn validate(&self, conn: &Connection) -> Result<()> {
        let normalize = |sql: &str| {
            sql.to_ascii_uppercase()
                .replace("IF NOT EXISTS", "")
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>()
        };
        for (name, expected) in self.declarations() {
            let stored: Option<String> = conn
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                    [name],
                    |row| row.get(0),
                )
                .optional()
                .map_err(during("inspect existing schema"))?;
            let stored =
                stored.ok_or_else(|| StorageError::not_found("driver table does not exist"))?;
            let legacy_blobs = name == self.blobs
                && !upgrade::has_column(conn, &self.blobs, "written_ms")?
                && normalize(&stored).replace("WRITTEN_MSINTEGER,", "")
                    == normalize(&expected).replace("WRITTEN_MSINTEGER,", "");
            if normalize(&stored) != normalize(&expected) && !legacy_blobs {
                return Err(StorageError::invalid_input(
                    "existing driver table has incompatible schema",
                ));
            }
        }
        match self.layout(conn)?.as_deref() {
            Some(LAYOUT) => Ok(()),
            _ => Err(StorageError::invalid_input(
                "existing driver tables are not layout 2",
            )),
        }
    }

    pub(crate) fn blob_write_times(&self, conn: &Connection) -> Result<bool> {
        upgrade::has_column(conn, &self.blobs, "written_ms")
    }
}

/// Whether a table (or virtual table) named `name` exists.
pub(crate) fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [name],
        |row| row.get(0),
    )
    .map_err(during("inspect tables"))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
