//! Persistent scope generations and atomic all-port erasure.
use crate::{
    connection::Db,
    error::during,
    fence::{self, Fencing},
    sql::{Tables, ident},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::Arc;
use tinystoragedrivers_core::{
    PurgeReceipt, PurgeRequest, PurgeStatus, Result, Scope, ScopeGeneration, ScopePurge,
    StorageError, async_trait,
};

fn admission(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
) -> Result<Option<(ScopeGeneration, String)>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            &format!(
                "SELECT generation, operation FROM {} WHERE scope = ?1",
                ident(&tables.admissions)
            ),
            [scope],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(during("read scope admission"))?;
    row.map(|(generation, operation)| {
        Ok((
            ScopeGeneration(
                u64::try_from(generation)
                    .map_err(|_| StorageError::serialization("invalid scope generation"))?,
            ),
            operation,
        ))
    })
    .transpose()
}

fn completed_receipt(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    operation: &str,
) -> Result<Option<ScopeGeneration>> {
    let generation: Option<i64> = conn
        .query_row(
            &format!(
                "SELECT generation FROM {} WHERE scope = ?1 AND operation = ?2",
                ident(&tables.purge_receipts)
            ),
            params![scope, operation],
            |row| row.get(0),
        )
        .optional()
        .map_err(during("read completed reset receipt"))?;
    generation
        .map(|generation| {
            u64::try_from(generation)
                .map(ScopeGeneration)
                .map_err(|_| StorageError::serialization("invalid receipt generation"))
        })
        .transpose()
}

/// Must run inside the same immediate transaction as every mutation.
pub(crate) fn check(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    expected: ScopeGeneration,
) -> Result<()> {
    let current = if let Some((generation, _)) = admission(conn, tables, scope)? {
        generation
    } else {
        let reset_history: bool = conn
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {} WHERE scope = ?1)",
                    ident(&tables.purge_receipts)
                ),
                [scope],
                |row| row.get(0),
            )
            .map_err(during("verify missing scope admission"))?;
        if reset_history {
            return Err(StorageError::serialization(
                "scope admission is missing despite reset history",
            ));
        }
        ScopeGeneration::default()
    };
    if current != expected {
        return Err(StorageError::scope_generation_rejected(
            "scope writer generation has been revoked",
        ));
    }
    Ok(())
}

pub(crate) struct SqlitePurge {
    pub(crate) db: Arc<Db>,
    pub(crate) tables: Arc<Tables>,
    pub(crate) scope: Scope,
    pub(crate) generation: ScopeGeneration,
    pub(crate) fencing: Option<Arc<Fencing>>,
}
impl std::fmt::Debug for SqlitePurge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePurge")
            .field("scope", &self.scope)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
#[async_trait]
impl ScopePurge for SqlitePurge {
    fn supports_binding_validation(&self) -> bool {
        true
    }

    fn supports_generation_validation(&self) -> bool {
        true
    }

    async fn validate_generation(&self) -> Result<()> {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let generation = self.generation;
        self.db
            .run(move |conn| {
                fence::write(conn, None, &tables, |tx| {
                    check(tx, &tables, scope.as_str(), generation)
                })
            })
            .await
    }

    async fn validate_binding(&self) -> Result<()> {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let generation = self.generation;
        let fencing = self.fencing.clone();
        self.db
            .run(move |conn| {
                fence::write(conn, fencing.as_deref(), &tables, |tx| {
                    check(tx, &tables, scope.as_str(), generation)
                })
            })
            .await
    }

    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        if self
            .fencing
            .as_ref()
            .is_some_and(|binding| binding.guard_scope() == &self.scope)
        {
            return Err(StorageError::invalid_input(
                "purge guard must be outside the erased scope",
            ));
        }
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let expected = self.generation;
        let fencing = self.fencing.clone();
        let request = request.clone();
        self.db.run(move |conn| fence::write(conn, fencing.as_deref(), &tables, |tx| {
            if let Some(generation) = completed_receipt(tx, &tables, scope.as_str(), &request.operation_id)? {
                    return Ok(PurgeReceipt { operation_id: request.operation_id.clone(), generation, status: PurgeStatus::Complete, stage: tinystoragedrivers_core::PurgeStage::Complete });
            }
            check(tx, &tables, scope.as_str(), expected)?;
            let generation = expected.0.checked_add(1).and_then(|next| i64::try_from(next).ok())
                .ok_or_else(|| StorageError::backend("scope generation space is exhausted"))?;
            for table in [&tables.docs, &tables.fields, &tables.fts, &tables.streams, &tables.stream_heads, &tables.blobs] {
                tx.execute(&format!("DELETE FROM {} WHERE scope = ?1", ident(table)), [scope.as_str()])
                    .map_err(during("purge scope data"))?;
            }
            tx.execute(&format!("INSERT INTO {} (scope, generation, operation) VALUES (?1, ?2, ?3)
                ON CONFLICT(scope) DO UPDATE SET generation=excluded.generation, operation=excluded.operation", ident(&tables.admissions)),
                params![scope.as_str(), generation, request.operation_id]).map_err(during("issue scope generation"))?;
            tx.execute(&format!("INSERT INTO {} (scope, operation, generation) VALUES (?1, ?2, ?3)", ident(&tables.purge_receipts)),
                params![scope.as_str(), request.operation_id, generation]).map_err(during("persist completed reset receipt"))?;
            Ok(PurgeReceipt { operation_id: request.operation_id.clone(), generation: ScopeGeneration(u64::try_from(generation).unwrap_or(0)), status: PurgeStatus::Complete, stage: tinystoragedrivers_core::PurgeStage::Complete })
        })).await
    }
    async fn status(&self, operation_id: &str) -> Result<Option<PurgeReceipt>> {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let operation_id = operation_id.to_owned();
        let fencing = self.fencing.clone();
        self.db
            .run(move |conn| {
                fence::write(conn, fencing.as_deref(), &tables, |tx| {
                    Ok(
                        completed_receipt(tx, &tables, scope.as_str(), &operation_id)?.map(
                            |generation| PurgeReceipt {
                                operation_id: operation_id.clone(),
                                generation,
                                status: PurgeStatus::Complete,
                                stage: tinystoragedrivers_core::PurgeStage::Complete,
                            },
                        ),
                    )
                })
            })
            .await
    }
}
