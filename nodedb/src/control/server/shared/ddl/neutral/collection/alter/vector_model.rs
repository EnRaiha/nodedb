// SPDX-License-Identifier: BUSL-1.1

//! Embedding-model row upkeep for the column-shape `ALTER COLLECTION` handlers.
//!
//! `_system.vector_model_metadata` keys a row by
//! `(database_id, tenant_id, collection, column)`, so a column name change or
//! removal strands the row under the name the column no longer has. A stranded
//! row is invisible to every reader and is inherited whole — model, dimensions,
//! `strict_dimensions` — by the next column that takes the name.
//!
//! Each helper reads the row first and proposes nothing when the column carries
//! none, so a plain non-vector column never puts an entry through the metadata
//! raft group.

use nodedb_types::DatabaseId;

use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

use super::super::super::vector_replicate::{propose_delete_model, propose_put_model};
use super::support::err;

/// Drop `column`'s embedding-model row on every node.
pub(super) fn drop_vector_model_row(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
    column: &str,
) -> Result<(), DdlError> {
    let db = database_id.as_u64();
    if !model_row_exists(state, db, tenant_id, collection, column)? {
        return Ok(());
    }
    propose_delete_model(state, db, tenant_id, collection, column)
}

/// Re-key `old_column`'s embedding-model row onto `new_column` on every node.
///
/// The write lands before the delete, so an interrupted rename leaves the row
/// readable under one of the two names. The reverse order can lose it.
pub(super) fn move_vector_model_row(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
    old_column: &str,
    new_column: &str,
) -> Result<(), DdlError> {
    let db = database_id.as_u64();
    let Some(mut entry) = state
        .credentials
        .catalog()
        .get_vector_model(db, tenant_id, collection, old_column)
        .map_err(|e| err("XX000", format!("read vector model: {e}")))?
    else {
        return Ok(());
    };

    entry.column = new_column.to_string();
    propose_put_model(state, &entry)?;
    propose_delete_model(state, db, tenant_id, collection, old_column)
}

fn model_row_exists(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    column: &str,
) -> Result<bool, DdlError> {
    state
        .credentials
        .catalog()
        .get_vector_model(database_id, tenant_id, collection, column)
        .map(|row| row.is_some())
        .map_err(|e| err("XX000", format!("read vector model: {e}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::{VectorModelEntry, VectorModelMetadata};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    const TENANT: u64 = 4;
    const COLLECTION: &str = "chunks";

    fn test_state(name: &str) -> (tempfile::TempDir, Arc<SharedState>) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join(name)).expect("open test WAL"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (dir, state)
    }

    fn model(column: &str) -> VectorModelEntry {
        VectorModelEntry {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            column: column.to_string(),
            metadata: VectorModelMetadata {
                model: "all-MiniLM-L6-v2".to_string(),
                dimensions: 384,
                created_at: "2026-01-01".to_string(),
                strict_dimensions: true,
            },
        }
    }

    #[tokio::test]
    async fn a_rename_moves_the_model_row_instead_of_duplicating_it() {
        let (_dir, state) = test_state("vector-model-move.wal");
        let catalog = state.credentials.catalog();
        catalog.put_vector_model(&model("embedding")).expect("seed");

        move_vector_model_row(
            &state,
            DatabaseId::DEFAULT,
            TENANT,
            COLLECTION,
            "embedding",
            "vector",
        )
        .expect("move the row");

        let db = DatabaseId::DEFAULT.as_u64();
        assert!(
            catalog
                .get_vector_model(db, TENANT, COLLECTION, "embedding")
                .expect("read old")
                .is_none(),
            "the old key is gone, so nothing inherits it"
        );
        let moved = catalog
            .get_vector_model(db, TENANT, COLLECTION, "vector")
            .expect("read new")
            .expect("the row lands under the new column");
        assert_eq!(moved.metadata.dimensions, 384);
        assert_eq!(moved.column, "vector");
    }

    #[tokio::test]
    async fn a_rename_of_a_column_without_a_model_row_writes_nothing() {
        let (_dir, state) = test_state("vector-model-absent.wal");

        move_vector_model_row(
            &state,
            DatabaseId::DEFAULT,
            TENANT,
            COLLECTION,
            "quantity",
            "amount",
        )
        .expect("no row to move");

        let db = DatabaseId::DEFAULT.as_u64();
        let catalog = state.credentials.catalog();
        assert!(
            catalog
                .get_vector_model(db, TENANT, COLLECTION, "amount")
                .expect("read new")
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_drop_removes_the_model_row() {
        let (_dir, state) = test_state("vector-model-drop.wal");
        let catalog = state.credentials.catalog();
        catalog.put_vector_model(&model("embedding")).expect("seed");

        drop_vector_model_row(&state, DatabaseId::DEFAULT, TENANT, COLLECTION, "embedding")
            .expect("drop the row");

        assert!(
            catalog
                .get_vector_model(
                    DatabaseId::DEFAULT.as_u64(),
                    TENANT,
                    COLLECTION,
                    "embedding"
                )
                .expect("read")
                .is_none()
        );
    }
}
