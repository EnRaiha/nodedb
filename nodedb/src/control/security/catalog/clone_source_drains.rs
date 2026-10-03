// SPDX-License-Identifier: BUSL-1.1

//! `_system.clone_source_drains`: one row per clone collection whose
//! materializer drains its KV source.
//!
//! The row is written through the metadata log before the drain starts and
//! removed after it ends, so every node, and every later singleton worker,
//! knows which source drains a materialization owns. Recovery ends a drain
//! whose rows all name clone collections that no longer need a copy.

use nodedb_types::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};

use super::types::{SystemCatalog, catalog_err};

/// Key: `(clone database, tenant, clone collection)`. Value: zerompk
/// [`CloneSourceDrain`].
pub const CLONE_SOURCE_DRAINS: redb::TableDefinition<(u64, u64, &str), &[u8]> =
    redb::TableDefinition::new("_system.clone_source_drains");

/// One materialization's claim on its source collection's drain.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct CloneSourceDrain {
    pub clone_database: u64,
    pub tenant_id: u64,
    pub clone_collection: String,
    pub source_database: u64,
    pub source_collection: String,
}

impl CloneSourceDrain {
    pub fn clone_database_id(&self) -> DatabaseId {
        DatabaseId::new(self.clone_database)
    }

    pub fn source_database_id(&self) -> DatabaseId {
        DatabaseId::new(self.source_database)
    }
}

impl SystemCatalog {
    /// Record a materialization's drain claim. Overwrites the same claim.
    pub fn put_clone_source_drain(&self, row: &CloneSourceDrain) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(row)
            .map_err(|e| catalog_err("serialize clone_source_drain", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_source_drains write txn", e))?;
        {
            let mut table = txn
                .open_table(CLONE_SOURCE_DRAINS)
                .map_err(|e| catalog_err("open clone_source_drains", e))?;
            table
                .insert(
                    (
                        row.clone_database,
                        row.tenant_id,
                        row.clone_collection.as_str(),
                    ),
                    bytes.as_slice(),
                )
                .map_err(|e| catalog_err("insert clone_source_drains", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_source_drains commit", e))
    }

    /// Remove a drain claim. Idempotent.
    pub fn delete_clone_source_drain(
        &self,
        clone_database: u64,
        tenant_id: u64,
        clone_collection: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_source_drains delete txn", e))?;
        {
            let mut table = txn
                .open_table(CLONE_SOURCE_DRAINS)
                .map_err(|e| catalog_err("open clone_source_drains", e))?;
            table
                .remove((clone_database, tenant_id, clone_collection))
                .map_err(|e| catalog_err("remove clone_source_drains", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_source_drains delete commit", e))
    }

    /// Every drain claim.
    pub fn list_clone_source_drains(&self) -> crate::Result<Vec<CloneSourceDrain>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_source_drains read txn", e))?;
        let table = txn
            .open_table(CLONE_SOURCE_DRAINS)
            .map_err(|e| catalog_err("open clone_source_drains", e))?;
        let mut rows = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| catalog_err("iter clone_source_drains", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read clone_source_drains", e))?;
            rows.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser clone_source_drain", e))?,
            );
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_roundtrip_and_delete_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let row = CloneSourceDrain {
            clone_database: 1025,
            tenant_id: 1,
            clone_collection: "kv".into(),
            source_database: 1024,
            source_collection: "kv".into(),
        };
        catalog.put_clone_source_drain(&row).unwrap();
        catalog.put_clone_source_drain(&row).unwrap();
        assert_eq!(catalog.list_clone_source_drains().unwrap(), vec![row]);
        catalog.delete_clone_source_drain(1025, 1, "kv").unwrap();
        catalog.delete_clone_source_drain(1025, 1, "kv").unwrap();
        assert!(catalog.list_clone_source_drains().unwrap().is_empty());
    }
}
