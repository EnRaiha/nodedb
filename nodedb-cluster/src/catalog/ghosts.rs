// SPDX-License-Identifier: BUSL-1.1

//! Ghost stub refcount persistence — survives restarts so ghost
//! edges aren't counted twice after a crash.

use redb::ReadableDatabase;

use crate::error::Result;
use crate::ghost::GhostTable;

use super::core::ClusterCatalog;
use super::schema::{GHOST_TABLE, catalog_err};

impl ClusterCatalog {
    /// Persist ghost stubs for a vShard.
    ///
    /// Called after each sweep or after ghost table mutations to ensure
    /// refcounts survive crash/restart.
    pub fn save_ghosts(&self, vshard_id: u32, ghost_table: &GhostTable) -> Result<()> {
        let bytes = ghost_table.to_bytes();
        let key = format!("ghosts:{vshard_id}");

        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load ghost stubs for a vShard. Returns None if no ghosts persisted.
    pub fn load_ghosts(&self, vshard_id: u32) -> Result<Option<GhostTable>> {
        let key = format!("ghosts:{vshard_id}");

        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;

        match table.get(key.as_str()).map_err(catalog_err)? {
            Some(guard) => Ok(GhostTable::from_bytes(guard.value())),
            None => Ok(None),
        }
    }

    /// Load all persisted ghost tables across all vShards.
    ///
    /// Returns `(vshard_id, GhostTable)` pairs for all vShards that have ghosts.
    pub fn load_all_ghosts(&self) -> Result<Vec<(u32, GhostTable)>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;

        let mut results = Vec::new();
        let range = table.range::<&str>(..).map_err(catalog_err)?;
        for entry in range {
            let (key, value) = entry.map_err(catalog_err)?;
            let key_str = key.value();
            if let Some(id_str) = key_str.strip_prefix("ghosts:")
                && let Ok(vshard_id) = id_str.parse::<u32>()
                && let Some(ghost_table) = GhostTable::from_bytes(value.value())
            {
                results.push((vshard_id, ghost_table));
            }
        }
        Ok(results)
    }

    /// Delete persisted ghosts for a vShard (after all ghosts purged).
    pub fn delete_ghosts(&self, vshard_id: u32) -> Result<()> {
        let key = format!("ghosts:{vshard_id}");

        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;
            let _ = table.remove(key.as_str()).map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ghost::GhostStub;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[test]
    fn ghost_persistence_roundtrip() {
        let (_dir, catalog) = temp_catalog();

        let mut ghosts = GhostTable::new();
        ghosts.insert(GhostStub::new("node-A".into(), 5, 3));
        ghosts.insert(GhostStub::new("node-B".into(), 10, 1));

        catalog.save_ghosts(42, &ghosts).unwrap();

        let loaded = catalog.load_ghosts(42).unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.resolve("node-A"), Some(5));
        assert_eq!(loaded.resolve("node-B"), Some(10));
        assert_eq!(loaded.get("node-A").unwrap().refcount, 3);
    }

    #[test]
    fn ghost_load_all() {
        let (_dir, catalog) = temp_catalog();

        let mut g1 = GhostTable::new();
        g1.insert(GhostStub::new("x".into(), 1, 1));
        catalog.save_ghosts(10, &g1).unwrap();

        let mut g2 = GhostTable::new();
        g2.insert(GhostStub::new("y".into(), 2, 2));
        catalog.save_ghosts(20, &g2).unwrap();

        let all = catalog.load_all_ghosts().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn ghost_delete() {
        let (_dir, catalog) = temp_catalog();

        let mut ghosts = GhostTable::new();
        ghosts.insert(GhostStub::new("z".into(), 3, 1));
        catalog.save_ghosts(99, &ghosts).unwrap();

        catalog.delete_ghosts(99).unwrap();
        assert!(catalog.load_ghosts(99).unwrap().is_none());
    }
}
