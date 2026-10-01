// SPDX-License-Identifier: BUSL-1.1

//! Cleanup owed for nodes that left the cluster.
//!
//! Applying `TopologyChange::Leave` writes one row here before it returns.
//! The row stays until the metadata cache holds no lease of that node and
//! no drain it proposed. The Leave post-apply, the boot drain, and the retry
//! worker drive the release and drain-end proposals; the singleton worker
//! makes them.
//!
//! Table: `node_id` -> the log index of the `Leave` entry.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

pub(super) const PENDING_LEAVE_CLEANUP: TableDefinition<u64, u64> =
    TableDefinition::new("_system.pending_leave_cleanup");

impl SystemCatalog {
    /// Record that `node_id` left at log index `raft_index` and owes cleanup.
    pub fn enqueue_pending_leave_cleanup(
        &self,
        node_id: u64,
        raft_index: u64,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("pending_leave_cleanup write txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_LEAVE_CLEANUP)
                .map_err(|e| catalog_err("open pending_leave_cleanup", e))?;
            table
                .insert(node_id, raft_index)
                .map_err(|e| catalog_err("insert pending_leave_cleanup", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("pending_leave_cleanup commit", e))
    }

    /// Drop the row of `node_id`. Removing an absent row succeeds.
    pub fn remove_pending_leave_cleanup(&self, node_id: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("pending_leave_cleanup write txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_LEAVE_CLEANUP)
                .map_err(|e| catalog_err("open pending_leave_cleanup", e))?;
            table
                .remove(node_id)
                .map_err(|e| catalog_err("remove pending_leave_cleanup", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("pending_leave_cleanup commit", e))
    }

    /// Every node that owes cleanup.
    pub fn load_pending_leave_cleanups(&self) -> crate::Result<Vec<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("pending_leave_cleanup read txn", e))?;
        let table = txn
            .open_table(PENDING_LEAVE_CLEANUP)
            .map_err(|e| catalog_err("open pending_leave_cleanup", e))?;
        let mut out = Vec::new();
        for item in table
            .iter()
            .map_err(|e| catalog_err("iterate pending_leave_cleanup", e))?
        {
            let (node_id, _) = item.map_err(|e| catalog_err("read pending_leave_cleanup", e))?;
            out.push(node_id.value());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_survive_reopen_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog.enqueue_pending_leave_cleanup(4, 10).unwrap();
            catalog.enqueue_pending_leave_cleanup(5, 11).unwrap();
            catalog.remove_pending_leave_cleanup(4).unwrap();
            catalog.remove_pending_leave_cleanup(9).unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.load_pending_leave_cleanups().unwrap(), vec![5]);
    }
}
