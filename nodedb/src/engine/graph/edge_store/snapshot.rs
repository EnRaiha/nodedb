// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::store::{EDGES, EdgeStore, redb_err};

/// One exported forward edge: `(database, tenant, composite_key, properties)`.
pub type EdgeSnapshotRecord = (DatabaseId, TenantId, String, Vec<u8>);

impl EdgeStore {
    /// Export all forward edges as [`EdgeSnapshotRecord`] tuples for snapshot
    /// transfer. Reverse index is rebuilt on restore from the forward
    /// records — not shipped separately.
    pub fn export_edges(&self) -> crate::Result<Vec<EdgeSnapshotRecord>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut pairs = Vec::new();
        for entry in table.iter().map_err(|e| redb_err("iter edges", e))? {
            let (k, v) = entry.map_err(|e| redb_err("read edge", e))?;
            let (db, tid, composite) = k.value();
            pairs.push((
                DatabaseId::new(db),
                TenantId::new(tid),
                composite.to_string(),
                v.value().to_vec(),
            ));
        }
        Ok(pairs)
    }

    /// Import edges from a snapshot. Each record is inserted via
    /// [`EdgeStore::put_edge_raw`], which maintains the reverse index
    /// atomically.
    pub fn import_edges(&self, edges: &[EdgeSnapshotRecord]) -> crate::Result<()> {
        for (db, tid, key, value) in edges {
            self.put_edge_raw(db.as_u64(), *tid, key, value)?;
        }
        Ok(())
    }
}
