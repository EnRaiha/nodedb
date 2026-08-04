// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::store::{EDGES, REVERSE_EDGES, EdgeStore, redb_err};
use super::temporal::{is_sentinel, parse_versioned_edge_key, versioned_edge_key};

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

    /// Import a batch of forward edge records in one durable transaction.
    ///
    /// The caller controls transaction size by slicing a larger snapshot into
    /// bounded batches. The reverse index is rebuilt atomically with every
    /// forward record, matching [`EdgeStore::put_edge_raw`] semantics without
    /// paying one transaction commit per edge.
    pub fn import_edges(&self, edges: &[EdgeSnapshotRecord]) -> crate::Result<()> {
        let mut parsed = Vec::with_capacity(edges.len());
        for (db, tid, key, value) in edges {
            let (collection, src, label, dst, system_from) = parse_versioned_edge_key(key)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("import_edges: malformed versioned key {key:?}"),
                })?;
            parsed.push((
                db.as_u64(),
                tid.as_u64(),
                key.as_str(),
                value.as_slice(),
                versioned_edge_key(collection, dst, label, src, system_from)?,
            ));
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin snapshot import", e))?;
        {
            let mut forward = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let mut reverse = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
            for (db, tid, key, value, reverse_key) in &parsed {
                forward
                    .insert((*db, *tid, *key), *value)
                    .map_err(|e| redb_err("insert imported edge", e))?;
                let reverse_value: &[u8] = if is_sentinel(value) { value } else { &[] };
                reverse
                    .insert((*db, *tid, reverse_key.as_str()), reverse_value)
                    .map_err(|e| redb_err("insert imported reverse edge", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit snapshot import", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::EdgeValuePayload;

    #[test]
    fn batch_import_builds_forward_and_reverse_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        let tenant = TenantId::new(7);
        let value = EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec()).encode().unwrap();
        let records = [(
            DatabaseId::DEFAULT,
            tenant,
            versioned_edge_key("g", "a", "edge", "b", 1).unwrap(),
            value,
        )];

        store.import_edges(&records).unwrap();

        assert!(store.get_edge(0, tenant, "g", "a", "edge", "b").unwrap().is_some());
        assert_eq!(
            store
                .neighbors_in(0, tenant, "g", "b", Some("edge"))
                .unwrap()
                .len(),
            1
        );
    }
}
