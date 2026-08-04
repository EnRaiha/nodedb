// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};
#[cfg(any(feature = "graphalytics-runner", test))]
use redb::Durability;

use super::store::{EDGES, REVERSE_EDGES, EdgeStore, redb_err};
use super::temporal::{is_sentinel, parse_versioned_edge_key, versioned_edge_key};

/// One exported forward edge: `(database, tenant, composite_key, properties)`.
pub type EdgeSnapshotRecord = (DatabaseId, TenantId, String, Vec<u8>);

/// One bulk-import edge with precomputed forward and reverse keys:
/// `(database, tenant, forward_key, reverse_key, properties)`.
#[cfg(any(feature = "graphalytics-runner", test))]
pub(crate) type EdgeImportRecord = (DatabaseId, TenantId, String, String, Vec<u8>);

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

    /// Import records whose forward and reverse keys were prepared by the
    /// caller. Records are reordered by reverse key after the forward index
    /// has been written, avoiding key reparsing and random B-tree insertion.
    /// Import one chunk without forcing it to stable storage immediately.
    /// Call [`Self::flush_deferred_imports`] after the final chunk to make the
    /// complete bulk import durable.
    #[cfg(any(feature = "graphalytics-runner", test))]
    pub(crate) fn import_edge_pairs_deferred(
        &self,
        edges: &mut [EdgeImportRecord],
    ) -> crate::Result<()> {
        self.import_edge_pairs_inner(edges, false)
    }

    /// Persist all preceding deferred import commits.
    #[cfg(any(feature = "graphalytics-runner", test))]
    pub(crate) fn flush_deferred_imports(&self) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin import flush", e))?;
        write_txn
            .commit()
            .map_err(|e| redb_err("commit import flush", e))
    }

    #[cfg(any(feature = "graphalytics-runner", test))]
    fn import_edge_pairs_inner(
        &self,
        edges: &mut [EdgeImportRecord],
        durable: bool,
    ) -> crate::Result<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin edge-pair import", e))?;
        if !durable {
            write_txn
                .set_durability(Durability::None)
                .map_err(|e| redb_err("defer edge-pair durability", e))?;
        }
        {
            let mut forward = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            for (db, tid, key, _, value) in edges.iter() {
                forward
                    .insert((db.as_u64(), tid.as_u64(), key.as_str()), value.as_slice())
                    .map_err(|e| redb_err("insert imported edge", e))?;
            }
        }

        edges.sort_unstable_by(|left, right| {
            (left.0.as_u64(), left.1.as_u64(), left.3.as_str()).cmp(&(
                right.0.as_u64(),
                right.1.as_u64(),
                right.3.as_str(),
            ))
        });
        {
            let mut reverse = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
            for (db, tid, _, reverse_key, value) in edges.iter() {
                let reverse_value: &[u8] = if is_sentinel(value) { value } else { &[] };
                reverse
                    .insert(
                        (db.as_u64(), tid.as_u64(), reverse_key.as_str()),
                        reverse_value,
                    )
                    .map_err(|e| redb_err("insert imported reverse edge", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge-pair import", e))?;
        Ok(())
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
            for (db, tid, key, value, _) in &parsed {
                forward
                    .insert((*db, *tid, *key), *value)
                    .map_err(|e| redb_err("insert imported edge", e))?;
            }
        }

        // Snapshot input is normally ordered by the forward key. The reverse
        // keys are therefore effectively random, which causes pathological
        // B-tree page churn at Graphalytics scale. Insert each index in its
        // own key order while retaining one atomic transaction.
        parsed.sort_unstable_by(|left, right| {
            (left.0, left.1, left.4.as_str()).cmp(&(right.0, right.1, right.4.as_str()))
        });
        {
            let mut reverse = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
            for (db, tid, _, value, reverse_key) in &parsed {
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
    fn edge_pair_import_builds_forward_and_reverse_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        let tenant = TenantId::new(7);
        let value = EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec()).encode().unwrap();
        let mut records = [(
            DatabaseId::DEFAULT,
            tenant,
            versioned_edge_key("g", "a", "edge", "b", 1).unwrap(),
            versioned_edge_key("g", "b", "edge", "a", 1).unwrap(),
            value,
        )];

        store.import_edge_pairs_inner(&mut records, true).unwrap();

        assert!(store.get_edge(0, tenant, "g", "a", "edge", "b").unwrap().is_some());
        assert_eq!(
            store
                .neighbors_in(0, tenant, "g", "b", Some("edge"))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn deferred_edge_pair_import_is_durable_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let tenant = TenantId::new(7);
        let mut records = [(
            DatabaseId::DEFAULT,
            tenant,
            versioned_edge_key("g", "a", "edge", "b", 1).unwrap(),
            versioned_edge_key("g", "b", "edge", "a", 1).unwrap(),
            EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec()).encode().unwrap(),
        )];
        {
            let store = EdgeStore::open(&path).unwrap();
            store.import_edge_pairs_deferred(&mut records).unwrap();
            store.flush_deferred_imports().unwrap();
        }

        let reopened = EdgeStore::open(&path).unwrap();
        assert!(reopened.get_edge(0, tenant, "g", "a", "edge", "b").unwrap().is_some());
        assert_eq!(
            reopened
                .neighbors_in(0, tenant, "g", "b", Some("edge"))
                .unwrap()
                .len(),
            1
        );
    }

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
