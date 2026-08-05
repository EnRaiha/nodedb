// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};
use super::temporal::{is_sentinel, parse_versioned_edge_key, versioned_edge_key};

/// One exported forward edge: `(database, tenant, composite_key, properties)`.
pub type EdgeSnapshotRecord = (DatabaseId, TenantId, String, Vec<u8>);

impl EdgeStore {
    /// Export all forward edges for snapshot transfer.
    pub fn export_edges(&self) -> crate::Result<Vec<EdgeSnapshotRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|error| redb_err("read txn", error))?;
        let table = txn
            .open_table(EDGES)
            .map_err(|error| redb_err("open edges", error))?;
        let mut pairs = Vec::new();
        for entry in table
            .iter()
            .map_err(|error| redb_err("iterate edges", error))?
        {
            let (key, value) = entry.map_err(|error| redb_err("read edge", error))?;
            let (database, tenant, composite) = key.value();
            pairs.push((
                DatabaseId::new(database),
                TenantId::new(tenant),
                composite.to_owned(),
                value.value().to_vec(),
            ));
        }
        Ok(pairs)
    }

    /// Import bounded forward-edge batches in one durable transaction.
    ///
    /// The reverse index is rebuilt atomically from the supplied forward
    /// records. For pre-sorted whole-store restores, use
    /// [`Self::restore_sorted_at_path`] instead.
    pub fn import_edges(&self, edges: &[EdgeSnapshotRecord]) -> crate::Result<()> {
        let mut parsed = Vec::with_capacity(edges.len());
        for (database, tenant, key, value) in edges {
            let (collection, source, label, destination, system_from) =
                parse_versioned_edge_key(key).ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("import_edges: malformed versioned key {key:?}"),
                })?;
            parsed.push((
                database.as_u64(),
                tenant.as_u64(),
                key.as_str(),
                value.as_slice(),
                versioned_edge_key(collection, destination, label, source, system_from)?,
            ));
        }
        let transaction = self
            .db
            .begin_write()
            .map_err(|error| redb_err("begin snapshot import", error))?;
        {
            let mut forward = transaction
                .open_table(EDGES)
                .map_err(|error| redb_err("open edges", error))?;
            for (database, tenant, key, value, _) in &parsed {
                forward
                    .insert((*database, *tenant, *key), *value)
                    .map_err(|error| redb_err("insert imported edge", error))?;
            }
        }
        parsed.sort_unstable_by(|left, right| {
            (left.0, left.1, left.4.as_str()).cmp(&(right.0, right.1, right.4.as_str()))
        });
        {
            let mut reverse = transaction
                .open_table(REVERSE_EDGES)
                .map_err(|error| redb_err("open reverse edges", error))?;
            for (database, tenant, _, value, key) in &parsed {
                let reverse_value: &[u8] = if is_sentinel(value) { value } else { &[] };
                reverse
                    .insert((*database, *tenant, key.as_str()), reverse_value)
                    .map_err(|error| redb_err("insert imported reverse edge", error))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| redb_err("commit snapshot import", error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::EdgeValuePayload;

    #[test]
    fn batch_import_builds_forward_and_reverse_indexes() {
        let directory = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&directory.path().join("graph.redb")).unwrap();
        let tenant = TenantId::new(7);
        let value = EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec())
            .encode()
            .unwrap();
        let records = [(
            DatabaseId::DEFAULT,
            tenant,
            versioned_edge_key("g", "a", "edge", "b", 1).unwrap(),
            value,
        )];
        store.import_edges(&records).unwrap();
        assert!(
            store
                .get_edge(0, tenant, "g", "a", "edge", "b")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .neighbors_in(0, tenant, "g", "b", Some("edge"))
                .unwrap()
                .len(),
            1
        );
    }
}
