// SPDX-License-Identifier: BUSL-1.1

#[cfg(any(feature = "graphalytics-runner", test))]
use std::time::Instant;

use nodedb_types::{DatabaseId, TenantId};
#[cfg(any(feature = "graphalytics-runner", test))]
use redb::Durability;
use redb::{ReadableDatabase, ReadableTable};

use super::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};
use super::temporal::{is_sentinel, parse_versioned_edge_key, versioned_edge_key};

/// One exported forward edge: `(database, tenant, composite_key, properties)`.
pub type EdgeSnapshotRecord = (DatabaseId, TenantId, String, Vec<u8>);

/// One bulk-import edge with precomputed forward and reverse keys:
/// `(database, tenant, forward_key, reverse_key, properties)`.
#[cfg(any(feature = "graphalytics-runner", test))]
pub(crate) type EdgeImportRecord = (DatabaseId, TenantId, String, String, Vec<u8>);

/// Per-batch timings and byte counts for the Graphalytics deferred importer.
///
/// The importer runs on a worker thread. These are worker-active durations,
/// not additive wall-clock stages when the producer pipeline overlaps it.
#[cfg(any(feature = "graphalytics-runner", test))]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DeferredImportProfile {
    pub(crate) forward_records: u64,
    pub(crate) reverse_records: u64,
    pub(crate) forward_value_bytes: u64,
    pub(crate) reverse_value_bytes: u64,
    pub(crate) reverse_sort_seconds: f64,
    pub(crate) forward_insert_seconds: f64,
    pub(crate) reverse_insert_seconds: f64,
    pub(crate) deferred_commit_seconds: f64,
    pub(crate) final_durability_barrier_seconds: f64,
}

#[cfg(any(feature = "graphalytics-runner", test))]
impl DeferredImportProfile {
    pub(crate) fn merge(&mut self, other: Self) {
        self.forward_records += other.forward_records;
        self.reverse_records += other.reverse_records;
        self.forward_value_bytes += other.forward_value_bytes;
        self.reverse_value_bytes += other.reverse_value_bytes;
        self.reverse_sort_seconds += other.reverse_sort_seconds;
        self.forward_insert_seconds += other.forward_insert_seconds;
        self.reverse_insert_seconds += other.reverse_insert_seconds;
        self.deferred_commit_seconds += other.deferred_commit_seconds;
        self.final_durability_barrier_seconds += other.final_durability_barrier_seconds;
    }
}

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
        self.import_edge_pairs_inner(edges, false, None)
    }

    /// Persist all preceding deferred import commits.
    #[cfg(any(feature = "graphalytics-runner", test))]
    pub(crate) fn flush_deferred_imports(&self) -> crate::Result<()> {
        self.flush_deferred_imports_inner(None)
    }

    /// Persist preceding deferred commits and report its worker-active duration.
    #[cfg(any(feature = "graphalytics-runner", test))]
    pub(crate) fn flush_deferred_imports_profiled(&self) -> crate::Result<DeferredImportProfile> {
        let mut profile = DeferredImportProfile::default();
        self.flush_deferred_imports_inner(Some(&mut profile))?;
        Ok(profile)
    }

    #[cfg(any(feature = "graphalytics-runner", test))]
    fn flush_deferred_imports_inner(
        &self,
        profile: Option<&mut DeferredImportProfile>,
    ) -> crate::Result<()> {
        let started = profile.as_ref().map(|_| Instant::now());
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin import flush", e))?;
        write_txn
            .commit()
            .map_err(|e| redb_err("commit import flush", e))?;
        if let (Some(profile), Some(started)) = (profile, started) {
            profile.final_durability_barrier_seconds = started.elapsed().as_secs_f64();
        }
        Ok(())
    }

    #[cfg(any(feature = "graphalytics-runner", test))]
    fn import_edge_pairs_inner(
        &self,
        edges: &mut [EdgeImportRecord],
        durable: bool,
        mut profile: Option<&mut DeferredImportProfile>,
    ) -> crate::Result<()> {
        if let Some(profile) = profile.as_deref_mut() {
            profile.forward_records = edges.len() as u64;
            profile.reverse_records = edges.len() as u64;
            profile.forward_value_bytes = edges
                .iter()
                .map(|(_, _, _, _, value)| value.len() as u64)
                .sum();
            profile.reverse_value_bytes = edges
                .iter()
                .map(|(_, _, _, _, value)| {
                    is_sentinel(value)
                        .then_some(value.len() as u64)
                        .unwrap_or_default()
                })
                .sum();
        }
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin edge-pair import", e))?;
        if !durable {
            write_txn
                .set_durability(Durability::None)
                .map_err(|e| redb_err("defer edge-pair durability", e))?;
        }
        let forward_started = profile.as_ref().map(|_| Instant::now());
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
        if let (Some(profile), Some(started)) = (profile.as_deref_mut(), forward_started) {
            profile.forward_insert_seconds = started.elapsed().as_secs_f64();
        }

        let reverse_sort_started = profile.as_ref().map(|_| Instant::now());
        edges.sort_unstable_by(|left, right| {
            (left.0.as_u64(), left.1.as_u64(), left.3.as_str()).cmp(&(
                right.0.as_u64(),
                right.1.as_u64(),
                right.3.as_str(),
            ))
        });
        if let (Some(profile), Some(started)) = (profile.as_deref_mut(), reverse_sort_started) {
            profile.reverse_sort_seconds = started.elapsed().as_secs_f64();
        }

        let reverse_insert_started = profile.as_ref().map(|_| Instant::now());
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
        if let (Some(profile), Some(started)) = (profile.as_deref_mut(), reverse_insert_started) {
            profile.reverse_insert_seconds = started.elapsed().as_secs_f64();
        }

        let commit_started = profile.as_ref().map(|_| Instant::now());
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge-pair import", e))?;
        if let (Some(profile), Some(started)) = (profile, commit_started) {
            profile.deferred_commit_seconds = started.elapsed().as_secs_f64();
        }
        Ok(())
    }

    /// Import a deferred Graphalytics batch and report storage-worker stages.
    #[cfg(any(feature = "graphalytics-runner", test))]
    pub(crate) fn import_edge_pairs_deferred_profiled(
        &self,
        edges: &mut [EdgeImportRecord],
    ) -> crate::Result<DeferredImportProfile> {
        let mut profile = DeferredImportProfile::default();
        self.import_edge_pairs_inner(edges, false, Some(&mut profile))?;
        Ok(profile)
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
        let value = EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec())
            .encode()
            .unwrap();
        let mut records = [(
            DatabaseId::DEFAULT,
            tenant,
            versioned_edge_key("g", "a", "edge", "b", 1).unwrap(),
            versioned_edge_key("g", "b", "edge", "a", 1).unwrap(),
            value,
        )];

        store
            .import_edge_pairs_inner(&mut records, true, None)
            .unwrap();

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

    #[test]
    fn profiled_deferred_import_preserves_multiple_batches_after_final_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("profiled.redb");
        let store = EdgeStore::open(&path).unwrap();
        let tenant = TenantId::new(7);
        let value = EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec())
            .encode()
            .unwrap();
        let mut first = [(
            DatabaseId::DEFAULT,
            tenant,
            versioned_edge_key("g", "a", "edge", "b", 1).unwrap(),
            versioned_edge_key("g", "b", "edge", "a", 1).unwrap(),
            value.clone(),
        )];
        let mut second = [(
            DatabaseId::DEFAULT,
            tenant,
            versioned_edge_key("g", "c", "edge", "d", 2).unwrap(),
            versioned_edge_key("g", "d", "edge", "c", 2).unwrap(),
            value,
        )];

        let first_profile = store
            .import_edge_pairs_deferred_profiled(&mut first)
            .unwrap();
        let second_profile = store
            .import_edge_pairs_deferred_profiled(&mut second)
            .unwrap();
        store.flush_deferred_imports_profiled().unwrap();
        drop(store);

        let reopened = EdgeStore::open(&path).unwrap();
        assert_eq!(
            first_profile.forward_records + second_profile.forward_records,
            2
        );
        assert_eq!(
            first_profile.reverse_records + second_profile.reverse_records,
            2
        );
        assert!(
            reopened
                .get_edge(0, tenant, "g", "a", "edge", "b")
                .unwrap()
                .is_some()
        );
        assert!(
            reopened
                .get_edge(0, tenant, "g", "c", "edge", "d")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            reopened
                .neighbors_in(0, tenant, "g", "b", Some("edge"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            reopened
                .neighbors_in(0, tenant, "g", "d", Some("edge"))
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
            EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec())
                .encode()
                .unwrap(),
        )];
        {
            let store = EdgeStore::open(&path).unwrap();
            store.import_edge_pairs_deferred(&mut records).unwrap();
            store.flush_deferred_imports().unwrap();
        }

        let reopened = EdgeStore::open(&path).unwrap();
        assert!(
            reopened
                .get_edge(0, tenant, "g", "a", "edge", "b")
                .unwrap()
                .is_some()
        );
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
