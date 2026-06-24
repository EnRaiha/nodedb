// SPDX-License-Identifier: BUSL-1.1

//! Segment-level restore helpers: flushed timeseries partitions and
//! plain-columnar engines.
//!
//! Split from `restore.rs` to keep each file under the 500-line limit.
//! All methods are `pub(super)` — called only from `restore.rs`.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TsFlushedCollectionBlob;

impl CoreLoop {
    /// Restore flushed on-disk timeseries segment directories from snapshot blobs.
    ///
    /// For each captured partition:
    /// - Collision handling is fail-closed (no silent overwrites):
    ///   - If the partition dir already exists AND the registry already tracks a
    ///     partition at the same `min_ts`, we compare `row_count` and
    ///     `last_flushed_wal_lsn` to determine idempotency:
    ///     - Identical metadata → skip (idempotent re-apply).
    ///     - Different metadata → return `Storage` error (would clobber live data).
    ///   - Otherwise: create the directory, write all files, register in
    ///     `ts_registries` mirroring `flush_ts_collection`'s exact registration.
    pub(super) fn restore_flushed_ts_segments(
        &mut self,
        blobs: &[TsFlushedCollectionBlob],
    ) -> crate::Result<()> {
        for coll_blob in blobs {
            let (database_id, tenant_id, collection) =
                super::restore::parse_timeseries_snapshot_key(&coll_blob.collection_key);

            let segment_dir = super::super::timeseries::paths::ts_collection_dir(
                &self.data_dir,
                database_id,
                tenant_id,
                &collection,
            );

            let reg_key = (
                nodedb_types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tenant_id),
                collection.clone(),
            );

            for part_blob in &coll_blob.partitions {
                // Deserialize PartitionMeta from the embedded msgpack bytes.
                let meta: nodedb_types::timeseries::PartitionMeta =
                    zerompk::from_msgpack(&part_blob.meta_bytes).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!(
                                "restore: deserialize PartitionMeta for {}/{}: {e}",
                                collection, part_blob.dir_name
                            ),
                        }
                    })?;

                let partition_dir = segment_dir.join(&part_blob.dir_name);

                // Collision check: registry already knows this min_ts key.
                if let Some(registry) = self.ts_registries.get(&reg_key)
                    && let Some(existing) = registry.get(meta.min_ts)
                {
                    let is_identical = existing.meta.row_count == meta.row_count
                        && existing.meta.last_flushed_wal_lsn == meta.last_flushed_wal_lsn;
                    if is_identical {
                        // Idempotent: same partition already present, skip.
                        continue;
                    }
                    return Err(crate::Error::Storage {
                        engine: "timeseries".into(),
                        detail: format!(
                            "restore: partition collision for collection '{}' min_ts={}: \
                                 existing (rows={}, lsn={}) differs from snapshot \
                                 (rows={}, lsn={}); refusing to overwrite live data",
                            collection,
                            meta.min_ts,
                            existing.meta.row_count,
                            existing.meta.last_flushed_wal_lsn,
                            meta.row_count,
                            meta.last_flushed_wal_lsn,
                        ),
                    });
                }

                // Also check the filesystem: if the directory already exists
                // and is non-empty, treat it as a collision.
                if partition_dir.exists() {
                    let is_empty = std::fs::read_dir(&partition_dir)
                        .map_err(crate::Error::Io)
                        .map(|mut d| d.next().is_none())?;
                    if !is_empty {
                        return Err(crate::Error::Storage {
                            engine: "timeseries".into(),
                            detail: format!(
                                "restore: partition directory '{}' already exists for \
                                 collection '{}'; refusing to overwrite live data",
                                part_blob.dir_name, collection,
                            ),
                        });
                    }
                }

                // Create the partition directory and write all captured files.
                std::fs::create_dir_all(&partition_dir)?;
                for (filename, bytes) in &part_blob.files {
                    std::fs::write(partition_dir.join(filename), bytes)?;
                }

                // Register the restored partition in ts_registries, mirroring
                // exactly the registration step in flush_ts_collection.
                let registry = self
                    .ts_registries
                    .entry(reg_key.clone())
                    .or_insert_with(|| {
                        crate::engine::timeseries::partition_registry::PartitionRegistry::new(
                            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
                        )
                    });
                let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                    meta,
                    dir_name: part_blob.dir_name.clone(),
                };
                registry.import(vec![(pe.meta.min_ts, pe)]);
            }
        }
        Ok(())
    }

    /// Restore plain-columnar (and spatial) engine state from snapshot entries.
    ///
    /// For each `(collection_key, msgpack_bytes)` entry:
    /// - Deserialises the `ColumnarEngineSnapshot` from `msgpack_bytes`.
    /// - Reconstructs the `MutationEngine` via `MutationEngine::from_snapshot`.
    /// - Inserts the engine into `columnar_engines` and any returned flushed
    ///   segment blobs into `columnar_flushed_segments`.
    ///
    /// **Collision handling is fail-closed:** if either `columnar_engines` or
    /// `columnar_flushed_segments` already contains an entry for the key, this
    /// returns `Error::Storage` rather than silently overwriting live data.
    /// Idempotent re-restore (same snapshot applied twice to an empty engine set)
    /// can be added later as a follow-up when it is needed.
    pub(super) fn restore_columnar_engines(
        &mut self,
        entries: &[(String, Vec<u8>)],
    ) -> crate::Result<()> {
        for (collection_key, bytes) in entries {
            let (database_id, tenant_id, collection) =
                super::restore::parse_timeseries_snapshot_key(collection_key);

            let engine_key = (
                nodedb_types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tenant_id),
                collection.clone(),
            );

            // Fail-closed: refuse to overwrite any live engine or flushed segment
            // state that was present before this restore call.
            if self.columnar_engines.contains_key(&engine_key) {
                return Err(crate::Error::Storage {
                    engine: "columnar".into(),
                    detail: format!(
                        "restore: columnar engine already exists for collection '{collection}' \
                         (db={database_id}, tenant={tenant_id}); refusing to overwrite live data"
                    ),
                });
            }
            if self.columnar_flushed_segments.contains_key(&engine_key) {
                return Err(crate::Error::Storage {
                    engine: "columnar".into(),
                    detail: format!(
                        "restore: flushed segment state already exists for collection \
                         '{collection}' (db={database_id}, tenant={tenant_id}); \
                         refusing to overwrite live data"
                    ),
                });
            }

            let snap: nodedb_columnar::ColumnarEngineSnapshot = zerompk::from_msgpack(bytes)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "restore: deserialize ColumnarEngineSnapshot for '{collection}': {e}"
                    ),
                })?;

            let (engine, flushed) =
                nodedb_columnar::MutationEngine::from_snapshot(snap).map_err(|e| {
                    crate::Error::Storage {
                        engine: "columnar".into(),
                        detail: format!(
                            "restore: from_snapshot for collection '{collection}': {e}"
                        ),
                    }
                })?;

            self.columnar_engines.insert(engine_key.clone(), engine);
            if !flushed.is_empty() {
                self.columnar_flushed_segments.insert(engine_key, flushed);
            }
        }
        Ok(())
    }
}
