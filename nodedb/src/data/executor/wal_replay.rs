// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for CoreLoop startup recovery: KV and Array engines.
//!
//! Vector replay lives in `wal_replay_vector.rs`.

use super::core_loop::CoreLoop;
use std::sync::Arc;

impl CoreLoop {
    fn ensure_array_open_for_replay(
        &mut self,
        array_id: &nodedb_array::types::ArrayId,
    ) -> crate::Result<()> {
        let (schema_msgpack, schema_hash) = {
            let cat = self
                .array_catalog
                .read()
                .map_err(|_| crate::Error::Internal {
                    detail: "array catalog lock poisoned during WAL replay".into(),
                })?;
            let entry =
                cat.lookup_by_name(&array_id.name)
                    .ok_or_else(|| crate::Error::Internal {
                        detail: format!(
                            "array '{}' missing from catalog during WAL replay",
                            array_id.name
                        ),
                    })?;
            (entry.schema_msgpack.clone(), entry.schema_hash)
        };
        let schema = zerompk::from_msgpack::<nodedb_array::schema::ArraySchema>(&schema_msgpack)
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array schema decode during WAL replay: {e}"),
            })?;
        self.array_engine
            .open_array(array_id.clone(), Arc::new(schema), schema_hash)
            .map_err(|e| crate::Error::Internal {
                detail: format!("array open during WAL replay: {e}"),
            })?;
        Ok(())
    }

    /// Replay WAL KV records to rebuild in-memory hash tables after crash.
    ///
    /// KV records use generic `RecordType::Put` and `RecordType::Delete` with
    /// a discriminator prefix in the MessagePack payload: `("kv_put", ...)`
    /// or `("kv_delete", ...)`.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Each core only replays records routed to its vShard.
    pub fn replay_kv_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::RecordType;

        let mut puts = 0usize;
        let mut deletes = 0usize;

        let now_ms = crate::engine::kv::current_ms();

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);
            let is_put = record_type == Some(RecordType::Put);
            let is_delete = record_type == Some(RecordType::Delete);
            if !is_put && !is_delete {
                continue;
            }

            // Route to the correct core by vShard.
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let tenant_id = record.header.tenant_id;
            let record_lsn = record.header.lsn;

            // Try to detect KV records by discriminator prefix in the payload.
            if is_put {
                // kv_put: ("kv_put", collection, key, value, ttl_ms)
                if let Ok((disc, collection, key, value, ttl_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&record.payload)
                    && disc == "kv_put"
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine.put(
                        tenant_id,
                        &collection,
                        &key,
                        &value,
                        ttl_ms,
                        now_ms,
                        nodedb_types::Surrogate::ZERO,
                    );
                    puts += 1;
                    continue;
                }

                // kv_batch_put: ("kv_batch_put", collection, entries, ttl_ms)
                if let Ok((disc, collection, entries, ttl_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(
                        &record.payload,
                    )
                    && disc == "kv_batch_put"
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine
                        .batch_put(tenant_id, &collection, &entries, ttl_ms, now_ms);
                    puts += entries.len();
                    continue;
                }

                // kv_field_set: ("kv_field_set", collection, key, updates)
                // Replay as a full PUT (the value is the updated document).
                // We skip field_set replay because it requires the current value
                // which may not exist yet. The WAL should have a kv_put after.
            }

            if is_delete {
                // kv_delete: ("kv_delete", collection, keys)
                if let Ok((disc, collection, keys)) =
                    zerompk::from_msgpack::<(&str, String, Vec<Vec<u8>>)>(&record.payload)
                    && disc == "kv_delete"
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine.delete(tenant_id, &collection, &keys, now_ms);
                    deletes += keys.len();
                    continue;
                }

                // kv_truncate: ("kv_truncate", collection)
                if let Ok((disc, collection)) =
                    zerompk::from_msgpack::<(&str, String)>(&record.payload)
                    && disc == "kv_truncate"
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine.truncate(tenant_id, &collection);
                    deletes += 1;
                }
            }
        }

        if puts > 0 || deletes > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                collections = self.kv_engine.stats().collection_count,
                "WAL KV replay complete"
            );
        }
    }

    pub fn replay_array_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use crate::engine::array::wal::{decode_delete_with_version, decode_put_with_version};
        use nodedb_wal::record::RecordType;

        let mut puts = 0usize;
        let mut deletes = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);
            let is_put = record_type == Some(RecordType::ArrayPut);
            let is_delete = record_type == Some(RecordType::ArrayDelete);
            if !is_put && !is_delete {
                continue;
            }

            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let tenant_id = record.header.tenant_id;
            let record_lsn = record.header.lsn;

            if is_put {
                let Ok(payload) = decode_put_with_version(&record.payload) else {
                    continue;
                };
                if tombstones.is_tombstoned(tenant_id, &payload.array_id.name, record_lsn) {
                    continue;
                }
                if self
                    .ensure_array_open_for_replay(&payload.array_id)
                    .is_err()
                {
                    continue;
                }
                let cell_count = payload.cells.len();
                let prov = payload.provenance.clone();
                if self
                    .array_engine
                    .put_cells(&payload.array_id, payload.cells, record_lsn)
                    .is_ok()
                {
                    puts += cell_count;
                    // Rebuild the per-core HWM frontier from the WAL record's
                    // provenance. No fence check here — replay records are already
                    // durable and ordered; just advance the frontier.
                    if let Some(p) = &prov {
                        self.sync_commit(p);
                    }
                }
                continue;
            }

            let Ok(payload) = decode_delete_with_version(&record.payload) else {
                continue;
            };
            if tombstones.is_tombstoned(tenant_id, &payload.array_id.name, record_lsn) {
                continue;
            }
            if self
                .ensure_array_open_for_replay(&payload.array_id)
                .is_err()
            {
                continue;
            }
            let cell_count = payload.cells.len();
            let prov = payload.provenance.clone();
            if self
                .array_engine
                .delete_cells(&payload.array_id, payload.cells, record_lsn)
                .is_ok()
            {
                deletes += cell_count;
                if let Some(p) = &prov {
                    self.sync_commit(p);
                }
            }
        }

        if puts > 0 || deletes > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                "WAL array replay complete"
            );
        }
    }
}
