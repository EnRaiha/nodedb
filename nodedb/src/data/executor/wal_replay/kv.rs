// SPDX-License-Identifier: BUSL-1.1

//! KV WAL replay: rebuilds in-memory hash tables after crash.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
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
            let database_id = record.header.database_id;
            let record_lsn = record.header.lsn;

            // Try to detect KV records by discriminator prefix in the payload.
            if is_put {
                // kv_put with absolute expiry (redo sub-record):
                //   ("kv_put", collection, key, value, ttl_ms, expire_at_ms)
                //
                // zerompk enforces a strict array length, so this six-element
                // tuple decodes ONLY the extended shape and never the historical
                // five-element one below (and vice versa). When present, the
                // resolved absolute instant is installed verbatim instead of
                // recomputing `now_ms + ttl_ms`, which would drift the expiry
                // forward by the crash-to-restart delay.
                if let Ok((disc, collection, key, value, ttl_ms, expire_at_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(
                        &record.payload,
                    )
                    && disc == "kv_put"
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine.put_with_absolute_expiry(
                        crate::engine::kv::KvPutParams {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            key: &key,
                            value: &value,
                            ttl_ms,
                            now_ms,
                            surrogate: nodedb_types::Surrogate::ZERO,
                        },
                        expire_at_ms,
                    );
                    puts += 1;
                    continue;
                }

                // kv_put: ("kv_put", collection, key, value, ttl_ms)
                if let Ok((disc, collection, key, value, ttl_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&record.payload)
                    && disc == "kv_put"
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine.put(crate::engine::kv::KvPutParams {
                        database_id,
                        tenant_id,
                        collection: &collection,
                        key: &key,
                        value: &value,
                        ttl_ms,
                        now_ms,
                        surrogate: nodedb_types::Surrogate::ZERO,
                    });
                    puts += 1;
                    continue;
                }

                // kv_batch_put with absolute expiry (redo sub-record):
                //   ("kv_batch_put", collection, entries, ttl_ms, expire_at_ms)
                //
                // Same rationale as the six-element `kv_put` arm above: zerompk's
                // strict array-length check means this five-element tuple decodes
                // ONLY the extended shape, never the historical four-element one
                // below (and vice versa). The resolved absolute instant is
                // installed verbatim on every entry instead of recomputing
                // `now_ms + ttl_ms`, which would drift the expiry forward by the
                // crash-to-restart delay.
                if let Ok((disc, collection, entries, ttl_ms, expire_at_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64, u64)>(
                        &record.payload,
                    )
                    && disc == "kv_batch_put"
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    let surrogates = vec![nodedb_types::Surrogate::ZERO; entries.len()];
                    self.kv_engine.batch_put_with_absolute_expiry(
                        crate::engine::kv::KvBatchPutParams {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            entries: &entries,
                            ttl_ms,
                            now_ms,
                            surrogates: &surrogates,
                        },
                        expire_at_ms,
                    );
                    puts += entries.len();
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
                    // Same as the `kv_put` replay arm above: this local WAL
                    // record does not carry the surrogate (it lives in the
                    // separately-durable, redb-backed surrogate catalog, not
                    // this per-core WAL), so replay passes `Surrogate::ZERO`
                    // for every entry, matching single-`Put` replay exactly.
                    let surrogates = vec![nodedb_types::Surrogate::ZERO; entries.len()];
                    self.kv_engine
                        .batch_put(crate::engine::kv::KvBatchPutParams {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            entries: &entries,
                            ttl_ms,
                            now_ms,
                            surrogates: &surrogates,
                        });
                    puts += entries.len();
                    continue;
                }

                // kv_transfer (delta record, not a post-image): re-executes
                // `compute_transfer` against whatever source/dest values are
                // present in this core's KV engine at this point in LSN
                // order — see `wal_replay_kv_transfer.rs` for the full
                // rationale and the missing-source / compute-error policy.
                if let Some(applied) = self.try_replay_kv_transfer(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_transfer_item (delta record): re-verifies source
                // ownership and re-executes the delete+insert pair — see
                // `wal_replay_kv_transfer.rs`.
                if let Some((item_puts, item_deletes)) = self.try_replay_kv_transfer_item(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += item_puts;
                    deletes += item_deletes;
                    continue;
                }

                // kv_cas / kv_incr_float / kv_getset (delta records, not
                // post-images): re-run the same live computation against
                // whatever value is present in this core's KV engine at this
                // point in LSN order — see `wal_replay_kv_atomic.rs`.
                if let Some(applied) = self.try_replay_kv_atomic(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_field_set (delta record, not a post-image): re-runs the
                // same field merge against whatever value is present in this
                // core's KV engine at this point in LSN order — see
                // `wal_replay_kv_field.rs`.
                if let Some(applied) = self.try_replay_kv_field_set(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_register_index / kv_drop_index — see `wal_replay_kv_index.rs`.
                if let Some(applied) = self.try_replay_kv_index(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_register_sorted_index — see `wal_replay_kv_sorted_index.rs`.
                if let Some(applied) = self.try_replay_kv_register_sorted_index(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }
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
                    self.kv_engine
                        .delete(database_id, tenant_id, &collection, &keys, now_ms);
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
                    self.kv_engine.truncate(database_id, tenant_id, &collection);
                    deletes += 1;
                    continue;
                }

                // kv_drop_sorted_index — see `wal_replay_kv_sorted_index.rs`.
                // No tombstone gate here: the record carries only
                // `index_name`, no collection to gate on. See that module's
                // doc comment for why this is safe.
                if let Some(applied) =
                    self.try_replay_kv_drop_sorted_index(&record.payload, tenant_id, database_id)
                {
                    deletes += applied;
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
}
