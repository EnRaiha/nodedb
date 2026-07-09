// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for CoreLoop startup recovery: KV and Array engines.
//!
//! Vector replay lives in `wal_replay_vector.rs`. The `kv_transfer` /
//! `kv_transfer_item` delta-record replay (decode, tombstone gate, and
//! mutation) lives in `wal_replay_kv_transfer.rs`. The `kv_cas` /
//! `kv_incr_float` / `kv_getset` delta-record replay lives in
//! `wal_replay_kv_atomic.rs`.

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

                // kv_field_set: ("kv_field_set", collection, key, updates)
                // Not replayed: resolving the merge requires the pre-update
                // document, and (unlike kv_transfer/kv_transfer_item) there is
                // no delta-replay path for it. This falls through and the
                // record is silently dropped for every WAL record shape, not
                // just the autocommit path.
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

    /// Replay WAL CRDT delta records to rebuild Loro tenant state after crash.
    ///
    /// CRDT records use `RecordType::CrdtDelta`; the payload is a
    /// `CrdtDeltaWalPayload` as written by `append_crdt_delta` for both
    /// `CrdtOp::Apply` and `CrdtOp::ImportSnapshot`. Loro `import` is
    /// idempotent and commutative, so there is no LSN gate: re-importing a
    /// delta already folded into a loaded checkpoint is a safe no-op.
    ///
    /// CRDT deletes are encoded as Loro tombstone ops inside the delta itself,
    /// so no external `tombstones` filtering is applied here; the param is
    /// accepted for signature symmetry with the other per-engine replays.
    pub fn replay_crdt_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        _tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::RecordType;
        use tracing::warn;

        let mut replayed = 0usize;

        for record in records {
            if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::CrdtDelta) {
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

            let tid = crate::types::TenantId::new(record.header.tenant_id);

            // Single self-describing decode. The delta is routed to its
            // per-collection LoroDoc by `payload.collection`.
            let Ok(payload) =
                zerompk::from_msgpack::<crate::wal::CrdtDeltaWalPayload>(&record.payload)
            else {
                continue;
            };

            // Every CRDT delta / snapshot-import record written by the current
            // binary carries its collection. A record with no collection cannot
            // be routed to a per-collection doc; skip it (a pre-per-collection
            // record from an earlier dev binary — there is no released data to
            // preserve).
            let Some(collection) = payload.collection.as_deref() else {
                warn!(
                    core = self.core_id,
                    tenant = tid.as_u64(),
                    "CRDT WAL record without collection; skipping (cannot route per-collection)"
                );
                continue;
            };

            match self.get_crdt_engine(tid) {
                Ok(engine) => {
                    // NOTE: replays committed CRDT deltas via a bare import, with NO
                    // constraint validation. If deterministic apply-time validation is
                    // ever added to the live apply path, it MUST also gate this replay
                    // path (and the batch apply path) — otherwise a delta rejected live
                    // could be re-imported here on restart and diverge from peers.
                    if let Err(e) = engine.apply_committed_delta(collection, &payload.bytes) {
                        warn!(
                            core = self.core_id,
                            tenant = tid.as_u64(),
                            error = %e,
                            "CRDT WAL delta import failed during replay"
                        );
                    } else {
                        replayed += 1;
                    }
                }
                Err(e) => warn!(
                    core = self.core_id,
                    tenant = tid.as_u64(),
                    error = %e,
                    "failed to create CRDT engine during WAL replay"
                ),
            }
        }

        if replayed > 0 {
            tracing::info!(core = self.core_id, replayed, "WAL CRDT replay complete");
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

#[cfg(test)]
mod crdt_replay_tests {
    use super::CoreLoop;
    use crate::types::TenantId;
    use loro::LoroValue;
    use nodedb_wal::record::RecordType;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    /// The tests drive replay directly and never tick the event loop, so the
    /// far ends are unused — they just must not be dropped.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core(core_id: usize) -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            core_id,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// Build a CRDT snapshot for `tid` containing one row, then wrap it in a
    /// `CrdtDelta` WAL record exactly as `append_crdt_delta` does
    /// (`CrdtDeltaWalPayload` msgpack payload). Snapshot import and delta
    /// apply share the same idempotent Loro `state.import`, so a snapshot rides
    /// the delta record identically.
    fn make_crdt_record(
        tid: TenantId,
        vshard_id: u32,
        collection: &str,
        row_id: &str,
    ) -> nodedb_wal::WalRecord {
        // Build one collection's CRDT doc directly; the WAL record carries the
        // collection so replay routes the import to the matching per-collection
        // LoroDoc.
        let state = nodedb_crdt::state::CrdtState::new(0).expect("state");
        state
            .upsert(
                collection,
                row_id,
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("upsert");
        let snapshot = state.export_snapshot().expect("export");
        assert!(!snapshot.is_empty(), "snapshot must be non-empty");

        let wal_payload = crate::wal::CrdtDeltaWalPayload {
            bytes: snapshot,
            collection: Some(collection.to_string()),
            provenance: None,
        };
        let payload = zerompk::to_msgpack_vec(&wal_payload).expect("encode payload");
        nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 1,
            tenant_id: tid.as_u64(),
            vshard_id,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn replay_crdt_wal_restores_state() {
        let tid = TenantId::new(7);
        let record = make_crdt_record(tid, 0, "notes", "row1");

        // Fresh core with empty CRDT state, mimicking a restart with no
        // checkpoint — only the WAL is available.
        let mut h = make_core(0);
        let tombstones = nodedb_wal::TombstoneSet::new();

        h.core
            .replay_crdt_wal(std::slice::from_ref(&record), 1, &tombstones);

        let engine = h.core.get_crdt_engine(tid).expect("engine");
        assert!(
            engine.row_exists("notes", "row1"),
            "CRDT row must be restored from WAL replay"
        );
    }

    #[test]
    fn replay_crdt_wal_skips_other_cores() {
        // vshard 1 with num_cores 2 routes to core 1, so core 0 must skip it.
        let tid = TenantId::new(9);
        let record = make_crdt_record(tid, 1, "notes", "row1");

        let mut h = make_core(0);
        let tombstones = nodedb_wal::TombstoneSet::new();
        h.core
            .replay_crdt_wal(std::slice::from_ref(&record), 2, &tombstones);

        let engine = h.core.get_crdt_engine(tid).expect("engine");
        assert!(
            !engine.row_exists("notes", "row1"),
            "core 0 must not replay a record routed to core 1"
        );
    }
}
