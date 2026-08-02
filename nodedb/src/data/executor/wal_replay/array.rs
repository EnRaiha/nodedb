// SPDX-License-Identifier: BUSL-1.1

//! Array engine WAL replay: rebuilds tile state after crash.
//!
//! ## The durable watermark
//!
//! Each array's manifest carries a `durable_lsn` — the highest LSN whose cells
//! are already inside a flushed, on-disk segment (`Manifest::add_segment`
//! raises it). Replay skips every record at or below it, which is what makes
//! replaying the same retained tail twice a no-op and, more importantly, what
//! stops a cell version that a bitemporal audit purge physically removed from a
//! segment being re-materialised out of a still-retained `ArrayPut`. Without
//! the gate the purge is silently undone on the next boot.
//!
//! Records ABOVE the watermark are the tail the segments have not absorbed and
//! must be re-applied into the memtable.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::replay_abort::abort_replay;
use std::sync::Arc;

/// Outcome of preparing an array's store for replay.
enum ArrayOpen {
    /// The store is open; replay may proceed against it.
    Ready,
    /// The array has no catalog entry on this node. The catalog is a separate
    /// durable store from the WAL, so a WAL tail can legitimately outlive the
    /// catalog row it names — a DROP that committed in the catalog and had its
    /// WAL tombstone truncated, or a node that never received the DDL. There is
    /// no array to apply the cells to and no state to corrupt by not applying
    /// them, so this is a skip rather than an abort.
    NoCatalogEntry,
}

impl CoreLoop {
    fn ensure_array_open_for_replay(
        &mut self,
        array_id: &nodedb_array::types::ArrayId,
    ) -> crate::Result<ArrayOpen> {
        let entry = {
            let cat = self
                .array_catalog
                .read()
                .map_err(|_| crate::Error::Internal {
                    detail: "array catalog lock poisoned during WAL replay".into(),
                })?;
            cat.lookup_by_id(array_id)
                .map(|entry| (entry.schema_msgpack.clone(), entry.schema_hash))
        };
        let Some((schema_msgpack, schema_hash)) = entry else {
            return Ok(ArrayOpen::NoCatalogEntry);
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
        Ok(ArrayOpen::Ready)
    }

    /// The LSN this array's flushed segments are already durable through.
    ///
    /// `0` when the store is not open, which gates nothing — the safe
    /// direction, matching every other engine's unset replay floor.
    fn array_durable_lsn(&self, array_id: &nodedb_array::types::ArrayId) -> u64 {
        self.array_engine
            .store(array_id)
            .map_or(0, |store| store.manifest().durable_lsn)
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
        let mut skipped = 0usize;

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
                let payload = match decode_put_with_version(&record.payload) {
                    Ok(p) => p,
                    Err(e) => abort_replay(
                        "array",
                        "decode_put",
                        self.core_id,
                        record_lsn,
                        &format!("ArrayPut payload could not be decoded: {e}"),
                    ),
                };
                if tombstones.is_tombstoned(
                    record.header.database_id,
                    tenant_id,
                    &payload.array_id.name,
                    record_lsn,
                ) {
                    skipped += 1;
                    continue;
                }
                match self.ensure_array_open_for_replay(&payload.array_id) {
                    Ok(ArrayOpen::Ready) => {}
                    Ok(ArrayOpen::NoCatalogEntry) => {
                        tracing::warn!(
                            core = self.core_id,
                            array = %payload.array_id.name,
                            lsn = record_lsn,
                            "WAL array replay: no catalog entry for this array; \
                             skipping its retained cells"
                        );
                        skipped += 1;
                        continue;
                    }
                    Err(e) => abort_replay(
                        "array",
                        "open",
                        self.core_id,
                        record_lsn,
                        &format!("array '{}' could not be opened: {e}", payload.array_id.name),
                    ),
                }
                if record_lsn <= self.array_durable_lsn(&payload.array_id) {
                    skipped += 1;
                    continue;
                }
                let cell_count = payload.cells.len();
                let prov = payload.provenance.clone();
                if let Err(e) =
                    self.array_engine
                        .put_cells(&payload.array_id, payload.cells, record_lsn)
                {
                    abort_replay(
                        "array",
                        "put_cells",
                        self.core_id,
                        record_lsn,
                        &format!("committed cells could not be re-applied: {e}"),
                    );
                }
                puts += cell_count;
                // Rebuild the per-core HWM frontier from the WAL record's
                // provenance. No fence check here — replay records are already
                // durable and ordered; just advance the frontier.
                if let Some(p) = &prov {
                    self.sync_commit(p);
                }
                continue;
            }

            let payload = match decode_delete_with_version(&record.payload) {
                Ok(p) => p,
                Err(e) => abort_replay(
                    "array",
                    "decode_delete",
                    self.core_id,
                    record_lsn,
                    &format!("ArrayDelete payload could not be decoded: {e}"),
                ),
            };
            if tombstones.is_tombstoned(
                record.header.database_id,
                tenant_id,
                &payload.array_id.name,
                record_lsn,
            ) {
                skipped += 1;
                continue;
            }
            match self.ensure_array_open_for_replay(&payload.array_id) {
                Ok(ArrayOpen::Ready) => {}
                Ok(ArrayOpen::NoCatalogEntry) => {
                    tracing::warn!(
                        core = self.core_id,
                        array = %payload.array_id.name,
                        lsn = record_lsn,
                        "WAL array replay: no catalog entry for this array; \
                         skipping its retained tombstones"
                    );
                    skipped += 1;
                    continue;
                }
                Err(e) => abort_replay(
                    "array",
                    "open",
                    self.core_id,
                    record_lsn,
                    &format!("array '{}' could not be opened: {e}", payload.array_id.name),
                ),
            }
            if record_lsn <= self.array_durable_lsn(&payload.array_id) {
                skipped += 1;
                continue;
            }
            let cell_count = payload.cells.len();
            let prov = payload.provenance.clone();
            if let Err(e) =
                self.array_engine
                    .delete_cells(&payload.array_id, payload.cells, record_lsn)
            {
                abort_replay(
                    "array",
                    "delete_cells",
                    self.core_id,
                    record_lsn,
                    &format!("committed tombstones could not be re-applied: {e}"),
                );
            }
            deletes += cell_count;
            if let Some(p) = &prov {
                self.sync_commit(p);
            }
        }

        if puts > 0 || deletes > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                skipped,
                "WAL array replay complete"
            );
        }
    }
}
