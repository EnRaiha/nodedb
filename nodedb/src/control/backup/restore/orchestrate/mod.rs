// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT orchestrator logic.
//!
//! Validates a backup envelope, merges all sections into a single
//! `TenantDataSnapshot`, then splits the merged snapshot into per-node
//! sub-snapshots according to the *current* cluster topology and
//! dispatches `MetaOp::RestoreTenantSnapshot` to each owning node.
//!
//! Durable re-issue of columnar/timeseries/vector rows lives in [`reissue`];
//! post-install surrogate rebinding and tombstone warnings live in
//! [`rebind`].

mod rebind;
mod reissue;

use std::sync::Arc;

use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, parse_encrypted as parse_envelope_encrypted,
};
use serde::Serialize;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_physical::physical_plan::MetaOp;

use super::remote::{NODE_RESTORE_TIMEOUT, dispatch_remote, envelope_to_err};
use super::sections::{apply_metadata_sections, merge_sections};
use super::topology::{SplitOutput, is_self, split_by_current_topology};

/// Aggregate stats returned to the client at the end of a restore.
#[derive(Debug, Default, Clone, Serialize)]
pub struct RestoreStats {
    pub tenant_id: u64,
    pub dry_run: bool,
    pub sections: u16,
    pub source_vshard_count: u16,
    pub documents: usize,
    pub indexes: usize,
    pub edges: usize,
    pub vectors: usize,
    pub kv_tables: usize,
    pub crdt_state: usize,
    pub timeseries: usize,
    pub columnar_engines: usize,
    pub flushed_ts_segments: usize,
    /// Number of timeseries collections re-issued durably (Raft/WAL) on restore.
    pub timeseries_reissued: usize,
    /// Number of CRDT tenant-snapshot imports re-issued durably (Raft/WAL) on
    /// restore — one per distinct data group that owns any CRDT collection.
    pub crdt_reissued: usize,
    /// Number of individual vectors re-issued durably (Raft/WAL) on restore.
    pub vectors_reissued: usize,
    /// Number of (collection, field) vector-index HNSW/PQ/IVF configs
    /// re-issued durably (Raft/WAL) on restore.
    pub vector_params_reissued: usize,
    /// Number of PK→surrogate identity bindings rebound into the catalog.
    pub surrogate_pk: usize,
    pub nodes_dispatched: usize,
    /// Non-zero = snapshot contained unparseable keys (possible corruption).
    pub malformed_keys: usize,
    /// Non-zero = some entries were routed to local node due to missing shard leader.
    pub route_fallbacks: usize,
}

/// Restore a tenant from a fully-buffered backup envelope.
pub async fn restore_tenant(
    state: &Arc<SharedState>,
    tenant_id: u64,
    envelope_bytes: &[u8],
    dry_run: bool,
    force: bool,
) -> Result<RestoreStats, Error> {
    let env = match &state.backup_kek {
        Some(kek) => parse_envelope_encrypted(envelope_bytes, DEFAULT_MAX_TOTAL_BYTES, kek)
            .map_err(envelope_to_err)?,
        None => {
            return Err(Error::Internal {
                detail: "restore: envelope is encrypted but no backup KEK is configured; \
                         set [backup_encryption] in the server config"
                    .into(),
            });
        }
    };
    if env.meta.tenant_id != tenant_id {
        return Err(Error::Internal {
            detail: format!(
                "backup tenant mismatch: envelope has {}, request is for {}",
                env.meta.tenant_id, tenant_id
            ),
        });
    }

    if !dry_run && env.meta.snapshot_watermark != 0 {
        let current_high_water = state
            .tenant_write_hlc
            .lock()
            .ok()
            .and_then(|map| map.get(&tenant_id).copied())
            .unwrap_or(0);
        if env.meta.snapshot_watermark < current_high_water {
            if force {
                tracing::warn!(
                    tenant_id,
                    envelope_watermark = env.meta.snapshot_watermark,
                    current_high_water,
                    "restore staleness protection explicitly overridden via FORCE: \
                     envelope watermark is older than the destination cluster's last \
                     observed write-HLC for this tenant — newer writes will be overwritten"
                );
            } else {
                return Err(Error::Internal {
                    detail: format!(
                        "restore refused: envelope watermark {} is older than the \
                         destination cluster's last observed write-HLC {} for tenant \
                         {} — newer writes would be silently overwritten",
                        env.meta.snapshot_watermark, current_high_water, tenant_id
                    ),
                });
            }
        }
    }

    let mut stats = RestoreStats {
        tenant_id,
        dry_run,
        sections: env.sections.len() as u16,
        source_vshard_count: env.meta.source_vshard_count,
        ..Default::default()
    };

    if !dry_run {
        apply_metadata_sections(state, tenant_id, &env)?;
    }

    let mut merged = merge_sections(&env.sections)?;
    stats.documents = merged.documents.len();
    stats.indexes = merged.indexes.len();
    stats.edges = merged.edges.len();
    stats.vectors = merged.vectors.len();
    stats.kv_tables = merged.kv_tables.len();
    // CRDT state is one entry per (tenant, collection).
    stats.crdt_state = merged.crdt_state.len();
    stats.timeseries = merged.timeseries.len();
    stats.flushed_ts_segments = merged.flushed_ts_segments.len();
    stats.surrogate_pk = merged.surrogate_pk.len();

    rebind::warn_on_tombstoned_restores(state, tenant_id, &merged, env.meta.snapshot_watermark);

    if dry_run {
        stats.columnar_engines = merged.columnar_engines.len();
        return Ok(stats);
    }

    // Plain-columnar engine state is NOT installed via the snapshot path (that
    // lands in in-memory-only Data Plane maps — lost on restart, never
    // replicated). Drain it here and re-issue durably below as
    // `ColumnarOp::Insert`s. The topology split must therefore never see
    // columnar engines.
    let columnar_snapshots = std::mem::take(&mut merged.columnar_engines);

    // Timeseries engine state (memtable section + flushed on-disk segments) is
    // likewise NOT installed via the snapshot path — `restore_timeseries` and
    // `restore_flushed_ts_segments` do a per-node DIRECT install that is never
    // Raft-replicated, so on a multi-replica cluster the data lands on only one
    // node. Drain both sections here and re-issue durably below as
    // `TimeseriesOp::Ingest`s (Raft-replicated in cluster mode; WAL-appended
    // then installed in single-node mode). The topology split must therefore
    // never see timeseries data — otherwise it would be double-installed.
    let timeseries_memtables = std::mem::take(&mut merged.timeseries);
    let flushed_ts_segments = std::mem::take(&mut merged.flushed_ts_segments);

    // CRDT state is NOT installed via the per-node snapshot fan-out: that
    // dispatch is race-prone (skips data groups with no leader yet) and not
    // durable across restart. Drain the per-collection CRDT section here and
    // re-issue durably below as `CrdtOp::ImportSnapshot` (Raft-replicated in
    // cluster mode; WAL-appended then installed in single-node mode). The
    // topology split must therefore never see CRDT state — otherwise the
    // coordinator would double-import.
    let crdt_state = std::mem::take(&mut merged.crdt_state);

    // Vector engine state is likewise NOT installed via the snapshot path —
    // `restore_vector_collection` installs straight into the in-memory-only
    // `vector_collections` Data Plane map with no WAL record and no Raft
    // entry, so it is lost on restart (single-node) and never replicated
    // (cluster). Drain it here and re-issue durably below, one
    // `VectorOp::Insert` per restored vector (Raft-replicated in cluster
    // mode; WAL-appended then installed in single-node mode). The topology
    // split must therefore never see vector data — otherwise it would be
    // double-installed.
    let vector_snapshots = std::mem::take(&mut merged.vectors);

    // Vector-index HNSW/PQ/IVF configuration (metric, M, ef_construction,
    // quantization/index_type) is captured at backup alongside the raw
    // vectors above (see `TenantDataSnapshot::vector_params` /
    // `::index_configs` doc comments) but is likewise NOT installed via the
    // snapshot path. Drain both here and re-issue durably below as
    // `VectorOp::SetParams` — BEFORE the vector `Insert` re-issue, since
    // `get_or_create_vector_index` lazily creates the Data Plane HNSW index
    // from `self.vector_params` on the first `Insert` it sees for a
    // (collection, field), defaulting silently if no `SetParams` landed
    // first. The topology split must therefore never see these sections.
    let vector_params_snapshots = std::mem::take(&mut merged.vector_params);
    let index_config_snapshots = std::mem::take(&mut merged.index_configs);

    // Drain the PK→surrogate identity map before the topology split (the split
    // only routes per-key engine data). It is rebound into the destination
    // catalog after the data install dispatches succeed — without it restored
    // documents are unreachable by PK point-lookup (`WHERE id=<pk>`).
    let surrogate_binds = std::mem::take(&mut merged.surrogate_pk);

    let SplitOutput {
        buckets,
        malformed_keys,
        route_fallbacks,
    } = split_by_current_topology(state, tenant_id, merged);
    stats.nodes_dispatched = buckets.len();
    stats.malformed_keys = malformed_keys;
    stats.route_fallbacks = route_fallbacks;
    if malformed_keys > 0 {
        tracing::warn!(
            tenant_id,
            count = malformed_keys,
            "restore: snapshot contained keys that did not parse — possible corruption"
        );
    }
    if route_fallbacks > 0 {
        tracing::warn!(
            tenant_id,
            count = route_fallbacks,
            "restore: routed some entries to local node because no current leader was visible"
        );
    }

    let mut local_plan: Option<PhysicalPlan> = None;
    let mut remote_futs = Vec::with_capacity(buckets.len());
    for (node_id, sub) in buckets {
        let payload = zerompk::to_msgpack_vec(&sub).map_err(|e| Error::Internal {
            detail: format!("restore: snapshot encode failed: {e}"),
        })?;
        let plan = PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
            tenant_id,
            snapshot: payload,
            // User RESTORE keeps the fail-closed collision behavior.
            replace_mode: false,
            clear_vshards: Vec::new(),
            collections_to_clear: Vec::new(),
        });
        if is_self(state, node_id) {
            local_plan = Some(plan);
        } else {
            let state = state.clone();
            remote_futs
                .push(async move { dispatch_remote(&state, node_id, tenant_id, plan).await });
        }
    }
    if let Some(plan) = local_plan {
        sync_dispatch::dispatch_system(
            state,
            sync_dispatch::SystemTask::new(
                sync_dispatch::SystemReason::BackupRestore,
                TenantId::new(tenant_id),
                // TODO(A8-followup): backup/restore not yet multi-database.
                crate::types::DatabaseId::DEFAULT,
                "__system",
                plan,
            ),
            NODE_RESTORE_TIMEOUT,
        )
        .await?;
    }
    let results = futures::future::join_all(remote_futs).await;
    if let Some(first_err) = results.into_iter().find_map(Result::err) {
        return Err(first_err);
    }

    // Rebind the PK→surrogate identity map into the destination catalog now
    // that the data is installed. The catalog is the SOURCE OF TRUTH the
    // planner consults for PK point-lookups (`surrogate_assigner.lookup(pk)`);
    // a missing binding makes a restored row unreachable by PK even though it
    // is present in the doc store. A rebind failure is FATAL — silently
    // shipping unqueryable rows is the partial-success anti-pattern this
    // codebase forbids.
    rebind::rebind_surrogates(state, surrogate_binds)?;

    // Durable re-issue of plain-columnar rows. Each restored collection's live
    // rows are decoded from the snapshot and replayed as a durable
    // `ColumnarOp::Insert` (Raft-replicated in cluster mode; WAL-appended then
    // installed in single-node mode). Collections that decode to zero live rows
    // are skipped. Any failure is fatal — no warn-and-continue.
    stats.columnar_engines =
        reissue::reissue_columnar_snapshots(state, tenant_id, columnar_snapshots).await?;

    // Durable re-issue of timeseries rows. Each restored collection's memtable
    // rows plus every flushed partition's rows are decoded from the snapshot and
    // replayed as a durable `TimeseriesOp::Ingest` (Raft-replicated in cluster
    // mode; WAL-appended then installed in single-node mode). Collections that
    // decode to zero live rows are skipped. Any failure is fatal — no
    // warn-and-continue.
    stats.timeseries_reissued = reissue::reissue_timeseries_snapshots(
        state,
        tenant_id,
        timeseries_memtables,
        flushed_ts_segments,
    )
    .await?;

    // Durable re-issue of CRDT state. Each collection's Loro snapshot is
    // proposed through Raft to the data group owning that collection's vshard
    // (Raft-replicated in cluster mode; WAL-appended then installed in
    // single-node mode). Every replica applies the same idempotent Loro merge
    // and converges deterministically. Any failure is fatal — no
    // warn-and-continue.
    stats.crdt_reissued = super::crdt_reissue::reissue_crdt_snapshots(state, crdt_state).await?;

    // Durable re-issue of vector-index configuration. Each restored
    // (collection, field) HNSW/PQ/IVF config is replayed as a
    // `VectorOp::SetParams` (Raft-replicated in cluster mode; WAL-appended
    // then installed in single-node mode). MUST run before the vector-insert
    // re-issue below — see the `vector_params_snapshots` drain comment
    // above. Any failure is fatal — no warn-and-continue.
    stats.vector_params_reissued = reissue::reissue_vector_params(
        state,
        tenant_id,
        vector_params_snapshots,
        index_config_snapshots,
    )
    .await?;

    // Durable re-issue of vector rows. Each restored vector is replayed as an
    // individual `VectorOp::Insert` (Raft-replicated in cluster mode;
    // WAL-appended then installed in single-node mode). Collections that
    // decode to zero vectors are skipped. Any failure is fatal — no
    // warn-and-continue.
    stats.vectors_reissued =
        reissue::reissue_vector_snapshots(state, tenant_id, vector_snapshots).await?;

    Ok(stats)
}
