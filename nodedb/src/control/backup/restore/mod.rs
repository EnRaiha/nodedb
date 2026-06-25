// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT orchestrator.
//!
//! Validates a backup envelope, merges all sections into a single
//! `TenantDataSnapshot`, then splits the merged snapshot into per-node
//! sub-snapshots according to the *current* cluster topology and
//! dispatches `MetaOp::RestoreTenantSnapshot` to each owning node.

pub mod columnar_reissue;
mod remote;
mod sections;
mod topology;

use std::sync::Arc;

use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, parse_encrypted as parse_envelope_encrypted,
};
use serde::Serialize;

use nodedb_types::Surrogate;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot, TenantId};
use nodedb_physical::physical_plan::MetaOp;

use remote::{NODE_RESTORE_TIMEOUT, dispatch_remote, envelope_to_err};
use sections::{apply_metadata_sections, merge_sections};
use topology::{SplitOutput, is_self, split_by_current_topology};

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
    stats.crdt_state = merged.crdt_state.len();
    stats.timeseries = merged.timeseries.len();
    stats.flushed_ts_segments = merged.flushed_ts_segments.len();
    stats.surrogate_pk = merged.surrogate_pk.len();

    warn_on_tombstoned_restores(state, tenant_id, &merged, env.meta.snapshot_watermark);

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
        sync_dispatch::dispatch_async(
            state,
            TenantId::new(tenant_id),
            // TODO(A8-followup): backup/restore not yet multi-database.
            crate::types::DatabaseId::DEFAULT,
            "__system",
            plan,
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
    rebind_surrogates(state, surrogate_binds)?;

    // Durable re-issue of plain-columnar rows. Each restored collection's live
    // rows are decoded from the snapshot and replayed as a durable
    // `ColumnarOp::Insert` (Raft-replicated in cluster mode; WAL-appended then
    // installed in single-node mode). Collections that decode to zero live rows
    // are skipped. Any failure is fatal — no warn-and-continue.
    stats.columnar_engines =
        reissue_columnar_snapshots(state, tenant_id, columnar_snapshots).await?;

    Ok(stats)
}

/// Decode and durably re-issue every restored plain-columnar collection.
///
/// Returns the number of collections that produced at least one live row and
/// were re-issued. `entries` are `("{db}:{tid}:{collection}", msgpack)` pairs
/// (the `ColumnarEngineSnapshot` wire shape).
async fn reissue_columnar_snapshots(
    state: &Arc<SharedState>,
    tenant_id: u64,
    entries: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    // Columnar segment KEK == the WAL encryption key (segments are written via
    // `SegmentWriter::plain().write_segment(..., kek)` with this key). Absent
    // when at-rest encryption is not configured, in which case segments are
    // plaintext NDBS and decode with `kek = None`.
    let kek = state.wal.encryption_key().cloned();
    let database_id = crate::types::DatabaseId::DEFAULT;

    let mut reissued = 0usize;
    for (key, bytes) in entries {
        let Some((_db_id, tid, collection)) = parse_columnar_snapshot_key(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed columnar snapshot key '{key}'"),
            });
        };

        let snap: nodedb_columnar::ColumnarEngineSnapshot =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize ColumnarEngineSnapshot for '{collection}': {e}"
                ),
            })?;

        let decoded = columnar_reissue::decode_snapshot_live_rows(&collection, snap, kek.as_ref())?;
        if decoded.rows.is_empty() {
            continue;
        }

        let plan = columnar_reissue::build_columnar_insert_plan(&collection, decoded)?;
        columnar_reissue::reissue_columnar_durably(
            state,
            TenantId::new(tid),
            database_id,
            &collection,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}

/// Rebind every PK→surrogate identity carried in the backup into the
/// destination catalog so restored rows resolve by PK point-lookup.
///
/// No-op when the snapshot carried no bindings (e.g. an older backup created
/// before the surrogate-pk section existed) or when the node has no catalog.
/// Any catalog write failure is FATAL.
fn rebind_surrogates(
    state: &Arc<SharedState>,
    binds: Vec<SurrogateBindEntry>,
) -> Result<(), Error> {
    if binds.is_empty() {
        return Ok(());
    }
    let Some(catalog) = state.credentials.catalog() else {
        return Ok(());
    };
    let database_id = crate::types::DatabaseId::DEFAULT;
    for e in &binds {
        catalog.put_surrogate(
            database_id,
            TenantId::new(e.tenant_id),
            &e.collection,
            &e.pk,
            Surrogate::new(e.surrogate),
        )?;
    }
    Ok(())
}

/// Parse a `"{db}:{tid}:{collection}"` columnar snapshot key, verifying the
/// embedded tenant matches. The collection may itself contain `':'`. Returns
/// `None` on a malformed key or tenant mismatch.
fn parse_columnar_snapshot_key(key: &str, tenant_id: u64) -> Option<(u64, u64, String)> {
    let mut it = key.splitn(3, ':');
    let db = it.next()?.parse::<u64>().ok()?;
    let tid = it.next()?.parse::<u64>().ok()?;
    let coll = it.next()?;
    if tid != tenant_id || coll.is_empty() {
        return None;
    }
    Some((db, tid, coll.to_string()))
}

fn warn_on_tombstoned_restores(
    state: &Arc<SharedState>,
    tenant_id: u64,
    merged: &TenantDataSnapshot,
    snapshot_watermark: u64,
) {
    let Some(catalog) = state.credentials.catalog() else {
        return;
    };
    let Ok(tombstones) = catalog.load_wal_tombstones() else {
        return;
    };
    if tombstones.is_empty() {
        return;
    }

    let mut names = std::collections::BTreeSet::new();
    let sections: [&[(String, Vec<u8>)]; 6] = [
        &merged.documents,
        &merged.indexes,
        &merged.vectors,
        &merged.kv_tables,
        &merged.timeseries,
        &merged.edges,
    ];
    for section in sections {
        for (key, _) in section {
            if let Some(name) = collection_from_key(key) {
                names.insert(name.to_string());
            }
        }
    }

    for name in &names {
        let Some(purge_lsn) = tombstones.purge_lsn(tenant_id, name) else {
            continue;
        };
        if snapshot_watermark != 0 && snapshot_watermark >= purge_lsn {
            continue;
        }
        tracing::warn!(
            tenant_id,
            collection = %name,
            purge_lsn,
            snapshot_watermark,
            "RESTORE: bringing back a collection that was hard-deleted on this cluster"
        );
        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(TenantId::new(tenant_id)),
            "__restore",
            &format!(
                "restore resurrected tombstoned collection '{name}' \
                 (purge_lsn={purge_lsn}, snapshot_watermark={snapshot_watermark})"
            ),
        );
    }
}

fn collection_from_key(key: &str) -> Option<&str> {
    let tail = key.split_once(':')?.1;
    tail.split([':', '\0']).next()
}

#[cfg(test)]
mod columnar_key_tests {
    use super::parse_columnar_snapshot_key;

    #[test]
    fn parses_db_prefixed_key() {
        assert_eq!(
            parse_columnar_snapshot_key("0:7:metrics", 7),
            Some((0, 7, "metrics".to_string()))
        );
    }

    #[test]
    fn collection_retains_embedded_colon() {
        assert_eq!(
            parse_columnar_snapshot_key("0:7:a:b", 7),
            Some((0, 7, "a:b".to_string()))
        );
    }

    #[test]
    fn tenant_mismatch_rejected() {
        assert_eq!(parse_columnar_snapshot_key("0:8:metrics", 7), None);
    }

    #[test]
    fn missing_or_empty_collection_rejected() {
        assert_eq!(parse_columnar_snapshot_key("0:7", 7), None);
        assert_eq!(parse_columnar_snapshot_key("0:7:", 7), None);
    }
}

#[cfg(test)]
mod collection_key_tests {
    use super::collection_from_key;

    #[test]
    fn extracts_collection_with_colon_separator() {
        assert_eq!(collection_from_key("1:users:doc-1"), Some("users"));
    }

    #[test]
    fn extracts_collection_with_null_separator() {
        assert_eq!(collection_from_key("1:src\0label\0"), Some("src"));
    }

    #[test]
    fn vector_and_kv_key_shapes() {
        assert_eq!(collection_from_key("1:events"), Some("events"));
    }

    #[test]
    fn no_tenant_prefix_returns_none() {
        assert_eq!(collection_from_key("no_colon"), None);
    }
}
