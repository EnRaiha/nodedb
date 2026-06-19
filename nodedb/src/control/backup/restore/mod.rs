// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT orchestrator.
//!
//! Validates a backup envelope, merges all sections into a single
//! `TenantDataSnapshot`, then splits the merged snapshot into per-node
//! sub-snapshots according to the *current* cluster topology and
//! dispatches `MetaOp::RestoreTenantSnapshot` to each owning node.

mod remote;
mod sections;
mod topology;

use std::sync::Arc;

use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, parse_encrypted as parse_envelope_encrypted,
};
use serde::Serialize;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{TenantDataSnapshot, TenantId};
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
        apply_metadata_sections(state, tenant_id, &env);
    }

    let merged = merge_sections(&env.sections)?;
    stats.documents = merged.documents.len();
    stats.indexes = merged.indexes.len();
    stats.edges = merged.edges.len();
    stats.vectors = merged.vectors.len();
    stats.kv_tables = merged.kv_tables.len();
    stats.crdt_state = merged.crdt_state.len();
    stats.timeseries = merged.timeseries.len();

    warn_on_tombstoned_restores(state, tenant_id, &merged, env.meta.snapshot_watermark);

    if dry_run {
        return Ok(stats);
    }

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

    Ok(stats)
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
