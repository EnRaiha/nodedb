// SPDX-License-Identifier: BUSL-1.1

//! BACKUP TENANT orchestrator.
//!
//! Discovers every node that holds a vShard owning some of the
//! tenant's data, dispatches `MetaOp::CreateTenantSnapshot` to each
//! (local SPSC for self, `RaftRpc::ExecuteRequest` for remotes),
//! and packs the gathered per-node snapshots into a `BackupEnvelope`.
//!
//! Single-node mode is the degenerate case: routing table absent
//! (or 1 node) → 1 section, origin = self.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nodedb_cluster::routing::VSHARD_COUNT;
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc, TypedClusterError};
use nodedb_types::backup_envelope::{EnvelopeMeta, EnvelopeWriter};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{MetaOp, wire as plan_wire};

/// Default per-node snapshot dispatch timeout.
const NODE_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Build a complete tenant backup envelope by fanning out across the
/// cluster, gathering each node's slice, and framing the result.
///
/// Single-node and cluster paths converge here — a single-node server
/// produces a one-section envelope with origin = self.
pub async fn backup_tenant(state: &Arc<SharedState>, tenant_id: u64) -> Result<Bytes, Error> {
    let nodes = unique_origin_nodes(state);
    let snapshot_plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot { tenant_id });

    // Collect per-node sections first. The orchestrator's own
    // dispatches advance the tenant write-HLC high-water via
    // `dispatch_async`; capturing the envelope watermark AFTER the
    // fan-out guarantees `envelope.watermark ≥ tenant_write_hlc`
    // at backup time, so a subsequent restore of this envelope into
    // the same (unchanged) cluster passes the staleness gate.
    let mut sections = Vec::with_capacity(nodes.len());
    for node_id in nodes {
        let body = if is_self(state, node_id) {
            snapshot_self(state, tenant_id, &snapshot_plan).await?
        } else {
            snapshot_remote(state, node_id, tenant_id, &snapshot_plan).await?
        };
        sections.push((node_id, body));
    }

    // Capture a cluster-wide logical instant for the envelope via the
    // HLC. `hlc_clock.now()` advances past any previously observed
    // local or remote HLC — the wall-ns component is the scalar
    // watermark we stamp into the header. Restore compares this
    // against the destination's `tenant_write_hlc` to detect stale
    // envelopes.
    let snapshot_watermark = state.hlc_clock.now().wall_ns;
    let meta = EnvelopeMeta {
        tenant_id,
        source_vshard_count: VSHARD_COUNT as u16,
        hash_seed: 0, // VSHARD_COUNT-derived hash; no seed today
        snapshot_watermark,
    };
    let mut writer = EnvelopeWriter::new(meta);

    for (node_id, body) in sections {
        writer
            .push_section(node_id, body)
            .map_err(|e| Error::Internal {
                detail: format!("backup envelope: {e}"),
            })?;
    }

    // Metadata sections: catalog rows + source-side tombstones. These
    // live in dedicated sections with sentinel origin_node_ids so the
    // restore path can distinguish them from per-node engine data.
    // Without these, a backup taken during a collection's retention
    // window loses its soft-deleted row (UNDROP can't work after
    // restore), and a restore whose source has already purged a
    // collection can resurrect rows that were properly reaped.
    if let Some(catalog) = state.credentials.catalog() {
        if let Ok(all) = catalog.load_all_collections(DatabaseId::DEFAULT) {
            let mut blobs: Vec<nodedb_types::backup_envelope::StoredCollectionBlob> = Vec::new();
            for coll in all.iter().filter(|c| c.tenant_id == tenant_id) {
                if let Ok(bytes) = zerompk::to_msgpack_vec(coll) {
                    blobs.push(nodedb_types::backup_envelope::StoredCollectionBlob {
                        name: coll.name.clone(),
                        bytes,
                    });
                }
            }
            if !blobs.is_empty()
                && let Ok(body) = zerompk::to_msgpack_vec(&blobs)
            {
                writer
                    .push_section(
                        nodedb_types::backup_envelope::SECTION_ORIGIN_CATALOG_ROWS,
                        body,
                    )
                    .map_err(|e| Error::Internal {
                        detail: format!("backup envelope (catalog rows): {e}"),
                    })?;
            }
        }

        if let Ok(tset) = catalog.load_wal_tombstones() {
            let mut tombs: Vec<nodedb_types::backup_envelope::SourceTombstoneEntry> = Vec::new();
            for (tid, name, purge_lsn) in tset.iter() {
                if tid == tenant_id {
                    tombs.push(nodedb_types::backup_envelope::SourceTombstoneEntry {
                        collection: name.to_string(),
                        purge_lsn,
                    });
                }
            }
            if !tombs.is_empty()
                && let Ok(body) = zerompk::to_msgpack_vec(&tombs)
            {
                writer
                    .push_section(
                        nodedb_types::backup_envelope::SECTION_ORIGIN_SOURCE_TOMBSTONES,
                        body,
                    )
                    .map_err(|e| Error::Internal {
                        detail: format!("backup envelope (source tombstones): {e}"),
                    })?;
            }
        }
    }

    // A backup KEK must be configured; plaintext backup envelopes are no
    // longer supported.
    let envelope_bytes = match &state.backup_kek {
        Some(kek) => writer
            .finalize_encrypted(kek)
            .map_err(|e| Error::Internal {
                detail: format!("backup envelope encryption: {e}"),
            })?,
        None => {
            return Err(Error::Internal {
                detail: "backup: no [backup_encryption] KEK configured; \
                         plaintext backup envelopes are no longer supported"
                    .into(),
            });
        }
    };
    Ok(Bytes::from(envelope_bytes))
}

/// Enumerate the unique node IDs that hold any vShard right now.
///
/// In single-node mode (no routing table) returns `[self.node_id]`,
/// which may be 0 — that's fine; the section's origin_node_id is
/// only carried for traceability/debugging, not for routing.
fn unique_origin_nodes(state: &SharedState) -> Vec<u64> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return vec![state.node_id];
    };
    let table = routing.read().expect("routing table poisoned");
    let mut set = BTreeSet::new();
    for info in table.group_members().values() {
        if info.leader != 0 {
            set.insert(info.leader);
        }
        for &m in &info.members {
            set.insert(m);
        }
    }
    if set.is_empty() {
        vec![state.node_id]
    } else {
        set.into_iter().collect()
    }
}

fn is_self(state: &SharedState, node_id: u64) -> bool {
    node_id == state.node_id || node_id == 0 || state.cluster_transport.is_none()
}

async fn snapshot_self(
    state: &Arc<SharedState>,
    tenant_id: u64,
    plan: &PhysicalPlan,
) -> Result<Vec<u8>, Error> {
    sync_dispatch::dispatch_async(
        state,
        TenantId::new(tenant_id),
        // TODO(A8-followup): backup/restore not yet multi-database.
        DatabaseId::DEFAULT,
        "__system",
        plan.clone(),
        NODE_SNAPSHOT_TIMEOUT,
    )
    .await
}

async fn snapshot_remote(
    state: &Arc<SharedState>,
    node_id: u64,
    tenant_id: u64,
    plan: &PhysicalPlan,
) -> Result<Vec<u8>, Error> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| Error::Internal {
            detail: format!("backup: cluster_transport unavailable but node {node_id} is remote"),
        })?;

    let plan_bytes = plan_wire::encode(plan).map_err(|e| Error::Internal {
        detail: format!("backup: plan encode failed: {e}"),
    })?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        database_id: DatabaseId::DEFAULT.as_u64(),
        deadline_remaining_ms: NODE_SNAPSHOT_TIMEOUT.as_millis() as u64,
        trace_id: TraceId::generate().0,
        descriptor_versions: Vec::new(),
    });

    let resp = transport
        .send_rpc(node_id, req)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("backup: snapshot RPC to node {node_id} failed: {e}"),
        })?;
    match resp {
        RaftRpc::ExecuteResponse(ExecuteResponse {
            success: true,
            mut payloads,
            ..
        }) => {
            // CreateTenantSnapshot returns exactly one payload.
            if payloads.len() != 1 {
                return Err(Error::Internal {
                    detail: format!(
                        "backup: expected 1 payload from node {node_id}, got {}",
                        payloads.len()
                    ),
                });
            }
            Ok(payloads.remove(0))
        }
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(err), ..
        }) => Err(map_typed_error(err, node_id)),
        RaftRpc::ExecuteResponse(_) => Err(Error::Internal {
            detail: format!("backup: empty error response from node {node_id}"),
        }),
        other => Err(Error::Internal {
            detail: format!(
                "backup: unexpected RPC response variant from node {node_id}: {other:?}"
            ),
        }),
    }
}

fn map_typed_error(err: TypedClusterError, node_id: u64) -> Error {
    match err {
        TypedClusterError::Internal { message, .. } => Error::Internal {
            detail: format!("backup node {node_id}: {message}"),
        },
        TypedClusterError::DeadlineExceeded { elapsed_ms } => Error::Internal {
            detail: format!("backup node {node_id}: deadline exceeded after {elapsed_ms}ms"),
        },
        TypedClusterError::NotLeader { .. } => Error::Internal {
            detail: format!("backup node {node_id}: snapshot RPC routed to non-leader"),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::Internal {
            detail: format!(
                "backup node {node_id}: descriptor mismatch on collection {collection}"
            ),
        },
    }
}
