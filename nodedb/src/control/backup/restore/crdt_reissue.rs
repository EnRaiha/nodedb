// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored CRDT tenant state.
//!
//! The snapshot-install path lands CRDT state via a per-node DIRECT dispatch:
//! `RestoreTenantSnapshot` → `restore_crdt_state` → `import_snapshot_bytes`.
//! That dispatch is race-prone — on a freshly spawned cluster, if a data
//! group has not yet elected a leader at restore time, that group's nodes are
//! skipped and a later read returns `NotFound` — and it is not durable across
//! restart (no WAL record, no Raft entry).
//!
//! RESTORE instead re-issues the whole-tenant Loro snapshot durably through
//! Raft, branching on cluster vs single-node exactly like the columnar /
//! timeseries reissue:
//!
//! - Cluster (`async_raft_proposer` present): build a `ReplicatedEntry` via
//!   `to_replicated_entry` (which maps `CrdtOp::ImportSnapshot` →
//!   `ReplicatedWrite::CrdtImportTenant`) and propose it through Raft. Every
//!   replica of the data group applies `import_snapshot_bytes`, a monotonic,
//!   idempotent, commutative Loro merge that converges deterministically.
//! - Single-node: WAL-append the plan (durable for restart replay), then
//!   dispatch it into the Data Plane so it is installed live.
//!
//! ## Multi-group routing
//!
//! The tenant Loro doc is whole-tenant: a read for ANY collection routes to
//! that collection's data group. The import must therefore land on EVERY
//! distinct data group that owns any of the tenant's CRDT collections — not
//! just one. For each distinct group we pick one representative collection and
//! issue the import routed to it; the same snapshot bytes are imported on each
//! group, and the idempotent merge keeps every group's copy identical.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_types::id::DatabaseId;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::server::wal_dispatch::wal_append_if_write;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::CrdtOp;

/// Per-import dispatch timeout. Generous: a whole-tenant Loro snapshot may be
/// large.
const REISSUE_TIMEOUT: Duration = Duration::from_secs(120);

/// Re-issue one whole-tenant snapshot import to a single data group, routed by
/// `route_collection`.
///
/// Branches identically to a normal write (and to `reissue_timeseries_durably`):
/// - Cluster: `to_replicated_entry` + `propose_replicated_entry`.
/// - Single-node: `wal_append_if_write` then `sync_dispatch::dispatch_async`.
async fn reissue_crdt_to_group(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    route_collection: &str,
    bytes: Vec<u8>,
) -> crate::Result<()> {
    let vshard = VShardId::from_collection_in_database(database_id, route_collection);
    let plan = PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
        tenant_id: tenant_id.as_u64(),
        bytes,
    });

    if let Some(proposer) = state.async_raft_proposer.get() {
        let entry = crate::control::wal_replication::to_replicated_entry(tenant_id, vshard, &plan)
            .ok_or_else(|| Error::Internal {
                detail: "restore reissue: crdt import did not map to a replicated write".into(),
            })?;
        crate::control::wal_replication::propose_replicated_entry(state, proposer, entry).await?;
        return Ok(());
    }

    // Single-node: WAL first (durable for restart replay), then install live.
    wal_append_if_write(&state.wal, tenant_id, vshard, database_id, &plan)?;
    sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        route_collection,
        plan,
        REISSUE_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Resolve the set of distinct data groups owning any of `collections`, and for
/// each pick ONE representative collection whose vshard maps to it.
///
/// Reads the shared cluster routing under a read-lock (the same idiom the
/// topology split uses). When routing is unavailable (single-node) or no
/// collection resolves to a group, returns the first collection as the sole
/// representative so the import still lands once.
fn group_representatives(state: &SharedState, collections: &[String]) -> Vec<String> {
    let Some(routing_handle) = state.cluster_routing.as_ref() else {
        // Single-node: route by the first collection (any vshard resolves to
        // self). Empty collection list yields no representatives.
        return collections.first().cloned().into_iter().collect();
    };
    let routing = routing_handle
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let mut seen_groups: BTreeSet<u64> = BTreeSet::new();
    let mut reps: Vec<String> = Vec::new();
    for c in collections {
        let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, c);
        match routing.group_for_vshard(vshard.as_u32()) {
            Ok(group) => {
                if seen_groups.insert(group) {
                    reps.push(c.clone());
                }
            }
            // A collection whose vshard does not map to a group cannot route;
            // skip it. If NONE map, the fallback below picks the first.
            Err(_) => {}
        }
    }
    if reps.is_empty() {
        reps.extend(collections.first().cloned());
    }
    reps
}

/// Decode and durably re-issue every restored CRDT tenant snapshot.
///
/// Returns the number of imports that succeeded (one per distinct data group
/// per tenant snapshot). `tenant_crdt_state` entries are
/// `(tenant_id, collections, snapshot_bytes)`; `crdt_state` entries are legacy
/// `(tenant_id_string, snapshot_bytes)` pairs whose collection set is recovered
/// from `tenant_crdt_state` when available.
pub(crate) async fn reissue_crdt_snapshots(
    state: &SharedState,
    tenant_id: TenantId,
    crdt_state: Vec<(String, Vec<u8>)>,
    tenant_crdt_state: Vec<(u64, Vec<String>, Vec<u8>)>,
) -> crate::Result<usize> {
    let database_id = DatabaseId::DEFAULT;
    let mut imported = 0usize;

    // Remember each tenant's collection set so legacy `crdt_state` entries
    // (which carry no collections) can route the same per-group way.
    let mut collections_by_tid: std::collections::HashMap<u64, Vec<String>> =
        std::collections::HashMap::new();

    for (tid, collections, bytes) in tenant_crdt_state {
        collections_by_tid.insert(tid, collections.clone());
        let import_tid = TenantId::new(tid);
        let reps = group_representatives(state, &collections);
        if reps.is_empty() {
            // No collections at all — nothing to route. A whole-tenant doc with
            // zero collections has no readable data group; warn so this is not
            // silently dropped.
            tracing::warn!(
                tenant_id = tid,
                "restore reissue: tenant CRDT snapshot carried no collections; nothing to route"
            );
            continue;
        }
        for rep in reps {
            reissue_crdt_to_group(state, import_tid, database_id, &rep, bytes.clone()).await?;
            imported += 1;
        }
    }

    for (key, bytes) in crdt_state {
        // Legacy key is the tenant id as a string.
        let import_tid = match key.parse::<u64>() {
            Ok(tid) => TenantId::new(tid),
            // Unparseable legacy key: fall back to the restore's tenant scope so
            // the data is not dropped.
            Err(_) => tenant_id,
        };
        let known = collections_by_tid.get(&import_tid.as_u64());
        let reps = match known {
            Some(collections) if !collections.is_empty() => {
                group_representatives(state, collections)
            }
            _ => Vec::new(),
        };
        if reps.is_empty() {
            // No known collections for this tenant: route once via the legacy
            // key as the collection name. This still lands the import; warn that
            // per-group routing was unavailable so a multi-group cluster
            // restore of a legacy snapshot is not silently single-grouped.
            tracing::warn!(
                tenant_id = import_tid.as_u64(),
                route_collection = %key,
                "restore reissue: legacy CRDT snapshot has no known collections; \
                 routing once by key, per-group routing unavailable"
            );
            reissue_crdt_to_group(state, import_tid, database_id, &key, bytes.clone()).await?;
            imported += 1;
            continue;
        }
        for rep in reps {
            reissue_crdt_to_group(state, import_tid, database_id, &rep, bytes.clone()).await?;
            imported += 1;
        }
    }

    Ok(imported)
}
