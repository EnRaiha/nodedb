// SPDX-License-Identifier: BUSL-1.1

//! Topology-aware snapshot bucketing for RESTORE TENANT.

use std::collections::BTreeMap;

use nodedb_cluster::routing::{VSHARD_COUNT, vshard_for_collection};
use nodedb_types::id::DatabaseId;

use crate::control::backup::snapshot_keys::{
    extract_db_scoped_collection, extract_db_tenant_scoped_collection,
};
use crate::control::state::SharedState;
use crate::types::TenantDataSnapshot;

/// Bucketed output from `split_by_current_topology`.
pub(super) struct SplitOutput {
    pub buckets: BTreeMap<u64, TenantDataSnapshot>,
    pub malformed_keys: usize,
    pub route_fallbacks: usize,
}

enum RouteOutcome {
    Routed(u64),
    Malformed,
    NoLeader,
}

/// Bucket the merged snapshot per current vshard ownership.
///
/// Replicated-by-design data (graph edges, CRDT state) goes to every
/// owning node. Single-node mode is the degenerate case: everything to self.
pub(super) fn split_by_current_topology(
    state: &SharedState,
    tenant_id: u64,
    merged: TenantDataSnapshot,
) -> SplitOutput {
    let routing = state
        .cluster_routing
        .as_ref()
        .map(|r| r.read().unwrap_or_else(|poisoned| poisoned.into_inner()));
    let single_node = routing.is_none() || state.cluster_transport.is_none();

    if single_node {
        let mut out = BTreeMap::new();
        out.insert(state.node_id, merged);
        return SplitOutput {
            buckets: out,
            malformed_keys: 0,
            route_fallbacks: 0,
        };
    }
    let routing =
        routing.expect("invariant: single_node is false, so routing.is_some() is guaranteed");

    let mut all_owners = BTreeMap::<u64, TenantDataSnapshot>::new();
    for vshard in 0..VSHARD_COUNT {
        if let Ok(node) = routing.leader_for_vshard(vshard)
            && node != 0
        {
            all_owners.entry(node).or_default();
        }
    }
    if all_owners.is_empty() {
        all_owners.insert(state.node_id, TenantDataSnapshot::default());
    }

    // Restore today operates on `DatabaseId::DEFAULT`; the snapshot/topology
    // wire format gains a database_id alongside tenant_id when multi-database
    // restore lands, at which point this binding moves up to a parameter.
    let database_id = DatabaseId::DEFAULT;
    let route_collection = |coll: &str| -> RouteOutcome {
        let v = vshard_for_collection(database_id, coll);
        match routing.leader_for_vshard(v) {
            Ok(leader) if leader != 0 => RouteOutcome::Routed(leader),
            _ => RouteOutcome::NoLeader,
        }
    };
    // Documents / indexes / vectors / timeseries keys are db-tenant-scoped:
    // `"{db}:{tid}:{collection}[:suffix]"` (collection never contains ':'/'\0').
    let route_key = |key: &str| -> RouteOutcome {
        match extract_db_tenant_scoped_collection(key, tenant_id) {
            Some(coll) => route_collection(coll),
            None => RouteOutcome::Malformed,
        }
    };
    // Columnar / flushed-ts keys are db-scoped: `"{db}:{tid}:{collection}"`
    // where the collection is the whole remainder and may itself contain ':'.
    let route_db_scoped_key = |key: &str| -> RouteOutcome {
        match extract_db_scoped_collection(key, tenant_id) {
            Some(coll) => route_collection(coll),
            None => RouteOutcome::Malformed,
        }
    };

    let mut malformed = 0usize;
    let mut fallbacks = 0usize;
    let mut resolve = |outcome: RouteOutcome, key: Option<&str>| -> u64 {
        match outcome {
            RouteOutcome::Routed(node) => node,
            RouteOutcome::Malformed => {
                malformed += 1;
                if let Some(k) = key {
                    let prefix: String = k.chars().take(64).collect();
                    tracing::warn!(tenant_id, key_prefix = %prefix, "restore: malformed key");
                }
                state.node_id
            }
            RouteOutcome::NoLeader => {
                fallbacks += 1;
                state.node_id
            }
        }
    };

    for entry in merged.documents {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().documents.push(entry);
    }
    for entry in merged.indexes {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().indexes.push(entry);
    }
    for entry in merged.kv_tables {
        let node = resolve(route_collection(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().kv_tables.push(entry);
    }
    for entry in merged.timeseries {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().timeseries.push(entry);
    }
    // Plain-columnar engine state is NOT installed via the snapshot path: the
    // snapshot-install lands data in in-memory-only Data Plane maps with no WAL
    // record and no Raft entry, so it is lost on restart (single-node) and never
    // reaches replicas (cluster). RESTORE re-issues columnar rows as durable
    // `ColumnarOp::Insert`s instead (see `columnar_reissue`); `merged.columnar_engines`
    // is therefore drained by the caller before this split and never bucketed here.
    debug_assert!(
        merged.columnar_engines.is_empty(),
        "columnar engines must be drained before topology split"
    );
    // Vector engine state is likewise NOT installed via the snapshot path:
    // RESTORE re-issues each restored vector as a durable `VectorOp::Insert`
    // instead (see `vector_reissue`); `merged.vectors` is therefore drained
    // by the caller before this split and never bucketed here.
    debug_assert!(
        merged.vectors.is_empty(),
        "vectors must be drained before topology split"
    );
    for blob in merged.flushed_ts_segments {
        let node = resolve(
            route_db_scoped_key(&blob.collection_key),
            Some(&blob.collection_key),
        );
        all_owners
            .entry(node)
            .or_default()
            .flushed_ts_segments
            .push(blob);
    }

    // Replicated-by-design: every owning node gets a copy.
    for entry in &merged.edges {
        for snap in all_owners.values_mut() {
            snap.edges.push(entry.clone());
        }
    }
    // CRDT state is NOT bucketed here: the per-node snapshot fan-out is
    // race-prone (skips data groups that have not elected a leader yet) and not
    // durable across restart. RESTORE drains the CRDT section before this split
    // and re-issues each collection's Loro snapshot durably through Raft to its
    // owning data group (see `crdt_reissue`). The vec is therefore empty here by
    // contract.
    debug_assert!(
        merged.crdt_state.is_empty(),
        "CRDT state must be drained before topology split"
    );

    SplitOutput {
        buckets: all_owners,
        malformed_keys: malformed,
        route_fallbacks: fallbacks,
    }
}

pub(super) fn is_self(state: &SharedState, node_id: u64) -> bool {
    node_id == state.node_id || node_id == 0 || state.cluster_transport.is_none()
}
