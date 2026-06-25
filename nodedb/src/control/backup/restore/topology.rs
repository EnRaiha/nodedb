// SPDX-License-Identifier: BUSL-1.1

//! Topology-aware snapshot bucketing for RESTORE TENANT.

use std::collections::BTreeMap;

use nodedb_cluster::routing::{VSHARD_COUNT, vshard_for_collection};
use nodedb_types::id::DatabaseId;

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
    let routing = state.cluster_routing.as_ref().map(|r| {
        r.read()
            .expect("invariant: cluster_routing RwLock is not poisoned")
    });
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
    let route_key = |key: &str| -> RouteOutcome {
        match extract_collection(key, tenant_id) {
            Some(coll) => route_collection(coll),
            None => RouteOutcome::Malformed,
        }
    };
    // Columnar / flushed-ts keys are `"{db}:{tid}:{collection}"` (db-prefixed),
    // unlike the `extract_collection`/`route_key` shape (`"{tid}:{collection}:..."`).
    // Mirror `parse_timeseries_snapshot_key` (data-plane handler, different
    // module): split on the first two ':' — first=db, second=tid, rest=collection.
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
    for entry in merged.vectors {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().vectors.push(entry);
    }
    for entry in merged.kv_tables {
        let node = resolve(route_collection(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().kv_tables.push(entry);
    }
    for entry in merged.timeseries {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().timeseries.push(entry);
    }
    // Columnar/flushed-ts are sharded by collection (like timeseries), not
    // replicated. Both use the `"{db}:{tid}:{collection}"` key shape.
    for entry in merged.columnar_engines {
        let node = resolve(route_db_scoped_key(&entry.0), Some(&entry.0));
        all_owners
            .entry(node)
            .or_default()
            .columnar_engines
            .push(entry);
    }
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
    for entry in &merged.crdt_state {
        for snap in all_owners.values_mut() {
            snap.crdt_state.push(entry.clone());
        }
    }

    SplitOutput {
        buckets: all_owners,
        malformed_keys: malformed,
        route_fallbacks: fallbacks,
    }
}

pub(super) fn extract_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let prefix_owned = format!("{tenant_id}:");
    let after = key.strip_prefix(prefix_owned.as_str())?;
    let coll = after.split(['\0', ':']).next()?;
    if coll.is_empty() { None } else { Some(coll) }
}

/// Extract the collection from a db-scoped `"{db}:{tid}:{collection}"` key,
/// verifying the embedded tenant matches `tenant_id`.
///
/// Mirrors the data-plane `parse_timeseries_snapshot_key` format (first two
/// ':' are structural; the collection may itself contain ':'). Returns `None`
/// when the key has fewer than three parts or the tenant does not match.
pub(super) fn extract_db_scoped_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let mut it = key.splitn(3, ':');
    let _db = it.next()?;
    let tid = it.next()?;
    let coll = it.next()?;
    if tid.parse::<u64>().ok()? != tenant_id || coll.is_empty() {
        return None;
    }
    Some(coll)
}

pub(super) fn is_self(state: &SharedState, node_id: u64) -> bool {
    node_id == state.node_id || node_id == 0 || state.cluster_transport.is_none()
}

#[cfg(test)]
mod tests {
    use super::{extract_collection, extract_db_scoped_collection};

    #[test]
    fn extract_collection_strips_prefix() {
        assert_eq!(extract_collection("7:users:doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users\u{0}doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users", 7), Some("users"));
        assert_eq!(extract_collection("8:users:doc1", 7), None);
        assert_eq!(extract_collection("7:", 7), None);
    }

    #[test]
    fn extract_db_scoped_collection_parses_db_prefixed_key() {
        // "{db}:{tid}:{collection}" — first two ':' are structural.
        assert_eq!(
            extract_db_scoped_collection("0:7:metrics", 7),
            Some("metrics")
        );
        // Collection may itself contain ':'.
        assert_eq!(
            extract_db_scoped_collection("0:7:a:b", 7),
            Some("a:b"),
            "collection retains embedded ':'"
        );
        // Tenant mismatch → None.
        assert_eq!(extract_db_scoped_collection("0:8:metrics", 7), None);
        // Missing collection part → None.
        assert_eq!(extract_db_scoped_collection("0:7", 7), None);
        // Empty collection → None.
        assert_eq!(extract_db_scoped_collection("0:7:", 7), None);
    }
}
