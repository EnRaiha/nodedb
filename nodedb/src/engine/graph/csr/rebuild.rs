// SPDX-License-Identifier: BUSL-1.1

//! Origin-specific CSR rebuild from EdgeStore.

#[cfg(test)]
use nodedb_graph::CsrIndex;
use nodedb_graph::ShardedCsrIndex;
use nodedb_graph::csr::weights::extract_weight_from_properties;
#[cfg(test)]
use nodedb_types::{DatabaseId, TenantId};

use crate::engine::graph::edge_store::EdgeStore;

/// Rebuild the sharded CSR index from an EdgeStore at `system_as_of` (None =
/// current state).
pub fn rebuild_sharded_from_store(store: &EdgeStore) -> crate::Result<ShardedCsrIndex> {
    rebuild_sharded_from_store_as_of(store, None)
}

/// Rebuild the sharded CSR index from an EdgeStore using a specific
/// bitemporal cutoff.
pub fn rebuild_sharded_from_store_as_of(
    store: &EdgeStore,
    system_as_of: Option<i64>,
) -> crate::Result<ShardedCsrIndex> {
    let mut sharded = ShardedCsrIndex::new();
    let all_edges = store.scan_all_edges_decoded(system_as_of)?;

    // First pass: materialize every (database, tenant, node) so isolated
    // endpoints get stable node ids before edge insertion.
    // EdgeRecord is now (DatabaseId, TenantId, collection, src, label, dst, props).
    for (db, tid, _collection, src, _label, dst, _props) in &all_edges {
        let partition = sharded.get_or_create(*db, *tid);
        partition
            .add_node(src)
            .map_err(|e| crate::Error::Internal {
                detail: format!("CSR rebuild (add src node): {e}"),
            })?;
        partition
            .add_node(dst)
            .map_err(|e| crate::Error::Internal {
                detail: format!("CSR rebuild (add dst node): {e}"),
            })?;
    }

    // Second pass: insert edges into their tenant's partition.
    // The CSR is collection-agnostic in memory — all collections'
    // edges live in the same per-tenant partition.
    for (db, tid, _collection, src, label, dst, props) in &all_edges {
        let partition = sharded.get_or_create(*db, *tid);
        let weight = extract_weight_from_properties(props);
        let res = if weight != 1.0 {
            partition.add_edge_weighted(src, label, dst, weight)
        } else {
            partition.add_edge(src, label, dst)
        };
        res.map_err(|e| crate::Error::Internal {
            detail: format!("CSR rebuild: {e}"),
        })?;
    }

    if let Err(e) = sharded.compact_all() {
        tracing::warn!(
            layer = nodedb_types::diagnostic::DiagnosticLayer::Csr.as_str(),
            error = %e,
            "CSR compaction rejected by memory governor during rebuild; skipping"
        );
    }
    Ok(sharded)
}

/// Test shim: collapse the sharded rebuild into a single `CsrIndex`.
/// Used by test harnesses that insert under one tenant at a time.
#[cfg(test)]
pub fn rebuild_from_store(store: &EdgeStore) -> crate::Result<CsrIndex> {
    use std::collections::hash_map::Entry;

    let mut sharded = rebuild_sharded_from_store_as_of(store, None)?;
    let (db, tid) = sharded
        .iter()
        .map(|(key, _)| *key)
        .next()
        .unwrap_or((DatabaseId::DEFAULT, TenantId::new(0)));
    match sharded.entry(db, tid) {
        Entry::Occupied(entry) => Ok(entry.remove()),
        Entry::Vacant(_) => Ok(CsrIndex::new()),
    }
}
