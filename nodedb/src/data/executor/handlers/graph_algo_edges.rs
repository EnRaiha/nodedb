// SPDX-License-Identifier: BUSL-1.1

//! The edges a graph algorithm runs over, and the CSR built from them.
//!
//! The local stage reads this core's edge store. The export stage answers
//! those edges so the coordinator can gather them from every core. The
//! gathered stage builds its CSR from the union. All three go through
//! [`csr_from_edges`], so a CSR built from the same edges in the same order
//! is the same CSR on every path.

use nodedb_graph::CsrIndex;
use nodedb_graph::csr::weights::extract_weight_from_properties;
use nodedb_physical::physical_plan::AlgoEdge;
use nodedb_types::{DatabaseId, TenantId};

use crate::engine::graph::edge_store::EdgeStore;

/// The edges of `(database, tid, collection)` in this core's edge store,
/// optionally one label only, in edge-store order. `system_as_of` selects a
/// historical snapshot; `None` reads current state.
pub(super) fn collection_edges(
    edge_store: &EdgeStore,
    database_id: u64,
    tid: u64,
    collection: &str,
    edge_label: Option<&str>,
    system_as_of: Option<i64>,
) -> crate::Result<Vec<AlgoEdge>> {
    let records = edge_store.scan_all_edges_decoded(system_as_of)?;
    let target_db = DatabaseId::new(database_id);
    let target_tid = TenantId::new(tid);
    Ok(records
        .into_iter()
        .filter(|(rec_db, rec_tid, coll, _, label, _, _)| {
            *rec_db == target_db
                && *rec_tid == target_tid
                && coll == collection
                && edge_label.is_none_or(|el| label == el)
        })
        .map(|(_, _, _, src, label, dst, props)| AlgoEdge {
            weight: extract_weight_from_properties(&props),
            src,
            label,
            dst,
        })
        .collect())
}

/// Build a CSR from `edges`, in their order.
///
/// Two passes: first intern every endpoint so isolated nodes get stable ids,
/// then insert the edges. This matches `CsrSnapshot::from_edge_store_as_of`.
pub(super) fn csr_from_edges(
    edges: &[AlgoEdge],
    memory: nodedb_mem::ScopedMemory,
) -> crate::Result<CsrIndex> {
    let mut csr = CsrIndex::new(memory);
    for edge in edges {
        csr.add_node(&edge.src)?;
        csr.add_node(&edge.dst)?;
    }
    for edge in edges {
        if edge.weight != 1.0 {
            csr.add_edge_weighted(&edge.src, &edge.label, &edge.dst, edge.weight)?;
        } else {
            csr.add_edge(&edge.src, &edge.label, &edge.dst)?;
        }
    }
    csr.compact()?;
    Ok(csr)
}
