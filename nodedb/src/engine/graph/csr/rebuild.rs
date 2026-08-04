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
    let node_surrogates = store.scan_all_node_surrogates()?;

    // First pass: materialize every explicitly registered node, including
    // standalone vertices that do not occur as an edge endpoint.
    for (db, tid, node, _) in &node_surrogates {
        sharded
            .get_or_create(*db, *tid)
            .add_node(node)
            .map_err(|e| crate::Error::Internal {
                detail: format!("CSR rebuild (add registered node): {e}"),
            })?;
    }

    // Insert each edge once. CsrIndex edge insertion interns missing endpoints,
    // so a separate endpoint pass only repeats two hash lookups per edge.
    // Consuming the decoded records also releases their strings/properties as
    // rebuilding progresses instead of retaining the full vector until compact.
    store.for_each_edge_decoded(
        system_as_of,
        |(db, tid, collection, src, label, dst, props)| {
            let partition = sharded.get_or_create(db, tid);
            let weight = extract_weight_from_properties(&props);
            let result = if weight != 1.0 {
                partition.add_edge_weighted_in_collection(&src, &label, &dst, &collection, weight)
            } else {
                partition.add_edge_in_collection(&src, &label, &dst, &collection)
            };
            result.map_err(|error| crate::Error::Internal {
                detail: format!("CSR rebuild: {error}"),
            })
        },
    )?;

    // Third pass: restore each node's global identity. Without this the rebuilt
    // index knows the graph's shape but binds no surrogate, so every
    // cross-engine read — which meets the other engines on the surrogate — sees
    // an empty graph side until live writes happen to rebind the nodes.
    for (db, tid, node, raw) in node_surrogates {
        if let Some(partition) = sharded.partition_mut(db, tid) {
            partition.set_node_surrogate(&node, nodedb_types::Surrogate::new(raw));
        }
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

#[cfg(test)]
mod tests {
    use nodedb_graph::{Direction, SurrogateBfsParams};
    use nodedb_types::{DatabaseId, Surrogate, TenantId};

    use super::*;
    use crate::engine::graph::edge_store::EdgeRef;

    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn tenant() -> TenantId {
        TenantId::new(1)
    }

    fn store_with_a_bound_edge(path: &std::path::Path) -> EdgeStore {
        let store = EdgeStore::open(path).unwrap();
        store
            .put_edge_versioned(
                EdgeRef::new(DB, tenant(), "people", "a", "knows", "b")
                    .with_surrogates(Surrogate::new(10), Surrogate::new(20)),
                b"{}",
                100,
                100,
                i64::MAX,
            )
            .unwrap();
        store
    }

    /// The whole point of the durable binding: reopen the store, rebuild, and a
    /// surrogate-domain read still finds the graph.
    ///
    /// Before the identity table existed, the rebuild produced a structurally
    /// correct CSR with no surrogate bound to any node — so a cross-engine
    /// traversal reached everything and could report none of it, which reads as
    /// "the graph is empty" rather than "identity was lost on restart".
    #[test]
    fn a_reopened_store_rebuilds_with_node_identities_intact() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        drop(store_with_a_bound_edge(&path));

        // Reopen: this is what `CoreLoop::open` does on every start.
        let reopened = EdgeStore::open(&path).unwrap();
        let csr = rebuild_from_store(&reopened).unwrap();

        assert_eq!(csr.node_surrogate("a"), Some(Surrogate::new(10)));
        assert_eq!(csr.node_surrogate("b"), Some(Surrogate::new(20)));

        let seeds = [csr.local_id_for_surrogate(Surrogate::new(10)).expect(
            "a surrogate-seeded read must resolve after a restart, not just after a live write",
        )];
        let hops = csr.traverse_surrogates_in_collection(SurrogateBfsParams {
            seeds: &seeds,
            label_filter: None,
            direction: Direction::Out,
            max_depth: 2,
            max_visited: 100,
            collection: "people",
        });
        assert!(
            hops.reached.contains(Surrogate::new(20)),
            "the neighbour must be intersectable with another engine's bitmap"
        );
        assert_eq!(hops.unaddressable, 0);
    }

    #[test]
    fn explicit_node_binding_preserves_a_standalone_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        store
            .import_node_surrogates(&[(DB, tenant(), "isolated".to_string(), 99)])
            .unwrap();

        let csr = rebuild_from_store(&store).unwrap();
        assert!(csr.contains_node("isolated"));
        assert_eq!(csr.node_surrogate("isolated"), Some(Surrogate::new(99)));
    }

    /// A store written before the identity table existed rebuilds normally —
    /// with no bindings, which is exactly the state it was in.
    #[test]
    fn a_rebuild_without_any_bindings_still_produces_the_graph() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        store
            .put_edge_versioned(
                EdgeRef::new(DB, tenant(), "people", "a", "knows", "b"),
                b"{}",
                100,
                100,
                i64::MAX,
            )
            .unwrap();

        let csr = rebuild_from_store(&store).unwrap();
        assert!(csr.contains_node("a") && csr.contains_node("b"));
        assert_eq!(csr.node_surrogate("a"), None);
    }

    #[test]
    fn rebuild_includes_an_edge_resurrected_after_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        let edge = EdgeRef::new(DB, tenant(), "people", "a", "knows", "b");
        store
            .put_edge_versioned(edge, b"v1", 100, 100, i64::MAX)
            .unwrap();
        store.soft_delete_edge(edge, 200).unwrap();
        store
            .put_edge_versioned(edge, b"v3", 300, 300, i64::MAX)
            .unwrap();

        let csr = rebuild_from_store(&store).unwrap();
        let a = csr.node_id_raw("a").unwrap();
        let b = csr.node_id_raw("b").unwrap();
        assert!(
            csr.iter_out_edges_raw(a)
                .any(|(_, destination)| destination == b)
        );
    }

    /// A cascade delete takes the node's binding with it, and leaves the
    /// neighbours' alone.
    #[test]
    fn a_cascaded_node_delete_drops_only_that_nodes_binding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let store = store_with_a_bound_edge(&path);
        store
            .delete_edges_for_node(DB.as_u64(), tenant(), "a", 200)
            .unwrap();

        let remaining = store.scan_all_node_surrogates().unwrap();
        assert_eq!(remaining.len(), 1, "only `a` is gone: {remaining:?}");
        assert_eq!(remaining[0].2, "b");
    }
}
