// SPDX-License-Identifier: BUSL-1.1

//! Origin-specific CSR rebuild from EdgeStore.

use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
use nodedb_graph::CsrIndex;
use nodedb_graph::csr::weights::extract_weight_from_properties;
use nodedb_graph::{CsrBulkBuilder, ShardedCsrIndex};
#[cfg(test)]
use nodedb_types::{DatabaseId, TenantId};

use crate::engine::graph::edge_store::EdgeStore;

/// Rebuild the sharded CSR index from an EdgeStore at `system_as_of` (None =
/// current state).
pub fn rebuild_sharded_from_store(store: &EdgeStore) -> crate::Result<ShardedCsrIndex> {
    rebuild_sharded_from_store_with_governor(store, None, None)
}

/// Rebuild the current durable graph while enforcing the graph engine's
/// configured memory budget.
pub fn rebuild_sharded_from_store_governed(
    store: &EdgeStore,
    governor: Arc<nodedb_mem::MemoryGovernor>,
) -> crate::Result<ShardedCsrIndex> {
    rebuild_sharded_from_store_with_governor(store, None, Some(governor))
}

/// Rebuild the sharded CSR index from an EdgeStore using a specific
/// bitemporal cutoff.
pub fn rebuild_sharded_from_store_as_of(
    store: &EdgeStore,
    system_as_of: Option<i64>,
) -> crate::Result<ShardedCsrIndex> {
    rebuild_sharded_from_store_with_governor(store, system_as_of, None)
}

fn rebuild_sharded_from_store_with_governor(
    store: &EdgeStore,
    system_as_of: Option<i64>,
    governor: Option<Arc<nodedb_mem::MemoryGovernor>>,
) -> crate::Result<ShardedCsrIndex> {
    let node_surrogates = store.scan_all_node_surrogates()?;
    let mut builders: HashMap<(nodedb_types::DatabaseId, nodedb_types::TenantId), CsrBulkBuilder> =
        HashMap::new();

    // Register durable nodes first so standalone vertices survive and local-id
    // assignment remains stable across rebuilds.
    for (db, tid, node, _) in &node_surrogates {
        builders
            .entry((*db, *tid))
            .or_insert_with(|| new_builder(governor.as_ref()))
            .register_node(node)
            .map_err(|error| crate::Error::Internal {
                detail: format!("CSR rebuild (register node): {error}"),
            })?;
    }

    // The temporal scanner emits one resolved live record per complete edge
    // identity. Intern it once into a compact temporary stream, then build exact
    // dense arrays without mutation-buffer duplicate scans or compaction.
    store.for_each_edge_decoded(
        system_as_of,
        |(db, tid, collection, src, label, dst, props)| {
            builders
                .entry((db, tid))
                .or_insert_with(|| new_builder(governor.as_ref()))
                .push_unique_edge(
                    &src,
                    &label,
                    &dst,
                    &collection,
                    extract_weight_from_properties(&props),
                )
                .map_err(|error| crate::Error::Internal {
                    detail: format!("CSR rebuild (collect edge): {error}"),
                })
        },
    )?;

    let mut sharded = ShardedCsrIndex::new();
    for ((db, tid), builder) in builders {
        let partition = builder.finish().map_err(|error| crate::Error::Internal {
            detail: format!("CSR rebuild (dense build): {error}"),
        })?;
        sharded.install_partition(db, tid, partition);
    }

    // Restore each node's global identity after dense construction.
    for (db, tid, node, raw) in node_surrogates {
        if let Some(partition) = sharded.partition_mut(db, tid) {
            partition.set_node_surrogate(&node, nodedb_types::Surrogate::new(raw));
        }
    }
    Ok(sharded)
}

fn new_builder(governor: Option<&Arc<nodedb_mem::MemoryGovernor>>) -> CsrBulkBuilder {
    governor
        .cloned()
        .map(CsrBulkBuilder::with_governor)
        .unwrap_or_default()
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
    fn governed_rebuild_surfaces_dense_allocation_rejection() {
        use nodedb_mem::{EngineId, GovernorConfig, MemoryGovernor};

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
        let governor = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: 16,
                engine_limits: HashMap::from([(EngineId::Graph, 16)]),
            })
            .unwrap(),
        );

        let error = match rebuild_sharded_from_store_governed(&store, governor) {
            Err(error) => error,
            Ok(_) => panic!("expected governed rebuild rejection"),
        };
        assert!(error.to_string().contains("memory budget"));
    }

    #[test]
    fn bulk_rebuild_preserves_collections_weights_and_incremental_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        let mut weight = Vec::new();
        nodedb_query::msgpack_scan::write_map_header(&mut weight, 1);
        nodedb_query::msgpack_scan::write_kv_f64(&mut weight, "weight", 2.5);
        for collection in ["people", "archive"] {
            store
                .put_edge_versioned(
                    EdgeRef::new(DB, tenant(), collection, "a", "knows", "b"),
                    &weight,
                    100,
                    100,
                    i64::MAX,
                )
                .unwrap();
        }

        let mut csr = rebuild_from_store(&store).unwrap();
        let a = csr.node_id_raw("a").unwrap();
        let b = csr.node_id_raw("b").unwrap();
        for collection in ["people", "archive"] {
            let collection = csr.collection_id(collection).unwrap();
            assert_eq!(
                csr.iter_out_edges_raw_in(a, collection)
                    .filter(|(_, destination)| *destination == b)
                    .count(),
                1
            );
        }
        assert_eq!(
            csr.iter_out_edges_weighted_raw(a)
                .filter(|(_, destination, weight)| *destination == b && *weight == 2.5)
                .count(),
            2
        );

        csr.add_edge_in_collection("b", "knows", "c", "people")
            .unwrap();
        csr.compact().unwrap();
        assert!(csr.contains_node("c"));
        assert_eq!(csr.edge_count(), 3);
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
