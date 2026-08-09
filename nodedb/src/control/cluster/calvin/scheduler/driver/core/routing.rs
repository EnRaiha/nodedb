// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive, compile-enforced routing oracle: `PhysicalPlan` → `PlanRouting`.
//!
//! This is the single chokepoint the Calvin scheduler uses to decide, for
//! every plan carried in a `SequencedTxn`, whether it participates in THIS
//! node's vshard. The match is exhaustive over every `PhysicalPlan` variant
//! and every op variant nested inside it — a new variant is a compile error
//! here, not a silently-empty routing result. Mirrors the technique used by
//! the security tier's `required_permission` oracle.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};

use crate::types::{DatabaseId, VShardId};

/// Where a `PhysicalPlan` routes for Calvin cross-shard scheduling purposes.
///
/// `Vshards` carries 1 (collection-homed) or 2 (dual-homed graph edge /
/// cross-collection) participants. `Unroutable` is a KNOWN, named gap — never
/// a silent empty vec — so a caller can log/abort with the precise reason
/// instead of a generic "non-routable" message.
pub(crate) enum PlanRouting {
    /// Collection- or key-homed write: participates in exactly these vshards.
    Vshards(Vec<VShardId>),
    /// This plan must never reach the Data Plane (handled entirely on the
    /// Control Plane, e.g. cluster-fanned-out array ops).
    ControlPlaneOnly,
    /// A read or DDL op that `is_write_plan` already excludes upstream. The
    /// arm exists purely for match exhaustiveness; seeing it inside a
    /// Calvin-scheduled write txn is itself a bug.
    NotAWrite,
    /// A write whose vshard cannot be determined from the plan alone. Named
    /// so the caller's error states WHY, not just that routing failed.
    Unroutable(&'static str),
}

pub(crate) fn homes_versioned_read(
    reads: &nodedb_types::calvin::VersionedReadSet,
    database_id: DatabaseId,
    vshard_id: u32,
) -> bool {
    reads.iter().any(|entry| {
        VShardId::from_collection_in_database(database_id, &entry.collection).as_u32() == vshard_id
    })
}

fn collection_vshard_in_database(database_id: DatabaseId, collection: &str) -> VShardId {
    VShardId::from_collection_in_database(database_id, collection)
}

#[cfg(test)]
fn collection_vshard(collection: &str) -> VShardId {
    collection_vshard_in_database(DatabaseId::DEFAULT, collection)
}

/// Returns the routing decision for `plan`. Exhaustive over every
/// `PhysicalPlan` variant and every op nested inside it — adding a new
/// variant anywhere in this tree is a compile error, never a silent gap.
pub(crate) fn plan_vshard(plan: &PhysicalPlan) -> PlanRouting {
    plan_vshard_in_database(plan, DatabaseId::DEFAULT)
}

pub(crate) fn plan_vshard_in_database(plan: &PhysicalPlan, database_id: DatabaseId) -> PlanRouting {
    match plan {
        PhysicalPlan::Document(op) => document_routing(op, database_id),
        PhysicalPlan::Kv(op) => kv_routing(op, database_id),
        PhysicalPlan::Vector(op) => vector_routing(op, database_id),
        PhysicalPlan::Graph(op) => graph_routing(op),
        PhysicalPlan::Timeseries(op) => timeseries_routing(op, database_id),
        PhysicalPlan::Columnar(op) => columnar_routing(op, database_id),
        PhysicalPlan::Crdt(op) => crdt_routing(op, database_id),
        PhysicalPlan::Array(op) => array_routing(op),
        // Cluster-fanned-out array ops are handled entirely by the
        // Control-Plane `ArrayCoordinator` and never dispatched to the Data
        // Plane (see `data/executor/dispatch/visitor.rs`'s `unreachable!`).
        PhysicalPlan::ClusterArray(_) | PhysicalPlan::ClusterEvent(_) => {
            PlanRouting::ControlPlaneOnly
        }
        // Reads / query operators / metadata ops: `is_write_plan` already
        // excludes every variant of these four families upstream.
        PhysicalPlan::Text(_) => PlanRouting::NotAWrite,
        PhysicalPlan::Spatial(_) => PlanRouting::NotAWrite,
        PhysicalPlan::Query(_) => PlanRouting::NotAWrite,
        PhysicalPlan::Meta(_) => PlanRouting::NotAWrite,
    }
}

fn document_routing(op: &DocumentOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. }
        | DocumentOp::Truncate { collection, .. }
        // The balance write is homed on the TARGET collection it names, which
        // is the whole point of it being a task of its own: the source write it
        // was derived from homes elsewhere, and the pair is dual-homed by the
        // two tasks' own vshards rather than by one plan claiming both.
        | DocumentOp::ApplyBalanceDelta { collection, .. } => {
            PlanRouting::Vshards(vec![collection_vshard_in_database(database_id, collection)])
        }
        DocumentOp::InsertSelect {
            target_collection, ..
        } => PlanRouting::Vshards(vec![collection_vshard_in_database(
            database_id,
            target_collection,
        )]),
        // Both join the target with a DIFFERENT source collection; nothing on
        // the plan enforces the two live on the same vshard.
        DocumentOp::Merge { .. } | DocumentOp::UpdateFromJoin { .. } => PlanRouting::Unroutable(
            "cross-collection write: source/target co-location is not enforced",
        ),
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => PlanRouting::NotAWrite,
    }
}

fn kv_routing(op: &KvOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::Delete { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::Expire { collection, .. }
        | KvOp::Persist { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Truncate { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        // Transfer moves value between two KEYS in the SAME collection field,
        // so it stays single-home unlike `TransferItem` below.
        | KvOp::Transfer { collection, .. } => {
            PlanRouting::Vshards(vec![collection_vshard_in_database(database_id, collection)])
        }
        // Source and dest are DIFFERENT collections; no co-location guarantee.
        KvOp::TransferItem { .. } => PlanRouting::Unroutable(
            "cross-collection write: source/target co-location is not enforced",
        ),
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => PlanRouting::NotAWrite,
    }
}

fn vector_routing(op: &VectorOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        VectorOp::Insert { collection, .. }
        | VectorOp::BatchInsert { collection, .. }
        | VectorOp::Delete { collection, .. }
        | VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. }
        | VectorOp::DirectUpsert { collection, .. } => {
            PlanRouting::Vshards(vec![collection_vshard_in_database(database_id, collection)])
        }
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => PlanRouting::NotAWrite,
    }
}

fn graph_routing(op: &GraphOp) -> PlanRouting {
    match op {
        // Edge plans are key-homed (dual-homed across endpoints), not
        // collection-homed: route to from_key(src) ∪ from_key(dst).
        GraphOp::EdgePut { src_id, dst_id, .. } | GraphOp::EdgeDelete { src_id, dst_id, .. } => {
            let src_vshard = VShardId::from_key(src_id.as_bytes());
            let dst_vshard = VShardId::from_key(dst_id.as_bytes());
            if src_vshard.as_u32() == dst_vshard.as_u32() {
                PlanRouting::Vshards(vec![src_vshard])
            } else {
                PlanRouting::Vshards(vec![src_vshard, dst_vshard])
            }
        }
        // A batch is the union of its edges' homes, under the same key-homing
        // rule as the single-edge plans above.
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
            let mut vshards: Vec<VShardId> = Vec::new();
            for edge in edges {
                for endpoint in [edge.src_id.as_bytes(), edge.dst_id.as_bytes()] {
                    let vshard = VShardId::from_key(endpoint);
                    if !vshards.iter().any(|v| v.as_u32() == vshard.as_u32()) {
                        vshards.push(vshard);
                    }
                }
            }
            if vshards.is_empty() {
                // An empty batch touches nothing; it is a no-op write, not a
                // routing gap. Naming it keeps the empty vec from ever again
                // meaning "unrecognized variant".
                PlanRouting::Unroutable("edge batch carries no edges")
            } else {
                PlanRouting::Vshards(vshards)
            }
        }
        // Node-label writes are key-homed on `node_id`, the same mechanism the
        // edge plans use for their endpoints.
        GraphOp::SetNodeLabels { node_id, .. } | GraphOp::RemoveNodeLabels { node_id, .. } => {
            PlanRouting::Vshards(vec![VShardId::from_key(node_id.as_bytes())])
        }
        GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. } => PlanRouting::NotAWrite,
    }
}

fn timeseries_routing(op: &TimeseriesOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        TimeseriesOp::Ingest { collection, .. } => {
            PlanRouting::Vshards(vec![collection_vshard_in_database(database_id, collection)])
        }
        TimeseriesOp::Scan { .. } => PlanRouting::NotAWrite,
    }
}

fn columnar_routing(op: &ColumnarOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        ColumnarOp::Insert { collection, .. }
        | ColumnarOp::Update { collection, .. }
        | ColumnarOp::Delete { collection, .. } => {
            PlanRouting::Vshards(vec![collection_vshard_in_database(database_id, collection)])
        }
        ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. } => PlanRouting::NotAWrite,
    }
}

fn crdt_routing(op: &CrdtOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        CrdtOp::Apply { collection, .. }
        | CrdtOp::ApplyAuthenticated { collection, .. }
        | CrdtOp::ListInsert { collection, .. }
        | CrdtOp::ListDelete { collection, .. }
        | CrdtOp::ListMove { collection, .. }
        | CrdtOp::DocUpsert { collection, .. }
        | CrdtOp::DocDelete { collection, .. }
        | CrdtOp::SetConstraints { collection, .. }
        | CrdtOp::DropConstraints { collection, .. }
        | CrdtOp::RestoreToVersion { collection, .. }
        | CrdtOp::ImportSnapshot { collection, .. } => {
            PlanRouting::Vshards(vec![collection_vshard_in_database(database_id, collection)])
        }
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::CompactAtVersion { .. } => PlanRouting::NotAWrite,
    }
}

fn array_routing(op: &ArrayOp) -> PlanRouting {
    match op {
        // Array writes are tile-partitioned; tile->vshard needs catalog
        // tile_extents not present on the plan. `Flush` is a write per
        // `is_write_plan` (whole-memtable, not per-cell) but is likewise
        // keyed only by `ArrayId`, with no collection/tile vshard on the op.
        ArrayOp::Put { .. } | ArrayOp::Delete { .. } | ArrayOp::Flush { .. } => {
            PlanRouting::Unroutable(
                "array writes are tile-partitioned; tile->vshard needs catalog tile_extents not present on the plan",
            )
        }
        ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => PlanRouting::NotAWrite,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::{ClusterArrayOp, CrdtOp, GraphOp};
    use nodedb_types::{
        PayloadIndexKind, Surrogate, TenantId, VectorQuantization, VectorStorageDtype,
    };

    fn vshards_of(plan: &PhysicalPlan) -> Vec<u32> {
        match plan_vshard(plan) {
            PlanRouting::Vshards(v) => v.iter().map(|x| x.as_u32()).collect(),
            PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
                panic!("expected Vshards routing")
            }
        }
    }

    /// Find two distinct string keys whose `from_key` vShards differ.
    fn two_distinct_key_vshards() -> (String, String, u32, u32) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..2048 {
            let key = format!("node_{i}");
            let v = VShardId::from_key(key.as_bytes()).as_u32();
            if let Some((ref fkey, fv)) = first {
                if fv != v {
                    return (fkey.clone(), key, fv, v);
                }
            } else {
                first = Some((key, v));
            }
        }
        panic!("could not find two distinct-vshard keys in 2048 tries");
    }

    #[test]
    fn plan_vshard_routes_edge_to_both_endpoints() {
        let (src_id, dst_id, src_v, dst_v) = two_distinct_key_vshards();
        assert_ne!(src_v, dst_v);

        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: "follows".to_owned(),
            src_id: src_id.clone(),
            label: "knows".to_owned(),
            dst_id: dst_id.clone(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        });

        let mut got = vshards_of(&plan);
        got.sort_unstable();
        let mut want = vec![src_v, dst_v];
        want.sort_unstable();
        assert_eq!(got, want, "edge plan routes to both from_key endpoints");
    }

    #[test]
    fn plan_vshard_routes_edge_batch_to_union_of_endpoints() {
        let (src_id, dst_id, src_v, dst_v) = two_distinct_key_vshards();
        assert_ne!(src_v, dst_v);

        let edge = |src: &str, dst: &str| nodedb_physical::physical_plan::BatchEdge {
            collection: "follows".to_owned(),
            src_id: src.to_owned(),
            label: "knows".to_owned(),
            dst_id: dst.to_owned(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        };

        // Two edges over the same endpoint pair: the union must dedupe.
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: vec![edge(&src_id, &dst_id), edge(&src_id, &dst_id)],
        });

        let mut got = vshards_of(&plan);
        got.sort_unstable();
        let mut want = vec![src_v, dst_v];
        want.sort_unstable();
        assert_eq!(got, want, "edge batch routes to the union of its endpoints");
    }

    #[test]
    fn plan_vshard_routes_node_labels_by_node_key() {
        let (node_id, _, node_v, _) = two_distinct_key_vshards();

        for plan in [
            PhysicalPlan::Graph(GraphOp::SetNodeLabels {
                node_id: node_id.clone(),
                labels: vec!["Person".to_owned()],
            }),
            PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
                node_id: node_id.clone(),
                labels: vec!["Person".to_owned()],
            }),
        ] {
            assert_eq!(
                vshards_of(&plan),
                vec![node_v],
                "node-label write is key-homed on node_id"
            );
        }
    }

    #[test]
    fn plan_vshard_single_when_endpoints_collide() {
        // src == dst → a single deduped vShard.
        let key = "self".to_owned();
        let v = VShardId::from_key(key.as_bytes()).as_u32();
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: "follows".to_owned(),
            src_id: key.clone(),
            label: "knows".to_owned(),
            dst_id: key,
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(1),
        });
        assert_eq!(vshards_of(&plan), vec![v]);
    }

    #[test]
    fn plan_vshard_routes_crdt_list_ops_to_collection_vshard() {
        let list_ops = [
            (
                "ListInsert",
                PhysicalPlan::Crdt(CrdtOp::ListInsert {
                    collection: "docs".to_owned(),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    index: 0,
                    fields_json: "{}".to_owned(),
                    surrogate: Surrogate::new(1),
                }),
            ),
            (
                "ListDelete",
                PhysicalPlan::Crdt(CrdtOp::ListDelete {
                    collection: "docs".to_owned(),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    index: 0,
                    surrogate: Surrogate::new(1),
                }),
            ),
            (
                "ListMove",
                PhysicalPlan::Crdt(CrdtOp::ListMove {
                    collection: "docs".to_owned(),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    from_index: 0,
                    to_index: 1,
                    surrogate: Surrogate::new(1),
                }),
            ),
        ];

        for (name, plan) in &list_ops {
            assert!(
                matches!(plan_vshard(plan), PlanRouting::Vshards(_)),
                "{name} must be routable"
            );
        }
    }

    #[test]
    fn document_truncate_routes_to_collection_vshard() {
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: "docs".to_owned(),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
        });
        let want = collection_vshard("docs").as_u32();
        assert_eq!(vshards_of(&plan), vec![want]);
    }

    #[test]
    fn collection_routing_preserves_database_scope() {
        let collection = (0..2048)
            .map(|i| format!("db_scoped_{i}"))
            .find(|name| {
                collection_vshard_in_database(DatabaseId::DEFAULT, name)
                    != collection_vshard_in_database(DatabaseId::new(7), name)
            })
            .expect("collection whose home differs by database");
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: collection.clone(),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
        });
        let expected = collection_vshard_in_database(DatabaseId::new(7), &collection);
        match plan_vshard_in_database(&plan, DatabaseId::new(7)) {
            PlanRouting::Vshards(actual) => assert_eq!(actual, vec![expected]),
            PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
                panic!("document truncate must be database-scoped")
            }
        }
    }

    #[test]
    fn columnar_update_and_delete_route_to_collection_vshard() {
        let want = collection_vshard("metrics").as_u32();

        let update = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: "metrics".to_owned(),
            filters: Vec::new(),
            updates: Vec::new(),
            rls_write_check: Vec::new(),
        });
        assert_eq!(vshards_of(&update), vec![want]);

        let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: "metrics".to_owned(),
            filters: Vec::new(),
            rls_write_check: Vec::new(),
        });
        assert_eq!(vshards_of(&delete), vec![want]);
    }

    #[test]
    fn vector_direct_upsert_and_multi_vector_delete_route_to_collection_vshard() {
        let want = collection_vshard("vecs").as_u32();

        let direct_upsert = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: "vecs".to_owned(),
            field: "emb".to_owned(),
            surrogate: Surrogate::new(3),
            vector: vec![0.5, 0.6],
            payload: vec![1, 2, 3],
            quantization: VectorQuantization::None,
            storage_dtype: VectorStorageDtype::F32,
            payload_indexes: vec![("tenant_id".to_owned(), PayloadIndexKind::Equality)],
            returning: None,
            rls_filters: Vec::new(),
        });
        assert_eq!(vshards_of(&direct_upsert), vec![want]);

        let multi_vector_delete = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: "vecs".to_owned(),
            field_name: "colbert".to_owned(),
            document_surrogate: Surrogate::new(2),
        });
        assert_eq!(vshards_of(&multi_vector_delete), vec![want]);
    }

    #[test]
    fn crdt_set_drop_constraints_and_restore_route_to_collection_vshard() {
        let want = collection_vshard("docs").as_u32();

        let set_constraints = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: "docs".to_owned(),
            constraint_version: 1,
            constraints: Vec::new(),
        });
        assert_eq!(vshards_of(&set_constraints), vec![want]);

        let drop_constraints = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: "docs".to_owned(),
            constraint_version: 1,
        });
        assert_eq!(vshards_of(&drop_constraints), vec![want]);

        let restore = PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
            collection: "docs".to_owned(),
            document_id: "id1".to_owned(),
            target_version_json: "{}".to_owned(),
            surrogate: Surrogate::new(1),
        });
        assert_eq!(vshards_of(&restore), vec![want]);
    }

    #[test]
    fn cluster_array_routes_control_plane_only() {
        let plan = PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            array_id_msgpack: Vec::new(),
            cells: Vec::new(),
            wal_lsn: 0,
            prefix_bits: 8,
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::ControlPlaneOnly));
    }

    #[test]
    fn document_merge_is_unroutable() {
        let plan = PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: "docs".to_owned(),
            source_collection: "staging".to_owned(),
            source_alias: "s".to_owned(),
            target_join_col: "id".to_owned(),
            source_join_col: "id".to_owned(),
            clauses: Vec::new(),
            returning: None,
            resolve_only: false,
            resolved_inserts: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn array_put_is_unroutable() {
        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn document_update_from_join_is_unroutable() {
        let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: "docs".to_owned(),
            source_collection: "staging".to_owned(),
            source_alias: "s".to_owned(),
            target_join_col: "id".to_owned(),
            source_join_col: "id".to_owned(),
            updates: Vec::new(),
            target_filters: Vec::new(),
            returning: None,
            resolve_only: false,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn kv_transfer_item_is_unroutable() {
        let plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: "inbox".to_owned(),
            dest_collection: "archive".to_owned(),
            item_key: vec![1, 2, 3],
            dest_key: vec![4, 5, 6],
            surrogate: Surrogate::new(1),
            source_rls_write_check: Vec::new(),
            dest_rls_write_check: Vec::new(),
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn empty_edge_batch_is_unroutable_not_silently_empty() {
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: Vec::new() });
        assert!(
            matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)),
            "an empty edge batch must be a named Unroutable, never a silent empty vshard list"
        );
    }
}
