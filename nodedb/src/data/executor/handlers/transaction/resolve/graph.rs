// SPDX-License-Identifier: BUSL-1.1

//! Graph serializer for transaction resolve.
//!
//! Turns the graph edge post-images a transaction staged into its
//! [`GraphTxnOverlay`] into the engine-native WAL sub-record shapes the graph
//! redo replay path decodes (`wal_replay_redo_graph.rs`) — the SAME shapes the
//! autocommit graph WAL path produces, extended with the two endpoint
//! surrogates a redo PUT carries and an autocommit PUT does not:
//!
//! * A staged edge put → `RecordType::Put`, payload
//!   [`crate::wal::EdgePutRedo`]. Replay's `execute_edge_put` repopulates the
//!   CSR node→surrogate map from the two endpoint surrogates, so they must be
//!   present and correct.
//! * A staged edge tombstone → `RecordType::Delete`, payload
//!   [`crate::wal::EdgeDeleteRedo`]. `execute_edge_delete` needs no surrogate.
//!
//! Both payloads are named structs shared by every encode/decode site (a single
//! definition, not a positional tuple re-declared per site) so the field set is
//! a compile-time invariant that cannot silently drift in arity.
//!
//! ## Where the endpoint surrogates come from
//!
//! [`GraphTxnOverlay`] does NOT carry endpoint surrogates: `stage_edge_put`
//! stores only the identity `(src, label, dst)` and the properties blob (see
//! `stage_write::stage_graph::execute_stage_graph`, which destructures
//! `GraphOp::EdgePut` with `..` and drops `src_surrogate` / `dst_surrogate`
//! before calling `stage_edge_put`). The surrogates are NOT lost, though —
//! they were resolved once, at physical-plan construction time, and still
//! live on the `GraphOp::EdgePut` / `GraphOp::EdgePutBatch` plan nodes
//! themselves (`nodedb-physical/src/physical_plan/graph/op.rs` documents
//! `src_surrogate` / `dst_surrogate` as "resolved at construction time").
//! `entry.rs` collects them into an `edge_surrogates` map while classifying
//! the transaction's plans and passes that map in here, so this module reads
//! post-image identity + properties from the overlay and the matching
//! surrogate pair from that map — never inventing one.
//!
//! ## Node-label ops
//!
//! `SetNodeLabels` / `RemoveNodeLabels` stage a delta (`NodeLabelDelta`:
//! disjoint added/removed sets) under the fixed sentinel collection key
//! `GRAPH_LABEL_COLL_KEY` (`stage_write::stage_graph`), not an absolute
//! post-image. That delta maps directly onto the AUTOCOMMIT payload shape —
//! `RecordType::GraphNodeLabelSet` / `GraphNodeLabelRemove`, `(node_id,
//! labels)` where `labels` is applied additively / subtractively
//! (`wal_replay_graph_labels.rs`) — because `added` and `removed` are each
//! already exactly the touched-label list for their direction. So
//! [`serialize_node_label_deltas`] reuses the SAME record types and the
//! SAME `encode_graph_node_label_payload` encoder the autocommit path uses,
//! emitting one `GraphNodeLabelSet` sub-record per node with a non-empty
//! `added` set and one `GraphNodeLabelRemove` sub-record per node with a
//! non-empty `removed` set. No new `RedoSubRecord` shape or decoder is
//! needed: `try_replay_graph_node_label` already decodes this exact payload
//! for the autocommit path and is reused verbatim for redo replay
//! (`wal_replay_redo_graph.rs`'s `replay_graph_node_labels_redo`).
//!
//! ## Determinism
//!
//! The overlay keys edges (and node-label deltas) in `HashMap`/`HashSet`s, so
//! entries are collected into `BTreeMap`/`BTreeSet`s keyed by edge identity —
//! or, for labels, by node id with each label set sorted into a `Vec` — before
//! emitting. Two replicas resolving the same transaction produce
//! byte-identical redo ops.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::GraphOp;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::encode_graph_node_label_payload;
use crate::data::executor::handlers::transaction::overlay::{
    GraphCollKey, GraphTxnOverlay, NodeLabelDelta,
};
use crate::wal::RedoSubRecord;

/// Edge identity key: `(collection, src_id, label, dst_id)`. Scoped by
/// collection (unlike the overlay's own per-collection accessors) because
/// `entry.rs` collects surrogates for every graph collection the transaction
/// touched into one map before calling this serializer per collection.
pub(super) type EdgeIdentityKey = (String, String, String, String);

/// Append the redo sub-records for every graph edge post-image staged in
/// `overlay` for `coll_key` to `ops`, in deterministic edge-identity order.
///
/// `edge_surrogates` maps `(collection, src_id, label, dst_id)` to the
/// `(src_surrogate, dst_surrogate)` pair `entry.rs` collected from the
/// transaction's `EdgePut` / `EdgePutBatch` plan nodes — the overlay itself
/// carries no surrogates (see module docs).
pub(super) fn serialize_graph_collection(
    overlay: &GraphTxnOverlay,
    coll_key: &GraphCollKey,
    collection: &str,
    edge_surrogates: &BTreeMap<EdgeIdentityKey, (u32, u32)>,
    system_from: i64,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let mut puts: BTreeMap<(String, String, String), Vec<u8>> = BTreeMap::new();
    for (src, label, dst, properties) in overlay.staged_edges_for_collection(coll_key) {
        puts.insert(
            (src.to_string(), label.to_string(), dst.to_string()),
            properties.to_vec(),
        );
    }

    let mut deletes: BTreeSet<(String, String, String)> = BTreeSet::new();
    for (src, label, dst) in overlay.staged_tombstones_for_collection(coll_key) {
        deletes.insert((src.to_string(), label.to_string(), dst.to_string()));
    }

    for ((src, label, dst), properties) in puts {
        let identity_key = (
            collection.to_string(),
            src.clone(),
            label.clone(),
            dst.clone(),
        );
        let (src_surrogate, dst_surrogate) = edge_surrogates
            .get(&identity_key)
            .copied()
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "graph resolve: staged edge put '{collection}'/'{src}'-'{label}'->'{dst}' \
                         has no bound endpoint surrogates"
                ),
            })?;
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
            collection: collection.to_string(),
            src_id: src.clone(),
            label: label.clone(),
            dst_id: dst.clone(),
            properties: properties.clone(),
            src_surrogate,
            dst_surrogate,
            system_from: Some(system_from),
        })
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("graph resolve edge put: {e}"),
        })?;
        ops.push(RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        });
    }

    for (src, label, dst) in deletes {
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgeDeleteRedo {
            collection: collection.to_string(),
            src_id: src.clone(),
            label: label.clone(),
            dst_id: dst.clone(),
            system_from: Some(system_from),
        })
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("graph resolve edge delete: {e}"),
        })?;
        ops.push(RedoSubRecord {
            record_type: RecordType::Delete as u32,
            payload,
        });
    }
    Ok(())
}

/// Append the redo sub-records for every staged node-label delta in
/// `overlay` for the label sentinel collection `label_coll_key` to `ops`, in
/// deterministic node-id order.
///
/// `NodeLabelDelta.added` / `.removed` are disjoint by construction (see
/// `GraphTxnOverlay::stage_node_labels_set` / `stage_node_labels_remove`), so
/// each maps directly onto the autocommit `(node_id, labels)` payload shape:
/// one `GraphNodeLabelSet` sub-record for a non-empty `added` set, one
/// `GraphNodeLabelRemove` sub-record for a non-empty `removed` set. Reuses
/// `encode_graph_node_label_payload` so this producer and the autocommit
/// producer never drift on shape.
///
/// Each label set is a `HashSet<String>` (nondeterministic iteration order),
/// so it is sorted into a `Vec` before encoding — two replicas resolving the
/// same transaction must produce byte-identical redo payloads.
pub(super) fn serialize_node_label_deltas(
    overlay: &GraphTxnOverlay,
    label_coll_key: &GraphCollKey,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let mut deltas: BTreeMap<&str, &NodeLabelDelta> = BTreeMap::new();
    for (node_id, delta) in overlay.staged_node_label_deltas_for_collection(label_coll_key) {
        deltas.insert(node_id, delta);
    }

    for (node_id, delta) in deltas {
        if !delta.added.is_empty() {
            let payload = encode_graph_node_label_payload(node_id, &sorted_labels(&delta.added))?;
            ops.push(RedoSubRecord {
                record_type: RecordType::GraphNodeLabelSet as u32,
                payload,
            });
        }
        if !delta.removed.is_empty() {
            let payload = encode_graph_node_label_payload(node_id, &sorted_labels(&delta.removed))?;
            ops.push(RedoSubRecord {
                record_type: RecordType::GraphNodeLabelRemove as u32,
                payload,
            });
        }
    }
    Ok(())
}

/// Sort a staged label `HashSet` into a deterministic `Vec` before encoding.
fn sorted_labels(labels: &std::collections::HashSet<String>) -> Vec<String> {
    let mut sorted: Vec<String> = labels.iter().cloned().collect();
    sorted.sort();
    sorted
}

/// Classify a Graph op for transaction resolve: collect the collection of a
/// staged edge write into `collections` (so the serializer walks its
/// overlay), collect the endpoint surrogates of every staged edge PUT into
/// `edge_surrogates` (the overlay itself carries only identity + properties,
/// not surrogates — see this module's doc comment), skip read-only
/// traversal/algorithm ops, and skip node-label ops — their staged deltas
/// live in the graph overlay under a fixed sentinel collection key
/// (`GRAPH_LABEL_COLL_KEY`) and are serialized unconditionally by
/// `resolve_txn_ops`, not collected here.
pub(super) fn classify_graph_op(
    op: &GraphOp,
    collections: &mut BTreeSet<String>,
    edge_surrogates: &mut BTreeMap<EdgeIdentityKey, (u32, u32)>,
) -> crate::Result<()> {
    match op {
        // Edge put: the overlay holds the resolved post-image (identity +
        // properties); the endpoint surrogates are resolved once at
        // construction time and only live here on the plan node.
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            ..
        } => {
            collections.insert(collection.clone());
            edge_surrogates.insert(
                (
                    collection.clone(),
                    src_id.clone(),
                    label.clone(),
                    dst_id.clone(),
                ),
                (src_surrogate.as_u32(), dst_surrogate.as_u32()),
            );
            Ok(())
        }
        GraphOp::EdgePutBatch { edges } => {
            for edge in edges {
                collections.insert(edge.collection.clone());
                edge_surrogates.insert(
                    (
                        edge.collection.clone(),
                        edge.src_id.clone(),
                        edge.label.clone(),
                        edge.dst_id.clone(),
                    ),
                    (edge.src_surrogate.as_u32(), edge.dst_surrogate.as_u32()),
                );
            }
            Ok(())
        }

        // Edge delete: the redo delete tuple carries no surrogate, so only
        // the collection is needed to walk the overlay's tombstone set.
        GraphOp::EdgeDelete { collection, .. } => {
            collections.insert(collection.clone());
            Ok(())
        }
        GraphOp::EdgeDeleteBatch { edges } => {
            for edge in edges {
                collections.insert(edge.collection.clone());
            }
            Ok(())
        }

        // Read-only families: traversal, pattern matching, algorithms, and
        // stats carry no persisted post-image.
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
        | GraphOp::Stats { .. } => Ok(()),

        // Node-label mutations stage a delta (added/removed sets) under the
        // fixed sentinel collection key, not a per-collection post-image, so
        // there is nothing to collect here — `resolve_txn_ops` serializes
        // every staged node-label delta unconditionally via
        // `serialize_node_label_deltas` (see this module's doc comment).
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::types::{DatabaseId, TenantId};

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn coll_key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(DB), TenantId::new(TID), coll.to_string())
    }

    #[test]
    fn edge_put_emits_timestamped_tuple_with_surrogates() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", vec![1, 2, 3]);

        let mut surrogates = BTreeMap::new();
        surrogates.insert(
            (
                "g".to_string(),
                "a".to_string(),
                "knows".to_string(),
                "b".to_string(),
            ),
            (10u32, 20u32),
        );

        let mut ops = Vec::new();
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops)
            .expect("serialize edge put");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Put as u32);

        let decoded = zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&ops[0].payload)
            .expect("decode edge put redo");
        assert_eq!(decoded.collection, "g");
        assert_eq!(decoded.src_id, "a");
        assert_eq!(decoded.label, "knows");
        assert_eq!(decoded.dst_id, "b");
        assert_eq!(decoded.properties, vec![1, 2, 3]);
        assert_eq!(decoded.src_surrogate, 10);
        assert_eq!(decoded.dst_surrogate, 20);
        assert_eq!(decoded.system_from, Some(123));
    }

    #[test]
    fn edge_put_without_bound_surrogates_is_typed_error() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", vec![]);

        let surrogates = BTreeMap::new();
        let mut ops = Vec::new();
        let result =
            serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops);
        assert!(
            result.is_err(),
            "a staged put with no matching plan-carried surrogates must error, not invent one"
        );
    }

    #[test]
    fn edge_delete_emits_timestamped_redo() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");

        let surrogates = BTreeMap::new();
        let mut ops = Vec::new();
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops)
            .expect("serialize edge delete");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Delete as u32);

        let decoded = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(&ops[0].payload)
            .expect("decode edge delete redo");
        assert_eq!(decoded.collection, "g");
        assert_eq!(decoded.src_id, "a");
        assert_eq!(decoded.label, "knows");
        assert_eq!(decoded.dst_id, "b");
        assert_eq!(
            decoded.system_from,
            Some(123),
            "delete must carry the frozen system-time ordinal for deterministic replay"
        );
    }

    #[test]
    fn entries_emit_in_deterministic_edge_identity_order() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "c", "l", "z", vec![]);
        overlay.stage_edge_put(coll_key("g"), "a", "l", "x", vec![]);
        overlay.stage_edge_put(coll_key("g"), "b", "l", "y", vec![]);

        let mut surrogates = BTreeMap::new();
        for (s, d) in [("a", "x"), ("b", "y"), ("c", "z")] {
            surrogates.insert(
                (
                    "g".to_string(),
                    s.to_string(),
                    "l".to_string(),
                    d.to_string(),
                ),
                (1u32, 2u32),
            );
        }

        let mut ops = Vec::new();
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops)
            .expect("serialize");
        let srcs: Vec<String> = ops
            .iter()
            .map(|op| {
                zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&op.payload)
                    .expect("decode")
                    .src_id
            })
            .collect();
        assert_eq!(srcs, vec!["a", "b", "c"], "src-id ascending order");
    }

    #[test]
    fn node_label_set_emits_graph_node_label_set_subrecord() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_set(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::GraphNodeLabelSet as u32);

        let (node_id, labels) =
            zerompk::from_msgpack::<(String, Vec<String>)>(&ops[0].payload).expect("decode");
        assert_eq!(node_id, "n1");
        assert_eq!(labels, vec!["Person".to_string()]);
    }

    #[test]
    fn node_label_remove_emits_graph_node_label_remove_subrecord() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_remove(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::GraphNodeLabelRemove as u32);

        let (node_id, labels) =
            zerompk::from_msgpack::<(String, Vec<String>)>(&ops[0].payload).expect("decode");
        assert_eq!(node_id, "n1");
        assert_eq!(labels, vec!["Person".to_string()]);
    }

    #[test]
    fn node_label_added_and_removed_on_same_node_emit_both_subrecords() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_set(coll_key("g"), "n1", &["Robot".to_string()]);
        overlay.stage_node_labels_remove(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
        assert_eq!(
            ops.len(),
            2,
            "a node with both an added and a removed label emits both sub-records"
        );

        let types: Vec<u32> = ops.iter().map(|op| op.record_type).collect();
        assert!(types.contains(&(RecordType::GraphNodeLabelSet as u32)));
        assert!(types.contains(&(RecordType::GraphNodeLabelRemove as u32)));
    }

    #[test]
    fn node_label_deltas_emit_deterministic_bytes_regardless_of_insertion_order() {
        // Two overlays, same final delta, different HashSet insertion order —
        // the encoded redo sub-record bytes must be byte-identical.
        let mut overlay_a = GraphTxnOverlay::new();
        overlay_a.stage_node_labels_set(
            coll_key("g"),
            "n1",
            &["Zeta".to_string(), "Alpha".to_string(), "Mu".to_string()],
        );

        let mut overlay_b = GraphTxnOverlay::new();
        overlay_b.stage_node_labels_set(
            coll_key("g"),
            "n1",
            &["Mu".to_string(), "Zeta".to_string(), "Alpha".to_string()],
        );

        let mut ops_a = Vec::new();
        serialize_node_label_deltas(&overlay_a, &coll_key("g"), &mut ops_a).expect("serialize a");
        let mut ops_b = Vec::new();
        serialize_node_label_deltas(&overlay_b, &coll_key("g"), &mut ops_b).expect("serialize b");

        assert_eq!(ops_a.len(), 1);
        assert_eq!(
            ops_a[0].payload, ops_b[0].payload,
            "sorted labels must produce byte-identical payloads regardless of \
             HashSet insertion order"
        );
    }

    #[test]
    fn no_staged_labels_emits_nothing() {
        let overlay = GraphTxnOverlay::new();
        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
        assert!(ops.is_empty());
    }
}
