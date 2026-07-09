// SPDX-License-Identifier: BUSL-1.1

//! Graph serializer for transaction resolve.
//!
//! Turns the graph edge post-images a transaction staged into its
//! [`GraphTxnOverlay`] into the engine-native WAL sub-record shapes the graph
//! redo replay path decodes (`wal_replay_redo_graph.rs`) — the SAME shapes the
//! autocommit graph WAL path produces, extended with the two endpoint
//! surrogates a redo PUT carries and an autocommit PUT does not:
//!
//! * A staged edge put → `RecordType::Put`, `(collection, src_id, label,
//!   dst_id, properties, src_surrogate, dst_surrogate)`. Replay's
//!   `execute_edge_put` repopulates the CSR node→surrogate map from the two
//!   trailing surrogates, so they must be present and correct.
//! * A staged edge tombstone → `RecordType::Delete`, `(collection, src_id,
//!   label, dst_id)`. `execute_edge_delete` needs no surrogate.
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
//! added/removed sets), not an absolute post-image, and no `RecordType`
//! variant or redo decoder exists for a node-label WAL sub-record anywhere in
//! the codebase (`wal_replay_redo_graph.rs` decodes only edge Put/Delete; the
//! `ReplicatedWrite::SetNodeLabels` shape belongs to the unrelated Raft
//! replication encode/decode path, not the `RedoSubRecord` family transaction
//! resolve produces). `entry.rs` therefore raises a typed error for both
//! rather than silently dropping the label change from the redo record.
//!
//! ## Determinism
//!
//! The overlay keys edges in a `HashMap`, so entries are collected into
//! `BTreeMap`/`BTreeSet`s keyed by edge identity before emitting. Two
//! replicas resolving the same transaction produce byte-identical redo ops.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_wal::record::RecordType;

use crate::data::executor::handlers::transaction::overlay::{GraphCollKey, GraphTxnOverlay};
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
        let payload = zerompk::to_msgpack_vec(&(
            collection,
            src.as_str(),
            label.as_str(),
            dst.as_str(),
            properties,
            src_surrogate,
            dst_surrogate,
        ))
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
        let payload =
            zerompk::to_msgpack_vec(&(collection, src.as_str(), label.as_str(), dst.as_str()))
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
    fn edge_put_emits_seven_element_tuple_with_surrogates() {
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
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, &mut ops)
            .expect("serialize edge put");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Put as u32);

        let (collection, src, label, dst, properties, src_sur, dst_sur) =
            zerompk::from_msgpack::<(String, String, String, String, Vec<u8>, u32, u32)>(
                &ops[0].payload,
            )
            .expect("decode edge put tuple");
        assert_eq!(collection, "g");
        assert_eq!(src, "a");
        assert_eq!(label, "knows");
        assert_eq!(dst, "b");
        assert_eq!(properties, vec![1, 2, 3]);
        assert_eq!(src_sur, 10);
        assert_eq!(dst_sur, 20);
    }

    #[test]
    fn edge_put_without_bound_surrogates_is_typed_error() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", vec![]);

        let surrogates = BTreeMap::new();
        let mut ops = Vec::new();
        let result =
            serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, &mut ops);
        assert!(
            result.is_err(),
            "a staged put with no matching plan-carried surrogates must error, not invent one"
        );
    }

    #[test]
    fn edge_delete_emits_four_element_tuple() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");

        let surrogates = BTreeMap::new();
        let mut ops = Vec::new();
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, &mut ops)
            .expect("serialize edge delete");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Delete as u32);

        let (collection, src, label, dst) =
            zerompk::from_msgpack::<(String, String, String, String)>(&ops[0].payload)
                .expect("decode edge delete tuple");
        assert_eq!(collection, "g");
        assert_eq!(src, "a");
        assert_eq!(label, "knows");
        assert_eq!(dst, "b");
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
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, &mut ops)
            .expect("serialize");
        let srcs: Vec<String> = ops
            .iter()
            .map(|op| {
                zerompk::from_msgpack::<(String, String, String, String, Vec<u8>, u32, u32)>(
                    &op.payload,
                )
                .expect("decode")
                .1
            })
            .collect();
        assert_eq!(srcs, vec!["a", "b", "c"], "src-id ascending order");
    }
}
