// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Graph(GraphOp)`, plus the helpers for
//! batched graph edge writes (`GraphOp::EdgePutBatch` / `GraphOp::EdgeDeleteBatch`),
//! produced by `CREATE GRAPH INDEX`.
//!
//! Each edge in a batch is appended as its own single-edge `Put` / `Delete`
//! WAL record, byte-identical in shape to what the non-batch
//! `GraphOp::EdgePut` / `EdgeDelete` arms append (collection,
//! src_id, label, dst_id[, properties]) — so a batch needs no new
//! `RecordType` and no new replay decoder: it is simply N individually
//! durable edges, applied via the same `execute_edge_put` / `execute_edge_delete`
//! replay path graph edges already rely on. `properties` is always encoded as
//! empty for a batch edge: `BatchEdge` carries no properties field, and the
//! Data Plane's `execute_edge_put_batch` applies every edge with a hardcoded
//! `&[]`, so an empty payload here is a faithful pre-image of what is
//! actually applied, not an assumed default.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Append the WAL record for a single `GraphOp`, returning the allocated LSN
/// for the edge / node-label write variants (`Some`) or `None` for every
/// traversal / algorithm / read variant that carries no durable per-write
/// effect.
///
/// The match over [`GraphOp`] is **exhaustive** (`wildcard_enum_match_arm` is
/// denied), so a future write variant cannot silently become non-durable. The
/// batched-edge variants delegate to the per-edge append helpers below.
pub(super) fn wal_append_graph_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &GraphOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate: _,
            dst_surrogate: _,
        } => {
            let entry = zerompk::to_msgpack_vec(&(collection, src_id, label, dst_id, properties))
                .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal edge put: {e}"),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        // The compiled write predicate is a planning-time artifact of the
        // session that produced the plan, not part of the durable edge, so it
        // is deliberately not part of the WAL entry.
        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate: _,
            dst_surrogate: _,
            rls_write_check: _,
        } => {
            let entry =
                zerompk::to_msgpack_vec(&(collection, src_id, label, dst_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal edge delete: {e}"),
                    }
                })?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        GraphOp::SetNodeLabels { node_id, labels } => {
            let entry = super::encode_graph_node_label_payload(node_id, labels)?;
            Some(wal.append_graph_node_label_set(tenant_id, vshard_id, database_id, &entry)?)
        }
        GraphOp::RemoveNodeLabels { node_id, labels } => {
            let entry = super::encode_graph_node_label_payload(node_id, labels)?;
            Some(wal.append_graph_node_label_remove(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Batched edge writes (`CREATE GRAPH INDEX` build / rollback). Each
        // edge is appended as its own single-edge `Put`/`Delete` record,
        // byte-identical in shape to the non-batch `EdgePut`/`EdgeDelete`
        // arms above — see the module docs for the encoding and the
        // documented last-LSN-as-watermark / explicit-empty-batch contract.
        GraphOp::EdgePutBatch { edges } => {
            wal_append_graph_edge_put_batch(wal, tenant_id, vshard_id, database_id, edges)?
        }
        GraphOp::EdgeDeleteBatch { edges } => {
            wal_append_graph_edge_delete_batch(wal, tenant_id, vshard_id, database_id, edges)?
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation here
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
        | GraphOp::Stats { .. } => None,
    };
    Ok(appended)
}

/// Append one `Put` WAL record per edge in a batched edge insert.
///
/// Returns the LSN of the LAST appended record. WAL LSNs are allocated
/// monotonically increasing per append, so the last edge's LSN is the
/// natural "this write is durable through here" watermark for a caller that
/// threads a single LSN forward — every earlier edge in the batch is a real,
/// independently persisted WAL record on disk; only the single number this
/// function returns collapses to the last one, it does not erase the
/// earlier appends.
///
/// An empty batch appends nothing and returns `Ok(None)` deliberately: a
/// zero-edge `EdgePutBatch` has no logical write to make durable, so `None`
/// here carries its usual meaning in this module (no durable record for this
/// plan) rather than being an accidental fallthrough.
pub(crate) fn wal_append_graph_edge_put_batch(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    edges: &[BatchEdge],
) -> crate::Result<Option<Lsn>> {
    let mut last_lsn = None;
    for edge in edges {
        let properties: Vec<u8> = Vec::new();
        let entry = zerompk::to_msgpack_vec(&(
            &edge.collection,
            &edge.src_id,
            &edge.label,
            &edge.dst_id,
            &properties,
        ))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal edge put batch: {e}"),
        })?;
        last_lsn = Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?);
    }
    Ok(last_lsn)
}

/// Append one `Delete` WAL record per edge in a batched edge delete
/// (`CREATE GRAPH INDEX` rollback).
///
/// Same last-LSN-as-watermark and explicit-empty-batch contract as
/// [`wal_append_graph_edge_put_batch`] above.
pub(crate) fn wal_append_graph_edge_delete_batch(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    edges: &[BatchEdge],
) -> crate::Result<Option<Lsn>> {
    let mut last_lsn = None;
    for edge in edges {
        let entry =
            zerompk::to_msgpack_vec(&(&edge.collection, &edge.src_id, &edge.label, &edge.dst_id))
                .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal edge delete batch: {e}"),
            })?;
        last_lsn = Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?);
    }
    Ok(last_lsn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Surrogate;

    fn edge(collection: &str, src: &str, label: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: collection.to_string(),
            src_id: src.to_string(),
            label: label.to_string(),
            dst_id: dst.to_string(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        }
    }

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    #[test]
    fn put_batch_appends_one_record_per_edge_and_returns_last_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let edges = vec![
            edge("knows", "a", "KNOWS", "b"),
            edge("knows", "b", "KNOWS", "c"),
            edge("knows", "c", "KNOWS", "d"),
        ];

        let lsn = wal_append_graph_edge_put_batch(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &edges,
        )
        .expect("append put batch")
        .expect("non-empty batch must produce Some(lsn)");

        wal.sync().expect("sync wal");
        let records = wal.replay().expect("read wal");
        let puts: Vec<_> = records
            .iter()
            .filter(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(nodedb_wal::record::RecordType::Put)
            })
            .collect();
        assert_eq!(puts.len(), 3, "one Put record per edge");

        let (collection, src_id, label, dst_id, properties) =
            zerompk::from_msgpack::<(String, String, String, String, Vec<u8>)>(&puts[0].payload)
                .expect("decode edge put payload");
        assert_eq!(collection, "knows");
        assert_eq!(src_id, "a");
        assert_eq!(label, "KNOWS");
        assert_eq!(dst_id, "b");
        assert!(properties.is_empty(), "batch edges carry no properties");

        assert_eq!(
            lsn.as_u64(),
            puts.last().expect("at least one record").header.lsn,
            "returned LSN is the last appended record's LSN"
        );
    }

    #[test]
    fn delete_batch_appends_one_record_per_edge_and_returns_last_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let edges = vec![
            edge("knows", "a", "KNOWS", "b"),
            edge("knows", "b", "KNOWS", "c"),
        ];

        let lsn = wal_append_graph_edge_delete_batch(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &edges,
        )
        .expect("append delete batch")
        .expect("non-empty batch must produce Some(lsn)");

        wal.sync().expect("sync wal");
        let records = wal.replay().expect("read wal");
        let deletes: Vec<_> = records
            .iter()
            .filter(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(nodedb_wal::record::RecordType::Delete)
            })
            .collect();
        assert_eq!(deletes.len(), 2, "one Delete record per edge");

        let (collection, src_id, label, dst_id) =
            zerompk::from_msgpack::<(String, String, String, String)>(&deletes[0].payload)
                .expect("decode edge delete payload");
        assert_eq!(collection, "knows");
        assert_eq!(src_id, "a");
        assert_eq!(label, "KNOWS");
        assert_eq!(dst_id, "b");

        assert_eq!(
            lsn.as_u64(),
            deletes.last().expect("at least one record").header.lsn
        );
    }

    #[test]
    fn empty_put_batch_returns_none_explicitly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());

        let lsn = wal_append_graph_edge_put_batch(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &[],
        )
        .expect("append empty put batch");

        assert_eq!(lsn, None, "empty batch has no durable record");
    }

    #[test]
    fn empty_delete_batch_returns_none_explicitly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());

        let lsn = wal_append_graph_edge_delete_batch(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &[],
        )
        .expect("append empty delete batch");

        assert_eq!(lsn, None, "empty batch has no durable record");
    }

    fn has_record_of_type(wal: &WalManager, record_type: nodedb_wal::record::RecordType) -> bool {
        wal.sync().expect("sync wal");
        wal.replay().expect("read wal").into_iter().any(|r| {
            nodedb_wal::record::RecordType::from_raw(r.logical_record_type()) == Some(record_type)
        })
    }

    #[test]
    fn edge_put_appends_put_record() {
        use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: "knows".to_string(),
            src_id: "a".to_string(),
            label: "KNOWS".to_string(),
            dst_id: "b".to_string(),
            properties: vec![],
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "EdgePut must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::Put
        ));
    }

    #[test]
    fn set_node_labels_appends_label_set_record() {
        use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "a".to_string(),
            labels: vec!["Person".to_string()],
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "SetNodeLabels must produce a durable LSN"
        );
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::GraphNodeLabelSet
        ));
    }

    #[test]
    fn read_op_appends_nothing() {
        use nodedb_graph::Direction;
        use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "a".to_string(),
            edge_label: None,
            direction: Direction::Out,
            rls_filters: vec![],
            collection: None,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "read op must produce no durable LSN");
    }
}
