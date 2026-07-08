// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Graph` variants into `ReplicatedWrite`.

use super::super::types::{ReplicatedBatchEdge, ReplicatedWrite};
use nodedb_physical::physical_plan::BatchEdge;

pub(super) fn edge_put(
    collection: &str,
    src_id: &str,
    label: &str,
    dst_id: &str,
    properties: &[u8],
    src_surrogate: u32,
    dst_surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::EdgePut {
        collection: collection.to_owned(),
        src_id: src_id.to_owned(),
        label: label.to_owned(),
        dst_id: dst_id.to_owned(),
        properties: properties.to_vec(),
        src_surrogate,
        dst_surrogate,
    }
}

pub(super) fn edge_delete(
    collection: &str,
    src_id: &str,
    label: &str,
    dst_id: &str,
    src_surrogate: u32,
    dst_surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::EdgeDelete {
        collection: collection.to_owned(),
        src_id: src_id.to_owned(),
        label: label.to_owned(),
        dst_id: dst_id.to_owned(),
        src_surrogate,
        dst_surrogate,
    }
}

pub(super) fn set_node_labels(node_id: &str, labels: &[String]) -> ReplicatedWrite {
    ReplicatedWrite::SetNodeLabels {
        node_id: node_id.to_owned(),
        labels: labels.to_vec(),
    }
}

pub(super) fn remove_node_labels(node_id: &str, labels: &[String]) -> ReplicatedWrite {
    ReplicatedWrite::RemoveNodeLabels {
        node_id: node_id.to_owned(),
        labels: labels.to_vec(),
    }
}

fn to_replicated_batch_edges(edges: &[BatchEdge]) -> Vec<ReplicatedBatchEdge> {
    edges
        .iter()
        .map(|e| ReplicatedBatchEdge {
            collection: e.collection.clone(),
            src_id: e.src_id.clone(),
            label: e.label.clone(),
            dst_id: e.dst_id.clone(),
            src_surrogate: e.src_surrogate.as_u32(),
            dst_surrogate: e.dst_surrogate.as_u32(),
        })
        .collect()
}

pub(super) fn edge_put_batch(edges: &[BatchEdge]) -> ReplicatedWrite {
    ReplicatedWrite::EdgePutBatch {
        edges: to_replicated_batch_edges(edges),
    }
}

pub(super) fn edge_delete_batch(edges: &[BatchEdge]) -> ReplicatedWrite {
    ReplicatedWrite::EdgeDeleteBatch {
        edges: to_replicated_batch_edges(edges),
    }
}
