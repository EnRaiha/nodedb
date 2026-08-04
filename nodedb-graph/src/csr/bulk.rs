// SPDX-License-Identifier: Apache-2.0

//! One-pass construction of a compact CSR from a unique edge stream.

use std::mem::size_of;
use std::sync::Arc;

use nodedb_mem::{EngineId, MemoryGovernor};

use super::dense_array::DenseArray;
use super::index::CsrIndex;
use crate::GraphError;

#[derive(Clone, Copy)]
struct BulkEdge {
    source: u32,
    destination: u32,
    label: u32,
    collection: u32,
    weight: f64,
}

/// Builds exact dense adjacency arrays without mutation buffers or compaction.
///
/// The input must contain at most one live edge for each
/// `(source, label, destination, collection)` identity. Reciprocal edges,
/// different labels, and different collections remain distinct. This contract
/// matches Origin's ordered durable edge snapshot, where temporal versions
/// have already been resolved.
pub struct CsrBulkBuilder {
    index: CsrIndex,
    edges: Vec<BulkEdge>,
    out_degrees: Vec<u32>,
    in_degrees: Vec<u32>,
    has_weights: bool,
}

impl Default for CsrBulkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CsrBulkBuilder {
    /// Create an ungoverned builder, matching [`CsrIndex::new`].
    pub fn new() -> Self {
        Self::from_index(CsrIndex::new())
    }

    /// Create a builder whose final dense allocation is checked against the
    /// graph engine's memory budget.
    pub fn with_governor(governor: Arc<MemoryGovernor>) -> Self {
        Self::from_index(CsrIndex::with_governor(governor))
    }

    fn from_index(index: CsrIndex) -> Self {
        Self {
            index,
            edges: Vec::new(),
            out_degrees: Vec::new(),
            in_degrees: Vec::new(),
            has_weights: false,
        }
    }

    /// Register a node that may not occur as an edge endpoint.
    pub fn register_node(&mut self, node: &str) -> Result<u32, GraphError> {
        let id = self.index.ensure_node(node)?;
        self.extend_degrees();
        Ok(id)
    }

    /// Add one unique live edge to the temporary compact edge stream.
    pub fn push_unique_edge(
        &mut self,
        source: &str,
        label: &str,
        destination: &str,
        collection: &str,
        weight: f64,
    ) -> Result<(), GraphError> {
        if self.edges.len() >= u32::MAX as usize {
            return Err(GraphError::EdgeOverflow {
                used: self.edges.len(),
            });
        }
        let source = self.index.ensure_node(source)?;
        let destination = self.index.ensure_node(destination)?;
        let label = self.index.ensure_label(label)?;
        let collection = self.index.ensure_collection(collection);
        self.extend_degrees();
        let out = &mut self.out_degrees[source as usize];
        *out = out.checked_add(1).ok_or(GraphError::EdgeOverflow {
            used: self.edges.len(),
        })?;
        let in_ = &mut self.in_degrees[destination as usize];
        *in_ = in_.checked_add(1).ok_or(GraphError::EdgeOverflow {
            used: self.edges.len(),
        })?;
        self.has_weights |= weight != 1.0;
        self.edges.push(BulkEdge {
            source,
            destination,
            label,
            collection,
            weight,
        });
        Ok(())
    }

    /// Allocate and fill both dense directions, consuming the temporary edge
    /// stream. If a governor is attached to the underlying index, the complete
    /// dense allocation is reserved before any graph-sized output is created.
    pub fn finish(mut self) -> Result<CsrIndex, GraphError> {
        let node_count = self.index.id_to_node.len();
        let edge_count = self.edges.len();
        let per_direction = 3 * size_of::<u32>()
            + if self.has_weights {
                size_of::<f64>()
            } else {
                0
            };
        let reserve_bytes =
            2 * (node_count + 1) * size_of::<u32>() + 2 * edge_count * per_direction;
        let _budget_guard = self
            .index
            .governor
            .as_ref()
            .map(|governor| governor.reserve(EngineId::Graph, reserve_bytes))
            .transpose()?;

        let out_offsets = offsets_from_degrees(&self.out_degrees)?;
        let in_offsets = offsets_from_degrees(&self.in_degrees)?;
        let mut out_cursor: Vec<usize> = out_offsets[..node_count]
            .iter()
            .map(|offset| *offset as usize)
            .collect();
        let mut in_cursor: Vec<usize> = in_offsets[..node_count]
            .iter()
            .map(|offset| *offset as usize)
            .collect();
        let mut out_targets = vec![0; edge_count];
        let mut out_labels = vec![0; edge_count];
        let mut out_collections = vec![0; edge_count];
        let mut in_targets = vec![0; edge_count];
        let mut in_labels = vec![0; edge_count];
        let mut in_collections = vec![0; edge_count];
        let mut out_weights = self.has_weights.then(|| vec![1.0; edge_count]);
        let mut in_weights = self.has_weights.then(|| vec![1.0; edge_count]);

        for edge in self.edges.drain(..) {
            let out = out_cursor[edge.source as usize];
            out_cursor[edge.source as usize] += 1;
            out_targets[out] = edge.destination;
            out_labels[out] = edge.label;
            out_collections[out] = edge.collection;
            if let Some(weights) = &mut out_weights {
                weights[out] = edge.weight;
            }

            let in_ = in_cursor[edge.destination as usize];
            in_cursor[edge.destination as usize] += 1;
            in_targets[in_] = edge.source;
            in_labels[in_] = edge.label;
            in_collections[in_] = edge.collection;
            if let Some(weights) = &mut in_weights {
                weights[in_] = edge.weight;
            }
        }

        self.index.out_offsets = out_offsets;
        self.index.out_targets = DenseArray::from(out_targets);
        self.index.out_labels = DenseArray::from(out_labels);
        self.index.out_collections = out_collections;
        self.index.out_weights = out_weights.map(DenseArray::from);
        self.index.in_offsets = in_offsets;
        self.index.in_targets = DenseArray::from(in_targets);
        self.index.in_labels = DenseArray::from(in_labels);
        self.index.in_collections = in_collections;
        self.index.in_weights = in_weights.map(DenseArray::from);
        self.index.has_weights = self.has_weights;
        Ok(self.index)
    }

    fn extend_degrees(&mut self) {
        self.out_degrees.resize(self.index.id_to_node.len(), 0);
        self.in_degrees.resize(self.index.id_to_node.len(), 0);
    }
}

fn offsets_from_degrees(degrees: &[u32]) -> Result<Vec<u32>, GraphError> {
    let mut offsets = Vec::with_capacity(degrees.len() + 1);
    let mut offset = 0u32;
    offsets.push(offset);
    for degree in degrees {
        offset = offset
            .checked_add(*degree)
            .ok_or(GraphError::EdgeOverflow {
                used: offset as usize,
            })?;
        offsets.push(offset);
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_build_matches_unique_incremental_graph() {
        let edges = [
            ("a", "L", "b", "one", 1.0),
            ("a", "W", "c", "two", 2.5),
            ("b", "W", "c", "one", 1.0),
            ("b", "W", "a", "one", 3.5),
        ];
        let mut builder = CsrBulkBuilder::new();
        builder.register_node("isolated").unwrap();
        for edge in edges {
            builder
                .push_unique_edge(edge.0, edge.1, edge.2, edge.3, edge.4)
                .unwrap();
        }
        let bulk = builder.finish().unwrap();

        let mut incremental = CsrIndex::new();
        incremental.add_node("isolated").unwrap();
        for edge in edges {
            incremental
                .add_edge_weighted_in_collection(edge.0, edge.1, edge.2, edge.3, edge.4)
                .unwrap();
        }
        incremental.compact_initial_build().unwrap();

        assert_eq!(bulk.id_to_node, incremental.id_to_node);
        assert_eq!(bulk.id_to_label, incremental.id_to_label);
        assert_eq!(bulk.id_to_collection, incremental.id_to_collection);
        assert_eq!(bulk.out_offsets, incremental.out_offsets);
        assert_eq!(&*bulk.out_targets, &*incremental.out_targets);
        assert_eq!(&*bulk.out_labels, &*incremental.out_labels);
        assert_eq!(bulk.out_collections, incremental.out_collections);
        assert_eq!(
            bulk.out_weights.as_deref(),
            incremental.out_weights.as_deref()
        );
        assert_eq!(bulk.in_offsets, incremental.in_offsets);
        assert_eq!(&*bulk.in_targets, &*incremental.in_targets);
        assert_eq!(&*bulk.in_labels, &*incremental.in_labels);
        assert_eq!(bulk.in_collections, incremental.in_collections);
        assert_eq!(
            bulk.in_weights.as_deref(),
            incremental.in_weights.as_deref()
        );
        assert!(bulk.contains_node("isolated"));
    }

    #[test]
    fn unweighted_bulk_build_omits_weight_arrays() {
        let mut builder = CsrBulkBuilder::new();
        builder.push_unique_edge("a", "L", "b", "one", 1.0).unwrap();
        let bulk = builder.finish().unwrap();
        assert!(!bulk.has_weighted_edges());
        assert!(bulk.out_weights.is_none());
        assert!(bulk.in_weights.is_none());
    }

    #[test]
    fn governed_bulk_build_rejects_dense_allocation_over_budget() {
        use std::collections::HashMap;

        use nodedb_mem::GovernorConfig;

        let governor = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: 16,
                engine_limits: HashMap::from([(EngineId::Graph, 16)]),
            })
            .unwrap(),
        );
        let mut builder = CsrBulkBuilder::with_governor(governor);
        builder.push_unique_edge("a", "L", "b", "one", 2.5).unwrap();
        assert!(matches!(builder.finish(), Err(GraphError::MemoryBudget(_))));
    }
}
