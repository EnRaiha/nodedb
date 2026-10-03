// SPDX-License-Identifier: Apache-2.0

//! An edge-label filter resolved against one CSR partition.
//!
//! A filter names a label as a string, and the CSR stores labels as dense
//! ids. A label this partition has never interned carries no edge here, so
//! the filter keeps no durable edge. Falling back to "no filter" instead
//! would return every other label's edges under the caller's label. In a
//! cluster that happens whenever the label lives only on other nodes.

use super::types::CsrIndex;

/// Which durable edges a label filter keeps in one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelFilter {
    /// No filter: every edge.
    Any,
    /// Edges with this dense label id.
    Only(u32),
    /// A label this partition has never seen: no edge.
    Unknown,
}

impl LabelFilter {
    /// Whether an edge with dense label id `lid` passes the filter.
    #[inline]
    pub fn keeps(self, lid: u32) -> bool {
        match self {
            LabelFilter::Any => true,
            LabelFilter::Only(id) => id == lid,
            LabelFilter::Unknown => false,
        }
    }
}

impl CsrIndex {
    /// Resolve `filter` against this partition's interned labels.
    pub fn label_filter(&self, filter: Option<&str>) -> LabelFilter {
        match filter {
            None => LabelFilter::Any,
            Some(label) => match self.label_to_id.get(label) {
                Some(&id) => LabelFilter::Only(id),
                None => LabelFilter::Unknown,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr::index::Direction;
    use crate::test_support::test_memory;

    fn csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_in_collection("a", "knows", "b", "people")
            .unwrap_or_else(|e| panic!("seed edge: {e}"));
        csr
    }

    #[test]
    fn an_unknown_label_keeps_no_edge() {
        let csr = csr();
        assert_eq!(csr.label_filter(Some("likes")), LabelFilter::Unknown);
        assert!(!csr.label_filter(Some("likes")).keeps(0));
        assert!(csr.neighbors("a", Some("likes"), Direction::Out).is_empty());
        assert!(
            csr.neighbors_in_collection("a", Some("likes"), Direction::Out, "people")
                .is_empty()
        );
    }

    #[test]
    fn no_filter_and_a_known_label_keep_their_edges() {
        let csr = csr();
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
        assert_eq!(csr.neighbors("a", Some("knows"), Direction::Out).len(), 1);
    }
}
