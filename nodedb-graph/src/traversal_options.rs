// SPDX-License-Identifier: Apache-2.0

//! Per-query graph traversal configuration.

/// Largest accepted value for any graph-DSL depth parameter
/// (`DEPTH`, `MAX_DEPTH`, `EXPANSION_DEPTH`).
///
/// Enforced at every ingress (pgwire, native protocol) and at the
/// engine boundary as defence-in-depth so a single statement cannot
/// saturate `cross_core_bfs`, `csr.shortest_path`, or the subgraph
/// materializer with an unbounded fan-out per hop.
pub const MAX_GRAPH_TRAVERSAL_DEPTH: usize = 64;

use serde::{Deserialize, Serialize};

/// Per-query graph traversal configuration.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct GraphTraversalOptions {
    /// Cap on total visited nodes across all shards.
    ///
    /// Once this limit is reached, no further node exploration occurs.
    /// Default: 100_000
    pub max_visited: usize,
}

impl Default for GraphTraversalOptions {
    fn default() -> Self {
        Self {
            max_visited: 100_000,
        }
    }
}

impl GraphTraversalOptions {
    /// Create a new `GraphTraversalOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs;

    #[test]
    fn default_options_have_expected_values() {
        let opts = GraphTraversalOptions::default();
        assert_eq!(opts.max_visited, 100_000);
    }

    #[test]
    fn new_returns_defaults() {
        let opts = GraphTraversalOptions::new();
        assert_eq!(opts, GraphTraversalOptions::default());
    }

    #[test]
    fn serialization_roundtrip() {
        let opts = GraphTraversalOptions {
            max_visited: 50_000,
        };
        let json = sonic_rs::to_string(&opts).unwrap();
        let deserialized: GraphTraversalOptions = sonic_rs::from_str(&json).unwrap();
        assert_eq!(opts, deserialized);
    }
}
