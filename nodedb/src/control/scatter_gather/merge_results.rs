// SPDX-License-Identifier: BUSL-1.1

//! The gather side of a hop: folding per-shard partial results into one answer.
//!
//! Split from the dispatch that produces those partials because deduplication
//! is where a traversal's correctness is decided independently of how many
//! shards replied — the same local + remote node lists must collapse to the
//! same set whether they arrived from one shard or twelve. Isolating it keeps
//! that property testable without a cluster.

use std::collections::HashSet;

/// Merge partial traversal results from multiple shards.
///
/// Deduplicates node IDs and accumulates all discovered nodes.
pub fn merge_traversal_results(
    local_nodes: Vec<String>,
    shard_results: &[Vec<String>],
) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut merged = Vec::new();

    for node in local_nodes {
        if seen.insert(node.clone()) {
            merged.push(node);
        }
    }

    for result in shard_results {
        for node in result {
            if seen.insert(node.clone()) {
                merged.push(node.clone());
            }
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_deduplicates() {
        let local = vec!["a".into(), "b".into(), "c".into()];
        let shard1 = vec!["b".into(), "d".into()];
        let shard2 = vec!["c".into(), "e".into()];

        let merged = merge_traversal_results(local, &[shard1, shard2]);
        assert_eq!(merged.len(), 5);
        assert!(merged.contains(&"a".to_string()));
        assert!(merged.contains(&"d".to_string()));
        assert!(merged.contains(&"e".to_string()));
    }
}
