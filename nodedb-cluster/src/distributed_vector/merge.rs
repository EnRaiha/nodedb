// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard k-NN merge for distributed vector search.
//!
//! Each shard runs local HNSW search and returns its top-K hits with
//! distances. The coordinator merges results from all shards by distance
//! re-ranking: sort all hits globally, take the top-K.
//!
//! This is the standard scatter-gather k-NN merge used by Milvus, Qdrant,
//! Weaviate, and every distributed vector database.

use serde::{Deserialize, Serialize};

/// A single vector search hit from a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorHit {
    /// Internal vector ID within the shard.
    pub vector_id: u32,
    /// Distance to the query vector (lower = closer for L2/cosine).
    pub distance: f32,
    /// Which shard produced this hit.
    pub shard_id: u32,
    /// Optional document ID associated with this vector.
    pub doc_id: Option<String>,
}

/// Results from a single shard's local k-NN search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSearchResult {
    pub shard_id: u32,
    pub hits: Vec<VectorHit>,
    pub success: bool,
    pub error: Option<String>,
}

/// Merges k-NN results from multiple shards.
///
/// Standard algorithm: collect all shard-local top-K results, sort globally
/// by distance, return the top-K. Over-fetch factor is handled per-shard
/// (each shard returns `top_k * over_fetch_factor` to account for filter
/// reduction).
pub struct VectorMerger {
    /// All hits collected from all shards, unsorted.
    all_hits: Vec<VectorHit>,
    /// Number of shards that have responded.
    responded: usize,
    /// Total number of shards expected.
    expected: usize,
}

impl VectorMerger {
    pub fn new(expected_shards: usize) -> Self {
        Self {
            all_hits: Vec::new(),
            responded: 0,
            expected: expected_shards,
        }
    }

    /// Add a shard's search results.
    pub fn add_shard_result(&mut self, result: &ShardSearchResult) {
        if result.success {
            self.all_hits.extend_from_slice(&result.hits);
        }
        self.responded += 1;
    }

    /// Whether all expected shards have responded.
    pub fn all_responded(&self) -> bool {
        self.responded >= self.expected
    }

    /// Merge all shard results and return the global top-K.
    ///
    /// Sorts by distance ascending (nearest first) and truncates to `top_k`.
    /// Ties are broken by shard_id for deterministic ordering.
    ///
    /// Module-private: the merged ranking leaves this module only through
    /// `VectorScatterGather::merge_top_k`, which checks shard completeness
    /// first. A merge short of one shard is a wrong ranking that reads as a
    /// correct one.
    pub(super) fn top_k(&mut self, top_k: usize) -> Vec<VectorHit> {
        self.all_hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.shard_id.cmp(&b.shard_id))
        });
        self.all_hits.truncate(top_k);
        self.all_hits.clone()
    }

    /// Number of total hits collected (before merge).
    pub fn total_hits(&self) -> usize {
        self.all_hits.len()
    }

    /// Number of shards that responded.
    pub fn response_count(&self) -> usize {
        self.responded
    }
}

/// Determine the per-shard over-fetch factor.
///
/// When metadata filters are active, some shard-local results will be
/// filtered out during post-filter. Over-fetching compensates:
/// - No filter: 1x (exact top-K per shard)
/// - Light filter (>50% pass rate): 2x
/// - Heavy filter (<50% pass rate): 3x
pub fn over_fetch_factor(has_filter: bool, estimated_pass_rate: f64) -> usize {
    if !has_filter {
        return 1;
    }
    if estimated_pass_rate > 0.5 { 2 } else { 3 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_two_shards() {
        let mut merger = VectorMerger::new(2);

        merger.add_shard_result(&ShardSearchResult {
            shard_id: 0,
            hits: vec![
                VectorHit {
                    vector_id: 1,
                    distance: 0.1,
                    shard_id: 0,
                    doc_id: None,
                },
                VectorHit {
                    vector_id: 2,
                    distance: 0.3,
                    shard_id: 0,
                    doc_id: None,
                },
                VectorHit {
                    vector_id: 3,
                    distance: 0.5,
                    shard_id: 0,
                    doc_id: None,
                },
            ],
            success: true,
            error: None,
        });

        merger.add_shard_result(&ShardSearchResult {
            shard_id: 1,
            hits: vec![
                VectorHit {
                    vector_id: 10,
                    distance: 0.05,
                    shard_id: 1,
                    doc_id: None,
                },
                VectorHit {
                    vector_id: 11,
                    distance: 0.2,
                    shard_id: 1,
                    doc_id: None,
                },
                VectorHit {
                    vector_id: 12,
                    distance: 0.4,
                    shard_id: 1,
                    doc_id: None,
                },
            ],
            success: true,
            error: None,
        });

        assert!(merger.all_responded());
        let top3 = merger.top_k(3);
        assert_eq!(top3.len(), 3);
        // Nearest hit should be shard 1's vector_id 10 (distance 0.05).
        assert_eq!(top3[0].vector_id, 10);
        assert_eq!(top3[0].shard_id, 1);
        assert!((top3[0].distance - 0.05).abs() < f32::EPSILON);
        // Second should be shard 0's vector_id 1 (distance 0.1).
        assert_eq!(top3[1].vector_id, 1);
        // Third should be shard 1's vector_id 11 (distance 0.2).
        assert_eq!(top3[2].vector_id, 11);
    }

    #[test]
    fn merge_with_failed_shard() {
        let mut merger = VectorMerger::new(2);

        merger.add_shard_result(&ShardSearchResult {
            shard_id: 0,
            hits: vec![VectorHit {
                vector_id: 1,
                distance: 0.1,
                shard_id: 0,
                doc_id: None,
            }],
            success: true,
            error: None,
        });

        // Shard 1 failed — no hits.
        merger.add_shard_result(&ShardSearchResult {
            shard_id: 1,
            hits: vec![],
            success: false,
            error: Some("timeout".into()),
        });

        assert!(merger.all_responded());
        let top1 = merger.top_k(1);
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].vector_id, 1);
    }

    #[test]
    fn top_k_truncation() {
        let mut merger = VectorMerger::new(1);
        merger.add_shard_result(&ShardSearchResult {
            shard_id: 0,
            hits: (0..100)
                .map(|i| VectorHit {
                    vector_id: i,
                    distance: i as f32 * 0.01,
                    shard_id: 0,
                    doc_id: None,
                })
                .collect(),
            success: true,
            error: None,
        });

        let top5 = merger.top_k(5);
        assert_eq!(top5.len(), 5);
        // Nearest 5 by distance.
        for (i, hit) in top5.iter().enumerate() {
            assert_eq!(hit.vector_id, i as u32);
        }
    }

    #[test]
    fn over_fetch_no_filter() {
        assert_eq!(over_fetch_factor(false, 1.0), 1);
    }

    #[test]
    fn over_fetch_light_filter() {
        assert_eq!(over_fetch_factor(true, 0.7), 2);
    }

    #[test]
    fn over_fetch_heavy_filter() {
        assert_eq!(over_fetch_factor(true, 0.2), 3);
    }

    #[test]
    fn deterministic_tie_breaking() {
        let mut merger = VectorMerger::new(2);
        merger.add_shard_result(&ShardSearchResult {
            shard_id: 0,
            hits: vec![VectorHit {
                vector_id: 1,
                distance: 0.5,
                shard_id: 0,
                doc_id: None,
            }],
            success: true,
            error: None,
        });
        merger.add_shard_result(&ShardSearchResult {
            shard_id: 1,
            hits: vec![VectorHit {
                vector_id: 2,
                distance: 0.5,
                shard_id: 1,
                doc_id: None,
            }],
            success: true,
            error: None,
        });

        let top2 = merger.top_k(2);
        // Same distance — broken by shard_id (0 < 1).
        assert_eq!(top2[0].shard_id, 0);
        assert_eq!(top2[1].shard_id, 1);
    }
}
