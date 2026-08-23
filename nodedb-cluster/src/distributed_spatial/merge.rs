// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard spatial result merging.
//!
//! Unlike vector search (which needs distance re-ranking), spatial predicates
//! are boolean — a document either matches or it doesn't. The merge is a
//! simple concatenation of shard results with deduplication.

use serde::{Deserialize, Serialize};

/// A single spatial match from a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialHit {
    /// Document ID.
    pub doc_id: String,
    /// Which shard produced this hit.
    pub shard_id: u32,
    /// Distance to query geometry in meters (for ST_DWithin ordering).
    /// 0.0 for non-distance predicates (ST_Contains, ST_Intersects).
    pub distance_meters: f64,
}

/// Results from a single shard's local spatial query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSpatialResult {
    pub shard_id: u32,
    pub hits: Vec<SpatialHit>,
    pub success: bool,
    pub error: Option<String>,
}

/// Merges spatial results from multiple shards.
///
/// For boolean predicates (ST_Contains, ST_Intersects): simple concatenation.
/// For distance predicates (ST_DWithin): merge and sort by distance, take limit.
pub struct SpatialResultMerger {
    all_hits: Vec<SpatialHit>,
    responded: usize,
    expected: usize,
}

impl SpatialResultMerger {
    pub fn new(expected_shards: usize) -> Self {
        Self {
            all_hits: Vec::new(),
            responded: 0,
            expected: expected_shards,
        }
    }

    /// Add a shard's results.
    pub fn add_shard_result(&mut self, result: &ShardSpatialResult) {
        if result.success {
            self.all_hits.extend_from_slice(&result.hits);
        }
        self.responded += 1;
    }

    /// Whether all expected shards have responded.
    pub fn all_responded(&self) -> bool {
        self.responded >= self.expected
    }

    /// Merge all results: deduplicate by doc_id, optionally sort by distance.
    ///
    /// For ST_DWithin, results are sorted by distance (nearest first).
    /// For boolean predicates, order is arbitrary. Truncates to `limit`.
    ///
    /// Module-private: the merged hit set leaves this module only through
    /// `SpatialScatterGather::merge_results`, which checks shard completeness
    /// first. A merge short of one shard silently drops a region of the query
    /// extent.
    pub(super) fn merge(&mut self, limit: usize, sort_by_distance: bool) -> Vec<SpatialHit> {
        // Deduplicate by doc_id (a document can only appear on one shard,
        // but defensive in case of ghost stubs or migration overlap).
        let mut seen = std::collections::HashSet::new();
        self.all_hits.retain(|h| seen.insert(h.doc_id.clone()));

        if sort_by_distance {
            self.all_hits.sort_by(|a, b| {
                a.distance_meters
                    .partial_cmp(&b.distance_meters)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        self.all_hits.truncate(limit);
        self.all_hits.clone()
    }

    /// Total hits collected (before merge).
    pub fn total_hits(&self) -> usize {
        self.all_hits.len()
    }

    pub fn response_count(&self) -> usize {
        self.responded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_two_shards_boolean() {
        let mut merger = SpatialResultMerger::new(2);
        merger.add_shard_result(&ShardSpatialResult {
            shard_id: 0,
            hits: vec![
                SpatialHit {
                    doc_id: "a".into(),
                    shard_id: 0,
                    distance_meters: 0.0,
                },
                SpatialHit {
                    doc_id: "b".into(),
                    shard_id: 0,
                    distance_meters: 0.0,
                },
            ],
            success: true,
            error: None,
        });
        merger.add_shard_result(&ShardSpatialResult {
            shard_id: 1,
            hits: vec![SpatialHit {
                doc_id: "c".into(),
                shard_id: 1,
                distance_meters: 0.0,
            }],
            success: true,
            error: None,
        });

        assert!(merger.all_responded());
        let results = merger.merge(10, false);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn merge_with_distance_sort() {
        let mut merger = SpatialResultMerger::new(2);
        merger.add_shard_result(&ShardSpatialResult {
            shard_id: 0,
            hits: vec![SpatialHit {
                doc_id: "far".into(),
                shard_id: 0,
                distance_meters: 500.0,
            }],
            success: true,
            error: None,
        });
        merger.add_shard_result(&ShardSpatialResult {
            shard_id: 1,
            hits: vec![SpatialHit {
                doc_id: "near".into(),
                shard_id: 1,
                distance_meters: 100.0,
            }],
            success: true,
            error: None,
        });

        let results = merger.merge(10, true);
        assert_eq!(results[0].doc_id, "near");
        assert_eq!(results[1].doc_id, "far");
    }

    #[test]
    fn merge_with_failed_shard() {
        let mut merger = SpatialResultMerger::new(2);
        merger.add_shard_result(&ShardSpatialResult {
            shard_id: 0,
            hits: vec![SpatialHit {
                doc_id: "a".into(),
                shard_id: 0,
                distance_meters: 0.0,
            }],
            success: true,
            error: None,
        });
        merger.add_shard_result(&ShardSpatialResult {
            shard_id: 1,
            hits: vec![],
            success: false,
            error: Some("timeout".into()),
        });

        let results = merger.merge(10, false);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn merge_respects_limit() {
        let mut merger = SpatialResultMerger::new(1);
        merger.add_shard_result(&ShardSpatialResult {
            shard_id: 0,
            hits: (0..100)
                .map(|i| SpatialHit {
                    doc_id: format!("d{i}"),
                    shard_id: 0,
                    distance_meters: i as f64,
                })
                .collect(),
            success: true,
            error: None,
        });
        let results = merger.merge(5, true);
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].distance_meters, 0.0);
    }
}
