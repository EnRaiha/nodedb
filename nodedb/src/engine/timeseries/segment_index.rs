// SPDX-License-Identifier: BUSL-1.1

//! Read-only index over flushed L1 segments for time-range queries.

use std::collections::{BTreeMap, HashMap};

use nodedb_types::timeseries::{SegmentRef, SeriesId, TimeRange};

/// Maps (series_id, time_range) → segment file references.
///
/// Used by the query path to locate relevant segments without scanning.
#[derive(Debug)]
pub struct SegmentIndex {
    entries: HashMap<SeriesId, BTreeMap<i64, SegmentRef>>,
    total_segments: usize,
    total_bytes: u64,
}

impl SegmentIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            total_segments: 0,
            total_bytes: 0,
        }
    }

    /// Register a flushed segment.
    pub fn add(&mut self, series_id: SeriesId, seg: SegmentRef) {
        self.total_bytes += seg.size_bytes;
        self.total_segments += 1;
        self.entries
            .entry(series_id)
            .or_default()
            .insert(seg.min_ts, seg);
    }

    /// Find segments overlapping a time range for a given series.
    pub fn query(&self, series_id: SeriesId, range: &TimeRange) -> Vec<&SegmentRef> {
        let Some(tree) = self.entries.get(&series_id) else {
            return Vec::new();
        };
        tree.values()
            .filter(|seg| seg.max_ts >= range.start_ms && seg.min_ts <= range.end_ms)
            .collect()
    }

    /// Find ALL segments older than a given timestamp (for retention/compaction).
    pub fn segments_older_than(&self, cutoff_ts: i64) -> Vec<(SeriesId, i64, SegmentRef)> {
        let mut result = Vec::new();
        for (&series_id, tree) in &self.entries {
            for (&min_ts, seg) in tree {
                if seg.max_ts < cutoff_ts {
                    result.push((series_id, min_ts, seg.clone()));
                }
            }
        }
        result
    }

    /// Remove a segment from the index.
    pub fn remove(&mut self, series_id: SeriesId, min_ts: i64) -> Option<SegmentRef> {
        let tree = self.entries.get_mut(&series_id)?;
        let seg = tree.remove(&min_ts)?;
        self.total_bytes = self.total_bytes.saturating_sub(seg.size_bytes);
        self.total_segments = self.total_segments.saturating_sub(1);
        if tree.is_empty() {
            self.entries.remove(&series_id);
        }
        Some(seg)
    }

    pub fn series_count(&self) -> usize {
        self.entries.len()
    }

    pub fn total_segments(&self) -> usize {
        self.total_segments
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl Default for SegmentIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::SegmentKind;

    use super::*;

    #[test]
    fn segment_index_basic() {
        let mut idx = SegmentIndex::new();
        idx.add(
            1,
            SegmentRef {
                path: "seg1.bin".into(),
                min_ts: 1000,
                max_ts: 2000,
                kind: SegmentKind::Metric,
                size_bytes: 4096,
                created_at_ms: 0,
            },
        );
        idx.add(
            1,
            SegmentRef {
                path: "seg2.bin".into(),
                min_ts: 3000,
                max_ts: 4000,
                kind: SegmentKind::Metric,
                size_bytes: 8192,
                created_at_ms: 0,
            },
        );

        let hits = idx.query(1, &TimeRange::new(1500, 2500));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "seg1.bin");

        let all = idx.query(1, &TimeRange::new(0, 5000));
        assert_eq!(all.len(), 2);

        assert_eq!(idx.total_segments(), 2);
        assert_eq!(idx.total_bytes(), 4096 + 8192);
    }
}
