// SPDX-License-Identifier: Apache-2.0

//! Level-based compaction for the FTS LSM engine.
//!
//! Uses tiered compaction with configurable levels and segments-per-level.
//! When a level fills (exceeds `max_segments_per_level`), all segments at
//! that level are merged into a single segment at the next level.

use crate::backend::FtsBackend;
use crate::index::FtsIndexError;
use crate::scope::IndexScope;

use super::merge;
use super::query::LiveSegment;
use super::segment::{reader::SegmentReader, writer};
use super::segment_deletes::SegmentDeletes;

use std::sync::Arc;

use nodedb_mem::MemoryGovernor;

use crate::mem_scope::fts_scope;

/// Compaction configuration.
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    /// Maximum number of levels.
    pub max_levels: usize,
    /// Maximum segments per level before triggering compaction.
    pub max_segments_per_level: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_levels: 8,
            max_segments_per_level: 8,
        }
    }
}

/// Segment metadata for level tracking.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// Segment id (unscoped, e.g. `"L{level}:{id:016x}"`).
    pub segment_id: String,
    /// Compaction level (0 = freshly flushed from memtable).
    pub level: u32,
    /// Byte size of the segment.
    pub size: u64,
}

/// Check which level needs compaction and return the level number, or None.
pub fn needs_compaction(segments: &[SegmentMeta], config: &CompactionConfig) -> Option<u32> {
    let mut counts = vec![0usize; config.max_levels];
    for seg in segments {
        let level = seg.level as usize;
        if level < config.max_levels {
            counts[level] += 1;
        }
    }

    for (level, &count) in counts.iter().enumerate() {
        if count >= config.max_segments_per_level && level + 1 < config.max_levels {
            return Some(level as u32);
        }
    }
    None
}

/// Result of a compaction: new segment bytes and ids of merged (to-remove) segments.
pub type CompactionResult = (Vec<u8>, Vec<String>);

/// Errors from `compact_level` — wraps the backend error, budget exhaustion,
/// and index state that cannot be read or written.
#[derive(Debug)]
pub enum CompactError<E: std::fmt::Display> {
    /// Underlying backend storage error.
    Backend(E),
    /// Memory budget exhausted.
    Budget(nodedb_mem::MemError),
    /// A source segment or the index's delete sets are corrupt or missing,
    /// or the merged segment cannot be encoded. No segment is replaced.
    Index(FtsIndexError<E>),
}

impl<E: std::fmt::Display> std::fmt::Display for CompactError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactError::Backend(e) => write!(f, "compaction backend error: {e}"),
            CompactError::Budget(e) => write!(f, "compaction budget exhausted: {e}"),
            CompactError::Index(e) => write!(f, "compaction index error: {e}"),
        }
    }
}

impl<E: std::fmt::Display> From<FtsIndexError<E>> for CompactError<E> {
    fn from(e: FtsIndexError<E>) -> Self {
        match e {
            FtsIndexError::Backend(inner) => CompactError::Backend(inner),
            FtsIndexError::BudgetExhausted(inner) => CompactError::Budget(inner),
            other => CompactError::Index(other),
        }
    }
}

/// Inputs to [`compact_level`].
///
/// Groups the backend handle, the `(database_id, tid, index)` scope, the
/// candidate segment list, the target level, and the optional memory governor.
pub struct CompactLevelParams<'a, B: FtsBackend> {
    /// Backend the source segments are read from.
    pub backend: &'a B,
    /// Owning database id.
    pub database_id: u64,
    /// Owning tenant id.
    pub tid: u64,
    /// Index whose segments are being compacted.
    pub index: IndexScope<'a>,
    /// All known segments of the index (filtered to `level` internally).
    pub segments: &'a [SegmentMeta],
    /// Level whose segments are merged into `level + 1`.
    pub level: u32,
    /// Memory governor budgeting each `Vec::with_capacity`.
    pub governor: &'a Arc<MemoryGovernor>,
}

/// Perform compaction: merge all segments at `level` into one segment at `level + 1`.
///
/// Returns the merged segment bytes and the ids of segments that were merged
/// (which should be removed from storage after the new segment is written).
/// The merge drops each source segment's postings of its deleted documents,
/// so the merged segment carries no delete set. Once a source segment is
/// removed, reads ignore its delete set and the next delete drops it.
///
/// A source segment that is missing or fails validation fails the
/// compaction: merging without it and then removing it would lose its
/// postings.
///
/// Each `Vec::with_capacity` allocation is budgeted via
/// [`nodedb_mem::ScopedMemory::reserve`]. If the budget is exhausted the
/// function returns [`CompactError::Budget`] before allocating.
pub fn compact_level<B: FtsBackend>(
    params: CompactLevelParams<'_, B>,
) -> Result<Option<CompactionResult>, CompactError<B::Error>> {
    let CompactLevelParams {
        backend,
        database_id,
        tid,
        index,
        segments,
        level,
        governor,
    } = params;
    let to_merge: Vec<&SegmentMeta> = segments.iter().filter(|s| s.level == level).collect();
    if to_merge.len() < 2 {
        return Ok(None);
    }

    let memory = fts_scope(governor, database_id, tid);

    let _readers_guard = memory
        .reserve(to_merge.len() * size_of::<LiveSegment>())
        .map_err(CompactError::Budget)?;
    let mut sources = Vec::with_capacity(to_merge.len());

    let _ids_guard = memory
        .reserve(to_merge.len() * size_of::<String>())
        .map_err(CompactError::Budget)?;
    let mut merged_ids = Vec::with_capacity(to_merge.len());

    let merge_ids: Vec<String> = to_merge.iter().map(|m| m.segment_id.clone()).collect();
    let mut deletes = SegmentDeletes::load(backend, database_id, tid, index, &merge_ids)?;
    for segment_id in merge_ids {
        let Some(data) = backend
            .read_segment(database_id, tid, index, &segment_id)
            .map_err(CompactError::Backend)?
        else {
            return Err(CompactError::Index(FtsIndexError::MissingSegment {
                segment_id,
            }));
        };
        let reader = SegmentReader::open(data).map_err(|source| {
            CompactError::Index(FtsIndexError::CorruptSegment {
                segment_id: segment_id.clone(),
                source,
            })
        })?;
        let deleted = deletes.take(&segment_id);
        merged_ids.push(segment_id.clone());
        sources.push(LiveSegment {
            segment_id,
            reader,
            deleted,
        });
    }

    let merged_term_blocks = merge::merge_segments::<B::Error>(&sources, &memory)?;
    let new_segment = writer::build_from_blocks(&merged_term_blocks)
        .map_err(|e| CompactError::Index(FtsIndexError::from(e)))?;

    Ok(Some((new_segment, merged_ids)))
}

/// Generate a segment id (unscoped).
pub fn segment_id(segment_id: u64, level: u32) -> String {
    format!("L{level}:{segment_id:016x}")
}

/// Parse the level from a segment id. Returns 0 if unparseable.
pub fn parse_level(id: &str) -> u32 {
    // Format: "L{level}:{id}"
    id.strip_prefix('L')
        .and_then(|s| s.split(':').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Parse the segment number from a segment id.
pub fn parse_segment_number(id: &str) -> u64 {
    id.rsplit(':')
        .next()
        .and_then(|s| u64::from_str_radix(s, 16).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(id: &str, level: u32) -> SegmentMeta {
        SegmentMeta {
            segment_id: id.to_string(),
            level,
            size: 1000,
        }
    }

    #[test]
    fn needs_compaction_basic() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 3,
        };

        let segments = vec![make_meta("s1", 0), make_meta("s2", 0), make_meta("s3", 0)];
        assert_eq!(needs_compaction(&segments, &config), Some(0));
    }

    #[test]
    fn no_compaction_under_threshold() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 3,
        };
        let segments = vec![make_meta("s1", 0), make_meta("s2", 0)];
        assert_eq!(needs_compaction(&segments, &config), None);
    }

    #[test]
    fn compaction_at_higher_level() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 2,
        };
        let segments = vec![make_meta("s1", 0), make_meta("s2", 1), make_meta("s3", 1)];
        assert_eq!(needs_compaction(&segments, &config), Some(1));
    }

    #[test]
    fn segment_id_format() {
        let id = segment_id(42, 0);
        assert!(id.starts_with("L0:"));
        assert_eq!(parse_level(&id), 0);
        assert_eq!(parse_segment_number(&id), 42);
    }

    #[test]
    fn parse_level_and_id() {
        assert_eq!(parse_level("L2:000000000000002a"), 2);
        assert_eq!(parse_segment_number("L2:000000000000002a"), 42);
    }
}
