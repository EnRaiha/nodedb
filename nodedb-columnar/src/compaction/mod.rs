// SPDX-License-Identifier: Apache-2.0

//! Single-segment compaction: rewrite a segment without its deleted rows.
//!
//! Origin never rewrites a flushed segment — its segments are write-once and
//! tombstones are resolved at read time. Embedded (Lite) deployments have no
//! background reclaim of their own, so they rewrite a segment in place once
//! its delete ratio crosses a threshold. The rewrite reads the source segment,
//! skips rows marked in the delete bitmap, and encodes a new segment from the
//! survivors. The caller owns the metadata swap: store the new bytes, update
//! the row count, and drop the stale delete bitmap.

pub mod segment;

#[cfg(test)]
mod tests;

pub use segment::{CompactionResult, DEFAULT_DELETE_RATIO_THRESHOLD, compact_segment};
