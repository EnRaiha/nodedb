// SPDX-License-Identifier: BUSL-1.1

//! Compaction handler: periodic and on-demand engine compaction.
//!
//! Compaction removes tombstoned vectors from HNSW indexes, compacts CSR
//! write buffers into dense arrays, sweeps dangling edges from deleted
//! nodes, merges sealed timeseries partitions, and merges FTS LSM segment
//! levels. All operations run on the Data Plane (single-core, no locks).

mod budget;
mod fts;
mod maintenance;
mod runner;
mod segments;
mod stats;

pub use stats::CompactionStats;
