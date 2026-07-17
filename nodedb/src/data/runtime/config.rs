// SPDX-License-Identifier: BUSL-1.1

//! Compaction / maintenance tuning handed to each Data Plane core at spawn.

/// Compaction configuration passed to each Data Plane core.
#[derive(Debug, Clone)]
pub struct CoreCompactionConfig {
    /// How often to run automatic compaction.
    pub interval: std::time::Duration,
    /// Tombstone ratio threshold for auto-compaction.
    pub tombstone_threshold: f64,
    /// Query execution tuning parameters.
    pub query: nodedb_types::config::tuning::QueryTuning,
    /// Graph engine tuning (traversal limits + variable-length expansion caps).
    pub graph: nodedb_types::config::tuning::GraphTuning,
    /// How often this core's event loop flushes vector + sparse-vector
    /// indexes to disk (the per-core backstop checkpoint, distinct from but
    /// sourced from the same `[checkpoint].interval_secs` as the Control
    /// Plane's coordinated `checkpoint_manager` cycle).
    pub checkpoint_interval: std::time::Duration,
}

impl Default for CoreCompactionConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(600),
            tombstone_threshold: 0.2,
            query: nodedb_types::config::tuning::QueryTuning::default(),
            graph: nodedb_types::config::tuning::GraphTuning::default(),
            checkpoint_interval: std::time::Duration::from_secs(300),
        }
    }
}
