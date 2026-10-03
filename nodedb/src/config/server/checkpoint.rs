// SPDX-License-Identifier: BUSL-1.1

use serde::{Deserialize, Serialize};

/// Checkpoint and WAL segment management configuration.
///
/// Controls how often engine state is flushed to disk and how the WAL
/// is segmented and truncated. All intervals are in seconds.
///
/// Example TOML:
/// ```toml
/// [checkpoint]
/// interval_secs = 300
/// core_timeout_secs = 30
/// wal_segment_target_mb = 64
/// wal_archive_interval_secs = 10
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointSettings {
    /// How often the checkpoint manager runs (seconds). Each cycle:
    /// dispatches `Checkpoint` to all cores, collects watermark LSNs,
    /// writes a WAL checkpoint marker, and truncates old WAL segments.
    /// Default: 300 (5 minutes).
    #[serde(default = "default_checkpoint_interval")]
    pub interval_secs: u64,

    /// Maximum time to wait for each Data Plane core to complete its
    /// checkpoint flush (seconds). Cores that don't respond in time are
    /// skipped — the global checkpoint LSN uses only responding cores.
    /// Default: 30.
    #[serde(default = "default_core_timeout")]
    pub core_timeout_secs: u64,

    /// Target WAL segment file size in MiB. When the active segment
    /// exceeds this, the writer rolls to a new segment. Old segments
    /// are deleted after checkpoint confirmation.
    /// This is a soft limit — the current record is always completed
    /// before rolling.
    /// Default: 64 MiB.
    #[serde(default = "default_wal_segment_target_mb")]
    pub wal_segment_target_mb: u64,

    /// How often sealed WAL segments are uploaded to cold storage (seconds).
    /// Runs only when `[cold_storage]` is configured. A sealed segment
    /// reaches the archive within one interval.
    /// Default: 10.
    #[serde(default = "default_wal_archive_interval")]
    pub wal_archive_interval_secs: u64,

    /// How often each Data Plane core runs automatic compaction (seconds).
    /// Compaction removes tombstoned vectors from HNSW indexes, compacts
    /// CSR write buffers, and sweeps dangling edges.
    /// Default: 600 (10 minutes).
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_secs: u64,

    /// Tombstone ratio threshold for automatic vector compaction (0.0–1.0).
    /// Collections with tombstone ratio below this are skipped during
    /// periodic compaction. On-demand compaction (`COMPACT`) ignores this.
    /// Default: 0.2 (20%).
    #[serde(default = "default_compaction_tombstone_threshold")]
    pub compaction_tombstone_threshold: f64,
}

impl Default for CheckpointSettings {
    fn default() -> Self {
        Self {
            interval_secs: default_checkpoint_interval(),
            core_timeout_secs: default_core_timeout(),
            wal_segment_target_mb: default_wal_segment_target_mb(),
            wal_archive_interval_secs: default_wal_archive_interval(),
            compaction_interval_secs: default_compaction_interval(),
            compaction_tombstone_threshold: default_compaction_tombstone_threshold(),
        }
    }
}

impl CheckpointSettings {
    /// Convert to the checkpoint manager config used by the Control Plane.
    ///
    /// Fails with `Error::Config` when `interval_secs` or
    /// `wal_archive_interval_secs` is zero, whatever path set it.
    pub fn to_manager_config(
        &self,
    ) -> crate::Result<crate::control::checkpoint_manager::CheckpointManagerConfig> {
        crate::control::checkpoint_manager::CheckpointManagerConfig::new(
            std::time::Duration::from_secs(self.interval_secs),
            std::time::Duration::from_secs(self.core_timeout_secs),
            std::time::Duration::from_secs(self.wal_archive_interval_secs),
        )
    }

    /// WAL segment target size in bytes.
    pub fn wal_segment_target_bytes(&self) -> u64 {
        self.wal_segment_target_mb * 1024 * 1024
    }

    /// Compaction interval as `Duration`.
    pub fn compaction_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.compaction_interval_secs)
    }
}

fn default_checkpoint_interval() -> u64 {
    300
}

fn default_core_timeout() -> u64 {
    30
}

fn default_wal_segment_target_mb() -> u64 {
    64
}

fn default_wal_archive_interval() -> u64 {
    10
}

fn default_compaction_interval() -> u64 {
    600
}

fn default_compaction_tombstone_threshold() -> f64 {
    0.2
}
