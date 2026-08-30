// SPDX-License-Identifier: Apache-2.0

//! Tiered partition configuration.

use serde::{Deserialize, Serialize};

use super::partition::PartitionInterval;

/// Error validating a `TieredPartitionConfig`.
#[derive(Debug, thiserror::Error)]
#[error("config validation: {field} — {reason}")]
pub struct ConfigValidationError {
    pub field: String,
    pub reason: String,
}

/// Compression codec for archived (cold) partitions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ArchiveCompression {
    #[default]
    #[serde(rename = "zstd")]
    Zstd,
    #[serde(rename = "lz4")]
    Lz4,
    #[serde(rename = "snappy")]
    Snappy,
}

/// Full lifecycle configuration for a timeseries collection.
///
/// All fields have sensible defaults. Platform-aware: Origin and Lite
/// use different defaults for memory budgets and retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredPartitionConfig {
    // -- Tier 0: Hot (RAM memtable) --
    pub memtable_flush_interval_ms: u64,
    pub memtable_max_memory_bytes: u64,

    // -- Tier 1: Warm (NVMe, columnar partitions) --
    pub partition_by: PartitionInterval,
    pub merge_after_ms: u64,
    pub merge_count: u32,

    // -- Tier 2: Cold (S3 Parquet) --
    pub archive_after_ms: u64,
    pub archive_compression: ArchiveCompression,

    // -- Retention --
    pub retention_period_ms: u64,

    // -- Timestamp --
    pub timestamp_column: String,

    // -- Tags --
    pub max_tag_cardinality: u32,

    // -- WAL --
    pub wal_enabled: bool,

    // -- CDC --
    pub cdc_enabled: bool,

    // -- Sync (Lite only) --
    pub sync_resolution_ms: u64,
    pub sync_interval_ms: u64,
    pub retain_until_synced: bool,

    // -- Battery (Lite-B mobile only) --
    #[serde(default)]
    pub battery_aware: bool,

    // -- Burst ingestion (Lite-C) --
    #[serde(default)]
    pub bulk_import_threshold_rows: u64,

    // -- Partition size targets (Lite-C) --
    #[serde(default)]
    pub partition_size_target_bytes: u64,

    // -- Compaction (Lite-C) --
    #[serde(default)]
    pub compaction_partition_threshold: u32,
}

impl TieredPartitionConfig {
    /// Default configuration for Origin (cloud server).
    pub fn origin_defaults() -> Self {
        Self {
            memtable_flush_interval_ms: 10_000,
            memtable_max_memory_bytes: 64 * 1024 * 1024,
            partition_by: PartitionInterval::Auto,
            merge_after_ms: 30 * 86_400_000,
            merge_count: 10,
            archive_after_ms: 0,
            retention_period_ms: 0,
            archive_compression: ArchiveCompression::Zstd,
            timestamp_column: String::new(),
            max_tag_cardinality: 100_000,
            wal_enabled: true,
            cdc_enabled: false,
            sync_resolution_ms: 0,
            sync_interval_ms: 0,
            retain_until_synced: false,
            battery_aware: false,
            bulk_import_threshold_rows: 0,
            partition_size_target_bytes: 0,
            compaction_partition_threshold: 0,
        }
    }

    /// Default configuration for Lite (edge device).
    pub fn lite_defaults() -> Self {
        Self {
            memtable_flush_interval_ms: 30_000,
            memtable_max_memory_bytes: 4 * 1024 * 1024,
            partition_by: PartitionInterval::Auto,
            merge_after_ms: 7 * 86_400_000,
            merge_count: 4,
            archive_after_ms: 0,
            retention_period_ms: 7 * 86_400_000,
            archive_compression: ArchiveCompression::Zstd,
            timestamp_column: String::new(),
            max_tag_cardinality: 10_000,
            wal_enabled: true,
            cdc_enabled: false,
            sync_resolution_ms: 0,
            sync_interval_ms: 30_000,
            retain_until_synced: false,
            battery_aware: false,
            bulk_import_threshold_rows: 1_000_000,
            partition_size_target_bytes: 1_024 * 1_024,
            compaction_partition_threshold: 20,
        }
    }

    /// Validate configuration consistency.
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.merge_count < 2 {
            return Err(ConfigValidationError {
                field: "merge_count".into(),
                reason: "must be >= 2".into(),
            });
        }
        if self.retention_period_ms > 0
            && self.archive_after_ms > 0
            && self.retention_period_ms < self.archive_after_ms
        {
            return Err(ConfigValidationError {
                field: "retention_period".into(),
                reason: "must be >= archive_after".into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiered_config_validation() {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        assert!(cfg.validate().is_ok());

        cfg.merge_count = 1;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "merge_count");

        cfg.merge_count = 10;
        cfg.retention_period_ms = 1000;
        cfg.archive_after_ms = 2000;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.field, "retention_period");
    }
}
