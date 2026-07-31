// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_CHECKPOINT_INTERVAL_SECS` / `NODEDB_WAL_SEGMENT_TARGET_MB`
//! overrides.
//!
//! Both drive the crash-recovery test harness: a short interval forces a
//! checkpoint cycle quickly, and a small segment target forces WAL rotation
//! so sealed segments exist for truncation to act on.

use crate::config::server::ServerConfig;

pub(super) fn apply_checkpoint_tuning(config: &mut ServerConfig) {
    if let Ok(val) = std::env::var("NODEDB_CHECKPOINT_INTERVAL_SECS") {
        match val.trim().parse::<u64>() {
            Ok(secs) if secs > 0 => {
                tracing::info!(
                    env_var = "NODEDB_CHECKPOINT_INTERVAL_SECS",
                    value = secs,
                    "environment variable override applied"
                );
                config.checkpoint.interval_secs = secs;
            }
            Ok(secs) => {
                tracing::warn!(
                    env_var = "NODEDB_CHECKPOINT_INTERVAL_SECS",
                    value = secs,
                    "ignoring value of 0 (checkpoint interval must be positive), using config value"
                );
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_CHECKPOINT_INTERVAL_SECS",
                    value = %val,
                    "ignoring malformed environment variable (expected u64 seconds), using config value"
                );
            }
        }
    }

    if let Ok(val) = std::env::var("NODEDB_WAL_SEGMENT_TARGET_MB") {
        match val.trim().parse::<u64>() {
            Ok(mb) if mb > 0 => {
                tracing::info!(
                    env_var = "NODEDB_WAL_SEGMENT_TARGET_MB",
                    value = mb,
                    "environment variable override applied"
                );
                config.checkpoint.wal_segment_target_mb = mb;
            }
            Ok(mb) => {
                tracing::warn!(
                    env_var = "NODEDB_WAL_SEGMENT_TARGET_MB",
                    value = mb,
                    "ignoring value of 0 (WAL segment target must be positive), using config value"
                );
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_WAL_SEGMENT_TARGET_MB",
                    value = %val,
                    "ignoring malformed environment variable (expected u64 MiB), using config value"
                );
            }
        }
    }
}
