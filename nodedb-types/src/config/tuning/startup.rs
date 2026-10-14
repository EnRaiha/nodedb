// SPDX-License-Identifier: Apache-2.0

//! Startup tuning: the stall bounds of the boot readiness waits.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default stall bound of the metadata group's replay wait.
pub const DEFAULT_RAFT_READY_TIMEOUT_MS: u64 = 30_000;

/// Default stall bound of the data groups' replay wait and the Calvin
/// scheduler catch-up wait.
pub const DEFAULT_DATA_GROUP_RECOVERY_TIMEOUT_MS: u64 = 60_000;

/// Stall bound of the metadata group's replay wait.
///
/// A distinct type from [`DataGroupRecoveryTimeout`], so the two bounds
/// cannot swap places as adjacent arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftReadyTimeout(pub Duration);

/// Stall bound of the data groups' replay wait and the Calvin scheduler
/// catch-up wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataGroupRecoveryTimeout(pub Duration);

impl From<Duration> for RaftReadyTimeout {
    fn from(value: Duration) -> Self {
        Self(value)
    }
}

impl From<Duration> for DataGroupRecoveryTimeout {
    fn from(value: Duration) -> Self {
        Self(value)
    }
}

fn default_raft_ready_timeout_ms() -> u64 {
    DEFAULT_RAFT_READY_TIMEOUT_MS
}

fn default_data_group_recovery_timeout_ms() -> u64 {
    DEFAULT_DATA_GROUP_RECOVERY_TIMEOUT_MS
}

/// `[tuning.startup]`: how long each boot wait can go without progress.
///
/// A wait that keeps progressing never fails, however long the replay takes.
/// The server range-checks both values at load. An unknown key is refused, so
/// a misspelt bound never leaves the default in force silently.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupTuning {
    /// Stall bound of the metadata group's replay. Every applied entry resets
    /// it. Default: 30_000.
    #[serde(default = "default_raft_ready_timeout_ms")]
    pub raft_ready_timeout_ms: u64,

    /// Stall bound of the data groups' replay, then of the Calvin scheduler
    /// catch-up. An election, a commit or apply advance, a scheduler epoch, or
    /// a finished group or scheduler resets it. Default: 60_000.
    #[serde(default = "default_data_group_recovery_timeout_ms")]
    pub data_group_recovery_timeout_ms: u64,
}

impl Default for StartupTuning {
    fn default() -> Self {
        Self {
            raft_ready_timeout_ms: default_raft_ready_timeout_ms(),
            data_group_recovery_timeout_ms: default_data_group_recovery_timeout_ms(),
        }
    }
}

impl StartupTuning {
    /// Stall bound of the metadata group's replay wait.
    pub fn raft_ready_timeout(&self) -> RaftReadyTimeout {
        RaftReadyTimeout(Duration::from_millis(self.raft_ready_timeout_ms))
    }

    /// Stall bound of the data groups' replay and Calvin catch-up waits.
    pub fn data_group_recovery_timeout(&self) -> DataGroupRecoveryTimeout {
        DataGroupRecoveryTimeout(Duration::from_millis(self.data_group_recovery_timeout_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_thirty_and_sixty_seconds() {
        let cfg = StartupTuning::default();
        assert_eq!(
            cfg.raft_ready_timeout(),
            RaftReadyTimeout(Duration::from_secs(30))
        );
        assert_eq!(
            cfg.data_group_recovery_timeout(),
            DataGroupRecoveryTimeout(Duration::from_secs(60))
        );
    }

    #[test]
    fn one_bound_set_leaves_the_other_at_its_default() {
        let cfg: StartupTuning =
            toml::from_str("raft_ready_timeout_ms = 1234").expect("deserialize");
        assert_eq!(cfg.raft_ready_timeout_ms, 1234);
        assert_eq!(
            cfg.data_group_recovery_timeout_ms,
            DEFAULT_DATA_GROUP_RECOVERY_TIMEOUT_MS
        );
    }

    #[test]
    fn an_unknown_key_is_refused() {
        let error = toml::from_str::<StartupTuning>("data_group_recovery_timeout = 1800000")
            .expect_err("a key without the _ms suffix is not a bound");
        let message = error.to_string();
        assert!(message.contains("data_group_recovery_timeout"), "{message}");
        assert!(
            message.contains("data_group_recovery_timeout_ms"),
            "the message must list the keys that exist: {message}"
        );
    }
}
