// SPDX-License-Identifier: BUSL-1.1

use std::num::NonZeroUsize;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::ServerConfig;

/// Default seconds between two base snapshots: one day.
const DEFAULT_BASE_SNAPSHOT_INTERVAL_SECS: u64 = 86_400;

/// Default number of base snapshots retention keeps.
const DEFAULT_BASE_SNAPSHOT_RETENTION: u64 = 7;

/// Default seconds between two periodic cluster restore points: none.
const DEFAULT_RESTORE_POINT_INTERVAL_SECS: u64 = 0;

/// Point-in-time recovery configuration.
///
/// PITR needs every WAL segment in the archive before truncation deletes it.
/// Without `[cold_storage]` there is no archive, and truncation deletes
/// segments that were never archived. That is correct only when PITR is off.
///
/// Base snapshots are encrypted with the WAL key, so PITR also needs
/// `[encryption]`.
///
/// Example TOML:
/// ```toml
/// [pitr]
/// enabled = true
/// base_snapshot_interval_secs = 86400
/// base_snapshot_retention = 7
/// restore_point_interval_secs = 3600
///
/// [cold_storage]
/// bucket = "my-nodedb-cold"
///
/// [encryption]
/// key_path = "/etc/nodedb/keys/wal.key"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PitrSettings {
    /// Require a continuous WAL archive and take periodic base snapshots.
    /// Boot fails without `[cold_storage]` and `[encryption]`. Default: false.
    #[serde(default)]
    pub enabled: bool,
    /// Seconds between two base snapshots. It also bounds how long one
    /// snapshot can take. Must be positive. Default: 86400.
    #[serde(default = "default_base_snapshot_interval_secs")]
    pub base_snapshot_interval_secs: u64,
    /// Base snapshots kept. Older bases are deleted, then archived WAL that
    /// only they needed. Must be positive. Default: 7.
    #[serde(default = "default_base_snapshot_retention")]
    pub base_snapshot_retention: u64,
    /// Seconds between two periodic cluster restore points. `0` takes none;
    /// `CREATE RESTORE POINT` still takes one on demand. Only a cluster takes
    /// restore points. Default: 0.
    #[serde(default = "default_restore_point_interval_secs")]
    pub restore_point_interval_secs: u64,
}

impl Default for PitrSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            base_snapshot_interval_secs: DEFAULT_BASE_SNAPSHOT_INTERVAL_SECS,
            base_snapshot_retention: DEFAULT_BASE_SNAPSHOT_RETENTION,
            restore_point_interval_secs: DEFAULT_RESTORE_POINT_INTERVAL_SECS,
        }
    }
}

impl PitrSettings {
    pub fn base_snapshot_interval(&self) -> Duration {
        Duration::from_secs(self.base_snapshot_interval_secs)
    }

    /// Interval between periodic restore points, `None` when off.
    pub fn restore_point_interval(&self) -> Option<Duration> {
        (self.restore_point_interval_secs > 0)
            .then(|| Duration::from_secs(self.restore_point_interval_secs))
    }

    /// Base snapshots retention keeps. A zero value is a config error.
    pub fn retention(&self) -> crate::Result<NonZeroUsize> {
        super::domain::positive_u64(self.base_snapshot_retention, "pitr.base_snapshot_retention")?;
        Ok(usize::try_from(self.base_snapshot_retention)
            .ok()
            .and_then(NonZeroUsize::new)
            .unwrap_or(NonZeroUsize::MAX))
    }
}

fn default_base_snapshot_interval_secs() -> u64 {
    DEFAULT_BASE_SNAPSHOT_INTERVAL_SECS
}

fn default_base_snapshot_retention() -> u64 {
    DEFAULT_BASE_SNAPSHOT_RETENTION
}

fn default_restore_point_interval_secs() -> u64 {
    DEFAULT_RESTORE_POINT_INTERVAL_SECS
}

/// Refuse a config that enables PITR with no archive to recover from, with no
/// key to write base snapshots, or with a zero interval or retention.
pub(super) fn validate_pitr(config: &ServerConfig) -> crate::Result<()> {
    super::domain::positive_u64(
        config.pitr.base_snapshot_interval_secs,
        "pitr.base_snapshot_interval_secs",
    )?;
    super::domain::positive_u64(
        config.pitr.base_snapshot_retention,
        "pitr.base_snapshot_retention",
    )?;
    if !config.pitr.enabled {
        return Ok(());
    }
    if config.cold_storage.is_none() {
        return Err(missing_cold_storage());
    }
    if config.encryption.is_none() {
        return Err(crate::Error::Config {
            detail: "pitr.enabled = true requires an [encryption] section: base snapshots \
                     are encrypted with the WAL key. Add [encryption] key_path or set \
                     pitr.enabled = false"
                .into(),
        });
    }
    Ok(())
}

/// The error for `pitr.enabled = true` with no usable `[cold_storage]`.
pub fn missing_cold_storage() -> crate::Error {
    crate::Error::Config {
        detail: "pitr.enabled = true requires a [cold_storage] section: the WAL archive is \
                 the only copy of a segment once truncation deletes it. Add [cold_storage] \
                 or set pitr.enabled = false"
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COLD: &str = "\n[cold_storage]\nbucket = \"b\"\n";
    const KEY: &str = "\n[encryption]\nkey_path = \"/k\"\n";

    fn parse(raw: &str) -> ServerConfig {
        toml::from_str(raw).expect("deserialize")
    }

    #[test]
    fn pitr_is_off_by_default() {
        let cfg = ServerConfig::default();
        assert!(!cfg.pitr.enabled);
        assert_eq!(cfg.pitr.base_snapshot_interval_secs, 86_400);
        assert_eq!(cfg.pitr.base_snapshot_retention, 7);
        cfg.validate().expect("PITR off needs no cold storage");
    }

    #[test]
    fn pitr_without_cold_storage_refuses_to_boot() {
        let cfg = parse(&format!("[pitr]\nenabled = true\n{KEY}"));
        let msg = cfg.validate().unwrap_err().to_string();
        assert!(msg.contains("pitr.enabled"), "{msg}");
        assert!(msg.contains("[cold_storage]"), "{msg}");
    }

    #[test]
    fn pitr_without_encryption_refuses_to_boot() {
        let cfg = parse(&format!("[pitr]\nenabled = true\n{COLD}"));
        let msg = cfg.validate().unwrap_err().to_string();
        assert!(msg.contains("[encryption]"), "{msg}");
    }

    #[test]
    fn pitr_with_cold_storage_and_a_key_boots() {
        let cfg = parse(&format!("[pitr]\nenabled = true\n{COLD}{KEY}"));
        cfg.validate()
            .expect("PITR with cold storage and a key is valid");
    }

    #[test]
    fn a_zero_base_snapshot_interval_is_a_config_error() {
        let cfg = parse(&format!(
            "[pitr]\nenabled = true\nbase_snapshot_interval_secs = 0\n{COLD}{KEY}"
        ));
        match cfg.validate() {
            Err(crate::Error::Config { detail }) => {
                assert!(
                    detail.contains("pitr.base_snapshot_interval_secs"),
                    "{detail}"
                );
            }
            other => panic!("expected a config error, got {other:?}"),
        }
    }

    #[test]
    fn a_zero_base_snapshot_retention_is_a_config_error() {
        let cfg = parse("[pitr]\nbase_snapshot_retention = 0\n");
        match cfg.validate() {
            Err(crate::Error::Config { detail }) => {
                assert!(detail.contains("pitr.base_snapshot_retention"), "{detail}");
            }
            other => panic!("expected a config error, got {other:?}"),
        }
    }

    #[test]
    fn from_file_applies_the_pitr_gate() {
        let path = std::env::temp_dir().join("nodedb-pitr-gate.toml");
        std::fs::write(&path, "[pitr]\nenabled = true\n").expect("write temp config");
        let err = ServerConfig::from_file(&path).unwrap_err();
        std::fs::remove_file(&path).ok();
        assert!(err.to_string().contains("[cold_storage]"), "{err}");
    }

    #[test]
    fn unknown_pitr_field_rejected() {
        let result: Result<ServerConfig, _> = toml::from_str("[pitr]\nenable = true\n");
        assert!(result.is_err(), "a misspelled pitr field must be rejected");
    }
}
