// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::ServerConfig;

/// Scheduled logical backups.
///
/// Each entry runs `BACKUP DATABASE <database>` on its cron schedule. A run
/// writes one envelope named `<database>-<unix_ms>.ndbb` under `target`, where
/// `<unix_ms>` is the scheduled minute. It then deletes the oldest envelopes
/// of that database under `target` beyond `keep`. Other objects under
/// `target` are never touched.
///
/// Example TOML:
/// ```toml
/// [[backup.schedule]]
/// database = "sales"
/// target = "s3://my-backups/nightly/sales"
/// cron = "0 3 * * *"
/// keep = 7
///
/// [backup_encryption]
/// key_path = "/etc/nodedb/keys/backup.key"
/// ```
///
/// `target` resolves like a `BACKUP DATABASE ... TO` URI, against
/// `[backup_storage]`. A `file://` target lies inside `local_root`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupSettings {
    #[serde(default)]
    pub schedule: Vec<BackupScheduleSettings>,
}

/// One scheduled backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupScheduleSettings {
    /// Database to back up.
    pub database: String,
    /// URI prefix the envelopes go under: `file:///<dir>` or
    /// `s3://<bucket>/<prefix>`.
    pub target: String,
    /// 5-field cron expression, evaluated in `[scheduler] cron_timezone`.
    pub cron: String,
    /// Envelopes of this database kept under `target`. Must be positive.
    pub keep: u64,
}

impl BackupScheduleSettings {
    /// `target` without trailing slashes.
    pub fn target_prefix(&self) -> &str {
        self.target.trim_end_matches('/')
    }

    /// The job name the scheduler records history under.
    pub fn job_name(&self) -> String {
        format!("backup:{}:{}", self.database, self.target_prefix())
    }

    /// The config incarnation: a fingerprint of the database, target and
    /// cron. Every node computes the same value for the same entry. A
    /// changed entry is a new incarnation, with its own schedule mark.
    pub fn incarnation(&self) -> u64 {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for part in [
            self.database.as_str(),
            self.target_prefix(),
            self.cron.trim(),
        ] {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        let digest = hasher.finalize();
        let mut head = [0u8; 8];
        head.copy_from_slice(&digest[..8]);
        u64::from_le_bytes(head)
    }
}

/// Refuse a schedule that can never run: a bad cron, a zero `keep`, a target
/// no backup URI accepts, no backup key, or two entries sharing one database
/// and target, whose `keep` retention deletes each other's envelopes.
pub(super) fn validate_backup(config: &ServerConfig) -> crate::Result<()> {
    let schedules = &config.backup.schedule;
    if schedules.is_empty() {
        return Ok(());
    }
    if config.backup_encryption.is_none() {
        return Err(invalid(
            "backup.schedule needs a [backup_encryption] section: every backup envelope is \
             encrypted. Add [backup_encryption] key_path or remove backup.schedule"
                .into(),
        ));
    }
    let mut seen = HashSet::new();
    for (index, schedule) in schedules.iter().enumerate() {
        let entry = format!("backup.schedule[{index}]");
        if schedule.database.trim().is_empty() {
            return Err(invalid(format!("{entry}.database is empty")));
        }
        if schedule.keep == 0 {
            return Err(invalid(format!(
                "{entry}.keep is 0: expected a positive number of envelopes to keep"
            )));
        }
        crate::event::scheduler::cron::CronExpr::parse(&schedule.cron)
            .map_err(|e| invalid(format!("{entry}.cron '{}': {e}", schedule.cron)))?;
        check_target(&entry, schedule, config)?;
        if !seen.insert((schedule.database.as_str(), schedule.target_prefix())) {
            return Err(invalid(format!(
                "{entry} repeats database '{}' with target '{}'; each pair runs once",
                schedule.database, schedule.target
            )));
        }
    }
    Ok(())
}

fn check_target(
    entry: &str,
    schedule: &BackupScheduleSettings,
    config: &ServerConfig,
) -> crate::Result<()> {
    let target = schedule.target_prefix();
    if let Some(path) = target.strip_prefix("file://") {
        let root = config
            .backup_storage
            .as_ref()
            .and_then(|storage| storage.local_root.as_deref())
            .ok_or_else(|| {
                invalid(format!(
                    "{entry}.target '{target}' is a file:// URI, which needs \
                     [backup_storage] local_root"
                ))
            })?;
        let inside = std::path::Path::new(path)
            .strip_prefix(root)
            .is_ok_and(|rest| !rest.as_os_str().is_empty());
        if !inside {
            return Err(invalid(format!(
                "{entry}.target '{target}' must name a directory inside [backup_storage] \
                 local_root '{}'",
                root.display()
            )));
        }
        return Ok(());
    }
    if let Some(rest) = target.strip_prefix("s3://") {
        let has_prefix = rest
            .split_once('/')
            .is_some_and(|(bucket, prefix)| !bucket.is_empty() && !prefix.is_empty());
        if has_prefix {
            return Ok(());
        }
        return Err(invalid(format!(
            "{entry}.target '{target}': expected s3://<bucket>/<prefix>"
        )));
    }
    Err(invalid(format!(
        "{entry}.target '{target}': expected file:///<dir> or s3://<bucket>/<prefix>"
    )))
}

fn invalid(detail: String) -> crate::Error {
    crate::Error::Config { detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "\n[backup_encryption]\nkey_path = \"/k\"\n";
    const ROOT: &str = "\n[backup_storage]\nlocal_root = \"/srv/backups\"\n";

    fn parse(raw: &str) -> ServerConfig {
        toml::from_str(raw).expect("deserialize")
    }

    fn entry(target: &str, cron: &str, keep: u64) -> String {
        format!(
            "[[backup.schedule]]\ndatabase = \"sales\"\ntarget = \"{target}\"\n\
             cron = \"{cron}\"\nkeep = {keep}\n"
        )
    }

    fn error(raw: &str) -> String {
        match parse(raw).validate() {
            Err(crate::Error::Config { detail }) => detail,
            other => panic!("expected a config error, got {other:?}"),
        }
    }

    #[test]
    fn a_valid_schedule_parses() {
        let raw = format!("{}{KEY}{ROOT}", entry("s3://b/nightly/", "0 3 * * *", 7));
        let cfg = parse(&raw);
        cfg.validate().expect("valid schedule");
        let schedule = &cfg.backup.schedule[0];
        assert_eq!(schedule.keep, 7);
        assert_eq!(schedule.target_prefix(), "s3://b/nightly");
        assert_eq!(schedule.job_name(), "backup:sales:s3://b/nightly");
        let local = format!(
            "{}{KEY}{ROOT}",
            entry("file:///srv/backups/sales", "* * * * *", 1)
        );
        parse(&local)
            .validate()
            .expect("file target inside the root");
    }

    #[test]
    fn the_incarnation_follows_database_target_and_cron_only() {
        let base = BackupScheduleSettings {
            database: "sales".into(),
            target: "s3://b/p".into(),
            cron: "0 3 * * *".into(),
            keep: 2,
        };
        let same = BackupScheduleSettings {
            target: "s3://b/p/".into(),
            keep: 9,
            ..base.clone()
        };
        assert_eq!(base.incarnation(), same.incarnation());
        let recron = BackupScheduleSettings {
            cron: "0 4 * * *".into(),
            ..base.clone()
        };
        assert_ne!(base.incarnation(), recron.incarnation());
    }

    #[test]
    fn no_schedule_needs_nothing() {
        ServerConfig::default().validate().expect("no schedule");
    }

    #[test]
    fn invalid_schedules_are_config_errors() {
        let s3 = "s3://b/p";
        assert!(error(&entry(s3, "0 3 * * *", 1)).contains("[backup_encryption]"));
        assert!(error(&format!("{}{KEY}", entry(s3, "0 3 * * *", 0))).contains("keep"));
        assert!(error(&format!("{}{KEY}", entry(s3, "bad", 1))).contains("cron"));
        assert!(error(&format!("{}{KEY}", entry("s3://b", "0 3 * * *", 1))).contains("target"));
        assert!(error(&format!("{}{KEY}", entry("ftp://x/y", "0 3 * * *", 1))).contains("target"));
        let no_root = format!("{}{KEY}", entry("file:///srv/backups/x", "0 3 * * *", 1));
        assert!(error(&no_root).contains("local_root"));
        let outside = format!("{}{KEY}{ROOT}", entry("file:///etc/x", "0 3 * * *", 1));
        assert!(error(&outside).contains("inside"));
        let twice = format!(
            "{}{}{KEY}",
            entry(s3, "0 3 * * *", 1),
            entry("s3://b/p/", "0 4 * * *", 2)
        );
        assert!(error(&twice).contains("repeats"));
    }

    #[test]
    fn unknown_schedule_field_rejected() {
        let raw = format!("{}retain = 3\n", entry("s3://b/p", "0 3 * * *", 1));
        assert!(toml::from_str::<ServerConfig>(&raw).is_err());
    }
}
