// SPDX-License-Identifier: BUSL-1.1

//! Flags of `nodedb restore`.

use std::path::PathBuf;

use super::error::RestoreError;
use crate::ctl::args::parse_flags;
use crate::storage::snapshot_restore::parse_utc_timestamp;

const FLAGS: &[&str] = &[
    "config",
    "target-time",
    "target-lsn",
    "restore-point",
    "cluster",
    "incarnation",
    "dry-run",
];

/// The point a restore rewinds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreTarget {
    /// Every record at or below this LSN.
    Lsn(u64),
    /// Every record committed at or before this instant.
    Time {
        /// The operator's text, echoed in the plan.
        input: String,
        micros: u64,
    },
}

/// What a restore rewinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreScope {
    /// This node alone, to a point of its own WAL.
    Node(RestoreTarget),
    /// This node's part of a cluster restore to the restore point with this
    /// id. Every node of the cluster restores to it.
    Cluster { restore_point: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreArgs {
    pub config: PathBuf,
    pub scope: RestoreScope,
    /// The node life to restore. Required only when several hold bases.
    pub incarnation: Option<String>,
    pub dry_run: bool,
}

/// Parse the flags after `restore`.
pub fn parse_restore_args(tail: &[String]) -> Result<RestoreArgs, RestoreError> {
    let flags = parse_flags(tail).map_err(|detail| RestoreError::Usage { detail })?;
    if let Some(unknown) = flags.keys().find(|k| !FLAGS.contains(&k.as_str())) {
        return Err(usage(format!("restore does not take --{unknown}")));
    }
    let value = |name: &str| -> Result<Option<String>, RestoreError> {
        match flags.get(name) {
            Some(v) if v.is_empty() => Err(usage(format!("--{name} needs a value"))),
            Some(v) => Ok(Some(v.clone())),
            None => Ok(None),
        }
    };

    let config = value("config")?
        .map(PathBuf::from)
        .ok_or_else(|| usage("restore requires --config <path>".into()))?;
    let scope = match (
        value("target-time")?,
        value("target-lsn")?,
        value("restore-point")?,
    ) {
        (Some(input), None, None) => {
            let micros = parse_utc_timestamp(&input)?;
            RestoreScope::Node(RestoreTarget::Time { input, micros })
        }
        (None, Some(raw), None) => {
            RestoreScope::Node(RestoreTarget::Lsn(raw.parse().map_err(|_| {
                usage(format!(
                    "--target-lsn must be an unsigned integer, got {raw}"
                ))
            })?))
        }
        (None, None, Some(raw)) => RestoreScope::Cluster {
            restore_point: raw.parse().map_err(|_| {
                usage(format!(
                    "--restore-point must be an unsigned integer, got {raw}"
                ))
            })?,
        },
        _ => {
            return Err(usage(
                "restore requires exactly one of --target-time <RFC3339|epoch>, \
                 --target-lsn <N> and --cluster --restore-point <id>"
                    .into(),
            ));
        }
    };
    let cluster = switch(&flags, "cluster")?;
    match (&scope, cluster) {
        (RestoreScope::Cluster { .. }, false) => {
            return Err(usage(
                "--restore-point restores every node of a cluster; pass --cluster with it".into(),
            ));
        }
        (RestoreScope::Node(_), true) => {
            return Err(usage(
                "--cluster restores to a cluster restore point; pass --restore-point <id>".into(),
            ));
        }
        _ => {}
    }
    let dry_run = switch(&flags, "dry-run")?;
    Ok(RestoreArgs {
        config,
        scope,
        incarnation: value("incarnation")?,
        dry_run,
    })
}

/// A flag that takes no value.
fn switch(
    flags: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<bool, RestoreError> {
    match flags.get(name) {
        None => Ok(false),
        Some(v) if v.is_empty() => Ok(true),
        Some(v) => Err(usage(format!("--{name} takes no value, got {v}"))),
    }
}

fn usage(detail: String) -> RestoreError {
    RestoreError::Usage { detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn lsn_target_parses() {
        let parsed =
            parse_restore_args(&args(&["--config", "/c.toml", "--target-lsn", "42"])).unwrap();
        assert_eq!(parsed.config, PathBuf::from("/c.toml"));
        assert_eq!(parsed.scope, RestoreScope::Node(RestoreTarget::Lsn(42)));
        assert!(!parsed.dry_run);
        assert_eq!(parsed.incarnation, None);
    }

    #[test]
    fn time_target_parses_rfc3339_and_epoch() {
        let rfc = parse_restore_args(&args(&[
            "--config",
            "/c.toml",
            "--target-time",
            "2024-03-15T14:30:00Z",
            "--dry-run",
        ]))
        .unwrap();
        assert!(rfc.dry_run);
        let RestoreScope::Node(RestoreTarget::Time { micros, .. }) = rfc.scope else {
            panic!("expected a time target");
        };
        assert_eq!(micros, 1_710_513_000_000_000);

        let epoch = parse_restore_args(&args(&[
            "--dry-run",
            "--config",
            "/c.toml",
            "--target-time",
            "1710513000",
            "--incarnation",
            "abc",
        ]))
        .unwrap();
        assert!(epoch.dry_run);
        assert_eq!(epoch.incarnation.as_deref(), Some("abc"));
        assert!(matches!(
            epoch.scope,
            RestoreScope::Node(RestoreTarget::Time {
                micros: 1_710_513_000_000_000,
                ..
            })
        ));
    }

    #[test]
    fn both_or_neither_target_is_refused() {
        for tail in [
            args(&[
                "--config",
                "/c",
                "--target-lsn",
                "1",
                "--target-time",
                "1710513000",
            ]),
            args(&["--config", "/c"]),
        ] {
            assert!(matches!(
                parse_restore_args(&tail),
                Err(RestoreError::Usage { .. })
            ));
        }
    }

    #[test]
    fn a_restore_point_needs_the_cluster_flag() {
        let parsed = parse_restore_args(&args(&[
            "--config",
            "/c",
            "--cluster",
            "--restore-point",
            "42",
        ]))
        .unwrap();
        assert_eq!(parsed.scope, RestoreScope::Cluster { restore_point: 42 });
        for tail in [
            args(&["--config", "/c", "--restore-point", "42"]),
            args(&["--config", "/c", "--cluster", "--target-lsn", "1"]),
            args(&["--config", "/c", "--cluster", "--restore-point", "x"]),
            args(&[
                "--config",
                "/c",
                "--restore-point",
                "4",
                "--target-lsn",
                "1",
            ]),
        ] {
            assert!(
                matches!(parse_restore_args(&tail), Err(RestoreError::Usage { .. })),
                "{tail:?} must be refused"
            );
        }
    }

    #[test]
    fn missing_config_unknown_flag_and_bad_lsn_are_refused() {
        for tail in [
            args(&["--target-lsn", "1"]),
            args(&["--config", "/c", "--target-lsn", "1", "--force"]),
            args(&["--config", "/c", "--target-lsn", "x"]),
            args(&["--config", "/c", "--target-lsn", "1", "--dry-run", "yes"]),
        ] {
            assert!(
                matches!(parse_restore_args(&tail), Err(RestoreError::Usage { .. })),
                "{tail:?} must be refused"
            );
        }
    }
}
