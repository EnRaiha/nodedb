// SPDX-License-Identifier: BUSL-1.1

//! Writes the TOML config file the spawned server reads via `NODEDB_CONFIG`.
//!
//! pgwire authentication mode has no environment-variable override — it is
//! TOML only (`[auth] mode = "..."`). `ServerConfig` derives `[auth]` from
//! `#[serde(default)]` only when the `[auth]` table is absent entirely; once
//! the table is written, every one of its fields that lacks its own
//! `#[serde(default = ...)]` must be present or parsing fails. This writes
//! the full required set every time, never a bare `mode` line.

use std::path::{Path, PathBuf};

/// Which pgwire authentication mode the spawned server boots into.
#[derive(Clone, Copy)]
pub(super) enum AuthMode {
    /// No authentication, lockout disabled — `TestServer::start`'s default.
    Trust,
    /// SCRAM-SHA-256 with a lockout policy of 5 failures / 300s, matching
    /// `TestServer::start_password`.
    Password,
}

/// Write `nodedb.toml` into `dir` and return its path.
///
/// `columnar_flush_threshold`, when `Some`, overrides
/// `[tuning.query] columnar_flush_threshold` so a test can observe segment
/// flushes without inserting 65k rows.
pub(super) fn write_config(
    dir: &Path,
    auth_mode: AuthMode,
    columnar_flush_threshold: Option<usize>,
) -> PathBuf {
    let (mode, max_failed_logins, lockout_duration_secs) = match auth_mode {
        AuthMode::Trust => ("trust", 0u32, 0u64),
        AuthMode::Password => ("password", 5u32, 300u64),
    };
    let mut toml = format!(
        "[auth]\n\
         mode = \"{mode}\"\n\
         superuser_name = \"nodedb\"\n\
         min_password_length = 8\n\
         max_failed_logins = {max_failed_logins}\n\
         lockout_duration_secs = {lockout_duration_secs}\n\
         idle_timeout_secs = 0\n\
         max_connections_per_user = 0\n\
         password_expiry_days = 0\n\
         audit_retention_days = 0\n"
    );
    if let Some(threshold) = columnar_flush_threshold {
        toml.push_str(&format!(
            "\n[tuning.query]\ncolumnar_flush_threshold = {threshold}\n"
        ));
    }
    let path = dir.join("nodedb.toml");
    std::fs::write(&path, toml).expect("write test server config file");
    path
}
