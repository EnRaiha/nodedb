// SPDX-License-Identifier: BUSL-1.1

//! `TestServer` with a backup root: `[backup_storage] local_root` set, so
//! `BACKUP DATABASE ... TO 'file://...'`, `RESTORE DATABASE` and scheduled
//! backups write and read inside the server's data directory.

use std::path::PathBuf;

use super::start::StartConfig;
use super::types::TestServer;

/// The backup root, relative to the server's data directory.
pub(super) const BACKUP_DIR: &str = "backups";

impl TestServer {
    /// Spawn a single-core server whose `file://` backup URIs resolve inside
    /// [`Self::backup_root`]. All other settings stay at their defaults.
    pub async fn start_with_backup_root() -> Self {
        Self::start_with_config(StartConfig {
            backup_root: true,
            ..Default::default()
        })
        .await
    }

    /// [`Self::start_with_backup_root`] with usage metering replaced by
    /// `metering`, so a backup or restore runs quota admission and charges.
    pub async fn start_with_metering_and_backup_root(
        metering: nodedb::control::security::metering::config::MeteringConfig,
    ) -> Self {
        Self::start_with_config(StartConfig {
            backup_root: true,
            metering: Some(metering),
            ..Default::default()
        })
        .await
    }

    /// The server's `[backup_storage] local_root`, canonical so it matches
    /// the path in every `file://` URI the server accepts.
    pub fn backup_root(&self) -> PathBuf {
        let root = self._dir.path().join(BACKUP_DIR);
        root.canonicalize().unwrap_or(root)
    }

    /// A `file://` URI of `name` inside [`Self::backup_root`].
    pub fn backup_uri(&self, name: &str) -> String {
        format!("file://{}/{name}", self.backup_root().display())
    }
}
