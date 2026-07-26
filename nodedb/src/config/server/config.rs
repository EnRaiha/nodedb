// SPDX-License-Identifier: BUSL-1.1

//! Root configuration for the NodeDB server.

use std::net::SocketAddr;
use std::path::PathBuf;

use nodedb_types::config::TuningConfig;
use serde::{Deserialize, Serialize};

use super::checkpoint::CheckpointSettings;
use super::cluster::ClusterSettings;
use super::cold_storage::ColdStorageSettings;
use super::observability::ObservabilityConfig;
use super::retention::RetentionSettings;
use super::scheduler::SchedulerConfig;
use super::section::ServerSection;
use super::snapshot_storage::{QuarantineStorageSettings, SnapshotStorageSettings};
use super::tls::{BackupEncryptionSettings, EncryptionSettings};
use crate::config::EngineConfig;

/// Root configuration for the NodeDB server.
///
/// On disk this is a TOML document with `[server]` for runtime fields and
/// independent subsystem tables (`[auth]`, `[tls]`, `[cluster]`, `[engines]`,
/// …) as siblings at the root. `deny_unknown_fields` rejects typos and
/// stray tables so misconfiguration surfaces at startup instead of being
/// silently ignored.
///
/// Example:
///
/// ```toml
/// [server]
/// host         = "0.0.0.0"
/// data_dir     = "/var/lib/nodedb"
/// memory_limit = "4GiB"
///
/// [server.ports]
/// pgwire = 6432
/// native = 6433
/// http   = 6480
/// sync   = 9090
///
/// [auth]
/// # ...
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Server-level runtime fields (bind address, ports, resource budgets,
    /// on-disk location, log format).
    #[serde(default)]
    pub server: ServerSection,

    /// Per-engine budget configuration. Lives under `[engines]`.
    #[serde(default)]
    pub engines: EngineConfig,

    /// Authentication and authorization configuration.
    #[serde(default)]
    pub auth: crate::config::AuthConfig,

    /// Encryption at rest configuration. If present, WAL payloads are encrypted.
    #[serde(default)]
    pub encryption: Option<EncryptionSettings>,

    /// Per-backup encryption configuration. If present, every backup envelope
    /// is encrypted with a per-backup DEK wrapped by this KEK. If absent, a
    /// warning is emitted once per process at the first backup operation.
    /// The key MUST differ from the WAL key; a matching path triggers a warning.
    #[serde(default)]
    pub backup_encryption: Option<BackupEncryptionSettings>,

    /// Checkpoint and WAL management settings.
    #[serde(default)]
    pub checkpoint: CheckpointSettings,

    /// Collection-lifecycle retention settings. Drives when the
    /// Event-Plane collection-GC sweeper hard-deletes a soft-deleted
    /// collection, and how often it evaluates candidates.
    #[serde(default)]
    pub retention: RetentionSettings,

    /// Cluster mode settings. When present, the node participates in a
    /// distributed cluster via Multi-Raft consensus over QUIC transport.
    /// When absent, runs in single-node mode (default).
    #[serde(default)]
    pub cluster: Option<ClusterSettings>,

    /// Cold storage (L2 tiering) configuration.
    /// When present, old L1 segments are promoted to S3-compatible cold storage.
    #[serde(default)]
    pub cold_storage: Option<ColdStorageSettings>,

    /// Snapshot storage configuration.
    /// Controls where warm-tier snapshots are persisted. When absent, defaults
    /// to local filesystem at `{data_dir}/snapshots`.
    #[serde(default)]
    pub snapshot_storage: Option<SnapshotStorageSettings>,

    /// Quarantine storage configuration.
    /// Controls where corrupt-segment archives are stored. When absent, defaults
    /// to local filesystem at `{data_dir}/quarantine`.
    #[serde(default)]
    pub quarantine_storage: Option<QuarantineStorageSettings>,

    /// Performance tuning knobs for engines, query execution, WAL, bridge,
    /// network, and cluster transport. All fields have sensible defaults;
    /// override selectively via the `[tuning]` TOML section.
    #[serde(default)]
    pub tuning: TuningConfig,

    /// Observability integrations: PromQL, OTLP receiver/export.
    /// All capabilities are always compiled in; toggled at runtime via this config.
    #[serde(default)]
    pub observability: ObservabilityConfig,

    /// Cron scheduler settings (timezone offset, future tuning knobs).
    #[serde(default)]
    pub scheduler: SchedulerConfig,
}

impl ServerConfig {
    /// Load configuration from a TOML file, falling back to defaults.
    pub fn from_file(path: &std::path::Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| crate::Error::Config {
            detail: format!("failed to read config file {}: {e}", path.display()),
        })?;
        let parsed: Self = toml::from_str(&content).map_err(|e| crate::Error::Config {
            detail: format!("invalid TOML config: {e}"),
        })?;
        parsed.validate()?;
        Ok(parsed)
    }

    /// Validate cross-field invariants that serde cannot express. Called
    /// from [`Self::from_file`] so misconfiguration fails startup.
    pub fn validate(&self) -> crate::Result<()> {
        if let Some(ref jwt) = self.auth.jwt {
            jwt.validate()?;
        }
        Ok(())
    }

    /// Build a `SocketAddr` from the shared host and a port.
    pub fn addr(&self, port: u16) -> SocketAddr {
        SocketAddr::new(self.server.host, port)
    }

    /// Native protocol listen address.
    pub fn native_addr(&self) -> SocketAddr {
        self.addr(self.server.ports.native)
    }

    /// pgwire listen address.
    pub fn pgwire_addr(&self) -> SocketAddr {
        self.addr(self.server.ports.pgwire)
    }

    /// HTTP API listen address.
    pub fn http_addr(&self) -> SocketAddr {
        self.addr(self.server.ports.http)
    }

    /// Sync WebSocket listen address (NodeDB-Lite clients).
    pub fn sync_addr(&self) -> SocketAddr {
        self.addr(self.server.ports.sync)
    }

    /// RESP listen address (None if disabled).
    pub fn resp_addr(&self) -> Option<SocketAddr> {
        self.server.ports.resp.map(|p| self.addr(p))
    }

    /// ILP listen address (None if disabled).
    pub fn ilp_addr(&self) -> Option<SocketAddr> {
        self.server.ports.ilp.map(|p| self.addr(p))
    }

    /// WAL directory within the data directory.
    pub fn wal_dir(&self) -> PathBuf {
        self.server.data_dir.join("wal")
    }

    /// Segments directory within the data directory.
    pub fn segments_dir(&self) -> PathBuf {
        self.server.data_dir.join("segments")
    }

    /// System catalog (auth, roles, tenants) redb file.
    pub fn catalog_path(&self) -> PathBuf {
        self.server.data_dir.join("system.redb")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::server::log_format::LogFormat;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn default_config_valid() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.server.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(cfg.server.ports.native, 6433);
        assert_eq!(cfg.server.ports.pgwire, 6432);
        assert_eq!(cfg.server.ports.http, 6480);
        assert_eq!(cfg.server.ports.sync, 9090);
        assert!(cfg.server.ports.resp.is_none());
        assert!(cfg.server.ports.ilp.is_none());
        assert!(cfg.server.data_plane_cores >= 1);
        assert_eq!(cfg.server.memory_limit, 1024 * 1024 * 1024);
    }

    #[test]
    fn config_roundtrip() {
        let cfg = ServerConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).expect("serialize");
        let _parsed: ServerConfig = toml::from_str(&toml_str).expect("deserialize");
    }

    #[test]
    fn log_format_default_is_text() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.server.log_format, LogFormat::Text);
    }

    fn config_toml_with_log_format(value: &str) -> String {
        format!("[server]\nlog_format = {value}\n")
    }

    #[test]
    fn log_format_toml_text_parses() {
        let raw = config_toml_with_log_format("\"text\"");
        let cfg: ServerConfig = toml::from_str(&raw).expect("deserialize");
        assert_eq!(cfg.server.log_format, LogFormat::Text);
    }

    #[test]
    fn log_format_toml_json_parses() {
        let raw = config_toml_with_log_format("\"json\"");
        let cfg: ServerConfig = toml::from_str(&raw).expect("deserialize");
        assert_eq!(cfg.server.log_format, LogFormat::Json);
    }

    #[test]
    fn log_format_toml_unknown_rejected() {
        let raw = config_toml_with_log_format("\"yaml\"");
        let result: Result<ServerConfig, _> = toml::from_str(&raw);
        assert!(result.is_err(), "unknown log_format value must be rejected");
    }

    #[test]
    fn unknown_top_level_table_rejected() {
        // The misplaced `[server_typo]` table must surface, not be silently ignored.
        let raw = "[server]\n\n[server_typo]\nfoo = 1\n";
        let err = toml::from_str::<ServerConfig>(raw).unwrap_err().to_string();
        assert!(
            err.contains("unknown field") || err.contains("server_typo"),
            "unexpected error: {err}"
        );
    }
}
