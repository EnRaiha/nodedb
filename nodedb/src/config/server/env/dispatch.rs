// SPDX-License-Identifier: BUSL-1.1

use super::checkpoint::apply_checkpoint_tuning;
use super::cluster::apply_cluster_overrides;
use super::host_ports::apply_host_and_ports;
use super::numeric::apply_numeric_settings;
use super::timeseries::apply_timeseries_overrides;
use super::tls::apply_tls_overrides;
use super::wal::apply_wal_tuning;
use crate::config::server::ServerConfig;

/// Apply environment variable overrides to a loaded `ServerConfig`.
///
/// Priority order: env var > TOML value > compiled default.
///
/// Handled variables:
/// - `NODEDB_HOST`             — overrides `config.host` (bind address, e.g., `0.0.0.0`)
/// - `NODEDB_PORT_NATIVE`      — overrides `config.ports.native` (default 6433)
/// - `NODEDB_PORT_PGWIRE`      — overrides `config.ports.pgwire` (default 6432)
/// - `NODEDB_PORT_HTTP`        — overrides `config.ports.http` (default 6480)
/// - `NODEDB_PORT_SYNC`        — overrides `config.ports.sync` (default 9090)
/// - `NODEDB_PORT_RESP`        — overrides `config.ports.resp` (set to enable RESP)
/// - `NODEDB_PORT_ILP`         — overrides `config.ports.ilp` (set to enable ILP)
/// - `NODEDB_DATA_DIR`         — overrides `config.data_dir`
/// - `NODEDB_MEMORY_LIMIT`     — overrides `config.memory_limit`
/// - `NODEDB_DATA_PLANE_CORES` — overrides `config.data_plane_cores` (parse as usize)
/// - `NODEDB_MAX_CONNECTIONS`  — overrides `config.max_connections` (parse as usize)
/// - `NODEDB_LOG_FORMAT`       — overrides `config.log_format` ("text" or "json")
/// - `NODEDB_TLS_NATIVE`      — enable/disable TLS on native protocol ("true"/"false")
/// - `NODEDB_TLS_PGWIRE`      — enable/disable TLS on pgwire ("true"/"false")
/// - `NODEDB_TLS_HTTP`        — enable/disable TLS on HTTP ("true"/"false")
/// - `NODEDB_TLS_RESP`        — enable/disable TLS on RESP ("true"/"false")
/// - `NODEDB_TLS_ILP`         — enable/disable TLS on ILP ("true"/"false")
/// - `NODEDB_NODE_ID`          — overrides `config.cluster.node_id` (parse as u64)
/// - `NODEDB_SEED_NODES`       — overrides `config.cluster.seed_nodes`
///   (comma-separated `SocketAddr` list)
/// - `NODEDB_CHECKPOINT_INTERVAL_SECS` — overrides `config.checkpoint.interval_secs`
///   (parse as u64 seconds; 0 is rejected)
/// - `NODEDB_WAL_SEGMENT_TARGET_MB`    — overrides `config.checkpoint.wal_segment_target_mb`
///   (parse as u64 MiB; 0 is rejected)
/// - `NODEDB_WAL_DIRECT_IO`            — overrides `config.tuning.wal.direct_io`
///   ("true"/"false"; default true)
/// - `NODEDB_TS_MEMTABLE_BUDGET_BYTES` — overrides
///   `config.tuning.timeseries.memtable_budget_bytes` (parse as usize; 0 is rejected)
/// - `NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES` — overrides
///   `config.tuning.timeseries.memtable_hard_limit_bytes` (parse as usize; 0 is rejected)
/// - `NODEDB_TS_MAX_TAG_CARDINALITY`   — overrides
///   `config.tuning.timeseries.max_tag_cardinality` (parse as u32; 0 is rejected)
///
/// `NODEDB_CONFIG` (config file path) is handled upstream in `main.rs`
/// before this function is called, so it is not processed here.
///
/// `NODEDB_SUPERUSER_PASSWORD` is intentionally absent from this list. It is
/// handled separately by `crate::config::auth::AuthConfig::resolve_superuser_password()`
/// (called from `main.rs`) so that the value is never passed through logging
/// code paths or stored in `ServerConfig` where it could appear in debug output.
pub fn apply_env_overrides(config: &mut ServerConfig) {
    apply_host_and_ports(config);
    apply_cluster_overrides(config);
    apply_numeric_settings(config);
    apply_tls_overrides(config);
    apply_wal_tuning(config);
    apply_checkpoint_tuning(config);
    apply_timeseries_overrides(config);
    super::super::observability::apply_observability_env(&mut config.observability);
}
