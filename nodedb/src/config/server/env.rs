// SPDX-License-Identifier: BUSL-1.1

use std::net::{IpAddr, SocketAddr};

use super::ServerConfig;

/// Parse a human-readable memory size string into bytes.
///
/// Supported formats:
/// - `"512MiB"` / `"512M"` → mebibytes (base-1024)
/// - `"8GiB"` / `"8G"` → gibibytes (base-1024)
/// - `"1073741824"` → raw bytes (no suffix)
///
/// Matching is case-insensitive on the suffix.
pub fn parse_memory_size(s: &str) -> crate::Result<usize> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::Error::Config {
            detail: "empty string".to_string(),
        });
    }

    let split_pos = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());

    let (num_part, suffix) = s.split_at(split_pos);
    let suffix = suffix.trim();

    let cfg = |detail: String| crate::Error::Config { detail };

    let base: u64 = num_part
        .parse()
        .map_err(|_| cfg(format!("invalid number: {num_part}")))?;

    let bytes: u64 = match suffix.to_ascii_uppercase().as_str() {
        "" => base,
        "B" => base,
        "K" | "KB" | "KIB" => base
            .checked_mul(1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        "M" | "MB" | "MIB" => base
            .checked_mul(1024 * 1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        "G" | "GB" | "GIB" => base
            .checked_mul(1024 * 1024 * 1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        "T" | "TB" | "TIB" => base
            .checked_mul(1024 * 1024 * 1024 * 1024)
            .ok_or_else(|| cfg(format!("overflow parsing memory size: {s}")))?,
        other => {
            return Err(cfg(format!("unknown memory size suffix: '{other}'")));
        }
    };

    usize::try_from(bytes).map_err(|_| cfg(format!("memory size too large for this platform: {s}")))
}

/// Parse a u16 port from an env var into a required field.
fn apply_port_env(var: &str, target: &mut u16) {
    if let Ok(val) = std::env::var(var) {
        match val.trim().parse::<u16>() {
            Ok(port) => {
                tracing::info!(
                    env_var = var,
                    value = port,
                    "environment variable override applied"
                );
                *target = port;
            }
            Err(_) => {
                tracing::warn!(
                    env_var = var,
                    value = %val,
                    "ignoring malformed environment variable (expected port number), using config value"
                );
            }
        }
    }
}

/// Parse a strictly-positive integer from an env var into a required field.
///
/// Zero is rejected rather than applied: every knob routed through here is a
/// budget or a ceiling, and zero would mean "admit nothing", which is a
/// misconfiguration to warn about rather than a value to honour. One helper
/// keeps the info/warn wording identical across the knobs that share these
/// semantics instead of letting copy-pasted blocks drift.
fn apply_positive_env<T>(var: &str, target: &mut T)
where
    T: std::str::FromStr + PartialEq + From<u8> + std::fmt::Display + Copy,
{
    let Ok(val) = std::env::var(var) else {
        return;
    };
    match val.trim().parse::<T>() {
        Ok(parsed) if parsed != T::from(0) => {
            tracing::info!(
                env_var = var,
                value = %parsed,
                "environment variable override applied"
            );
            *target = parsed;
        }
        Ok(zero) => {
            tracing::warn!(
                env_var = var,
                value = %zero,
                "ignoring value of 0 (must be positive), using config value"
            );
        }
        Err(_) => {
            tracing::warn!(
                env_var = var,
                value = %val,
                "ignoring malformed environment variable (expected a positive integer), using config value"
            );
        }
    }
}

/// Parse a u16 port from an env var into an optional field (enables the listener).
fn apply_optional_port_env(var: &str, target: &mut Option<u16>) {
    if let Ok(val) = std::env::var(var) {
        match val.trim().parse::<u16>() {
            Ok(port) => {
                tracing::info!(
                    env_var = var,
                    value = port,
                    "environment variable override applied"
                );
                *target = Some(port);
            }
            Err(_) => {
                tracing::warn!(
                    env_var = var,
                    value = %val,
                    "ignoring malformed environment variable (expected port number), using config value"
                );
            }
        }
    }
}

/// Parse a boolean env var ("true"/"false") into a bool field.
fn apply_bool_env(var: &str, target: &mut bool) {
    if let Ok(val) = std::env::var(var) {
        match val.trim().to_lowercase().as_str() {
            "true" | "1" | "yes" => {
                tracing::info!(
                    env_var = var,
                    value = true,
                    "environment variable override applied"
                );
                *target = true;
            }
            "false" | "0" | "no" => {
                tracing::info!(
                    env_var = var,
                    value = false,
                    "environment variable override applied"
                );
                *target = false;
            }
            _ => {
                tracing::warn!(
                    env_var = var,
                    value = %val,
                    "ignoring malformed environment variable (expected true/false), using config value"
                );
            }
        }
    }
}

/// Apply environment variable overrides to a loaded `ServerConfig`.
///
/// Priority order: env var > TOML value > compiled default.
///
/// Handled variables:
/// - `NODEDB_HOST`             — overrides `config.host` (bind address, e.g., `0.0.0.0`)
/// - `NODEDB_PORT_NATIVE`      — overrides `config.ports.native` (default 6433)
/// - `NODEDB_PORT_PGWIRE`      — overrides `config.ports.pgwire` (default 6432)
/// - `NODEDB_PORT_HTTP`        — overrides `config.ports.http` (default 6480)
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
    // ── Host and ports ─────────────────────────────────────────────

    if let Ok(val) = std::env::var("NODEDB_HOST") {
        match val.trim().parse::<IpAddr>() {
            Ok(ip) => {
                tracing::info!(env_var = "NODEDB_HOST", value = %val, "environment variable override applied");
                config.server.host = ip;
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_HOST",
                    value = %val,
                    "ignoring malformed environment variable (expected IP address), using config value"
                );
            }
        }
    }

    apply_port_env("NODEDB_PORT_NATIVE", &mut config.server.ports.native);
    apply_port_env("NODEDB_PORT_PGWIRE", &mut config.server.ports.pgwire);
    apply_port_env("NODEDB_PORT_HTTP", &mut config.server.ports.http);
    apply_optional_port_env("NODEDB_PORT_RESP", &mut config.server.ports.resp);
    apply_optional_port_env("NODEDB_PORT_ILP", &mut config.server.ports.ilp);

    if let Ok(val) = std::env::var("NODEDB_DATA_DIR") {
        let path = std::path::PathBuf::from(&val);
        tracing::info!(
            env_var = "NODEDB_DATA_DIR",
            value = %val,
            "environment variable override applied"
        );
        config.server.data_dir = path;
    }

    if let Ok(val) = std::env::var("NODEDB_MEMORY_LIMIT") {
        match parse_memory_size(&val) {
            Ok(bytes) => {
                tracing::info!(
                    env_var = "NODEDB_MEMORY_LIMIT",
                    value = %val,
                    bytes,
                    "environment variable override applied"
                );
                config.server.memory_limit = bytes;
            }
            Err(e) => {
                tracing::warn!(
                    env_var = "NODEDB_MEMORY_LIMIT",
                    value = %val,
                    error = %e,
                    "ignoring malformed environment variable, using config value"
                );
            }
        }
    }

    if let Ok(val) = std::env::var("NODEDB_NODE_ID") {
        match val.trim().parse::<u64>() {
            Ok(node_id) => {
                if let Some(cluster) = config.cluster.as_mut() {
                    tracing::info!(
                        env_var = "NODEDB_NODE_ID",
                        value = node_id,
                        "environment variable override applied"
                    );
                    cluster.node_id = node_id;
                } else {
                    tracing::warn!(
                        env_var = "NODEDB_NODE_ID",
                        value = node_id,
                        "NODEDB_NODE_ID is set but no [cluster] section is present in config; \
                         ignoring (add a [cluster] section to enable cluster mode)"
                    );
                }
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_NODE_ID",
                    value = %val,
                    "ignoring malformed environment variable (expected u64), using config value"
                );
            }
        }
    }

    if let Ok(val) = std::env::var("NODEDB_SEED_NODES") {
        match parse_seed_nodes(&val) {
            Ok(addrs) => {
                if let Some(cluster) = config.cluster.as_mut() {
                    tracing::info!(
                        env_var = "NODEDB_SEED_NODES",
                        value = %val,
                        count = addrs.len(),
                        "environment variable override applied"
                    );
                    cluster.seed_nodes = addrs;
                } else {
                    tracing::warn!(
                        env_var = "NODEDB_SEED_NODES",
                        value = %val,
                        "NODEDB_SEED_NODES is set but no [cluster] section is present in config; \
                         ignoring (add a [cluster] section to enable cluster mode)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    env_var = "NODEDB_SEED_NODES",
                    value = %val,
                    error = %e,
                    "ignoring malformed environment variable, using config value"
                );
            }
        }
    }

    // ── Numeric settings ───────────────────────────────────────────

    if let Ok(val) = std::env::var("NODEDB_DATA_PLANE_CORES") {
        match val.trim().parse::<usize>() {
            Ok(cores) => {
                tracing::info!(
                    env_var = "NODEDB_DATA_PLANE_CORES",
                    value = cores,
                    "environment variable override applied"
                );
                config.server.data_plane_cores = cores;
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_DATA_PLANE_CORES",
                    value = %val,
                    "ignoring malformed environment variable (expected usize), using config value"
                );
            }
        }
    }

    if let Ok(val) = std::env::var("NODEDB_MAX_CONNECTIONS") {
        match val.trim().parse::<usize>() {
            Ok(n) => {
                tracing::info!(
                    env_var = "NODEDB_MAX_CONNECTIONS",
                    value = n,
                    "environment variable override applied"
                );
                config.server.max_connections = n;
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_MAX_CONNECTIONS",
                    value = %val,
                    "ignoring malformed environment variable (expected usize), using config value"
                );
            }
        }
    }

    if let Ok(val) = std::env::var("NODEDB_LOG_FORMAT") {
        let normalised = val.trim().to_lowercase();
        match normalised.as_str() {
            "text" => {
                tracing::info!(
                    env_var = "NODEDB_LOG_FORMAT",
                    value = "text",
                    "environment variable override applied"
                );
                config.server.log_format = super::LogFormat::Text;
            }
            "json" => {
                tracing::info!(
                    env_var = "NODEDB_LOG_FORMAT",
                    value = "json",
                    "environment variable override applied"
                );
                config.server.log_format = super::LogFormat::Json;
            }
            _ => {
                tracing::warn!(
                    env_var = "NODEDB_LOG_FORMAT",
                    value = %val,
                    "ignoring malformed environment variable (expected \"text\" or \"json\"), using config value"
                );
            }
        }
    }

    // ── Per-protocol TLS toggles ─────────────────────────────────

    if let Some(ref mut tls) = config.server.tls {
        apply_bool_env("NODEDB_TLS_NATIVE", &mut tls.native);
        apply_bool_env("NODEDB_TLS_PGWIRE", &mut tls.pgwire);
        apply_bool_env("NODEDB_TLS_HTTP", &mut tls.http);
        apply_bool_env("NODEDB_TLS_RESP", &mut tls.resp);
        apply_bool_env("NODEDB_TLS_ILP", &mut tls.ilp);
    }

    // ── WAL tuning ─────────────────────────────────────────────────

    if let Ok(val) = std::env::var("NODEDB_WAL_WRITE_BUFFER_SIZE") {
        match parse_memory_size(&val) {
            Ok(size) if size >= 64 * 1024 => {
                tracing::info!(
                    env_var = "NODEDB_WAL_WRITE_BUFFER_SIZE",
                    value = size,
                    "environment variable override applied"
                );
                config.tuning.wal.write_buffer_size = size;
            }
            Ok(size) => {
                tracing::warn!(
                    env_var = "NODEDB_WAL_WRITE_BUFFER_SIZE",
                    value = size,
                    "ignoring value below minimum 64KiB, using config value"
                );
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_WAL_WRITE_BUFFER_SIZE",
                    value = %val,
                    "ignoring malformed environment variable, using config value"
                );
            }
        }
    }

    // ── Checkpoint / WAL segment tuning ─────────────────────────────
    // Both drive the crash-recovery test harness: a short interval forces a
    // checkpoint cycle quickly, and a small segment target forces WAL
    // rotation so sealed segments exist for truncation to act on.

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

    // ── Timeseries memtable admission knobs ─────────────────────────
    // The three limits that decide whether a timeseries WAL record can be
    // taken into the memtable whole. They are env-reachable because the
    // record-boundary admission gate they drive is otherwise only observable
    // at 64/80 MiB and 100k distinct tags — a cost no test can pay, which is
    // how a mid-record flush stamping a partition with the WRONG WAL LSN went
    // unnoticed. Defaults are unchanged; this only makes them reachable.

    apply_positive_env(
        "NODEDB_TS_MEMTABLE_BUDGET_BYTES",
        &mut config.tuning.timeseries.memtable_budget_bytes,
    );
    apply_positive_env(
        "NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES",
        &mut config.tuning.timeseries.memtable_hard_limit_bytes,
    );
    apply_positive_env(
        "NODEDB_TS_MAX_TAG_CARDINALITY",
        &mut config.tuning.timeseries.max_tag_cardinality,
    );

    // ── Observability overrides (PromQL, OTLP) ─────────────────────

    super::observability::apply_observability_env(&mut config.observability);
}

/// Parse a comma-separated list of `SocketAddr` strings.
///
/// Returns `Ok(Vec<SocketAddr>)` if every entry parses successfully.
/// Returns `Err(bad_entry)` with the first entry that fails to parse,
/// so callers can log it and skip the entire override.
pub fn parse_seed_nodes(s: &str) -> crate::Result<Vec<SocketAddr>> {
    let mut addrs = Vec::new();
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        match entry.parse::<SocketAddr>() {
            Ok(addr) => addrs.push(addr),
            Err(_) => {
                return Err(crate::Error::Config {
                    detail: format!("invalid socket address: '{entry}'"),
                });
            }
        }
    }
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::super::ClusterSettings;
    use super::*;

    // ── parse_memory_size ───────────────────────────────────────────

    #[test]
    fn parse_raw_bytes() {
        assert_eq!(parse_memory_size("1073741824").unwrap(), 1_073_741_824);
        assert_eq!(parse_memory_size("0").unwrap(), 0);
        assert_eq!(parse_memory_size("1").unwrap(), 1);
    }

    #[test]
    fn parse_mib_suffix() {
        assert_eq!(parse_memory_size("512MiB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("512M").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("1MiB").unwrap(), 1024 * 1024);
    }

    #[test]
    fn parse_gib_suffix() {
        assert_eq!(parse_memory_size("8GiB").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("8G").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("8GB").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_kib_suffix() {
        assert_eq!(parse_memory_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_memory_size("64K").unwrap(), 64 * 1024);
        assert_eq!(parse_memory_size("64KB").unwrap(), 64 * 1024);
    }

    #[test]
    fn parse_bytes_suffix() {
        assert_eq!(parse_memory_size("100B").unwrap(), 100);
    }

    #[test]
    fn parse_case_insensitive() {
        assert_eq!(parse_memory_size("512mib").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("8gib").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("4g").unwrap(), 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_trims_whitespace() {
        assert_eq!(parse_memory_size("  512MiB  ").unwrap(), 512 * 1024 * 1024);
        assert_eq!(
            parse_memory_size("  8GiB  ").unwrap(),
            8 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn parse_unknown_suffix_is_error() {
        assert!(parse_memory_size("512X").is_err());
        assert!(parse_memory_size("8ZiB").is_err());
    }

    #[test]
    fn parse_empty_is_error() {
        assert!(parse_memory_size("").is_err());
        assert!(parse_memory_size("   ").is_err());
    }

    #[test]
    fn parse_non_numeric_is_error() {
        assert!(parse_memory_size("abc").is_err());
        assert!(parse_memory_size("GiB").is_err());
    }

    // ── apply_env_overrides ─────────────────────────────────────────

    fn make_cluster(node_id: u64) -> ClusterSettings {
        ClusterSettings {
            node_id,
            listen: "0.0.0.0:9400".parse().unwrap(),
            seed_nodes: vec!["127.0.0.1:9400".parse().unwrap()],
            num_groups: 4,
            replication_factor: 3,
            force_bootstrap: false,
            tls: None,
            max_active_sessions: 0,
            login_attempts_per_ip_per_min: 30,
            login_attempts_per_user_per_min: 10,
            insecure_transport: false,
            log_compaction_threshold: None,
        }
    }

    #[test]
    fn env_data_dir_override() {
        unsafe { std::env::set_var("NODEDB_DATA_DIR", "/tmp/test-nodedb") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert_eq!(
            cfg.server.data_dir,
            std::path::PathBuf::from("/tmp/test-nodedb")
        );
        unsafe { std::env::remove_var("NODEDB_DATA_DIR") };
    }

    /// Tests valid and malformed `NODEDB_MEMORY_LIMIT` sequentially to avoid
    /// env-var races (env vars are process-global, Rust tests run in parallel).
    #[test]
    fn env_memory_limit_overrides() {
        // ── Valid value → overrides memory_limit ──
        unsafe { std::env::set_var("NODEDB_MEMORY_LIMIT", "2GiB") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert_eq!(cfg.server.memory_limit, 2 * 1024 * 1024 * 1024);

        // ── Malformed value → memory_limit unchanged ──
        unsafe { std::env::set_var("NODEDB_MEMORY_LIMIT", "notanumber") };
        let mut cfg = ServerConfig::default();
        let before = cfg.server.memory_limit;
        apply_env_overrides(&mut cfg);
        assert_eq!(
            cfg.server.memory_limit, before,
            "malformed value must not change config"
        );

        unsafe { std::env::remove_var("NODEDB_MEMORY_LIMIT") };
    }

    #[test]
    fn env_cluster_overrides() {
        // Always start clean.
        unsafe {
            std::env::remove_var("NODEDB_NODE_ID");
            std::env::remove_var("NODEDB_SEED_NODES");
        }

        // ── NODEDB_NODE_ID: valid value with cluster present → overrides node_id ──

        unsafe { std::env::set_var("NODEDB_NODE_ID", "42") };
        let mut cfg = ServerConfig {
            cluster: Some(make_cluster(1)),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        assert_eq!(
            cfg.cluster.as_ref().unwrap().node_id,
            42,
            "NODEDB_NODE_ID=42 should override node_id"
        );
        unsafe { std::env::remove_var("NODEDB_NODE_ID") };

        // ── NODEDB_NODE_ID: cluster absent → config.cluster stays None ──

        unsafe { std::env::set_var("NODEDB_NODE_ID", "99") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert!(
            cfg.cluster.is_none(),
            "NODEDB_NODE_ID with no [cluster] section must not create cluster"
        );
        unsafe { std::env::remove_var("NODEDB_NODE_ID") };

        // ── NODEDB_NODE_ID: malformed value → node_id unchanged ──

        unsafe { std::env::set_var("NODEDB_NODE_ID", "not_a_number") };
        let mut cfg = ServerConfig {
            cluster: Some(make_cluster(7)),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        assert_eq!(
            cfg.cluster.as_ref().unwrap().node_id,
            7,
            "malformed NODEDB_NODE_ID must leave node_id unchanged"
        );
        unsafe { std::env::remove_var("NODEDB_NODE_ID") };

        // ── NODEDB_SEED_NODES: valid addresses with cluster present → overrides seed_nodes ──

        unsafe { std::env::set_var("NODEDB_SEED_NODES", "10.0.0.1:9400,10.0.0.2:9400") };
        let mut cfg = ServerConfig {
            cluster: Some(make_cluster(1)),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        let seeds = &cfg.cluster.as_ref().unwrap().seed_nodes;
        assert_eq!(seeds.len(), 2, "two seed addresses should be applied");
        assert_eq!(seeds[0].to_string(), "10.0.0.1:9400");
        assert_eq!(seeds[1].to_string(), "10.0.0.2:9400");
        unsafe { std::env::remove_var("NODEDB_SEED_NODES") };

        // ── NODEDB_SEED_NODES: malformed entry → seed_nodes unchanged (no partial apply) ──

        unsafe { std::env::set_var("NODEDB_SEED_NODES", "10.0.0.1:9400,garbage") };
        let existing_seed: SocketAddr = "192.168.1.1:9400".parse().unwrap();
        let mut cfg = ServerConfig {
            cluster: Some(ClusterSettings {
                seed_nodes: vec![existing_seed],
                ..make_cluster(1)
            }),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        let seeds = &cfg.cluster.as_ref().unwrap().seed_nodes;
        assert_eq!(
            seeds.len(),
            1,
            "malformed NODEDB_SEED_NODES must not partially apply"
        );
        assert_eq!(seeds[0], existing_seed);
        unsafe { std::env::remove_var("NODEDB_SEED_NODES") };
    }
}
