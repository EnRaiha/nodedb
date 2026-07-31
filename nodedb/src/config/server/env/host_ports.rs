// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_HOST` / `NODEDB_PORT_*` / `NODEDB_DATA_DIR` / `NODEDB_MEMORY_LIMIT`
//! overrides — the bind address and per-protocol listener configuration.

use std::net::IpAddr;

use super::helpers::{apply_optional_port_env, apply_port_env};
use super::memory_size::parse_memory_size;
use crate::config::server::ServerConfig;

pub(super) fn apply_host_and_ports(config: &mut ServerConfig) {
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
    apply_port_env("NODEDB_PORT_SYNC", &mut config.server.ports.sync);
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
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::apply_env_overrides;
    use super::*;

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

    /// Tests valid and malformed `NODEDB_PORT_SYNC` sequentially to avoid
    /// env-var races (env vars are process-global, Rust tests run in parallel).
    #[test]
    fn env_sync_port_overrides() {
        // ── Valid value → overrides ports.sync ──
        unsafe { std::env::set_var("NODEDB_PORT_SYNC", "19090") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert_eq!(cfg.server.ports.sync, 19090);

        // ── Malformed value → ports.sync unchanged ──
        unsafe { std::env::set_var("NODEDB_PORT_SYNC", "notaport") };
        let mut cfg = ServerConfig::default();
        let before = cfg.server.ports.sync;
        apply_env_overrides(&mut cfg);
        assert_eq!(
            cfg.server.ports.sync, before,
            "malformed value must not change config"
        );

        unsafe { std::env::remove_var("NODEDB_PORT_SYNC") };
    }
}
