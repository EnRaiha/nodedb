// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_DATA_PLANE_CORES` / `NODEDB_MAX_CONNECTIONS` / `NODEDB_LOG_FORMAT`
//! overrides.

use crate::config::server::{LogFormat, ServerConfig};

pub(super) fn apply_numeric_settings(config: &mut ServerConfig) {
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
                config.server.log_format = LogFormat::Text;
            }
            "json" => {
                tracing::info!(
                    env_var = "NODEDB_LOG_FORMAT",
                    value = "json",
                    "environment variable override applied"
                );
                config.server.log_format = LogFormat::Json;
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
}
