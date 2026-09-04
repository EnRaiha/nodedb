// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_DATA_PLANE_CORES` / `NODEDB_MAX_CONNECTIONS` / `NODEDB_LOG_FORMAT`
//! overrides.

use crate::config::server::{LogFormat, ServerConfig};

/// Parse a positive-or-nonzero usize env override, failing boot on malformed
/// input instead of silently keeping the compiled default (issue #277).
fn apply_positive_usize(var: &str, target: &mut usize, allow_zero: bool) -> crate::Result<()> {
    if let Ok(val) = std::env::var(var) {
        let parsed = val
            .trim()
            .parse::<usize>()
            .map_err(|_| crate::Error::Config {
                detail: format!("invalid value '{val}' for {var}: expected positive integer"),
            })?;
        if !allow_zero && parsed == 0 {
            return Err(crate::Error::Config {
                detail: format!("{var} must be greater than zero"),
            });
        }
        tracing::info!(
            env_var = var,
            value = parsed,
            "environment variable override applied"
        );
        *target = parsed;
    }
    Ok(())
}

pub(super) fn apply_numeric_settings(config: &mut ServerConfig) -> crate::Result<()> {
    apply_positive_usize(
        "NODEDB_DATA_PLANE_CORES",
        &mut config.server.data_plane_cores,
        false,
    )?;
    apply_positive_usize(
        "NODEDB_MAX_CONNECTIONS",
        &mut config.server.max_connections,
        false,
    )?;
    apply_log_format_override(config);
    Ok(())
}

/// NODEDB_LOG_FORMAT is a string-valued override. Malformed values stay a
/// warning (non-numeric path, out of C3 scope).
fn apply_log_format_override(config: &mut ServerConfig) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c3_malformed_env_errors_instead_of_silent_default() {
        let _env_guard = super::super::test_support::env_lock().lock().unwrap();
        unsafe { std::env::set_var("NODEDB_DATA_PLANE_CORES", "abc") };
        let mut cfg = ServerConfig::default();
        let err = apply_numeric_settings(&mut cfg).unwrap_err();
        assert!(
            err.to_string().contains("NODEDB_DATA_PLANE_CORES"),
            "C3: error must name the env var, got: {err}"
        );
        unsafe { std::env::remove_var("NODEDB_DATA_PLANE_CORES") };
    }

    #[test]
    fn c3_valid_env_still_overrides() {
        let _env_guard = super::super::test_support::env_lock().lock().unwrap();
        unsafe { std::env::set_var("NODEDB_DATA_PLANE_CORES", "8") };
        let mut cfg = ServerConfig::default();
        apply_numeric_settings(&mut cfg).unwrap();
        assert_eq!(cfg.server.data_plane_cores, 8);
        unsafe { std::env::remove_var("NODEDB_DATA_PLANE_CORES") };
    }

    #[test]
    fn c3_zero_value_rejected() {
        let _env_guard = super::super::test_support::env_lock().lock().unwrap();
        unsafe { std::env::set_var("NODEDB_DATA_PLANE_CORES", "0") };
        let mut cfg = ServerConfig::default();
        let err = apply_numeric_settings(&mut cfg).unwrap_err();
        assert!(
            err.to_string().contains("greater than zero"),
            "C3: zero cores must be rejected, got: {err}"
        );
        unsafe { std::env::remove_var("NODEDB_DATA_PLANE_CORES") };
    }
}
