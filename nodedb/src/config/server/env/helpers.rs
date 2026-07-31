// SPDX-License-Identifier: BUSL-1.1

//! Generic single-variable env parsing helpers shared across the override
//! sections. Each captures one recurring shape (port, positive integer,
//! optional port, bool) so the info/warn wording stays identical across the
//! knobs that share it instead of letting copy-pasted blocks drift.

/// Parse a u16 port from an env var into a required field.
pub(super) fn apply_port_env(var: &str, target: &mut u16) {
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
/// misconfiguration to warn about rather than a value to honour.
pub(super) fn apply_positive_env<T>(var: &str, target: &mut T)
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
pub(super) fn apply_optional_port_env(var: &str, target: &mut Option<u16>) {
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
pub(super) fn apply_bool_env(var: &str, target: &mut bool) {
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
