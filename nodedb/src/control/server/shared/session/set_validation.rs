// SPDX-License-Identifier: BUSL-1.1

//! The one contract every protocol applies to `SET` and `SHOW`.
//!
//! pgwire and the native MessagePack protocol write into the same session
//! parameter bag. A name one of them refuses and the other stores gives a
//! client two different servers on one connection pair, and a value stored
//! without its grammar checked is read back later as "no setting" — the
//! silent-store class. The allowlist check and the per-parameter value
//! grammar therefore live here, and both protocols call them.
//!
//! Identity and security keys (`tenant`, `nodedb.tenant_id`, `role`,
//! `session_authorization`) are known names, so they pass the allowlist. Each
//! protocol claims them in its own dispatch branch before reaching this
//! module, because honoring them takes enforcement this module has no access
//! to.
//!
//! Each refusal carries the SQLSTATE the protocol reports: pgwire renders it
//! in an `ErrorInfo`, native in the error frame's `code` field.

use nodedb_types::error::sqlstate;

use super::params::{is_known_pg_runtime_parameter, is_known_settable_runtime_parameter};
use super::statement_timeout::{InvalidStatementTimeout, parse_statement_timeout};
use super::{cross_shard_mode, read_consistency};
use crate::control::server::shared::planning_overrides::parse_bool_session_value;

/// A `SET` or `SHOW` the session-parameter contract refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionParameterError {
    /// The name is not a runtime parameter this server carries.
    #[error("unrecognized configuration parameter \"{name}\"")]
    Unknown {
        /// The name the client sent, lowercased.
        name: String,
    },
    /// The name is known and the value is outside its grammar.
    #[error("invalid value for {name}: '{value}'. {expected}")]
    InvalidValue {
        /// The parameter being set.
        name: String,
        /// The value the client sent, verbatim.
        value: String,
        /// What the parameter accepts instead.
        expected: String,
    },
    /// `statement_timeout` carries its own parser and its own message.
    #[error(transparent)]
    StatementTimeout(#[from] InvalidStatementTimeout),
}

impl SessionParameterError {
    /// The SQLSTATE a protocol reports for this refusal.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Unknown { .. } => sqlstate::UNDEFINED_OBJECT,
            Self::InvalidValue { .. } | Self::StatementTimeout(_) => {
                sqlstate::INVALID_PARAMETER_VALUE
            }
        }
    }
}

/// Build an [`SessionParameterError::InvalidValue`] for `name`.
fn invalid(name: &str, value: &str, expected: &str) -> SessionParameterError {
    SessionParameterError::InvalidValue {
        name: name.to_string(),
        value: value.to_string(),
        expected: expected.to_string(),
    }
}

/// Check one `SET <name> = <value>` before it is stored.
///
/// An unknown name is refused rather than stored, and a known name with a
/// value its own grammar refuses is refused rather than stored as text the
/// reader later drops.
pub fn validate_set_parameter(name: &str, value: &str) -> Result<(), SessionParameterError> {
    if !is_known_settable_runtime_parameter(name) {
        return Err(SessionParameterError::Unknown {
            name: name.to_string(),
        });
    }
    validate_value(name, value)
}

/// Check one `RESET <name>`.
///
/// `RESET` restores the connection default of a parameter `SET` can write, so
/// it takes the same allowlist and carries no value to check.
pub fn validate_reset_parameter(name: &str) -> Result<(), SessionParameterError> {
    if is_known_settable_runtime_parameter(name) {
        return Ok(());
    }
    Err(SessionParameterError::Unknown {
        name: name.to_string(),
    })
}

/// Check one `SHOW <name>` that the session bag has no value for.
///
/// A name nothing set resolves to the empty setting only when the server
/// carries that parameter. Every other name is an unknown object, not a blank
/// row that a client reads as a real value.
pub fn validate_show_parameter(name: &str) -> Result<(), SessionParameterError> {
    if is_known_pg_runtime_parameter(name) {
        return Ok(());
    }
    Err(SessionParameterError::Unknown {
        name: name.to_string(),
    })
}

/// The value grammar of every parameter that has one.
///
/// A parameter absent from this match takes free text.
fn validate_value(name: &str, value: &str) -> Result<(), SessionParameterError> {
    let lowered = name.to_ascii_lowercase();
    match lowered.as_str() {
        "statement_timeout" => {
            // The value bounds every later statement on the session. Text the
            // dispatcher cannot read leaves the session believing it holds a
            // cap that nothing enforces.
            parse_statement_timeout(value)?;
        }
        "nodedb.consistency" => match value {
            "strong" | "eventual" => {}
            other if other.starts_with("bounded_staleness") => {}
            _ => {
                return Err(invalid(
                    &lowered,
                    value,
                    "Valid: strong, bounded_staleness(<ms>), eventual",
                ));
            }
        },
        read_consistency::PARAM_KEY => {
            if read_consistency::parse_value(value).is_none() {
                return Err(invalid(
                    &lowered,
                    value,
                    "Valid: strong, bounded_staleness:<secs>, eventual",
                ));
            }
        }
        cross_shard_mode::PARAM_KEY => {
            if cross_shard_mode::parse_value(value).is_none() {
                return Err(invalid(
                    &lowered,
                    value,
                    "Valid values: 'strict', 'best_effort_non_atomic'",
                ));
            }
        }
        "nodedb.force_shuffle_join" | "nodedb.force_shuffle_agg" => {
            if parse_bool_session_value(value).is_none() {
                return Err(invalid(
                    &lowered,
                    value,
                    "Valid: on, off, true, false, 1, 0",
                ));
            }
        }
        "nodedb.shuffle_num_parts" | "nodedb.shuffle_agg_num_parts" => {
            if value.parse::<u32>().is_err() {
                return Err(invalid(
                    &lowered,
                    value,
                    "Must be a non-negative integer (0 = cluster default)",
                ));
            }
        }
        "nodedb.broadcast_threshold_bytes" => {
            return require_usize(
                &lowered,
                value,
                "Must be a non-negative integer (bytes; 0 = always shuffle \
                 when both sides are analyzed)",
            );
        }
        "nodedb.shuffle_agg_threshold" => {
            return require_usize(
                &lowered,
                value,
                "Must be a non-negative integer (distinct-group count; the GROUP \
                 BY is auto-shuffled when its estimated group cardinality exceeds \
                 this value)",
            );
        }
        _ => {}
    }
    Ok(())
}

/// Refuse a value that does not parse as a `usize`, reporting `expected`.
fn require_usize(name: &str, value: &str, expected: &str) -> Result<(), SessionParameterError> {
    if value.parse::<usize>().is_err() {
        return Err(invalid(name, value, expected));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_parameter_is_refused() {
        let error = validate_set_parameter("not_a_parameter", "1")
            .expect_err("an unknown name must not be stored");
        assert_eq!(error.sqlstate(), "42704");
        assert!(
            error.to_string().contains("not_a_parameter"),
            "the message must name the parameter: {error}"
        );
    }

    #[test]
    fn a_known_parameter_with_a_valid_value_is_accepted() {
        assert!(validate_set_parameter("statement_timeout", "30s").is_ok());
        assert!(validate_set_parameter("application_name", "worker").is_ok());
        assert!(validate_set_parameter("nodedb.consistency", "eventual").is_ok());
        assert!(validate_set_parameter("cross_shard_txn", "strict").is_ok());
    }

    #[test]
    fn an_unparsable_statement_timeout_is_refused() {
        let error = validate_set_parameter("statement_timeout", "whenever")
            .expect_err("junk must not be stored");
        assert_eq!(error.sqlstate(), "22023");
        assert!(error.to_string().contains("whenever"), "{error}");
    }

    #[test]
    fn every_validated_knob_refuses_junk() {
        for name in [
            "nodedb.consistency",
            "default_read_consistency",
            "cross_shard_txn",
            "nodedb.force_shuffle_join",
            "nodedb.force_shuffle_agg",
            "nodedb.shuffle_num_parts",
            "nodedb.shuffle_agg_num_parts",
            "nodedb.broadcast_threshold_bytes",
            "nodedb.shuffle_agg_threshold",
        ] {
            match validate_set_parameter(name, "not-a-value") {
                Ok(()) => panic!("{name} must refuse junk"),
                Err(error) => assert_eq!(error.sqlstate(), "22023", "{name}"),
            }
        }
    }

    #[test]
    fn identity_keys_pass_the_allowlist_for_their_own_dispatch_branch() {
        // Each protocol claims these before validation. The allowlist must
        // still know them, or that branch never runs.
        for name in [
            "tenant",
            "nodedb.tenant_id",
            "role",
            "session_authorization",
        ] {
            assert!(
                validate_set_parameter(name, "value").is_ok(),
                "{name} must be a known name"
            );
        }
    }

    #[test]
    fn show_of_an_unknown_parameter_is_refused() {
        let error =
            validate_show_parameter("not_a_parameter").expect_err("an unknown name has no value");
        assert_eq!(error.sqlstate(), "42704");
    }

    #[test]
    fn show_of_a_read_only_server_parameter_resolves() {
        // `SHOW` reaches names `SET` refuses, so the two allowlists differ.
        assert!(validate_show_parameter("server_version").is_ok());
        assert!(validate_set_parameter("server_version", "99").is_err());
    }
}
