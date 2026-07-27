// SPDX-License-Identifier: BUSL-1.1

//! Session parameter commands: SET, SHOW, SHOW ALL, EXPLAIN.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::planning_overrides::parse_bool_session_value;
use crate::control::server::shared::session::SessionId;

use super::super::types::sqlstate_error;
use super::core::NodeDbPgHandler;

/// Outcome of classifying a `SET TRANSACTION` / `SET SESSION CHARACTERISTICS` command.
enum TransactionCmd {
    /// `READ ONLY` — store access mode, return SET.
    SetReadOnly,
    /// `READ WRITE` — store access mode, return SET.
    SetReadWrite,
    /// `ISOLATION LEVEL READ COMMITTED` — silent accept (Snapshot Isolation is strictly stronger).
    AcceptIsolation,
    /// Any unsupported isolation level or unknown option — reject with SQLSTATE 0A000.
    RejectIsolation(String),
}

/// Classify a `SET TRANSACTION` or `SET SESSION CHARACTERISTICS` SQL statement.
///
/// `upper` must be `sql.to_uppercase()`. `sql` is the original, used for error messages.
fn classify_transaction_cmd(upper: &str, sql: &str) -> TransactionCmd {
    // Isolation-level branch: check before READ ONLY/READ WRITE so that a statement
    // like "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED" does not accidentally
    // match the READ-only access-mode branch.
    if upper.contains("ISOLATION LEVEL") {
        // READ COMMITTED: silent accept.
        if upper.contains("READ COMMITTED") {
            return TransactionCmd::AcceptIsolation;
        }

        let level = if upper.contains("SERIALIZABLE") {
            Some("SERIALIZABLE")
        } else if upper.contains("REPEATABLE READ") {
            Some("REPEATABLE READ")
        } else if upper.contains("READ UNCOMMITTED") {
            Some("READ UNCOMMITTED")
        } else {
            None
        };

        let message = match level {
            Some(lvl) => format!(
                "SET TRANSACTION ISOLATION LEVEL {lvl} is not supported; \
                 NodeDB enforces Snapshot Isolation"
            ),
            None => format!(
                "unsupported SET TRANSACTION option: {}",
                sql.split_whitespace().skip(2).collect::<Vec<_>>().join(" ")
            ),
        };
        return TransactionCmd::RejectIsolation(message);
    }

    // Access-mode branch.
    if upper.contains("READ ONLY") {
        return TransactionCmd::SetReadOnly;
    }
    if upper.contains("READ WRITE") {
        return TransactionCmd::SetReadWrite;
    }

    // Unknown option.
    TransactionCmd::RejectIsolation(format!(
        "unsupported SET TRANSACTION option: {}",
        sql.split_whitespace().skip(2).collect::<Vec<_>>().join(" ")
    ))
}

impl NodeDbPgHandler {
    /// Handle SET commands: parse, validate, store in session.
    pub(super) fn handle_set(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use crate::control::server::shared::session::parse_set_command;
        use pgwire::api::results::Tag;

        // Handle SET TRANSACTION ... and SET SESSION CHARACTERISTICS AS TRANSACTION ...
        let upper = sql.to_uppercase();
        if upper.starts_with("SET TRANSACTION") || upper.starts_with("SET SESSION CHARACTERISTICS")
        {
            match classify_transaction_cmd(&upper, sql) {
                TransactionCmd::SetReadOnly => {
                    self.sessions.set_parameter(
                        session_id,
                        "transaction_access_mode".into(),
                        "read_only".into(),
                    );
                    return Ok(vec![Response::Execution(Tag::new("SET"))]);
                }
                TransactionCmd::SetReadWrite => {
                    self.sessions.set_parameter(
                        session_id,
                        "transaction_access_mode".into(),
                        "read_write".into(),
                    );
                    return Ok(vec![Response::Execution(Tag::new("SET"))]);
                }
                TransactionCmd::AcceptIsolation => {
                    return Ok(vec![Response::Execution(Tag::new("SET"))]);
                }
                TransactionCmd::RejectIsolation(message) => {
                    return Err(sqlstate_error(
                        nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                        &message,
                    ));
                }
            }
        }

        // `SET ROLE <name>` and `SET SESSION AUTHORIZATION '<name>'` use
        // PostgreSQL's space-not-equals syntax, so `parse_set_command` (which
        // splits on `=` / `TO`) returns `None` for them. Catch the keywords
        // before falling through to that parser — both must reject explicitly
        // rather than land on the silent success path (the root cause behind
        // SET TENANT looking like a no-op).
        if upper.starts_with("SET ROLE ") || upper == "SET ROLE" {
            return Err(sqlstate_error(
                nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                "SET ROLE is not supported: a session's role set is identity-bound \
                 at CREATE USER time. Use GRANT/REVOKE ROLE TO <user> to change \
                 a user's roles, or reconnect with a different user.",
            ));
        }
        if upper.starts_with("SET SESSION AUTHORIZATION") {
            return Err(sqlstate_error(
                nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                "SET SESSION AUTHORIZATION is not supported: identity is bound at \
                 connection time. Reconnect as the target user.",
            ));
        }

        let (key, value) = match parse_set_command(sql) {
            Some(kv) => kv,
            None => {
                // Statements that look like `SET <something>` but don't match
                // any of the recognized shapes (k=v, k TO v, TRANSACTION,
                // ROLE, SESSION AUTHORIZATION) must NOT silently succeed.
                // Silent success on unparsed SET is exactly the bug class
                // that allowed `SET TENANT = 'x'` to look like a no-op.
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("syntax error in SET command: {sql}"),
                ))));
            }
        };

        // Identity / security context keys are dispatched before the
        // generic store-in-session path. Storing them in the parameter bag
        // without an enforcement contract is the silent-no-op class — every
        // such key must either be honored end-to-end or rejected explicitly.
        match key.as_str() {
            "tenant" => {
                return self.handle_set_tenant_name_or_id(identity, session_id, &value);
            }
            "nodedb.tenant_id" => {
                return self.handle_set_tenant_by_id(identity, session_id, &value);
            }
            "role" => {
                return Err(sqlstate_error(
                    nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                    "SET ROLE is not supported: a session's role set is identity-bound \
                     at CREATE USER time. Use GRANT/REVOKE ROLE TO <user> to change \
                     a user's roles, or reconnect with a different user.",
                ));
            }
            "session_authorization" => {
                return Err(sqlstate_error(
                    nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                    "SET SESSION AUTHORIZATION is not supported: identity is bound at \
                     connection time. Reconnect as the target user.",
                ));
            }
            _ => {}
        }

        if key == "nodedb.consistency" {
            match value.as_str() {
                "strong" | "eventual" => {}
                s if s.starts_with("bounded_staleness") => {}
                _ => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22023".to_owned(),
                        format!(
                            "invalid value for nodedb.consistency: '{value}'. Valid: strong, bounded_staleness(<ms>), eventual"
                        ),
                    ))));
                }
            }
        }

        if key == crate::control::server::shared::session::read_consistency::PARAM_KEY
            && crate::control::server::shared::session::read_consistency::parse_value(&value)
                .is_none()
        {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for {}: '{value}'. Valid: strong, bounded_staleness:<secs>, eventual",
                    crate::control::server::shared::session::read_consistency::PARAM_KEY
                ),
            ))));
        }

        if key == crate::control::server::shared::session::cross_shard_mode::PARAM_KEY
            && crate::control::server::shared::session::cross_shard_mode::parse_value(&value)
                .is_none()
        {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for {}: '{value}'. Valid values: 'strict', 'best_effort_non_atomic'",
                    crate::control::server::shared::session::cross_shard_mode::PARAM_KEY
                ),
            ))));
        }

        // Validate the distributed shuffle-join override knobs eagerly so a bad
        // value is rejected at SET time, not silently stored. `force_shuffle_join`
        // is a boolean (`on`/`off`/`true`/`false`/`1`/`0`); `shuffle_num_parts`
        // is a non-negative integer (`0` = let the planner default to the
        // cluster data-node count).
        if key == "nodedb.force_shuffle_join" && parse_bool_session_value(&value).is_none() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for nodedb.force_shuffle_join: '{value}'. \
                     Valid: on, off, true, false, 1, 0"
                ),
            ))));
        }
        if key == "nodedb.shuffle_num_parts" && value.parse::<u32>().is_err() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for nodedb.shuffle_num_parts: '{value}'. \
                     Must be a non-negative integer (0 = cluster default)"
                ),
            ))));
        }
        // Validate the distributed shuffle-aggregate override knobs eagerly so a
        // bad value is rejected at SET time, not silently stored.
        // `force_shuffle_agg` is a boolean; `shuffle_agg_num_parts` is a
        // non-negative integer (`0` = let the planner default to the cluster
        // data-node count). Mirrors the force_shuffle_join knobs above.
        if key == "nodedb.force_shuffle_agg" && parse_bool_session_value(&value).is_none() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for nodedb.force_shuffle_agg: '{value}'. \
                     Valid: on, off, true, false, 1, 0"
                ),
            ))));
        }
        if key == "nodedb.shuffle_agg_num_parts" && value.parse::<u32>().is_err() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for nodedb.shuffle_agg_num_parts: '{value}'. \
                     Must be a non-negative integer (0 = cluster default)"
                ),
            ))));
        }
        if key == "nodedb.broadcast_threshold_bytes" && value.parse::<usize>().is_err() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for nodedb.broadcast_threshold_bytes: '{value}'. \
                     Must be a non-negative integer (bytes; 0 = always shuffle \
                     when both sides are analyzed)"
                ),
            ))));
        }
        if key == "nodedb.shuffle_agg_threshold" && value.parse::<usize>().is_err() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for nodedb.shuffle_agg_threshold: '{value}'. \
                     Must be a non-negative integer (distinct-group count; the GROUP \
                     BY is auto-shuffled when its estimated group cardinality exceeds \
                     this value)"
                ),
            ))));
        }

        // Eager validation for `nodedb.auth_session`: drive the resolve path
        // now so rate-limit / audit / fingerprint checks fire on each SET
        // rather than being deferred to the next query. A probing client
        // that hammers SET LOCAL with bogus handles and never runs a query
        // must still be throttled and observed.
        if key == "nodedb.auth_session" {
            use crate::control::security::session_handle::{ClientFingerprint, ResolveOutcome};
            let peer_addr = match session_id {
                SessionId::Connection(connection_id) => self
                    .sessions
                    .connection_metadata(connection_id)
                    .map(|metadata| metadata.peer_addr),
                SessionId::LegacySocket(peer_addr) => Some(peer_addr),
            }
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "XX000".to_owned(),
                    "connection metadata is missing".to_owned(),
                )))
            })?;
            let caller_fp = ClientFingerprint::from_peer(identity.tenant_id, &peer_addr);
            let conn_key = match session_id {
                SessionId::Connection(connection_id) => connection_id.to_string(),
                SessionId::LegacySocket(peer_addr) => peer_addr.to_string(),
            };
            match self
                .state
                .session_handles
                .resolve(&value, &conn_key, &caller_fp)
            {
                ResolveOutcome::RateLimited => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "53300".to_owned(),
                        "session handle resolve rate limit exceeded on this \
                         connection — closing"
                            .to_owned(),
                    ))));
                }
                ResolveOutcome::Resolved(_) | ResolveOutcome::Miss => {
                    // Store the raw value either way — Miss might be a
                    // handle that was valid previously and expired; the
                    // next query's resolve will fall back to base identity.
                }
            }
        }

        // Any key that reaches this point must be a known runtime parameter.
        // Mirroring the `SHOW` side (params.rs `is_known_pg_runtime_parameter`),
        // unknown keys return `42704 undefined_object` instead of being
        // silently stored — silent storage is the class of bug that allowed
        // `SET TENANT` to look successful while routing nothing.
        if !crate::control::server::shared::session::is_known_settable_runtime_parameter(&key) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!("unrecognized configuration parameter \"{key}\""),
            ))));
        }

        self.sessions.set_parameter(session_id, key, value);
        Ok(vec![Response::Execution(Tag::new("SET"))])
    }
}

#[cfg(test)]
mod tests {
    use super::{TransactionCmd, classify_transaction_cmd};

    /// tenant_id values above u32::MAX must parse without error via u64.
    #[test]
    fn tenant_id_above_u32_max_parses_as_u64() {
        let big = "4294967296"; // u32::MAX + 1
        assert!(big.parse::<u64>().is_ok(), "should parse as u64");
        assert!(big.parse::<u32>().is_err(), "should NOT parse as u32");
    }

    fn run(sql: &str) -> TransactionCmd {
        let upper = sql.to_uppercase();
        classify_transaction_cmd(&upper, sql)
    }

    fn is_accept(cmd: TransactionCmd) -> bool {
        matches!(
            cmd,
            TransactionCmd::SetReadOnly
                | TransactionCmd::SetReadWrite
                | TransactionCmd::AcceptIsolation
        )
    }

    fn rejection_code(cmd: TransactionCmd) -> Option<String> {
        match cmd {
            TransactionCmd::RejectIsolation(msg) => Some(msg),
            _ => None,
        }
    }

    #[test]
    fn set_transaction_read_only() {
        assert!(is_accept(run("SET TRANSACTION READ ONLY")));
        assert!(matches!(
            run("SET TRANSACTION READ ONLY"),
            TransactionCmd::SetReadOnly
        ));
    }

    #[test]
    fn set_transaction_read_write() {
        assert!(matches!(
            run("SET TRANSACTION READ WRITE"),
            TransactionCmd::SetReadWrite
        ));
    }

    #[test]
    fn set_transaction_read_committed() {
        assert!(matches!(
            run("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            TransactionCmd::AcceptIsolation
        ));
    }

    #[test]
    fn set_transaction_serializable() {
        let msg = rejection_code(run("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
            .expect("expected rejection");
        assert!(
            msg.contains("SERIALIZABLE"),
            "message should name the level: {msg}"
        );
        assert!(
            msg.contains("Snapshot Isolation"),
            "message should mention Snapshot Isolation: {msg}"
        );
    }

    #[test]
    fn set_transaction_repeatable_read() {
        let msg = rejection_code(run("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            .expect("expected rejection");
        assert!(msg.contains("REPEATABLE READ"), "{msg}");
    }

    #[test]
    fn set_transaction_read_uncommitted() {
        let msg = rejection_code(run("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
            .expect("expected rejection");
        assert!(msg.contains("READ UNCOMMITTED"), "{msg}");
    }

    #[test]
    fn set_session_characteristics_serializable() {
        let sql = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        let msg = rejection_code(run(sql)).expect("expected rejection");
        assert!(msg.contains("SERIALIZABLE"), "{msg}");
    }

    #[test]
    fn set_transaction_unknown_option() {
        let msg = rejection_code(run("SET TRANSACTION DEFERRABLE"))
            .expect("expected rejection for unknown option");
        assert!(
            msg.contains("unsupported"),
            "message should say unsupported: {msg}"
        );
    }
}
