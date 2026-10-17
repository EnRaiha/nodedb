// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral audit-log SHOW commands: `SHOW AUDIT LOG`,
//! `SHOW AUDIT WHERE`, `SHOW AUDIT IN DATABASE`, and `EXPORT AUDIT`.
//!
//! Every SHOW command reads through [`SharedState::recent_audit_entries`].
//! That reader merges the in-memory buffer with the catalog.
//! A SHOW therefore returns every entry recorded before it started.
//! Rows are most-recent-first. The result is the protocol-neutral
//! `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::audit::AuditEntry;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Default row count when a SHOW AUDIT command has no `LIMIT`.
const DEFAULT_LIMIT: usize = 100;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// The SQLSTATE and message reach the client unchanged.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Reject a caller that is not a superuser.
fn require_superuser(identity: &AuthenticatedIdentity) -> Result<(), DdlError> {
    if identity.is_superuser {
        Ok(())
    } else {
        Err(ddl_err(
            "42501",
            "permission denied: only superuser can view audit log",
        ))
    }
}

/// Shared column names + types for all audit SHOW commands.
fn audit_columns() -> (Vec<String>, Vec<DdlColType>) {
    (
        vec![
            "seq".to_string(),
            "timestamp_us".to_string(),
            "event".to_string(),
            "tenant_id".to_string(),
            "database_id".to_string(),
            "source".to_string(),
            "detail".to_string(),
        ],
        vec![
            DdlColType::Int8,
            DdlColType::Int8,
            DdlColType::Text,
            DdlColType::Int8,
            DdlColType::Int8,
            DdlColType::Text,
            DdlColType::Text,
        ],
    )
}

/// One SHOW AUDIT row. The `event` column is the snake_case event name.
fn audit_row(entry: &AuditEntry) -> Map<String, JsonValue> {
    let mut row = Map::new();
    row.insert(
        "seq".to_string(),
        JsonValue::String((entry.seq as i64).to_string()),
    );
    row.insert(
        "timestamp_us".to_string(),
        JsonValue::String((entry.timestamp_us as i64).to_string()),
    );
    row.insert(
        "event".to_string(),
        JsonValue::String(entry.event.snake_name().to_string()),
    );
    row.insert(
        "tenant_id".to_string(),
        JsonValue::String((entry.tenant_id.map_or(0i64, |t| t.as_u64() as i64)).to_string()),
    );
    row.insert(
        "database_id".to_string(),
        JsonValue::String((entry.database_id.map_or(0i64, |d| d.as_u64() as i64)).to_string()),
    );
    row.insert(
        "source".to_string(),
        JsonValue::String(entry.source.clone()),
    );
    row.insert(
        "detail".to_string(),
        JsonValue::String(entry.detail.clone()),
    );
    row
}

/// Read the newest `limit` entries that pass `filter` and shape them most-recent-first.
fn audit_rows(
    state: &SharedState,
    limit: usize,
    filter: impl Fn(&AuditEntry) -> bool,
) -> Result<Vec<DdlResult>, DdlError> {
    let entries = state
        .recent_audit_entries(limit, filter)
        .map_err(|e| DdlError::from_error(&e))?;
    let rows = entries.iter().rev().map(audit_row).collect();
    let (columns, column_types) = audit_columns();
    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// The only WHERE form every SHOW AUDIT command accepts.
const WHERE_FORM: &str = "event_type = '<event_name>'";

const LOG_SYNTAX: &str = "syntax: SHOW AUDIT LOG [WHERE event_type = '<event_name>'] [LIMIT <n>]";
const WHERE_SYNTAX: &str = "syntax: SHOW AUDIT WHERE event_type = '<event_name>' [LIMIT <n>]";
const IN_DATABASE_SYNTAX: &str =
    "syntax: SHOW AUDIT IN DATABASE <name> [WHERE event_type = '<event_name>'] [LIMIT <n>]";

/// The optional clauses that follow a SHOW AUDIT command prefix.
#[derive(Debug, PartialEq, Eq)]
struct AuditClauses {
    /// Lowercase snake_case event name from `WHERE event_type = '...'`.
    event_type: Option<String>,
    limit: usize,
}

impl AuditClauses {
    /// Return true when `entry` passes the `event_type` filter.
    fn matches(&self, entry: &AuditEntry) -> bool {
        self.event_type
            .as_deref()
            .is_none_or(|name| entry.event.snake_name() == name)
    }
}

/// Parse `[WHERE event_type = '<name>'] [LIMIT <n>]` from the tokens after a command prefix.
///
/// Any other WHERE form is refused with 42601 naming the clause.
/// Any other trailing token is refused with 42601 and `syntax`.
fn parse_audit_clauses(tail: &[&str], syntax: &str) -> Result<AuditClauses, DdlError> {
    let tokens: Vec<&str> = tail
        .iter()
        .map(|t| t.trim_end_matches(';'))
        .filter(|t| !t.is_empty())
        .collect();

    let limit_at = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("LIMIT"))
        .unwrap_or(tokens.len());
    let (where_tokens, limit_tokens) = tokens.split_at(limit_at);

    let event_type = match where_tokens.split_first() {
        None => None,
        Some((first, clause)) if first.eq_ignore_ascii_case("WHERE") => {
            Some(parse_event_type_clause(&clause.join(" "))?)
        }
        Some(_) => {
            return Err(ddl_err(
                "42601",
                format!("unexpected `{}`; {syntax}", where_tokens.join(" ")),
            ));
        }
    };

    let limit = match limit_tokens {
        [] => DEFAULT_LIMIT,
        [_, value] => value.parse::<usize>().map_err(|_| {
            ddl_err(
                "42601",
                format!("LIMIT must be a non-negative integer, got `{value}`; {syntax}"),
            )
        })?,
        _ => {
            return Err(ddl_err(
                "42601",
                format!("unexpected `{}`; {syntax}", limit_tokens.join(" ")),
            ));
        }
    };

    Ok(AuditClauses { event_type, limit })
}

/// Parse `event_type = '<name>'` and return the lowercase name.
///
/// Every other predicate is refused with 42601 naming the clause.
fn parse_event_type_clause(clause: &str) -> Result<String, DdlError> {
    let unsupported = || {
        ddl_err(
            "42601",
            format!(
                "unsupported SHOW AUDIT WHERE clause `{clause}`: \
                 only `{WHERE_FORM}` is supported"
            ),
        )
    };
    let column_len = "event_type".len();
    let column = clause.get(..column_len).ok_or_else(unsupported)?;
    if !column.eq_ignore_ascii_case("event_type") {
        return Err(unsupported());
    }
    let literal = clause[column_len..]
        .trim_start()
        .strip_prefix('=')
        .ok_or_else(unsupported)?
        .trim();
    let name = literal
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
        .ok_or_else(unsupported)?;
    if name.is_empty() || name.contains('\'') {
        return Err(unsupported());
    }
    Ok(name.to_ascii_lowercase())
}

/// SHOW AUDIT LOG [WHERE event_type = '<snake_name>'] [LIMIT <n>]
///
/// Shows the most recent audit entries. Superuser only.
/// `SHOW AUDIT_LOG` is the same command.
pub fn show_audit_log(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity)?;

    let prefix_len = match (parts.get(1), parts.get(2)) {
        (Some(word), _) if word.trim_end_matches(';').eq_ignore_ascii_case("AUDIT_LOG") => 2,
        (_, Some(word)) if word.trim_end_matches(';').eq_ignore_ascii_case("LOG") => 3,
        _ => return Err(ddl_err("42601", LOG_SYNTAX)),
    };
    let clauses = parse_audit_clauses(&parts[prefix_len..], LOG_SYNTAX)?;

    audit_rows(state, clauses.limit, |entry| clauses.matches(entry))
}

/// SHOW AUDIT WHERE event_type = '<snake_name>' [LIMIT <n>]
///
/// Filters audit entries by event type. Superuser only.
/// The filter value must be the snake_case event name, e.g.
/// `'permission_denied'`, `'rls_rejected'`, `'lockout_triggered'`.
pub fn show_audit_where(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity)?;

    // parts: ["SHOW", "AUDIT", "WHERE", "event_type", "=", "'permission_denied'", ...]
    let clauses = parse_audit_clauses(parts.get(2..).unwrap_or_default(), WHERE_SYNTAX)?;
    if clauses.event_type.is_none() {
        return Err(ddl_err("42601", WHERE_SYNTAX));
    }

    audit_rows(state, clauses.limit, |entry| clauses.matches(entry))
}

/// `SHOW AUDIT IN DATABASE <name> [WHERE event_type = '<snake_name>'] [LIMIT <n>]`
///
/// Returns the most recent audit entries whose `database_id` matches the
/// named database and that pass the `event_type` filter. Superuser only.
pub fn show_audit_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity)?;

    // parts: ["SHOW", "AUDIT", "IN", "DATABASE", "<name>", ...]
    let db_name = parts
        .get(4)
        .map(|name| name.trim_end_matches(';'))
        .filter(|name| !name.is_empty())
        .ok_or_else(|| ddl_err("42601", IN_DATABASE_SYNTAX))?;
    let clauses = parse_audit_clauses(&parts[5..], IN_DATABASE_SYNTAX)?;

    let db_id = state
        .credentials
        .catalog()
        .get_database_id_by_name(db_name)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{db_name}' does not exist")))?;

    audit_rows(state, clauses.limit, |entry| {
        entry.database_id == Some(db_id) && clauses.matches(entry)
    })
}

/// Audit entries are read with a regular `SELECT` query against
/// `system.audit_log`; the client redirects the result.
pub fn export_audit_log(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can export audit log",
        ));
    }
    Err(ddl_err(
        "0A000",
        "use `SELECT ... FROM system.audit_log` and redirect the query \
         result on the client",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditEvent;

    fn clauses(sql_tail: &str) -> Result<AuditClauses, DdlError> {
        let tail: Vec<&str> = sql_tail.split_whitespace().collect();
        parse_audit_clauses(&tail, WHERE_SYNTAX)
    }

    #[test]
    fn empty_tail_uses_default_limit() {
        assert_eq!(
            clauses("").expect("empty tail"),
            AuditClauses {
                event_type: None,
                limit: DEFAULT_LIMIT
            }
        );
    }

    #[test]
    fn event_type_filter_and_limit_parse() {
        assert_eq!(
            clauses("WHERE event_type = 'Permission_Denied' LIMIT 7").expect("filter"),
            AuditClauses {
                event_type: Some("permission_denied".to_string()),
                limit: 7
            }
        );
        assert_eq!(
            clauses("where EVENT_TYPE='auth_success';").expect("compact filter"),
            AuditClauses {
                event_type: Some("auth_success".to_string()),
                limit: DEFAULT_LIMIT
            }
        );
    }

    #[test]
    fn unsupported_where_is_refused_with_clause_named() {
        for tail in [
            "WHERE detail = 'x'",
            "WHERE seq > 5",
            "WHERE event_type = 'a' AND seq > 5",
            "WHERE event_type = 'a' OR event_type = 'b'",
            "WHERE event_type <> 'a'",
            "WHERE event_type = ''",
            "WHERE",
        ] {
            let err = clauses(tail).expect_err(tail);
            assert_eq!(err.sqlstate, "42601", "{tail}");
            assert!(
                err.message.contains("unsupported SHOW AUDIT WHERE clause"),
                "{tail}: {}",
                err.message
            );
            let clause = tail.trim_start_matches("WHERE").trim();
            assert!(
                err.message.contains(&format!("`{clause}`")),
                "{tail}: {}",
                err.message
            );
        }
    }

    #[test]
    fn stray_tokens_and_bad_limit_are_refused() {
        for tail in ["ORDER BY seq", "LIMIT", "LIMIT -1", "LIMIT 5 OFFSET 2"] {
            let err = clauses(tail).expect_err(tail);
            assert_eq!(err.sqlstate, "42601", "{tail}");
        }
    }

    #[test]
    fn filter_matches_snake_name_only() {
        let parsed = clauses("WHERE event_type = 'privilege_change'").expect("filter");
        let entry = |event| AuditEntry {
            seq: 1,
            timestamp_us: 1,
            event,
            tenant_id: None,
            database_id: None,
            auth_user_id: String::new(),
            auth_user_name: String::new(),
            session_id: String::new(),
            source: String::new(),
            detail: String::new(),
            prev_hash: String::new(),
        };
        assert!(parsed.matches(&entry(AuditEvent::PrivilegeChange)));
        assert!(!parsed.matches(&entry(AuditEvent::AuthSuccess)));
        let unfiltered = clauses("").expect("empty tail");
        assert!(unfiltered.matches(&entry(AuditEvent::AuthSuccess)));
    }
}
