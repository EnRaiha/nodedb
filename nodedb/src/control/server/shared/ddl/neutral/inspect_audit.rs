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

/// SHOW AUDIT LOG [LIMIT <n>]
///
/// Shows the most recent audit entries. Superuser only.
pub fn show_audit_log(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity)?;

    let limit = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("LIMIT") {
        parts[4].parse::<usize>().unwrap_or(DEFAULT_LIMIT)
    } else {
        DEFAULT_LIMIT
    };

    audit_rows(state, limit, |_| true)
}

/// SHOW AUDIT WHERE event_type = '<snake_name>'
///
/// Filters audit entries by event type.
/// The filter value must be the snake_case event name, e.g.
/// `'permission_denied'`, `'rls_rejected'`, `'lockout_triggered'`.
pub fn show_audit_where(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity)?;

    // Parse: SHOW AUDIT WHERE event_type = '<value>' [LIMIT <n>]
    // parts: ["SHOW", "AUDIT", "WHERE", "event_type", "=", "'permission_denied'", ...]
    let event_filter = if parts.len() >= 6 && parts[3].eq_ignore_ascii_case("event_type") {
        parts[5].trim_matches('\'').to_ascii_lowercase()
    } else {
        return Err(ddl_err(
            "42601",
            "syntax: SHOW AUDIT WHERE event_type = '<event_name>' [LIMIT <n>]",
        ));
    };

    let limit = if parts.len() >= 8 && parts[6].eq_ignore_ascii_case("LIMIT") {
        parts[7].parse::<usize>().map_err(|_| {
            ddl_err(
                "42601",
                "syntax: SHOW AUDIT WHERE event_type = '<event_name>' [LIMIT <n>] (LIMIT must be a non-negative integer)",
            )
        })?
    } else {
        DEFAULT_LIMIT
    };

    audit_rows(state, limit, |entry| {
        entry.event.snake_name() == event_filter
    })
}

/// `SHOW AUDIT IN DATABASE <name> [LIMIT <n>]`
///
/// Returns the most recent audit entries whose `database_id` matches the
/// named database. Superuser only.
pub fn show_audit_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    db_name: &str,
    limit: usize,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity)?;

    let db_id = state
        .credentials
        .catalog()
        .get_database_id_by_name(db_name)
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{db_name}' does not exist")))?;

    audit_rows(state, limit, |entry| entry.database_id == Some(db_id))
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
