// SPDX-License-Identifier: BUSL-1.1

//! Handlers for LISTEN, NOTIFY, and UNLISTEN SQL statements.
//!
//! These commands intercept before the normal planner since they have no
//! SQL-standard representation in sqlparser-rs and are pure Control-Plane
//! operations.
//!
//! Transaction semantics:
//! - `LISTEN` and `UNLISTEN` take effect immediately regardless of transaction
//!   state (matching PostgreSQL behaviour).
//! - `NOTIFY` issued outside a transaction fires immediately.
//! - `NOTIFY` issued inside a `BEGIN` block is buffered until `COMMIT`
//!   and silently dropped on `ROLLBACK`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::notify_bus::normalize_channel;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::DatabaseId;

use super::core::NodeDbPgHandler;
use crate::control::server::shared::session::{SessionId, TransactionState};

impl NodeDbPgHandler {
    /// Handle `LISTEN <channel>`.
    ///
    /// Subscribes the current session to the named channel within the session's
    /// tenant scope.  Duplicate LISTEN on the same channel is a no-op (PG behaviour).
    pub(super) fn handle_listen(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let channel = parse_listen_channel(sql).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                format!("syntax error in LISTEN: expected LISTEN <channel>, got: {sql}"),
            )))
        })?;
        let channel = normalize_channel(&channel);
        validate_channel_name(&channel)?;

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(DatabaseId::DEFAULT);
        self.sessions.listen_channel(
            session_id,
            database_id,
            identity.tenant_id,
            &channel,
            &self.state.notify_bus,
        );
        Ok(vec![Response::Execution(Tag::new("LISTEN"))])
    }

    /// Handle `NOTIFY <channel> [, '<payload>']`.
    ///
    /// Outside a transaction: delivers immediately.
    /// Inside a transaction: buffers until COMMIT (dropped on ROLLBACK).
    pub(super) fn handle_notify(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let (channel, payload) = parse_notify(sql).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                format!(
                    "syntax error in NOTIFY: expected NOTIFY <channel> [, '<payload>'], got: {sql}"
                ),
            )))
        })?;
        let channel = normalize_channel(&channel);
        validate_channel_name(&channel)?;

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(DatabaseId::DEFAULT);
        let tx_state = self.sessions.transaction_state(session_id);
        if tx_state == TransactionState::InBlock {
            // Buffer the database binding with the notification until COMMIT.
            self.sessions
                .buffer_notify(session_id, database_id, channel, payload);
        } else {
            // Fire immediately in the currently selected database.
            self.state
                .notify_bus
                .notify(database_id, identity.tenant_id, &channel, &payload);
        }
        Ok(vec![Response::Execution(Tag::new("NOTIFY"))])
    }

    /// Handle `UNLISTEN <channel>` or `UNLISTEN *`.
    pub(super) fn handle_unlisten(
        &self,
        _identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let upper = sql.trim().to_uppercase();
        if upper == "UNLISTEN *" {
            self.sessions
                .unlisten_all_channels(session_id, &self.state.notify_bus);
        } else {
            let channel = parse_unlisten_channel(sql).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("syntax error in UNLISTEN: expected UNLISTEN <channel>, got: {sql}"),
                )))
            })?;
            let channel = normalize_channel(&channel);
            validate_channel_name(&channel)?;
            self.sessions
                .unlisten_channel(session_id, &channel, &self.state.notify_bus);
        }
        Ok(vec![Response::Execution(Tag::new("UNLISTEN"))])
    }
}

// ── Parsing helpers ──────────────────────────────────────────────────────────

/// Parse `LISTEN <channel>` → channel name.
///
/// Accepts both quoted (`LISTEN "my channel"`) and unquoted identifiers.
fn parse_listen_channel(sql: &str) -> Option<String> {
    let rest = sql.trim().strip_prefix_ci("LISTEN")?;
    Some(extract_identifier(rest.trim()))
}

/// Parse `UNLISTEN <channel>` → channel name (returns None for `UNLISTEN *`).
fn parse_unlisten_channel(sql: &str) -> Option<String> {
    let rest = sql.trim().strip_prefix_ci("UNLISTEN")?;
    let rest = rest.trim();
    if rest == "*" {
        return None; // caller handles * separately
    }
    Some(extract_identifier(rest))
}

/// Parse `NOTIFY <channel>` or `NOTIFY <channel>, '<payload>'` → (channel, payload).
fn parse_notify(sql: &str) -> Option<(String, String)> {
    let rest = sql.trim().strip_prefix_ci("NOTIFY")?;
    let rest = rest.trim();

    // Split on the first comma to separate channel from optional payload.
    if let Some(comma_pos) = find_top_level_comma(rest) {
        let channel_part = rest[..comma_pos].trim();
        let payload_part = rest[comma_pos + 1..].trim();
        let channel = extract_identifier(channel_part);
        let payload = extract_string_literal(payload_part)
            .unwrap_or_else(|| extract_identifier(payload_part));
        Some((channel, payload))
    } else {
        let channel = extract_identifier(rest);
        Some((channel, String::new()))
    }
}

/// Find the position of the first comma that is not inside quotes or parens.
fn find_top_level_comma(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    for (i, c) in s.char_indices() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => depth = depth.saturating_sub(1),
            ',' if !in_single && !in_double && depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Extract a quoted or unquoted identifier from the start of `s`.
///
/// - `"my channel"` → `my channel` (preserves case inside quotes).
/// - `orders` → `orders` (caller normalises via `normalize_channel`).
fn extract_identifier(s: &str) -> String {
    let s = s.trim().trim_end_matches(';');
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        // Double-quoted identifier — preserve internal case (PG folds to lowercase
        // only if unquoted; we do the same in normalize_channel for unquoted).
        s[1..s.len() - 1].replace("\"\"", "\"")
    } else {
        // Unquoted — strip trailing whitespace/semicolons.
        s.split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';')
            .to_string()
    }
}

/// Extract a single-quoted string literal payload, stripping the outer quotes
/// and un-escaping doubled single-quotes.
fn extract_string_literal(s: &str) -> Option<String> {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        Some(s[1..s.len() - 1].replace("''", "'"))
    } else {
        None
    }
}

/// Validate that a channel name is a legal PG identifier (non-empty, ≤ 63 bytes).
fn validate_channel_name(channel: &str) -> PgWireResult<()> {
    if channel.is_empty() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "channel name must not be empty".to_owned(),
        ))));
    }
    if channel.len() > 63 {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42622".to_owned(),
            format!("channel name too long: {} bytes (max 63)", channel.len()),
        ))));
    }
    Ok(())
}

// ── Extension trait for case-insensitive prefix stripping ────────────────────

trait StripPrefixCi {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str>;
}

impl StripPrefixCi for str {
    fn strip_prefix_ci(&self, prefix: &str) -> Option<&str> {
        if self.len() < prefix.len() {
            return None;
        }
        let (head, tail) = self.split_at(prefix.len());
        if head.eq_ignore_ascii_case(prefix) {
            Some(tail)
        } else {
            None
        }
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_listen_simple() {
        assert_eq!(
            parse_listen_channel("LISTEN orders"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn parse_listen_quoted() {
        assert_eq!(
            parse_listen_channel(r#"LISTEN "My Channel""#),
            Some("My Channel".to_string())
        );
    }

    #[test]
    fn parse_listen_semicolon() {
        assert_eq!(
            parse_listen_channel("LISTEN orders;"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn parse_notify_no_payload() {
        assert_eq!(
            parse_notify("NOTIFY orders"),
            Some(("orders".to_string(), "".to_string()))
        );
    }

    #[test]
    fn parse_notify_with_payload() {
        assert_eq!(
            parse_notify("NOTIFY orders, 'hello world'"),
            Some(("orders".to_string(), "hello world".to_string()))
        );
    }

    #[test]
    fn parse_notify_escaped_quotes_in_payload() {
        assert_eq!(
            parse_notify("NOTIFY ch, 'it''s fine'"),
            Some(("ch".to_string(), "it's fine".to_string()))
        );
    }

    #[test]
    fn parse_unlisten_named() {
        assert_eq!(
            parse_unlisten_channel("UNLISTEN orders"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn parse_unlisten_star() {
        // The handler deals with * before calling parse_unlisten_channel.
        assert_eq!(parse_unlisten_channel("UNLISTEN *"), None);
    }
}
