// SPDX-License-Identifier: BUSL-1.1

//! Exact-ID parsing and errors for pgwire connection administration.

use std::str::FromStr;

use pgwire::error::{ErrorInfo, PgWireError};

use crate::control::server::shared::session::ConnectionId;

pub(super) fn denied() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42501".to_owned(),
        "permission denied: only superuser can administer connections".to_owned(),
    )))
}

pub(super) fn invalid_id() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42601".to_owned(),
        "invalid connection id; use SHOW CONNECTIONS to list exact IDs".to_owned(),
    )))
}

/// Parse only `KILL CONNECTION <decimal-id>` with optional matching simple
/// quotes. Address-shaped and non-ASCII identifiers are deliberately rejected.
pub(super) fn parse_kill(sql: &str) -> Option<Result<ConnectionId, ()>> {
    let mut tokens = sql.split_ascii_whitespace();
    let command = tokens.next()?;
    let subject = tokens.next()?;
    if !command.eq_ignore_ascii_case("KILL") || !subject.eq_ignore_ascii_case("CONNECTION") {
        return None;
    }
    let target = match (tokens.next(), tokens.next()) {
        (Some(target), None) => target,
        _ => return Some(Err(())),
    };
    if !target.is_ascii() {
        return Some(Err(()));
    }
    let unquoted = target
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(target);
    Some(ConnectionId::from_str(unquoted).map_err(|_| ()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_only_exact_decimal_ids() {
        let parsed = parse_kill("KILL CONNECTION '42'")
            .and_then(|result| result.ok())
            .map(ConnectionId::get);
        assert_eq!(parsed, Some(42));
        assert_eq!(parse_kill("KILL CONNECTION 0"), Some(Err(())));
        assert_eq!(
            parse_kill("KILL CONNECTION 18446744073709551616"),
            Some(Err(()))
        );
        assert_eq!(parse_kill("KILL CONNECTION 127.0.0.1:5432"), Some(Err(())));
        assert_eq!(parse_kill("KILL CONNECTION 2 trailing"), Some(Err(())));
        assert_eq!(parse_kill("KILL CONNECTION \"42\""), Some(Err(())));
        assert_eq!(parse_kill("KILL CONNECTION ４２"), Some(Err(())));
        assert_eq!(parse_kill("SELECT 1"), None);
    }
}
