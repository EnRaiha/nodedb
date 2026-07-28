// SPDX-License-Identifier: BUSL-1.1

//! `current_setting(setting_name [, missing_ok])` — PostgreSQL's function
//! form of `SHOW <param>`. Resolves from the same GUC sources as `SHOW`
//! (see `session_show::resolve_guc`), but as a scalar function call so it
//! can appear in `SELECT` lists like any other function.

use std::sync::Arc;

use nodedb_types::strip_prefix_ascii_case_insensitive;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::super::types::text_field;
use crate::control::server::shared::session::SessionId;

use super::core::NodeDbPgHandler;

/// Parse `SELECT current_setting('name')` or
/// `SELECT current_setting('name', true|false)` into `(setting_name,
/// missing_ok)`. Case-insensitive on the keyword; the setting name is
/// lowercased to match `resolve_guc`'s expectation. Returns `None` if the
/// SQL is not a `current_setting` call so normal planning can proceed.
///
/// Only string-literal arguments are handled. If the second argument isn't
/// a clear `true`/`false` literal, `missing_ok` defaults to `false`
/// (conservative — an unrecognised second arg should not silently suppress
/// errors).
pub fn parse_current_setting(sql: &str) -> Option<(String, bool)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let rest = strip_prefix_ascii_case_insensitive(trimmed, "SELECT ")?.trim();
    strip_prefix_ascii_case_insensitive(rest, "CURRENT_SETTING(")?;

    let paren = rest.find('(')?;
    let close = rest.rfind(')')?;
    if close <= paren + 1 {
        return None;
    }
    let args = &rest[paren + 1..close];

    let parts: Vec<&str> = args.split(',').map(str::trim).collect();
    let (name_arg, missing_ok_arg) = match parts.as_slice() {
        [name] => (*name, None),
        [name, missing_ok] => (*name, Some(*missing_ok)),
        _ => return None,
    };

    let name = strip_quotes(name_arg)?;
    if name.is_empty() {
        return None;
    }

    let missing_ok = match missing_ok_arg {
        Some(v) => v.eq_ignore_ascii_case("true"),
        None => false,
    };

    Some((name.to_lowercase(), missing_ok))
}

/// Strip a single-quoted SQL string literal into its raw inner value.
/// Escapes are not honored.
fn strip_quotes(s: &str) -> Option<String> {
    let s = s.trim();
    let inner = s.strip_prefix('\'')?.strip_suffix('\'')?;
    (!inner.contains('\'')).then(|| inner.to_string())
}

impl NodeDbPgHandler {
    /// Handle `SELECT current_setting('name' [, missing_ok])`.
    pub(super) fn handle_current_setting(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let (name, missing_ok) = match parse_current_setting(sql) {
            Some(parsed) => parsed,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "syntax error: current_setting('name' [, missing_ok])".to_owned(),
                ))));
            }
        };

        let schema = Arc::new(vec![text_field("current_setting")]);
        let mut encoder = DataRowEncoder::new(schema.clone());

        match self.resolve_guc(session_id, &name) {
            Ok(value) => {
                encoder.encode_field(&value)?;
            }
            Err(e) => {
                if missing_ok {
                    encoder.encode_field(&Option::<String>::None)?;
                } else {
                    return Err(e);
                }
            }
        }

        let row = encoder.take_row();
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(vec![Ok(row)]),
        ))])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_arg_form() {
        assert_eq!(
            parse_current_setting("SELECT current_setting('server_version_num')"),
            Some(("server_version_num".to_string(), false))
        );
    }

    #[test]
    fn parses_missing_ok_true() {
        assert_eq!(
            parse_current_setting("SELECT current_setting('nodedb.foo', true)"),
            Some(("nodedb.foo".to_string(), true))
        );
    }

    #[test]
    fn parses_missing_ok_false() {
        assert_eq!(
            parse_current_setting("SELECT current_setting('nodedb.foo', false)"),
            Some(("nodedb.foo".to_string(), false))
        );
    }

    #[test]
    fn lowercases_setting_name() {
        assert_eq!(
            parse_current_setting("SELECT current_setting('SERVER_VERSION')"),
            Some(("server_version".to_string(), false))
        );
    }

    #[test]
    fn is_case_insensitive_on_keyword() {
        assert_eq!(
            parse_current_setting("select CURRENT_SETTING('server_version')"),
            Some(("server_version".to_string(), false))
        );
    }

    #[test]
    fn accepts_trailing_semicolon() {
        assert_eq!(
            parse_current_setting("SELECT current_setting('server_version');"),
            Some(("server_version".to_string(), false))
        );
    }

    #[test]
    fn unclear_second_arg_defaults_to_false() {
        assert_eq!(
            parse_current_setting("SELECT current_setting('server_version', maybe)"),
            Some(("server_version".to_string(), false))
        );
    }

    #[test]
    fn rejects_unrelated_select() {
        assert_eq!(parse_current_setting("SELECT 1"), None);
        assert_eq!(parse_current_setting("SELECT version()"), None);
    }

    #[test]
    fn unicode_setting_name_preserves_original_argument_boundaries() {
        assert_eq!(
            parse_current_setting("SeLeCt current_setting('Straße.setting', TRUE)"),
            Some(("straße.setting".to_string(), true))
        );
    }

    #[test]
    fn rejects_empty_arg() {
        assert_eq!(parse_current_setting("SELECT current_setting()"), None);
        assert_eq!(parse_current_setting("SELECT current_setting('')"), None);
    }
}
