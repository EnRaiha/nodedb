// SPDX-License-Identifier: BUSL-1.1

//! Unified SQL dispatcher: handles NodeDB SQL extensions that cannot be
//! handled by `plan_sql` (sqlparser-based), without requiring pgwire context.
//!
//! Used by:
//! - The procedural statement executor (trigger bodies, procedures)
//! - The pgwire streaming router (as a thin adapter)
//!
//! For INSERT/UPDATE/DELETE and other plan_sql-bound statements, returns `None`
//! so the caller can use `QueryContext::plan_sql` directly (the procedural
//! executor applies transaction buffering on that path).

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::DispatchOutcome;

/// Attempt to dispatch `sql` as a NodeDB SQL extension.
///
/// Returns `Some(result)` if the statement was handled (e.g. `PUBLISH TO`),
/// or `None` if the SQL should be handled by the caller via `plan_sql`.
///
/// The caller is responsible for handling `None`.
pub async fn dispatch_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<crate::Result<DispatchOutcome>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    // ── PUBLISH TO <topic> <payload> ─────────────────────────────────────────
    if nodedb_types::starts_with_ascii_case_insensitive(trimmed, "PUBLISH TO ") {
        return Some(handle_publish(state, identity, trimmed).await);
    }

    // Not a handled NodeDB extension — caller should use plan_sql.
    None
}

/// Handle `PUBLISH TO <topic> <payload>` without pgwire coupling.
async fn handle_publish(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> crate::Result<DispatchOutcome> {
    let prefix = "PUBLISH TO ";
    if !nodedb_types::starts_with_ascii_case_insensitive(sql, prefix) {
        return Err(crate::Error::BadRequest {
            detail: "expected PUBLISH TO <topic> <payload>".into(),
        });
    }

    let rest = sql[prefix.len()..].trim();

    let (topic_name, payload_part) =
        rest.split_once(char::is_whitespace)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: "expected payload after topic name in PUBLISH TO".into(),
            })?;
    let topic_name = topic_name.to_lowercase();

    let payload = parse_payload(payload_part.trim())?;

    let tenant_id = identity.tenant_id.as_u64();
    let tenant = identity.tenant_id;

    use crate::event::topic::publish::PublishError;

    match crate::event::topic::publish::publish_to_topic(state, tenant_id, &topic_name, &payload) {
        Ok(_seq) => Ok(DispatchOutcome {
            rows_affected: 1,
            rows: Vec::new(),
        }),
        Err(PublishError::RemoteHome { leader_node, .. }) => {
            crate::event::topic::publish::publish_remote(
                state,
                tenant_id,
                &topic_name,
                &payload,
                leader_node,
            )
            .await
            .map_err(|e| crate::Error::Dispatch {
                detail: format!("remote publish to '{topic_name}' failed: {e}"),
            })?;
            Ok(DispatchOutcome {
                rows_affected: 1,
                rows: Vec::new(),
            })
        }
        Err(PublishError::TopicNotFound(t)) => Err(crate::Error::CollectionNotFound {
            tenant_id: tenant,
            collection: t,
        }),
        Err(PublishError::RemoteError(e)) => Err(crate::Error::Dispatch {
            detail: format!("remote publish to '{topic_name}' failed: {e}"),
        }),
    }
}

/// Decode a SQL-literal payload or pass through a bare payload.
///
/// If the input is single-quoted, strip the outer quotes and unescape doubled
/// quotes (`''` → `'`) per SQL string-literal rules. Reject unterminated
/// quotes rather than silently treating them as bare payloads.
fn parse_payload(input: &str) -> crate::Result<String> {
    if input.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "PUBLISH TO payload is empty".into(),
        });
    }
    if !input.starts_with('\'') {
        return Ok(input.to_string());
    }
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push('\'');
                i += 2;
                continue;
            }
            if i + 1 != bytes.len() {
                return Err(crate::Error::BadRequest {
                    detail: "PUBLISH TO payload has trailing tokens after closing quote".into(),
                });
            }
            return Ok(out);
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Err(crate::Error::BadRequest {
        detail: "PUBLISH TO payload has unterminated string literal".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_payload;

    #[test]
    fn plain_quoted() {
        assert_eq!(parse_payload("'hello'").unwrap(), "hello");
    }

    #[test]
    fn escaped_quote_unescaped() {
        assert_eq!(parse_payload("'it''s'").unwrap(), "it's");
        assert_eq!(parse_payload("''''").unwrap(), "'");
    }

    #[test]
    fn empty_quoted_is_empty_string() {
        assert_eq!(parse_payload("''").unwrap(), "");
    }

    #[test]
    fn bare_payload_passes_through() {
        assert_eq!(parse_payload("{\"k\":1}").unwrap(), "{\"k\":1}");
    }

    #[test]
    fn unterminated_rejected() {
        assert!(parse_payload("'oops").is_err());
    }

    #[test]
    fn empty_input_rejected() {
        assert!(parse_payload("").is_err());
    }

    #[test]
    fn trailing_tokens_after_close_rejected() {
        assert!(parse_payload("'a' junk").is_err());
    }
}
