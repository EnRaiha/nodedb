// SPDX-License-Identifier: BUSL-1.1

//! `AuthContext` construction, scope enrichment, and per-query `ON DENY`
//! extraction.

use nodedb_sql::parser::preprocess::lex::rfind_ascii_case_insensitive;

use crate::control::security::auth_context::{AuthContext, generate_session_id};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;

/// Build an `AuthContext` from an `AuthenticatedIdentity`.
///
/// This is the centralized factory used by password, API-key, certificate,
/// and trust flows. JWT flows use the opaque verified-claims constructor so
/// unverified token fields cannot enrich authorization context.
pub fn build_auth_context(identity: &AuthenticatedIdentity) -> AuthContext {
    let mut ctx = AuthContext::from_identity(identity, generate_session_id());
    // Stamp the per-user default database so `$auth.database_id` is available
    // for RLS predicates even before a `USE DATABASE` command.
    ctx.database_id = identity.default_database;
    ctx
}

/// Enrich AuthContext with scope status data from the scope grant store.
///
/// Populates metadata entries for `scope_status.<name>` and `scope_expires_at.<name>`
/// so RLS predicates can reference `$auth.metadata.scope_status.pro:all`.
pub fn enrich_auth_context_with_scopes(
    ctx: &mut AuthContext,
    scope_grants: &crate::control::security::scope::grant::ScopeGrantStore,
    org_ids: &[String],
) {
    let effective = scope_grants.effective_scopes(&ctx.id, org_ids);
    for scope_name in &effective {
        let status = scope_grants.scope_status(scope_name, "user", &ctx.id);
        ctx.metadata
            .insert(format!("scope_status.{scope_name}"), status.to_string());
        let expires_at = scope_grants.scope_expires_at(scope_name, "user", &ctx.id);
        if expires_at > 0 {
            ctx.metadata.insert(
                format!("scope_expires_at.{scope_name}"),
                expires_at.to_string(),
            );
        }
    }
    // Also set a comma-separated list of effective scopes.
    let scope_list: Vec<String> = effective.into_iter().collect();
    if !scope_list.is_empty() {
        ctx.metadata.insert("scopes".into(), scope_list.join(","));
    }
}

/// Build an `AuthContext` with pgwire session overrides applied.
///
/// Authorization identity always comes from the authenticated connection.
/// Session parameters may tune denial behavior and select a validated opaque
/// session handle, but raw JWT text is never decoded as an identity override.
pub fn build_auth_context_with_session(
    identity: &AuthenticatedIdentity,
    sessions: &crate::control::server::shared::session::SessionStore,
    session_id: impl Into<SessionId>,
) -> AuthContext {
    let session_id = session_id.into();
    let mut ctx = build_auth_context(identity);

    // Read ON DENY override from SET LOCAL nodedb.on_deny = '...'.
    if let Some(on_deny_val) = sessions.get_parameter(session_id, "nodedb.on_deny")
        && let Ok(mode) = crate::control::security::deny::parse_on_deny(&[&on_deny_val])
    {
        ctx.on_deny_override = Some(mode);
    }

    // The active session database overrides the per-user default so that
    // `$auth.database_id` tracks `USE DATABASE` commands within a session.
    if let Some(db) = sessions.get_current_database(session_id) {
        ctx.database_id = Some(db);
    }

    ctx
}

/// Extract a per-query `ON DENY` clause from SQL and apply it to the auth context.
///
/// Parses: `SELECT ... ON DENY ERROR 'CODE' MESSAGE '...'`
/// Strips the `ON DENY` clause from the SQL and sets `auth_ctx.on_deny_override`.
/// Returns the cleaned SQL.
pub fn extract_and_apply_on_deny(
    sql: &str,
    auth_ctx: &mut crate::control::security::auth_context::AuthContext,
) -> String {
    let Some(idx) = rfind_ascii_case_insensitive(sql, "on deny ") else {
        return sql.to_string();
    };

    // Only strip ON DENY from SELECT/WITH queries (not CREATE RLS POLICY).
    let trimmed = sql.trim_start();
    if !trimmed
        .get(.."select".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        && !trimmed
            .get(.."with".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("with"))
    {
        return sql.to_string();
    }

    let on_deny_part = &sql[idx + "on deny ".len()..];
    let parts: Vec<&str> = on_deny_part.split_whitespace().collect();
    match crate::control::security::deny::parse_on_deny(&parts) {
        Ok(mode) => {
            auth_ctx.on_deny_override = Some(mode);
            sql[..idx].trim_end().to_string()
        }
        Err(_) => sql.to_string(),
    }
}
