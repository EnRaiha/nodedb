// SPDX-License-Identifier: BUSL-1.1

//! String-recognized introspection DDL arms: admin inspection & audit,
//! impersonation, session management, observability, permission/scope explain,
//! usage metering, organizations, scopes, and collection introspection.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::collection;
use super::super::explain_ddl;
use super::super::impersonation;
use super::super::inspect;
use super::super::inspect_audit;
use super::super::metering_ddl;
use super::super::observability;
use super::super::org_ddl;
use super::super::scope_ddl;
use super::super::scope_query_ddl;
use super::super::session_admin;

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Administrative introspection & audit: SHOW USERS, SHOW TENANTS, SHOW
    // ROLES, SHOW SESSION, EXPORT AUDIT, SHOW AUDIT IN DATABASE / WHERE / LOG,
    // SHOW GRANTS. `SHOW USERS`, `SHOW GRANTS`, and `SHOW AUDIT…` parse into
    // typed AST variants (`AuthStmt::ShowUsers` / `ShowGrants`,
    // `MiscStmt::ShowAuditLog`) and bare `SHOW TENANTS` into
    // `DatabaseStmt::ShowTenants`, but the pgwire typed-AST path has no arm for
    // any of them — they fell through to the admin/observability string router,
    // which dispatched them by prefix from the raw token slice. `SHOW ROLES`,
    // `SHOW SESSION`, and `EXPORT AUDIT` parse into no typed DDL variant.
    // Replicate the string dispatch exactly here, before the parse gate, so the
    // prefix recognition and the `parts`-based extraction stay byte-identical.
    // `SHOW SESSIONS` is excluded here (see the `session_admin::show_sessions`
    // arm above, which is now checked first) so the two never race.
    //
    // Audit-log truncation is rejected: entries are pruned only by the retention
    // policy. Recognized by string prefix before the parse gate so the message
    // stays byte-identical regardless of how the tail parses.
    if upper.starts_with("TRUNCATE AUDIT")
        || upper.starts_with("DELETE AUDIT")
        || upper.starts_with("CLEAR AUDIT")
    {
        return Some(Err(DdlError {
            sqlstate: "42501".to_string(),
            message: "audit log cannot be manually truncated. Entries are pruned automatically by the retention policy (audit_retention_days in config).".to_string(),
        }));
    }
    if upper.starts_with("SHOW USERS") {
        return Some(inspect::show_users(state, identity));
    }
    // Exact-match only. Filtered forms (`SHOW TENANTS WITH NAME <name>`,
    // `SHOW TENANT <ident>`) are parsed into typed variants and routed through
    // the typed match below; a prefix match here would silently drop the filter
    // and list every tenant.
    if upper == "SHOW TENANTS" {
        return Some(inspect::show_tenants(state, identity));
    }
    if upper == "SHOW ROLES" || upper.starts_with("SHOW ROLES ") {
        return Some(inspect::show_roles(state, identity));
    }
    if upper.starts_with("SHOW SESSION") && !upper.starts_with("SHOW SESSIONS") {
        return Some(inspect::show_session(identity));
    }
    if upper.starts_with("EXPORT AUDIT") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect_audit::export_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUDIT IN DATABASE") {
        // SHOW AUDIT IN DATABASE <name> [LIMIT <n>]
        // parts: ["SHOW", "AUDIT", "IN", "DATABASE", "<name>", ...]
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let db_name = if parts.len() >= 5 {
            parts[4]
        } else {
            return Some(Err(DdlError {
                sqlstate: "42601".to_string(),
                message: "syntax: SHOW AUDIT IN DATABASE <name> [LIMIT <n>]".to_string(),
            }));
        };
        let limit = if parts.len() >= 7 && parts[5].eq_ignore_ascii_case("LIMIT") {
            parts[6].parse::<usize>().unwrap_or(100)
        } else {
            100
        };
        return Some(inspect_audit::show_audit_in_database(
            state, identity, db_name, limit,
        ));
    }
    if upper.starts_with("SHOW AUDIT WHERE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect_audit::show_audit_where(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUDIT LOG") || upper.starts_with("SHOW AUDIT_LOG") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect_audit::show_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW GRANTS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(inspect::show_grants(state, identity, &parts));
    }

    // Impersonation & delegation: IMPERSONATE AUTH USER, STOP IMPERSONATION,
    // DELEGATE AUTH USER, REVOKE DELEGATION, SHOW DELEGATIONS. None of these
    // parse into any typed AST variant — the pgwire admin router dispatched
    // all five by string prefix from the raw token slice. Replicate that
    // exactly here, before the parse gate, so the prefix recognition and the
    // `parts`-based extraction / syntax messages stay byte-identical.
    if upper.starts_with("IMPERSONATE AUTH USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::impersonate(state, identity, &parts));
    }
    if upper.starts_with("STOP IMPERSONATION") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::stop_impersonation(state, identity, &parts));
    }
    if upper.starts_with("DELEGATE AUTH USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::delegate(state, identity, &parts));
    }
    if upper.starts_with("REVOKE DELEGATION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::revoke_delegation(state, identity, &parts));
    }
    if upper.starts_with("SHOW DELEGATIONS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(impersonation::show_delegations(state, identity, &parts));
    }

    // Session management: SHOW SESSIONS, KILL SESSION, KILL USER SESSIONS,
    // VERIFY AUDIT CHAIN. None of these parse into any typed AST variant —
    // the pgwire admin router dispatched all four by string prefix from the
    // raw token slice. Replicate that exactly here, before the parse gate, so
    // the prefix recognition and the `parts`-based extraction / syntax
    // messages stay byte-identical. `SHOW SESSIONS` is matched here (before
    // the observability `SHOW SESSION` prefix below), mirroring the pgwire
    // admin router's precedence over the pgwire observability router; the
    // `SHOW SESSION` guard below already excludes `SHOW SESSIONS` explicitly,
    // so the two never race regardless of which is checked first.
    if upper.starts_with("SHOW SESSIONS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::show_sessions(state, identity, &parts));
    }
    if upper.starts_with("KILL SESSION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::kill_session(state, identity, &parts));
    }
    if upper.starts_with("KILL USER SESSIONS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::kill_user_sessions(state, identity, &parts));
    }
    if upper.starts_with("VERIFY AUDIT CHAIN") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(session_admin::verify_audit_chain(state, identity, &parts));
    }

    // Administrative observability: SHOW SERVER STATS / SHOW STATS / SHOW
    // METRICS / SHOW MEMORY. None of these parse into a typed DDL AST variant —
    // the pgwire admin observability router recognized all four by the
    // exact-or-trailing-space prefix from the raw SQL. Replicate that exactly
    // here, before the parse gate, so the recognition (and the `SHOW SERVER
    // STATS` / `SHOW STATS` shared handler) stays byte-identical. `SHOW SERVER
    // STATS` is checked before `SHOW STATS` exactly as the pgwire router did.
    if upper == "SHOW SERVER STATS" || upper.starts_with("SHOW SERVER STATS ") {
        return Some(observability::show_server_stats(state, identity));
    }
    if upper == "SHOW STATS" || upper.starts_with("SHOW STATS ") {
        return Some(observability::show_server_stats(state, identity));
    }
    if upper == "SHOW METRICS" || upper.starts_with("SHOW METRICS ") {
        return Some(observability::show_metrics(state, identity));
    }
    if upper == "SHOW MEMORY" || upper.starts_with("SHOW MEMORY ") {
        return Some(observability::show_memory(state, identity));
    }

    // Permission / scope introspection: EXPLAIN PERMISSION / EXPLAIN SCOPE.
    // Neither parses into a typed DDL AST variant — the pgwire admin router
    // recognized both by string prefix from the raw token slice. Replicate that
    // exactly here, before the parse gate, so the prefix recognition and the
    // `parts`-based extraction / syntax messages stay byte-identical. The
    // pgwire wire path reaches these full-`EXPLAIN …` statements through the
    // DDL dispatch (native / http always; pgwire only for the non-`EXPLAIN `
    // full-SQL dispatch), so recognizing them here preserves behavior; the
    // `EXPLAIN <query>` handler strips the leading `EXPLAIN ` and never yields a
    // `PERMISSION …` / `SCOPE …` prefix, so it is unaffected.
    if upper.starts_with("EXPLAIN PERMISSION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(explain_ddl::explain_permission(state, identity, &parts));
    }
    if upper.starts_with("EXPLAIN SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(explain_ddl::explain_scope(state, identity, &parts));
    }

    // Usage metering: DEFINE METERING DIMENSION, SHOW USAGE FOR TENANT, EXPORT
    // USAGE, SHOW USAGE, SHOW QUOTA. None of these parse into a typed DDL AST
    // variant — the pgwire admin router recognized all five by string prefix
    // from the raw token slice. Replicate that exactly here, before the parse
    // gate, so the prefix recognition and the `parts`-based extraction / syntax
    // messages stay byte-identical. Guard ordering (SHOW USAGE FOR TENANT and
    // EXPORT USAGE before the broader SHOW USAGE) mirrors the pgwire router.
    if upper.starts_with("DEFINE METERING DIMENSION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::define_dimension(state, identity, &parts));
    }
    if upper.starts_with("SHOW USAGE FOR TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::show_usage_for_tenant(state, identity, &parts));
    }
    if upper.starts_with("EXPORT USAGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::export_usage(state, identity, &parts));
    }
    if upper.starts_with("SHOW USAGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::show_usage(state, identity, &parts));
    }
    if upper.starts_with("SHOW QUOTA ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(metering_ddl::show_quota(state, identity, &parts));
    }

    // Organization management. None of `CREATE ORG`, `ALTER ORG`, `DROP ORG`,
    // `SHOW ORGS`, or `SHOW MEMBERS OF ORG` parse into any typed AST variant —
    // the pgwire admin router dispatched all of them by string prefix from the
    // raw token slice. Replicate that exactly here, before the parse gate, so
    // the prefix recognition and the `parts`-based extraction / syntax messages
    // stay byte-identical.
    if upper.starts_with("CREATE ORG ")
        || upper.starts_with("ALTER ORG ")
        || upper.starts_with("DROP ORG ")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(org_ddl::handle_org(state, identity, &parts));
    }
    if upper.starts_with("SHOW ORGS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(org_ddl::show_orgs(state, identity, &parts));
    }
    if upper.starts_with("SHOW MEMBERS OF ORG") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(org_ddl::show_members(state, identity, &parts));
    }

    // Scope management: DEFINE / DROP / GRANT / REVOKE / ALTER / RENEW SCOPE,
    // SHOW MY SCOPES, SHOW SCOPES FOR, SHOW SCOPE GRANTS, SHOW SCOPE(S). None of
    // these parse into any typed AST variant — `GRANT SCOPE` / `REVOKE SCOPE`
    // are explicitly excluded from the typed grant parser (returning `None`),
    // and the rest have no grammar at all — so the pgwire admin router
    // dispatched all of them by string prefix from the raw token slice.
    // Replicate that exactly here, before the parse gate, so the prefix
    // recognition and the `parts`-based extraction / syntax messages stay
    // byte-identical. Guard ordering mirrors the pgwire admin router: `SHOW MY
    // SCOPES` and `SHOW SCOPES FOR ` are matched before the broader `SHOW SCOPE
    // GRANTS` / `SHOW SCOPE` pair (nothing between them in the pgwire router
    // claimed a scope input, so grouping them here is behavior-preserving), and
    // `SHOW SCOPE GRANTS` is checked before the `SHOW SCOPE` catch-all.
    if upper.starts_with("DEFINE SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::define_scope(state, identity, &parts));
    }
    if upper.starts_with("DROP SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::drop_scope(state, identity, &parts));
    }
    if upper.starts_with("GRANT SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::grant_scope(state, identity, &parts));
    }
    if upper.starts_with("REVOKE SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::revoke_scope(state, identity, &parts));
    }
    if upper.starts_with("ALTER SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_query_ddl::alter_scope(state, identity, &parts));
    }
    if upper.starts_with("SHOW MY SCOPES") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_query_ddl::show_my_scopes(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCOPES FOR ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_query_ddl::show_scopes_for(state, identity, &parts));
    }
    if upper.starts_with("RENEW SCOPE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::renew_scope(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCOPE GRANTS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::show_scope_grants(state, identity, &parts));
    }
    if upper.starts_with("SHOW SCOPE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(scope_ddl::show_scopes(state, identity, &parts));
    }

    // Collection introspection: DESCRIBE <collection> / `\D <collection>`,
    // UNDROP COLLECTION|TABLE, SHOW COLLECTIONS, SHOW INDEXES|INDEX. All four
    // parse into typed `CollectionStmt` variants, but the pgwire schema string
    // router dispatched them by string prefix from the raw token slice, using
    // `parts`-based name / filter extraction and the `\D` alias that the typed
    // parser does not reproduce (`\D <coll>` never parses into
    // `DescribeCollection`; bare `\D` parses into `ShowCollections`; the
    // `SHOW INDEXES` typed `collection` field is `parts[2]`, not the handler's
    // `parts[3]` filter). Replicate the string dispatch exactly here, before the
    // parse gate, so the prefix recognition, `parts` extraction, and syntax
    // messages stay byte-identical. `DESCRIBE SEQUENCE` is excluded so it falls
    // through to the typed `DescribeSequence` arm (claimed by the sequence
    // family), exactly as it was before this block existed.
    if (upper.starts_with("DESCRIBE ") && !upper.starts_with("DESCRIBE SEQUENCE"))
        || upper.starts_with("\\D ")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::describe_collection(
            state,
            identity,
            &parts,
            database_id,
        ));
    }
    if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::undrop_collection(
            state,
            identity,
            &parts,
            database_id,
        ));
    }
    if upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(collection::show_collections(state, identity, database_id));
    }
    if upper.starts_with("SHOW INDEXES") || upper.starts_with("SHOW INDEX") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::show_indexes(state, identity, &parts));
    }

    // DROP INDEX <name>. Parses into a typed `CollectionStmt::DropIndex`, but
    // the pgwire schema string router dispatched it by string prefix from the
    // raw token slice (the pgwire typed guards / sync / async arms all returned
    // `None` for it), reading `parts[2]` for the name and handling IF EXISTS
    // inside `drop_index`. Replicate that exactly here, before the parse gate,
    // so the prefix recognition and `parts`-based name extraction stay
    // byte-identical.
    if upper.starts_with("DROP INDEX ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(collection::drop_index(state, identity, &parts, database_id).await);
    }

    None
}
