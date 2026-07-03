// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

pub(in crate::control::server::pgwire::ddl::router) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
    database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // BACKUP/RESTORE TENANT are fully dispatched via typed AST (ast.rs).

    // Schedules (CREATE/DROP/ALTER/SHOW SCHEDULE + SHOW SCHEDULE HISTORY) are
    // served by the protocol-neutral DDL router; the pgwire router no longer
    // routes them.

    // Sequences (CREATE/DROP/ALTER/SHOW/DESCRIBE) are served by the
    // protocol-neutral DDL router; the pgwire router no longer routes them.

    // Maintenance (ANALYZE, COMPACT, REINDEX, SHOW STORAGE, SHOW COMPACTION
    // STATUS, SHOW/ALTER VECTOR INDEX) is served by the protocol-neutral DDL
    // router; the pgwire router no longer routes it.

    // Alerts (CREATE/DROP/ALTER ALERT, SHOW ALERTS, SHOW ALERT STATUS) are
    // served by the protocol-neutral DDL router; the pgwire router no longer
    // routes them.

    // Retention policies (CREATE/DROP/ALTER/SHOW RETENTION POLICY) are served by
    // the protocol-neutral DDL router; the pgwire router no longer routes them.

    // Continuous aggregates (CREATE/DROP/SHOW CONTINUOUS AGGREGATE) are served by
    // the protocol-neutral DDL router; the pgwire router no longer routes them.

    // CONVERT COLLECTION.
    if upper.starts_with("CONVERT COLLECTION ")
        || upper.starts_with("CONVERT ") && upper.contains(" TO ")
    {
        return Some(
            super::super::super::convert::convert_collection(state, identity, database_id, sql)
                .await,
        );
    }

    // Materialized views (HTAP) — CREATE/DROP/REFRESH/SHOW MATERIALIZED VIEW are
    // served by the protocol-neutral DDL router; the pgwire router no longer
    // routes them.

    // Blacklist management (BLACKLIST ..., SHOW BLACKLIST) and auth user
    // management (DEACTIVATE / ALTER AUTH USER, PURGE AUTH USERS, SHOW AUTH
    // USERS) have been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::blacklist` / `shared::ddl::neutral::auth_user`),
    // which is tried before this transitional pgwire delegation runs.

    // Organization management.
    if upper.starts_with("CREATE ORG ")
        || upper.starts_with("ALTER ORG ")
        || upper.starts_with("DROP ORG ")
    {
        return Some(super::super::super::org_ddl::handle_org(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW ORGS") {
        return Some(super::super::super::org_ddl::show_orgs(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW MEMBERS OF ORG") {
        return Some(super::super::super::org_ddl::show_members(
            state, identity, parts,
        ));
    }

    // Scope management.
    if upper.starts_with("DEFINE SCOPE ") {
        return Some(super::super::super::scope_ddl::define_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("DROP SCOPE ") {
        return Some(super::super::super::scope_ddl::drop_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("GRANT SCOPE ") {
        return Some(super::super::super::scope_ddl::grant_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("REVOKE SCOPE ") {
        return Some(super::super::super::scope_ddl::revoke_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("ALTER SCOPE ") {
        return Some(super::super::super::scope_query_ddl::alter_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW MY SCOPES") {
        return Some(super::super::super::scope_query_ddl::show_my_scopes(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW SCOPES FOR ") {
        return Some(super::super::super::scope_query_ddl::show_scopes_for(
            state, identity, parts,
        ));
    }
    if upper.starts_with("RENEW SCOPE ") {
        return Some(super::super::super::scope_ddl::renew_scope(
            state, identity, parts,
        ));
    }

    // EXPLAIN TIERS ON <collection> [RANGE <start> <end>]
    if upper.starts_with("EXPLAIN TIERS ") {
        return Some(super::super::helpers::explain_tiers(
            state,
            identity,
            database_id,
            parts,
        ));
    }

    // EXPLAIN PERMISSION / EXPLAIN SCOPE have been migrated to the
    // protocol-neutral router (`shared::ddl::neutral::explain_ddl`), which is
    // tried before this transitional pgwire delegation runs.

    // Emergency response.
    if upper.starts_with("EMERGENCY LOCKDOWN") {
        return Some(super::super::super::emergency_ddl::emergency_lockdown(
            state, identity, parts,
        ));
    }
    if upper.starts_with("EMERGENCY UNLOCK") {
        return Some(super::super::super::emergency_ddl::emergency_unlock(
            state, identity, parts,
        ));
    }
    if upper.starts_with("BLACKLIST AUTH USERS WHERE") {
        return Some(super::super::super::emergency_ddl::bulk_blacklist(
            state, identity, parts,
        ));
    }

    // Auth-scoped API keys (CREATE / ROTATE / LIST AUTH KEY[S]) have been
    // migrated to the protocol-neutral router
    // (`shared::ddl::neutral::auth_key`), which is tried before this
    // transitional pgwire delegation runs.

    // Impersonation & delegation.
    if upper.starts_with("IMPERSONATE AUTH USER ") {
        return Some(super::super::super::impersonation_ddl::impersonate(
            state, identity, parts,
        ));
    }
    if upper.starts_with("STOP IMPERSONATION") {
        return Some(super::super::super::impersonation_ddl::stop_impersonation(
            state, identity, parts,
        ));
    }
    if upper.starts_with("DELEGATE AUTH USER ") {
        return Some(super::super::super::impersonation_ddl::delegate(
            state, identity, parts,
        ));
    }
    if upper.starts_with("REVOKE DELEGATION ") {
        return Some(super::super::super::impersonation_ddl::revoke_delegation(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW DELEGATIONS") {
        return Some(super::super::super::impersonation_ddl::show_delegations(
            state, identity, parts,
        ));
    }

    // Session management.
    if upper.starts_with("SHOW SESSIONS") {
        return Some(super::super::super::session_ddl::show_sessions(
            state, identity, parts,
        ));
    }
    if upper.starts_with("KILL SESSION ") {
        return Some(super::super::super::session_ddl::kill_session(
            state, identity, parts,
        ));
    }
    if upper.starts_with("KILL USER SESSIONS ") {
        return Some(super::super::super::session_ddl::kill_user_sessions(
            state, identity, parts,
        ));
    }
    if upper.starts_with("VERIFY AUDIT CHAIN") {
        return Some(super::super::super::session_ddl::verify_audit_chain(
            state, identity, parts,
        ));
    }

    // Usage metering (DEFINE METERING DIMENSION, SHOW USAGE [FOR TENANT],
    // EXPORT USAGE, SHOW QUOTA) has been migrated to the protocol-neutral
    // router (`shared::ddl::neutral::metering_ddl`), which is tried before this
    // transitional pgwire delegation runs.

    if upper.starts_with("SHOW SCOPE GRANTS") {
        return Some(super::super::super::scope_ddl::show_scope_grants(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW SCOPE") {
        return Some(super::super::super::scope_ddl::show_scopes(
            state, identity, parts,
        ));
    }

    // Read-only administrative routes (API keys, cluster, introspection,
    // observability, audit) live in observability_routes to keep this file
    // within the project file-size limit.
    if let Some(r) = super::observability_routes::dispatch(state, identity, upper, parts) {
        return Some(r);
    }

    None
}
