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

    // Organization management (CREATE/ALTER/DROP ORG, SHOW ORGS, SHOW MEMBERS OF
    // ORG) has been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::org_ddl`), which is tried before this transitional
    // pgwire delegation runs.

    // Scope management (DEFINE/DROP/GRANT/REVOKE/ALTER/RENEW SCOPE, SHOW MY
    // SCOPES, SHOW SCOPES FOR, SHOW SCOPE GRANTS, SHOW SCOPE[S]) has been migrated
    // to the protocol-neutral router (`shared::ddl::neutral::scope_ddl` /
    // `shared::ddl::neutral::scope_query_ddl`), which is tried before this
    // transitional pgwire delegation runs.

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

    // Emergency response (EMERGENCY LOCKDOWN / EMERGENCY UNLOCK / BLACKLIST AUTH
    // USERS WHERE) has been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::emergency_ddl`), which is tried before this
    // transitional pgwire delegation runs.

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

    // SHOW SCOPE GRANTS / SHOW SCOPE[S] have been migrated to the
    // protocol-neutral router (`shared::ddl::neutral::scope_ddl`), which is tried
    // before this transitional pgwire delegation runs.

    // Read-only administrative routes (API keys, cluster, introspection,
    // observability, audit) live in observability_routes to keep this file
    // within the project file-size limit.
    if let Some(r) = super::observability_routes::dispatch(state, identity, upper, parts) {
        return Some(r);
    }

    None
}
