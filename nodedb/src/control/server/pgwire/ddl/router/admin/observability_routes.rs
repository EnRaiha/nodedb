// SPDX-License-Identifier: BUSL-1.1

//! Read-only administrative routes: API keys, cluster management,
//! introspection, observability, and audit-log queries.
//!
//! Split out of the main admin dispatcher to keep each routing file under
//! the project's file-size limit. Called as the final fall-through block
//! from [`super::routes::dispatch`].

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // API keys.
    if upper.starts_with("CREATE API KEY ") {
        return Some(super::super::super::apikey::create_api_key(
            state, identity, parts,
        ));
    }
    if upper.starts_with("REVOKE API KEY ") {
        return Some(super::super::super::apikey::revoke_api_key(
            state, identity, parts,
        ));
    }
    if upper.starts_with("LIST API KEYS") {
        return Some(super::super::super::apikey::list_api_keys(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW API KEYS") {
        return Some(super::super::super::apikey::list_api_keys(
            state, identity, parts,
        ));
    }

    // Cluster management & observability DDL has been migrated to the
    // protocol-neutral router (`shared::ddl::neutral::cluster`), which is
    // tried before this transitional pgwire delegation runs.

    // Introspection.
    if upper.starts_with("SHOW USERS") {
        return Some(super::super::super::inspect::show_users(state, identity));
    }
    // Exact-match only. Filtered forms (`SHOW TENANTS WITH NAME <name>`,
    // `SHOW TENANT <ident>`) are parsed into typed variants and routed
    // through the AST dispatcher; a prefix match here would silently
    // drop the filter and list every tenant.
    if upper == "SHOW TENANTS" {
        return Some(super::super::super::inspect::show_tenants(state, identity));
    }
    if upper == "SHOW ROLES" || upper.starts_with("SHOW ROLES ") {
        return Some(super::super::super::inspect::show_roles(state, identity));
    }

    // Administrative observability — server-wide counters, per-engine
    // memory budgets. `SHOW STATS` and `SHOW SERVER STATS` share a
    // handler (UX synonyms over the same `SystemMetrics` source);
    // `SHOW METRICS` adds histogram percentiles; `SHOW MEMORY`
    // reports per-engine memory governor state.
    if upper == "SHOW SERVER STATS" || upper.starts_with("SHOW SERVER STATS ") {
        return Some(super::super::super::observability::show_server_stats(
            state, identity,
        ));
    }
    if upper == "SHOW STATS" || upper.starts_with("SHOW STATS ") {
        return Some(super::super::super::observability::show_server_stats(
            state, identity,
        ));
    }
    if upper == "SHOW METRICS" || upper.starts_with("SHOW METRICS ") {
        return Some(super::super::super::observability::show_metrics(
            state, identity,
        ));
    }
    if upper == "SHOW MEMORY" || upper.starts_with("SHOW MEMORY ") {
        return Some(super::super::super::observability::show_memory(
            state, identity,
        ));
    }
    if upper.starts_with("SHOW SESSION") {
        return Some(super::super::super::inspect::show_session(identity));
    }
    if upper.starts_with("TRUNCATE AUDIT")
        || upper.starts_with("DELETE AUDIT")
        || upper.starts_with("CLEAR AUDIT")
    {
        return Some(Err(super::super::super::super::types::sqlstate_error(
            "42501",
            "audit log cannot be manually truncated. Entries are pruned automatically by the retention policy (audit_retention_days in config).",
        )));
    }
    if upper.starts_with("EXPORT AUDIT") {
        return Some(super::super::super::inspect::export_audit_log(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW AUDIT IN DATABASE") {
        // SHOW AUDIT IN DATABASE <name> [LIMIT <n>]
        // parts: ["SHOW", "AUDIT", "IN", "DATABASE", "<name>", ...]
        let db_name = if parts.len() >= 5 {
            parts[4]
        } else {
            return Some(Err(super::super::super::super::types::sqlstate_error(
                "42601",
                "syntax: SHOW AUDIT IN DATABASE <name> [LIMIT <n>]",
            )));
        };
        let limit = if parts.len() >= 7 && parts[5].eq_ignore_ascii_case("LIMIT") {
            parts[6].parse::<usize>().unwrap_or(100)
        } else {
            100
        };
        return Some(super::super::super::inspect::show_audit_in_database(
            state, identity, db_name, limit,
        ));
    }
    if upper.starts_with("SHOW AUDIT WHERE") {
        return Some(super::super::super::inspect::show_audit_where(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW AUDIT LOG") || upper.starts_with("SHOW AUDIT_LOG") {
        return Some(super::super::super::inspect::show_audit_log(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW GRANTS") {
        return Some(super::super::super::inspect::show_grants(
            state, identity, parts,
        ));
    }

    None
}
