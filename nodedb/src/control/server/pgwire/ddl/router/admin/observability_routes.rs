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

    // Introspection (SHOW USERS / SHOW TENANTS / SHOW ROLES) has been migrated
    // to the protocol-neutral router (`shared::ddl::neutral::inspect`), which is
    // tried before this transitional pgwire delegation runs.

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
    // SHOW SESSION has been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::inspect`), which is tried before this transitional
    // pgwire delegation runs.
    if upper.starts_with("TRUNCATE AUDIT")
        || upper.starts_with("DELETE AUDIT")
        || upper.starts_with("CLEAR AUDIT")
    {
        return Some(Err(super::super::super::super::types::sqlstate_error(
            "42501",
            "audit log cannot be manually truncated. Entries are pruned automatically by the retention policy (audit_retention_days in config).",
        )));
    }
    // EXPORT AUDIT, SHOW AUDIT IN DATABASE / WHERE / LOG, and SHOW GRANTS have
    // been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::inspect` / `inspect_audit`), which is tried before
    // this transitional pgwire delegation runs.

    None
}
