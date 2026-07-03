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
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    upper: &str,
    _parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // API keys (CREATE / REVOKE / LIST / SHOW API KEY[S]) have been migrated to
    // the protocol-neutral router (`shared::ddl::neutral::apikey`), which is
    // tried before this transitional pgwire delegation runs.

    // Cluster management & observability DDL has been migrated to the
    // protocol-neutral router (`shared::ddl::neutral::cluster`), which is
    // tried before this transitional pgwire delegation runs.

    // Introspection (SHOW USERS / SHOW TENANTS / SHOW ROLES) has been migrated
    // to the protocol-neutral router (`shared::ddl::neutral::inspect`), which is
    // tried before this transitional pgwire delegation runs.

    // Administrative observability — server-wide counters (SHOW STATS / SHOW
    // SERVER STATS), histogram percentiles (SHOW METRICS), and per-engine
    // memory governor state (SHOW MEMORY) — has been migrated to the
    // protocol-neutral router (`shared::ddl::neutral::observability`), which is
    // tried before this transitional pgwire delegation runs.
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
