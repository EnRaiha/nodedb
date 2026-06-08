// SPDX-License-Identifier: BUSL-1.1

//! Tenant DDL handlers.
//!
//! - [`create`] — `CREATE TENANT` (proposes `CatalogEntry::PutTenant`).
//! - [`alter`] — `ALTER TENANT SET QUOTA` (in-memory; quota is not
//!   part of `StoredTenant` — quota replication is a separate concern).
//! - [`alter_quota`] — `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)` —
//!   persists quota to `_system.tenant_quotas`.
//! - [`drop`] — `DROP TENANT` (proposes `DeleteTenant`).
//! - [`purge`] — `PURGE TENANT <id> CONFIRM` (Data Plane meta op).
//! - [`show`] — `SHOW TENANT USAGE` / `SHOW TENANT QUOTA` reads.
//! - [`show_in_database`] — `SHOW TENANT QUOTA/USAGE FOR <name> IN DATABASE <db>`.

pub mod alter;
pub mod alter_quota;
pub mod create;
pub mod drop;
pub mod move_tenant;
pub mod purge;
pub mod show;
pub mod show_in_database;

pub use alter::alter_tenant;
pub use alter_quota::handle_alter_tenant_quota;
pub use create::create_tenant;
pub use drop::drop_tenant;
pub use move_tenant::handle_move_tenant;
pub use purge::purge_tenant;
pub use show::{show_tenant_quota, show_tenant_usage};
pub use show_in_database::{
    handle_show_tenant_quota_in_database, handle_show_tenant_usage_in_database,
};

use pgwire::error::PgWireResult;

use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::sqlstate_error;

/// Resolve a tenant reference token to a [`TenantId`], accepting either a
/// numeric id or a tenant name.
///
/// The numeric form is the legacy fast path and requires no catalog access;
/// any `u64`-parseable token returns `Ok(Some(TenantId::new(id)))` whether or
/// not that id currently exists (callers that need existence still rely on
/// their own `tenant_exists`-style check).
///
/// A non-numeric token is treated as a tenant name and resolved via
/// [`find_tenant_by_name`]. Single-quoted names are unwrapped, mirroring the
/// AST `TenantSelector` behavior introduced for the CREATE/SHOW paths.
/// `Ok(None)` is returned if the name does not match any tenant, so the
/// caller can decide between `IF EXISTS` no-op success and an explicit
/// `42704` error.
///
/// Errors:
/// - `42601` — empty token (after quote stripping).
/// - `42601` — non-numeric token but catalog is unavailable.
/// - `XX000` — catalog read failure.
///
/// Used by `DROP TENANT`, `ALTER TENANT SET QUOTA`, and `PURGE TENANT` to
/// accept names in addition to numeric ids, parallel to the existing
/// `CREATE TENANT <name>` and `SHOW TENANT <name>` support.
///
/// [`find_tenant_by_name`]:
/// crate::control::security::credential::store::CredentialStore::catalog
pub(super) fn resolve_tenant_ref(
    state: &SharedState,
    token: &str,
) -> PgWireResult<Option<TenantId>> {
    // Numeric id fast path — legacy compatible.
    if let Ok(id) = token.parse::<u64>() {
        return Ok(Some(TenantId::new(id)));
    }
    // Name resolution via catalog.
    let name = token.trim_matches('\'');
    if name.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "TENANT reference must be a numeric id or a tenant name",
        ));
    }
    let catalog = state.credentials.catalog().as_ref().ok_or_else(|| {
        sqlstate_error(
            "42601",
            "cannot resolve tenant by name: catalog unavailable; use numeric id",
        )
    })?;
    Ok(catalog
        .find_tenant_by_name(name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog read: {e}")))?
        .map(|stored| TenantId::new(stored.tenant_id)))
}
