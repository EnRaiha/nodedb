// SPDX-License-Identifier: BUSL-1.1

//! Dispatch arms for database DDL statement variants that remain on the pgwire
//! router.
//!
//! The database-scoped DDL family (CREATE / DROP / ALTER DATABASE, SHOW
//! DATABASES / QUOTA / USAGE / LINEAGE, CLONE / MIRROR / PROMOTE, BACKUP /
//! RESTORE, SHOW DATABASE MIRROR STATUS) has been migrated to the
//! protocol-neutral router (`shared::ddl::neutral::database`), which is tried
//! before this transitional pgwire delegation runs.
//!
//! What remains here:
//!   - The tenant-quota / tenant-usage arms, whose handlers live in the
//!     (not-yet-migrated) tenant family.
//!   - The defensive `UseDatabase` arm (the real handler is intercepted in
//!     `execute_single_sql` before the DDL router; reaching here is a bug).
//!   - `MoveTenant` returns `None` — it is async and handled in
//!     `try_dispatch_async` (async_ops.rs) by the tenant family.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{DatabaseStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::tenant::{
    handle_alter_tenant_quota, handle_show_tenant_quota_in_database,
    handle_show_tenant_usage_in_database,
};
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

/// Try to dispatch a database DDL statement that does NOT require session-store
/// access (i.e. everything except `USE DATABASE`).
///
/// `USE DATABASE` requires the per-handler `SessionStore` and `SocketAddr`
/// and is intercepted in `execute_single_sql` before the DDL router runs.
///
/// Returns `Some(result)` if handled, `None` to fall through.
pub(super) fn try_dispatch_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
) -> Option<PgWireResult<Vec<Response>>> {
    match stmt {
        NodedbStatement::Database(DatabaseStmt::AlterTenant {
            name,
            database,
            operation,
        }) => Some(handle_alter_tenant_quota(
            state, identity, name, database, operation,
        )),

        NodedbStatement::Database(DatabaseStmt::ShowTenantQuotaInDatabase { name, database }) => {
            Some(handle_show_tenant_quota_in_database(
                state, identity, name, database,
            ))
        }

        NodedbStatement::Database(DatabaseStmt::ShowTenantUsageInDatabase { name, database }) => {
            Some(handle_show_tenant_usage_in_database(
                state, identity, name, database,
            ))
        }

        // ShowTenantByIdentifier / ShowTenantsFilteredByName have been migrated
        // to the protocol-neutral router (`shared::ddl::neutral::inspect`), and
        // the database-scoped DDL family to `shared::ddl::neutral::database`;
        // both are tried before this transitional pgwire delegation runs.

        // UseDatabase is handled before the DDL router in execute_single_sql;
        // if it reaches here, something went wrong in the call chain.
        NodedbStatement::Database(DatabaseStmt::UseDatabase { name }) => Some(Err(sqlstate_error(
            "XX000",
            &format!("USE DATABASE {name}: reached router after expected intercept"),
        ))),

        NodedbStatement::Database(DatabaseStmt::MoveTenant { .. }) => {
            // Async — handled in try_dispatch_async (async_ops.rs).
            None
        }

        _ => None,
    }
}
