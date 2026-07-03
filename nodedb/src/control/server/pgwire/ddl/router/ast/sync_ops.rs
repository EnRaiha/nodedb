// SPDX-License-Identifier: BUSL-1.1

//! Synchronous DDL dispatch arms (no `.await`).

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{DatabaseStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::database_ops::try_dispatch_database;

/// Try to dispatch synchronous (non-async) DDL statement variants.
/// Returns `Some(result)` if handled, `None` to fall through.
pub(super) fn try_dispatch_sync(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
    _database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // Database DDL (all synchronous — catalog reads/writes only).
    if let Some(result) = try_dispatch_database(stmt) {
        return Some(result);
    }

    match stmt {
        // DROP { COLLECTION | TABLE } (DropCollection) is served by the
        // protocol-neutral DDL router (`shared::ddl::neutral::collection::drop`),
        // which is tried before this transitional pgwire delegation runs.
        NodedbStatement::Database(DatabaseStmt::BackupTenant { .. }) => {
            Some(Err(super::super::super::super::types::sqlstate_error(
                "0A000",
                "use `COPY (BACKUP TENANT <id>) TO STDOUT` to stream backup bytes to the client",
            )))
        }

        NodedbStatement::Database(DatabaseStmt::RestoreTenant { .. }) => {
            Some(Err(super::super::super::super::types::sqlstate_error(
                "0A000",
                "use `COPY tenant_restore(<id>) FROM STDIN` to stream backup bytes from the client",
            )))
        }

        // ShowPermissions has been migrated to the protocol-neutral router
        // (`shared::ddl::neutral::inspect`), which is tried before this
        // transitional pgwire delegation runs.
        _ => None,
    }
}
