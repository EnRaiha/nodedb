// SPDX-License-Identifier: BUSL-1.1

//! Dispatch arms for database DDL statement variants that remain on the pgwire
//! router.
//!
//! The database-scoped DDL family (CREATE / DROP / ALTER DATABASE, SHOW
//! DATABASES / QUOTA / USAGE / LINEAGE, CLONE / MIRROR / PROMOTE, BACKUP /
//! RESTORE, SHOW DATABASE MIRROR STATUS) has been migrated to the
//! protocol-neutral router (`shared::ddl::neutral::database`), and the
//! tenant-scoped DDL family (`ALTER TENANT ... IN DATABASE ...`,
//! `SHOW TENANT QUOTA|USAGE FOR ... IN DATABASE ...`, `MOVE TENANT ...`) to
//! `shared::ddl::neutral::tenant` — both tried before this transitional
//! pgwire delegation runs.
//!
//! What remains here: the defensive `UseDatabase` arm (the real handler is
//! intercepted in `execute_single_sql` before the DDL router; reaching here
//! is a bug).

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{DatabaseStmt, NodedbStatement};

use super::super::super::super::types::sqlstate_error;

/// Try to dispatch a database DDL statement that does NOT require session-store
/// access (i.e. everything except `USE DATABASE`).
///
/// `USE DATABASE` requires the per-handler `SessionStore` and `SocketAddr`
/// and is intercepted in `execute_single_sql` before the DDL router runs.
///
/// Returns `Some(result)` if handled, `None` to fall through.
pub(super) fn try_dispatch_database(stmt: &NodedbStatement) -> Option<PgWireResult<Vec<Response>>> {
    // UseDatabase is handled before the DDL router in execute_single_sql;
    // if it reaches here, something went wrong in the call chain.
    if let NodedbStatement::Database(DatabaseStmt::UseDatabase { name }) = stmt {
        return Some(Err(sqlstate_error(
            "XX000",
            &format!("USE DATABASE {name}: reached router after expected intercept"),
        )));
    }
    None
}
