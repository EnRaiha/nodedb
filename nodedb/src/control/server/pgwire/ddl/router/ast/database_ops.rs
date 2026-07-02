// SPDX-License-Identifier: BUSL-1.1

//! Dispatch arms for database DDL statement variants.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{DatabaseStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::database::{
    handle_alter_database, handle_clone_database, handle_create_database, handle_drop_database,
    handle_mirror_database, handle_show_database_lineage, handle_show_database_mirror_status,
    handle_show_database_quota, handle_show_database_usage, handle_show_databases,
};
use crate::control::server::pgwire::ddl::tenant::{
    handle_alter_tenant_quota, handle_show_tenant_quota_in_database,
    handle_show_tenant_usage_in_database,
};
use crate::control::state::SharedState;

use super::super::super::super::types::{
    require_database_owner_or_higher, require_superuser, sqlstate_error,
};

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
        NodedbStatement::Database(DatabaseStmt::CreateDatabase {
            name,
            if_not_exists,
            options,
        }) => Some(handle_create_database(
            state,
            identity,
            name,
            *if_not_exists,
            options,
        )),

        NodedbStatement::Database(DatabaseStmt::DropDatabase {
            name,
            if_exists,
            cascade,
        }) => Some(handle_drop_database(
            state, identity, name, *if_exists, *cascade,
        )),

        NodedbStatement::Database(DatabaseStmt::AlterDatabase { name, operation }) => {
            Some(handle_alter_database(state, identity, name, operation))
        }

        NodedbStatement::Database(DatabaseStmt::ShowDatabases) => {
            Some(handle_show_databases(state, identity))
        }

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseQuota { name }) => {
            Some(handle_show_database_quota(state, identity, name))
        }

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseUsage { name }) => {
            Some(handle_show_database_usage(state, identity, name))
        }

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseLineage { name }) => {
            Some(handle_show_database_lineage(state, identity, name))
        }

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
        // to the protocol-neutral router (`shared::ddl::neutral::inspect`),
        // which is tried before this transitional pgwire delegation runs.

        // UseDatabase is handled before the DDL router in execute_single_sql;
        // if it reaches here, something went wrong in the call chain.
        NodedbStatement::Database(DatabaseStmt::UseDatabase { name }) => Some(Err(sqlstate_error(
            "XX000",
            &format!("USE DATABASE {name}: reached router after expected intercept"),
        ))),

        NodedbStatement::Database(DatabaseStmt::CloneDatabase {
            new_name,
            source_name,
            as_of,
        }) => {
            use crate::control::server::pgwire::ddl::database::clone::CloneDatabaseParams;
            Some(handle_clone_database(
                state,
                identity,
                CloneDatabaseParams {
                    new_name,
                    source_name,
                    as_of,
                },
            ))
        }

        NodedbStatement::Database(DatabaseStmt::MirrorDatabase {
            local_name,
            source_cluster,
            source_database,
            mode,
        }) => Some(handle_mirror_database(
            state,
            identity,
            local_name,
            source_cluster,
            source_database,
            *mode,
        )),

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseMirrorStatus { name }) => Some(
            handle_show_database_mirror_status(state, identity, name.as_deref()),
        ),

        NodedbStatement::Database(DatabaseStmt::MoveTenant { .. }) => {
            // Async — handled in try_dispatch_async (async_ops.rs).
            None
        }

        NodedbStatement::Database(DatabaseStmt::BackupDatabase { name, .. }) => {
            // Gate: DatabaseOwner(db) or higher before the placeholder return.
            // Resolve db_id first; unknown name returns 3D000, not 42501.
            let catalog = match state.credentials.catalog() {
                Some(c) => c,
                None => {
                    return Some(Err(sqlstate_error("XX000", "system catalog unavailable")));
                }
            };
            let db_id = match catalog.get_database_id_by_name(name) {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return Some(Err(sqlstate_error(
                        "3D000",
                        &format!("database '{name}' does not exist"),
                    )));
                }
                Err(e) => {
                    return Some(Err(sqlstate_error(
                        "XX000",
                        &format!("catalog lookup failed: {e}"),
                    )));
                }
            };
            if let Err(e) = require_database_owner_or_higher(
                state,
                identity,
                db_id,
                &format!("BACKUP DATABASE {name}"),
            ) {
                return Some(Err(e));
            }
            Some(Err(sqlstate_error(
                "0A000",
                "BACKUP DATABASE is not yet implemented",
            )))
        }

        NodedbStatement::Database(DatabaseStmt::RestoreDatabase { name, .. }) => {
            // Gate: Superuser required before the placeholder return.
            // The target database may not exist yet; if it doesn't, pass db_id=None.
            let db_id_opt = state
                .credentials
                .catalog()
                .as_ref()
                .and_then(|c| c.get_database_id_by_name(name).ok().flatten());
            if let Err(e) = require_superuser(
                state,
                identity,
                db_id_opt,
                &format!("RESTORE DATABASE {name}"),
            ) {
                return Some(Err(e));
            }
            Some(Err(sqlstate_error(
                "0A000",
                "RESTORE DATABASE is not yet implemented",
            )))
        }

        _ => None,
    }
}
