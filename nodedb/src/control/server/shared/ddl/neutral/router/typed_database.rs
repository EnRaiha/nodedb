// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `DatabaseStmt`: database lifecycle & introspection,
//! tenant-in-database operations, tenant introspection, and tenant/database
//! backup/restore rejections.

use nodedb_sql::ddl_ast::statement::{DatabaseStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::database;
use super::super::inspect;
use super::super::tenant;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    _database_id: DatabaseId,
    stmt: &NodedbStatement,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        // Database DDL family (CREATE / DROP / ALTER DATABASE, SHOW DATABASES /
        // QUOTA / USAGE / LINEAGE, CLONE / MIRROR / PROMOTE, BACKUP / RESTORE,
        // SHOW DATABASE MIRROR STATUS). Migrated from the pgwire typed-AST
        // database router (`database_ops`); all catalog / audit / gate side
        // effects are preserved verbatim in `database`.
        //
        // NOT here: `UseDatabase` (session-coupled, intercepted before the DDL
        // router). `AlterTenant` / `ShowTenantQuotaInDatabase` /
        // `ShowTenantUsageInDatabase` / `MoveTenant` are typed `DatabaseStmt`
        // variants too, but dispatch to the `tenant` family below, not `database`.
        NodedbStatement::Database(DatabaseStmt::CreateDatabase {
            name,
            if_not_exists,
            options,
        }) => Some(database::create::create_database(
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
        }) => Some(database::drop::drop_database(
            state, identity, name, *if_exists, *cascade,
        )),

        NodedbStatement::Database(DatabaseStmt::AlterDatabase { name, operation }) => Some(
            database::alter::alter_database(state, identity, name, operation),
        ),

        NodedbStatement::Database(DatabaseStmt::ShowDatabases) => {
            Some(database::show::show_databases(state, identity))
        }

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseQuota { name }) => Some(
            database::show_quota::show_database_quota(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseUsage { name }) => Some(
            database::show_usage::show_database_usage(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseLineage { name }) => Some(
            database::show_lineage::show_database_lineage(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::CloneDatabase {
            new_name,
            source_name,
            as_of,
        }) => Some(database::clone::clone_database(
            state,
            identity,
            database::clone::CloneDatabaseParams {
                new_name,
                source_name,
                as_of,
            },
        )),

        NodedbStatement::Database(DatabaseStmt::MirrorDatabase {
            local_name,
            source_cluster,
            source_database,
            mode,
        }) => Some(database::mirror::create::mirror_database(
            state,
            identity,
            local_name,
            source_cluster,
            source_database,
            *mode,
        )),

        NodedbStatement::Database(DatabaseStmt::ShowDatabaseMirrorStatus { name }) => Some(
            database::mirror::show::show_database_mirror_status(state, identity, name.as_deref()),
        ),

        NodedbStatement::Database(DatabaseStmt::BackupDatabase { name, .. }) => Some(
            database::backup_restore::backup_database(state, identity, name),
        ),

        NodedbStatement::Database(DatabaseStmt::RestoreDatabase { name, .. }) => Some(
            database::backup_restore::restore_database(state, identity, name),
        ),

        // Tenant DDL family (`ALTER TENANT ... IN DATABASE ... SET QUOTA`,
        // `SHOW TENANT QUOTA|USAGE FOR ... IN DATABASE ...`). These parse into
        // typed `DatabaseStmt` variants and were dispatched from the pgwire
        // typed-AST database router (`database_ops`); all catalog / audit /
        // gate side effects are preserved verbatim in `tenant`.
        NodedbStatement::Database(DatabaseStmt::AlterTenant {
            name,
            database,
            operation,
        }) => Some(tenant::handle_alter_tenant_quota(
            state, identity, name, database, operation,
        )),

        NodedbStatement::Database(DatabaseStmt::ShowTenantQuotaInDatabase { name, database }) => {
            Some(tenant::handle_show_tenant_quota_in_database(
                state, identity, name, database,
            ))
        }

        NodedbStatement::Database(DatabaseStmt::ShowTenantUsageInDatabase { name, database }) => {
            Some(tenant::handle_show_tenant_usage_in_database(
                state, identity, name, database,
            ))
        }

        // `MOVE TENANT <name> FROM <source_db> TO <target_db>` — async,
        // 5-phase re-parenting sequence. Parses into a typed `DatabaseStmt`
        // variant and was dispatched from the pgwire typed-AST async router
        // (`async_ops`); every phase (pre-flight, drain, snapshot, cutover,
        // resume), the journal, and the compensation paths are preserved
        // verbatim in `tenant::move_tenant`.
        NodedbStatement::Database(DatabaseStmt::MoveTenant {
            tenant_name,
            from_db,
            to_db,
        }) => Some(tenant::handle_move_tenant(state, identity, tenant_name, from_db, to_db).await),

        // Tenant introspection by identifier / name filter. These parse into
        // typed `DatabaseStmt` variants and were dispatched from the pgwire
        // typed-AST database router (`database_ops`). The credential / usage
        // reads are preserved verbatim in `inspect`.
        NodedbStatement::Database(DatabaseStmt::ShowTenantByIdentifier { ident }) => {
            Some(inspect::show_tenant_by_identifier(state, identity, ident))
        }

        NodedbStatement::Database(DatabaseStmt::ShowTenantsFilteredByName { name }) => Some(
            inspect::show_tenants_filtered_by_name(state, identity, name),
        ),

        // Tenant backup/restore stream bytes over the COPY protocol; the bare
        // statement forms are rejected so callers use the streaming COPY forms.
        NodedbStatement::Database(DatabaseStmt::BackupTenant { .. }) => Some(Err(DdlError {
            sqlstate: "0A000".to_string(),
            message:
                "use `COPY (BACKUP TENANT <id>) TO STDOUT` to stream backup bytes to the client"
                    .to_string(),
        })),

        NodedbStatement::Database(DatabaseStmt::RestoreTenant { .. }) => Some(Err(DdlError {
            sqlstate: "0A000".to_string(),
            message:
                "use `COPY tenant_restore(<id>) FROM STDIN` to stream backup bytes from the client"
                    .to_string(),
        })),

        // USE DATABASE is intercepted in `execute_single_sql` before the DDL
        // router runs; reaching this arm means the intercept did not fire.
        NodedbStatement::Database(DatabaseStmt::UseDatabase { name }) => Some(Err(DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("USE DATABASE {name}: reached router after expected intercept"),
        })),

        _ => None,
    }
}
