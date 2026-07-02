// SPDX-License-Identifier: BUSL-1.1

//! Synchronous DDL dispatch arms (no `.await`).

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{
    AuthStmt, ClusterStmt, CollectionStmt, DatabaseStmt, NodedbStatement,
};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::cluster::alter_raft_group;
use crate::control::server::pgwire::ddl::collection::drop_collection;
use crate::control::server::pgwire::ddl::inspect::show_permissions;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::database_ops::try_dispatch_database;

/// Try to dispatch synchronous (non-async) DDL statement variants.
/// Returns `Some(result)` if handled, `None` to fall through.
pub(super) fn try_dispatch_sync(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
    _database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // Database DDL (all synchronous — catalog reads/writes only).
    if let Some(result) = try_dispatch_database(state, identity, stmt) {
        return Some(result);
    }

    match stmt {
        // DROP { COLLECTION | TABLE } [IF EXISTS] <name> [PURGE] [CASCADE]
        // — parser folds both spellings into `DropCollection`. The typed
        // handler honours `if_exists` correctly; previously the text-
        // based dispatcher read `parts[2]` and would treat "IF" as the
        // name.
        NodedbStatement::Collection(CollectionStmt::DropCollection {
            name,
            if_exists,
            purge,
            cascade,
            cascade_force,
        }) => Some(drop_collection(
            state,
            identity,
            name,
            *if_exists,
            *purge,
            *cascade,
            *cascade_force,
        )),

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

        NodedbStatement::Cluster(ClusterStmt::AlterRaftGroup {
            group_id,
            action,
            node_id,
        }) => Some(alter_raft_group(state, identity, group_id, action, node_id)),

        NodedbStatement::Auth(AuthStmt::ShowPermissions {
            on_collection,
            for_grantee,
        }) => Some(show_permissions(
            state,
            identity,
            on_collection.as_deref(),
            for_grantee.as_deref(),
        )),

        _ => None,
    }
}
