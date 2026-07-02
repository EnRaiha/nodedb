// SPDX-License-Identifier: BUSL-1.1

//! Synchronous DDL dispatch arms (no `.await`).

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{
    AuthStmt, AutomationStmt, ClusterStmt, CollectionStmt, DatabaseStmt, NodedbStatement,
    PolicyStmt, StreamViewStmt,
};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::alert::alter_alert;
use crate::control::server::pgwire::ddl::alert::{CreateAlertRequest, create_alert};
use crate::control::server::pgwire::ddl::change_stream::alter_change_stream;
use crate::control::server::pgwire::ddl::cluster::alter_raft_group;
use crate::control::server::pgwire::ddl::collection::drop_collection;
use crate::control::server::pgwire::ddl::consumer_group::create_consumer_group;
use crate::control::server::pgwire::ddl::grant::database_permission::{
    grant_database, revoke_database,
};
use crate::control::server::pgwire::ddl::grant::permission::{grant_permission, revoke_permission};
use crate::control::server::pgwire::ddl::grant::role::{grant_role, revoke_role};
use crate::control::server::pgwire::ddl::inspect::show_permissions;
use crate::control::server::pgwire::ddl::retention_policy::alter_retention_policy;
use crate::control::server::pgwire::ddl::rls::{CreateRlsPolicyRequest, create_rls_policy};
use crate::control::server::pgwire::ddl::role::alter_role_typed;
use crate::control::server::pgwire::ddl::schedule::alter_schedule;
use crate::control::server::pgwire::ddl::trigger::alter_trigger;
use crate::control::server::pgwire::ddl::user::{alter_user, create_user};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::database_ops::try_dispatch_database;

/// Try to dispatch synchronous (non-async) DDL statement variants.
/// Returns `Some(result)` if handled, `None` to fall through.
pub(super) fn try_dispatch_sync(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
    database_id: DatabaseId,
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

        NodedbStatement::Auth(AuthStmt::GrantRole { roles, grantee }) => {
            Some(grant_role(state, identity, roles, grantee))
        }

        NodedbStatement::Auth(AuthStmt::RevokeRole { roles, grantee }) => {
            Some(revoke_role(state, identity, roles, grantee))
        }

        NodedbStatement::Automation(AutomationStmt::AlterAlert { name, action }) => {
            Some(alter_alert(state, identity, database_id, name, action))
        }

        NodedbStatement::StreamView(StreamViewStmt::AlterChangeStream { name, action }) => {
            Some(alter_change_stream(state, identity, name, action))
        }

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

        NodedbStatement::Automation(AutomationStmt::AlterTrigger {
            name,
            action,
            new_owner,
        }) => Some(alter_trigger(
            state,
            identity,
            name,
            action,
            new_owner.as_deref(),
        )),

        NodedbStatement::Cluster(ClusterStmt::AlterRaftGroup {
            group_id,
            action,
            node_id,
        }) => Some(alter_raft_group(state, identity, group_id, action, node_id)),

        NodedbStatement::Auth(AuthStmt::GrantPermission {
            permissions,
            target_type,
            target_name,
            grantee,
        }) => Some(grant_permission(
            state,
            identity,
            permissions,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::Auth(AuthStmt::GrantDatabasePermission {
            permission,
            db_name,
            grantee,
        }) => Some(grant_database(
            state, identity, permission, db_name, grantee,
        )),

        NodedbStatement::Auth(AuthStmt::RevokePermission {
            permissions,
            target_type,
            target_name,
            grantee,
        }) => Some(revoke_permission(
            state,
            identity,
            permissions,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::Auth(AuthStmt::RevokeDatabasePermission {
            permission,
            db_name,
            grantee,
        }) => Some(revoke_database(
            state, identity, permission, db_name, grantee,
        )),

        NodedbStatement::Automation(AutomationStmt::AlterSchedule {
            name,
            action,
            cron_expr,
        }) => Some(alter_schedule(
            state,
            identity,
            name,
            action,
            cron_expr.as_deref(),
        )),

        NodedbStatement::Policy(PolicyStmt::AlterRetentionPolicy {
            name,
            action,
            set_key,
            set_value,
        }) => Some(alter_retention_policy(
            state,
            identity,
            database_id,
            name,
            action,
            set_key.as_deref(),
            set_value.as_deref(),
        )),

        NodedbStatement::StreamView(StreamViewStmt::CreateConsumerGroup {
            group_name,
            stream_name,
        }) => Some(create_consumer_group(
            state,
            identity,
            group_name,
            stream_name,
        )),

        NodedbStatement::Automation(AutomationStmt::CreateAlert {
            name,
            collection,
            where_filter,
            condition_raw,
            group_by,
            window_raw,
            fire_after,
            recover_after,
            severity,
            notify_targets_raw,
        }) => Some(create_alert(
            state,
            identity,
            &CreateAlertRequest {
                name,
                collection,
                where_filter: where_filter.as_deref(),
                condition_raw,
                group_by,
                window_raw,
                fire_after: *fire_after,
                recover_after: *recover_after,
                severity,
                notify_targets_raw,
                database_id,
            },
        )),

        NodedbStatement::Policy(PolicyStmt::CreateRlsPolicy {
            name,
            collection,
            policy_type,
            predicate_raw,
            is_restrictive,
            on_deny_raw,
            tenant_id_override,
        }) => Some(create_rls_policy(
            state,
            identity,
            &CreateRlsPolicyRequest {
                name,
                collection,
                policy_type_raw: policy_type,
                predicate_raw,
                is_restrictive: *is_restrictive,
                on_deny_raw: on_deny_raw.as_deref(),
                tenant_id_override: *tenant_id_override,
            },
        )),

        NodedbStatement::Auth(AuthStmt::CreateUser {
            username,
            password,
            role,
            tenant,
            if_not_exists,
        }) => Some(create_user(
            state,
            identity,
            username,
            password,
            role.as_deref(),
            tenant.as_ref(),
            *if_not_exists,
        )),

        NodedbStatement::Auth(AuthStmt::AlterUser { username, op }) => {
            Some(alter_user(state, identity, username, op))
        }

        NodedbStatement::Auth(AuthStmt::ShowPermissions {
            on_collection,
            for_grantee,
        }) => Some(show_permissions(
            state,
            identity,
            on_collection.as_deref(),
            for_grantee.as_deref(),
        )),

        NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) => {
            Some(alter_role_typed(state, identity, name, sub_op))
        }

        _ => None,
    }
}
