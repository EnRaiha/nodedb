// SPDX-License-Identifier: BUSL-1.1

//! ALTER COLLECTION dispatcher: maps `AlterCollectionOp` to individual handlers.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::AlterCollectionOp;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::collection::alter::add_materialized_sum;
use crate::control::server::pgwire::ddl::collection::{
    alter_collection_alter_column_type, alter_collection_drop_column,
    alter_collection_rename_column, alter_collection_set_append_only,
    alter_collection_set_last_value_cache, alter_collection_set_legal_hold,
    alter_collection_set_retention, alter_table_add_column,
};
use crate::control::server::pgwire::ddl::ownership::alter_collection_owner;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

pub(super) async fn dispatch_alter_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _database_id: DatabaseId,
    name: &str,
    operation: &AlterCollectionOp,
) -> PgWireResult<Vec<Response>> {
    match operation {
        AlterCollectionOp::AddColumn {
            column_name,
            column_type,
            not_null,
            default_expr,
        } => {
            let mut col_def = format!("{column_name} {column_type}");
            if *not_null {
                col_def.push_str(" NOT NULL");
            }
            if let Some(def) = default_expr {
                col_def.push_str(&format!(" DEFAULT {def}"));
            }
            alter_table_add_column(state, identity, name, &col_def).await
        }

        AlterCollectionOp::DropColumn { column_name } => {
            alter_collection_drop_column(state, identity, name, column_name).await
        }

        AlterCollectionOp::RenameColumn { old_name, new_name } => {
            alter_collection_rename_column(state, identity, name, old_name, new_name).await
        }

        AlterCollectionOp::AlterColumnType {
            column_name,
            new_type,
        } => alter_collection_alter_column_type(state, identity, name, column_name, new_type).await,

        AlterCollectionOp::OwnerTo { new_owner } => {
            alter_collection_owner(state, identity, name, new_owner)
        }

        AlterCollectionOp::SetRetention { value } => {
            alter_collection_set_retention(state, identity, name, value)
        }

        AlterCollectionOp::SetAppendOnly => alter_collection_set_append_only(state, identity, name),

        AlterCollectionOp::SetLastValueCache { enabled } => {
            alter_collection_set_last_value_cache(state, identity, name, *enabled)
        }

        AlterCollectionOp::SetLegalHold { enabled, tag } => {
            alter_collection_set_legal_hold(state, identity, name, *enabled, tag)
        }

        AlterCollectionOp::AddMaterializedSum {
            target_collection,
            target_column,
            source_collection,
            join_column,
            value_expr,
        } => add_materialized_sum(
            state,
            identity,
            target_collection,
            target_column,
            source_collection,
            join_column,
            value_expr,
        ),

        // SET ON CONFLICT is served by the protocol-neutral DDL router, which is
        // tried before this transitional pgwire delegation runs; this arm is
        // unreachable but kept so the `AlterCollectionOp` match stays exhaustive.
        AlterCollectionOp::SetOnConflict { .. } => {
            Err(crate::control::server::pgwire::types::sqlstate_error(
                "XX000",
                "ALTER COLLECTION ... SET ON CONFLICT is routed by the protocol-neutral DDL router",
            ))
        }
    }
}
