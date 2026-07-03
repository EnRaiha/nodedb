// SPDX-License-Identifier: BUSL-1.1

//! Asynchronous DDL dispatch arms (variants that require `.await`).

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{CollectionStmt, MiscStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::collection::copy_from::CopyFromOptions;
use crate::control::server::pgwire::ddl::collection::{
    CreateIndexRequest, copy_from_file, copy_to_file, create_index,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::alter::dispatch_alter_collection;

/// Try to dispatch asynchronous DDL statement variants.
/// Returns `Some(result)` if handled, `None` to fall through to legacy dispatch.
pub(super) async fn try_dispatch_async(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
    database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    match stmt {
        NodedbStatement::Collection(CollectionStmt::CreateIndex {
            unique,
            index_name,
            collection,
            field,
            case_insensitive,
            where_condition,
        }) => Some(
            create_index(
                state,
                identity,
                &CreateIndexRequest {
                    is_unique: *unique,
                    index_name_opt: index_name.as_deref(),
                    collection,
                    field,
                    case_insensitive: *case_insensitive,
                    where_condition: where_condition.as_deref(),
                },
            )
            .await,
        ),

        // CreateCollection / CreateTable are served by the protocol-neutral
        // DDL router (`shared::ddl::neutral::collection::create`), which is
        // tried before this transitional pgwire delegation runs. The
        // `if_not_exists: true` short-circuit lives in the neutral router's
        // typed-arm guard, replicated from this file's former `guards.rs`
        // sibling arms.
        NodedbStatement::Collection(CollectionStmt::AlterCollection { name, operation }) => {
            Some(dispatch_alter_collection(state, identity, database_id, name, operation).await)
        }

        // SHOW CONFLICT POLICY (PolicyStmt::ShowConflictPolicy) is served by the
        // protocol-neutral DDL router; the pgwire router no longer routes it.

        // REINDEX (CollectionStmt::Reindex) is served by the protocol-neutral
        // DDL router; the pgwire router no longer routes it.
        NodedbStatement::Misc(MiscStmt::CopyFromFile {
            collection,
            path,
            format,
            delimiter,
            header,
        }) => Some(
            copy_from_file(
                state,
                identity,
                collection,
                path,
                CopyFromOptions {
                    format: format.as_ref(),
                    delimiter: *delimiter,
                    header: *header,
                },
                database_id,
            )
            .await,
        ),

        NodedbStatement::Misc(MiscStmt::CopyToFile {
            source,
            path,
            format,
            delimiter,
            header,
        }) => Some(
            copy_to_file(
                state,
                identity,
                source,
                path,
                format.as_ref(),
                *delimiter,
                *header,
            )
            .await,
        ),

        // MOVE TENANT is served by the protocol-neutral DDL router
        // (`shared::ddl::neutral::tenant::move_tenant`), which is tried before
        // this transitional pgwire delegation runs.
        _ => None,
    }
}
