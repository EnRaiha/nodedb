// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `MiscStmt` (COPY FROM/TO file) and the `ClusterStmt`
//! `AlterRaftGroup` variant.

use nodedb_sql::ddl_ast::statement::{ClusterStmt, MiscStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::cluster;
use super::super::collection;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    database_id: DatabaseId,
    stmt: &NodedbStatement,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        // `COPY <collection> FROM '<path>' [WITH (...)]` bulk import. Ported
        // from the pgwire `ast::async_ops::try_dispatch_async` typed arm.
        NodedbStatement::Misc(MiscStmt::CopyFromFile {
            collection,
            path,
            format,
            delimiter,
            header,
        }) => Some(
            collection::copy_from_file(
                state,
                identity,
                collection,
                path,
                collection::CopyFromOptions {
                    format: format.as_ref(),
                    delimiter: *delimiter,
                    header: *header,
                },
                database_id,
                txn_ctx,
            )
            .await,
        ),

        // `COPY <source> TO '<path>' [WITH (...)]` bulk export. Ported from
        // the pgwire `ast::async_ops::try_dispatch_async` typed arm.
        NodedbStatement::Misc(MiscStmt::CopyToFile {
            source,
            path,
            format,
            delimiter,
            header,
        }) => Some(
            collection::copy_to_file(
                state,
                identity,
                source,
                path,
                collection::CopyToOptions {
                    format: format.as_ref(),
                    delimiter: *delimiter,
                    header: *header,
                },
                database_id,
            )
            .await,
        ),

        NodedbStatement::Cluster(ClusterStmt::AlterRaftGroup {
            group_id,
            action,
            node_id,
        }) => Some(cluster::alter_raft_group(
            state, identity, group_id, action, node_id,
        )),

        _ => None,
    }
}
