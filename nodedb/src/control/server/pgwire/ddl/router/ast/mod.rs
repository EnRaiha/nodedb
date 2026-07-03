// SPDX-License-Identifier: BUSL-1.1

mod alter;
mod async_ops;
mod database_ops;
mod guards;
mod sync_ops;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::NodedbStatement;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use async_ops::try_dispatch_async;
use guards::try_dispatch_guards;
use sync_ops::try_dispatch_sync;

/// Try to dispatch a parsed `NodedbStatement`. Returns `Some` if
/// fully handled, `None` if the statement should fall through to
/// the legacy dispatch.
pub(super) async fn try_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
    database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    if let Some(result) = try_dispatch_guards(state, identity, stmt, database_id) {
        return Some(result);
    }
    if let Some(result) = try_dispatch_sync(state, identity, stmt, database_id) {
        return Some(result);
    }
    try_dispatch_async(state, identity, stmt, database_id).await
}
