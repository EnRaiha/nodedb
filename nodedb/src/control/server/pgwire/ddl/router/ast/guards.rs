// SPDX-License-Identifier: BUSL-1.1

//! IF [NOT] EXISTS guard arms: return early on duplicate-creation or not-found-drop.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{CollectionStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Handle IF [NOT] EXISTS guard arms. Returns `Some(result)` if the statement
/// was handled (short-circuit), `None` if it should proceed to typed dispatch.
///
/// `CreateCollection` / `CreateTable`'s `if_not_exists: true` short-circuit
/// (and the `collection_exists` helper it used) moved to the protocol-neutral
/// DDL router (tried before this transitional pgwire delegation runs), which
/// replicates the same existence-check short-circuit in its typed arm.
pub(super) fn try_dispatch_guards(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
    _database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    match stmt {
        // `DropCollection` is fully owned by the sync_ops typed
        // handler, which honours `if_exists` correctly via the
        // existence-check matrix. No guard short-circuit needed.

        // ── IF EXISTS: swallow not-found errors on DROP ──────────
        NodedbStatement::Collection(CollectionStmt::DropIndex {
            if_exists: true, ..
        }) => None,

        _ => None,
    }
}
