// SPDX-License-Identifier: BUSL-1.1

//! IF [NOT] EXISTS guard arms: return early on duplicate-creation or not-found-drop.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::NodedbStatement;

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
/// `DropCollection` and `DropIndex` are likewise served by the protocol-neutral
/// router now (`DropIndex` handled IF EXISTS inside its own handler), so no
/// guard short-circuit remains here.
pub(super) fn try_dispatch_guards(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    _stmt: &NodedbStatement,
    _database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    None
}
