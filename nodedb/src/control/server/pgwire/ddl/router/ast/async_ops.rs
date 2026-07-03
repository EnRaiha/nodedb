// SPDX-License-Identifier: BUSL-1.1

//! Asynchronous DDL dispatch arms (variants that require `.await`).

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::NodedbStatement;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Try to dispatch asynchronous DDL statement variants.
/// Returns `Some(result)` if handled, `None` to fall through to legacy dispatch.
///
/// Every variant this file used to own has been migrated to the
/// protocol-neutral DDL router, which is tried before this transitional
/// pgwire delegation runs — see the comments below for where each landed.
pub(super) async fn try_dispatch_async(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    _stmt: &NodedbStatement,
    _database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // CREATE [UNIQUE] INDEX (CreateIndex) is served by the protocol-neutral
    // DDL router (`shared::ddl::neutral::collection::index`).

    // CreateCollection / CreateTable are served by the protocol-neutral DDL
    // router (`shared::ddl::neutral::collection::create`). The
    // `if_not_exists: true` short-circuit lives in the neutral router's
    // typed-arm guard, replicated from this file's former `guards.rs`
    // sibling arms.
    // AlterCollection (every `AlterCollectionOp` variant) is served by the
    // protocol-neutral DDL router (`shared::ddl::neutral::collection::alter`).

    // SHOW CONFLICT POLICY (PolicyStmt::ShowConflictPolicy) is served by the
    // protocol-neutral DDL router.

    // REINDEX (CollectionStmt::Reindex) is served by the protocol-neutral DDL
    // router.

    // CopyFromFile / CopyToFile (`COPY ... FROM/TO '<path>'`) are served by
    // the protocol-neutral DDL router (`shared::ddl::neutral::collection::{
    // copy_from, copy_to}`).

    // MOVE TENANT is served by the protocol-neutral DDL router
    // (`shared::ddl::neutral::tenant::move_tenant`).
    None
}
