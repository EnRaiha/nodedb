// SPDX-License-Identifier: BUSL-1.1

//! IF [NOT] EXISTS guard arms: return early on duplicate-creation or not-found-drop.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{
    AutomationStmt, CollectionStmt, NodedbStatement, PolicyStmt, StreamViewStmt,
};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::exists::{
    alert_exists, collection_exists, continuous_aggregate_exists, materialized_view_exists,
    retention_policy_exists,
};

/// Handle IF [NOT] EXISTS guard arms. Returns `Some(result)` if the statement
/// was handled (short-circuit), `None` if it should proceed to typed dispatch.
pub(super) fn try_dispatch_guards(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
    database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    match stmt {
        // ── IF NOT EXISTS: swallow duplicate-creation errors ──────
        NodedbStatement::Collection(CollectionStmt::CreateCollection {
            name,
            if_not_exists: true,
            ..
        }) => {
            if collection_exists(state, identity, name, database_id) {
                return Some(Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))]));
            }
            None // fall through to legacy CREATE handler
        }

        NodedbStatement::Collection(CollectionStmt::CreateTable {
            name,
            if_not_exists: true,
            ..
        }) => {
            if collection_exists(state, identity, name, database_id) {
                return Some(Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))]));
            }
            None // fall through to schema dispatcher
        }

        // `DropCollection` is fully owned by the sync_ops typed
        // handler, which honours `if_exists` correctly via the
        // existence-check matrix. No guard short-circuit needed.

        // ── IF EXISTS: swallow not-found errors on DROP ──────────
        NodedbStatement::Collection(CollectionStmt::DropIndex {
            if_exists: true, ..
        }) => None,

        NodedbStatement::Automation(AutomationStmt::DropAlert {
            name,
            if_exists: true,
        }) => {
            if !alert_exists(state, identity, database_id, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("DROP ALERT"))]));
            }
            None
        }

        NodedbStatement::Policy(PolicyStmt::DropRetentionPolicy {
            name,
            if_exists: true,
        }) => {
            if !retention_policy_exists(state, identity, database_id, name) {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP RETENTION POLICY",
                ))]));
            }
            None
        }

        NodedbStatement::StreamView(StreamViewStmt::DropMaterializedView {
            name,
            if_exists: true,
        }) => {
            if !materialized_view_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP MATERIALIZED VIEW",
                ))]));
            }
            None
        }

        NodedbStatement::StreamView(StreamViewStmt::DropContinuousAggregate {
            name,
            if_exists: true,
        }) => {
            if !continuous_aggregate_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP CONTINUOUS AGGREGATE",
                ))]));
            }
            None
        }

        _ => None,
    }
}
