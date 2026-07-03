// SPDX-License-Identifier: BUSL-1.1

mod admin;
mod ast;
mod dsl;
mod engine_ops;
mod helpers;
mod schema;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Try to handle a SQL statement as a Control Plane DDL command.
///
/// These execute directly on the Control Plane without going through
/// DataFusion or the Data Plane. Returns `None` if not recognized.
///
/// Async because DSL commands (SEARCH, CRDT) dispatch to the Data Plane
/// and must await the response without blocking the Tokio runtime.
pub async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // AST-typed fast path: parse once, handle IF [NOT] EXISTS at the
    // dispatch level, then fall through to legacy handlers for the
    // actual execution. This is the incremental migration path —
    // once every legacy handler has been ported to accept a typed
    // NodedbStatement, the string-prefix routers below can be
    // removed entirely.
    match nodedb_sql::ddl_ast::parse(sql) {
        Some(Err(e)) => {
            // UnsupportedConstraint → 0A000 (feature_not_supported).
            // All other parse errors → 42601 (syntax error).
            let sqlstate = match &e {
                nodedb_sql::SqlError::UnsupportedConstraint { .. } => "0A000",
                _ => "42601",
            };
            return Some(Err(super::super::types::sqlstate_error(
                sqlstate,
                &e.to_string(),
            )));
        }
        Some(Ok(stmt)) => {
            if let Some(r) = ast::try_dispatch(state, identity, &stmt, database_id).await {
                return Some(r);
            }
        }
        None => {}
    }

    let upper = sql.to_uppercase();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // Tenant management (CREATE / ALTER / DROP / PURGE TENANT), user/role,
    // service-account, and system-settings string dispatch have all been
    // migrated to the protocol-neutral router (`shared::ddl::neutral`),
    // which is tried before this transitional pgwire delegation runs.

    // Stream/topic consumption (`SELECT * FROM STREAM|TOPIC ... CONSUMER
    // GROUP ...`) and legacy pub/sub (`SUBSCRIBE TO ...`) are served by the
    // protocol-neutral DDL router (`shared::ddl::neutral::stream_select` /
    // `shared::ddl::neutral::topic_subscribe`), which is tried before this
    // transitional pgwire delegation runs.

    if let Some(r) = engine_ops::dispatch(state, identity, sql, &upper, &parts, database_id).await {
        return Some(r);
    }

    if let Some(r) = schema::dispatch(state, identity, sql, &upper, &parts).await {
        return Some(r);
    }

    if let Some(r) = admin::dispatch(state, identity, sql, &upper, &parts, database_id).await {
        return Some(r);
    }

    if let Some(r) = dsl::dispatch(state, identity, sql, &upper, &parts, database_id).await {
        return Some(r);
    }

    None
}
