// SPDX-License-Identifier: BUSL-1.1

//! `REINDEX [CONCURRENTLY] collection` — rebuild indexes.
//!
//! Grammar:
//!   REINDEX [INDEX <name>] [CONCURRENTLY] <collection>
//!
//! Non-concurrent path: dispatches `MetaOp::Checkpoint` (existing semantics).
//! Concurrent path: dispatches `MetaOp::RebuildIndex { concurrent: true }` to
//! every core and awaits the cross-core ACK barrier before returning.
//!
//! The grammar is parsed once by `nodedb_sql::ddl_ast::parse` into
//! `NodedbStatement::Reindex { .. }`; this handler receives the already-parsed
//! fields and never re-tokenises the SQL string.

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_physical::physical_plan::MetaOp;

/// Execute a parsed `REINDEX [INDEX name] [CONCURRENTLY] collection` statement.
pub async fn handle_reindex(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    index_name: Option<&str>,
    concurrent: bool,
    database_id: DatabaseId,
) -> PgWireResult<Vec<Response>> {
    let collection = collection.to_lowercase();
    let index_name = index_name.map(str::to_lowercase);
    let tenant_id = identity.tenant_id;

    // Verify the collection exists.
    if let Some(catalog) = state.credentials.catalog()
        && catalog
            .get_collection(database_id, tenant_id.as_u64(), &collection)
            .ok()
            .flatten()
            .is_none()
    {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("collection \"{collection}\" does not exist"),
        ))));
    }

    if concurrent {
        // Concurrent path: broadcast to all cores and await per-core ACK.
        let plan = crate::bridge::envelope::PhysicalPlan::Meta(MetaOp::RebuildIndex {
            collection: collection.clone(),
            index_name,
            concurrent: true,
        });
        let trace_id = TraceId::generate();
        crate::control::server::broadcast::broadcast_register_to_all_cores(
            state,
            tenant_id,
            database_id,
            plan,
            trace_id,
        )
        .await
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("REINDEX CONCURRENTLY failed: {e}"),
            )))
        })?;

        tracing::info!(
            %collection,
            concurrent = true,
            "REINDEX CONCURRENTLY dispatched and acknowledged by all cores"
        );
    } else {
        // Non-concurrent path: fire-and-forget (same as legacy Checkpoint).
        super::distributed::dispatch_maintenance_to_all_cores(state, tenant_id, MetaOp::Checkpoint);
        tracing::info!(%collection, concurrent = false, "REINDEX dispatched");
    }

    Ok(vec![Response::Execution(Tag::new("REINDEX"))])
}
