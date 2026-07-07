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

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// Execute a parsed `REINDEX [INDEX name] [CONCURRENTLY] collection` statement.
pub async fn handle_reindex(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    index_name: Option<&str>,
    concurrent: bool,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let collection = collection.to_lowercase();
    let index_name = index_name.map(str::to_lowercase);
    let tenant_id = identity.tenant_id;

    // Verify the collection exists.
    if state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .ok()
        .flatten()
        .is_none()
    {
        return Err(ddl_err(
            "42P01",
            format!("collection \"{collection}\" does not exist"),
        ));
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
        .map_err(|e| ddl_err("XX000", format!("REINDEX CONCURRENTLY failed: {e}")))?;

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

    Ok(vec![DdlResult::Status {
        command: "REINDEX".to_string(),
        rows_affected: None,
    }])
}
