// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP MATERIALIZED VIEW [IF EXISTS]` handler.
//!
//! Ported from the pgwire `ddl::materialized_view::drop` handler. The DIRECT
//! catalog path (`propose_catalog_entry` for the compound
//! `DeleteMaterializedView` definition+target deletion, with synchronous local
//! apply/reclaim when metadata Raft is absent), the token-based name / IF EXISTS
//! extraction, and the pre-check existence gate are shared by every protocol.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Whether a materialized view exists in the in-memory registry for the
/// identity tenant. Used by the router's IF EXISTS short-circuit guard.
pub fn materialized_view_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.mv_registry.get_def(database_id, tid, name).is_some()
}

pub fn drop_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 {
        return Err(err(
            "42601",
            "syntax: DROP MATERIALIZED VIEW [IF EXISTS] <name>".to_string(),
        ));
    }

    let tenant_id = identity.tenant_id;

    let (name, if_exists) = if parts.len() >= 6
        && parts[3].to_uppercase() == "IF"
        && parts[4].to_uppercase() == "EXISTS"
    {
        (parts[5].to_lowercase(), true)
    } else {
        (parts[3].to_lowercase(), false)
    };

    // Streaming MVs live in the Event-Plane registry (`mv_registry`), not the
    // periodic MV catalog. Handle them first: delete the catalog record and
    // unregister from the live registry. Falls through to the periodic path
    // below when no streaming MV of this name exists, preserving IF EXISTS.
    if state
        .mv_registry
        .get_def(database_id, tenant_id.as_u64(), &name)
        .is_some()
    {
        let entry = crate::control::catalog_entry::CatalogEntry::DeleteStreamingMaterializedView {
            database_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            name: name.clone(),
        };
        let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
            .map_err(|error| err("XX000", format!("metadata propose: {error}")))?;
        crate::control::catalog_entry::apply::local::apply_locally_if_needed(
            state, &entry, log_index,
        );
        if log_index == 0 {
            state
                .mv_registry
                .unregister(database_id, tenant_id.as_u64(), &name);
            state.permissions.install_replicated_remove_owner_in_database(
                crate::control::security::catalog::auth_types::object_type::STREAMING_MATERIALIZED_VIEW,
                database_id.as_u64(),
                tenant_id.as_u64(),
                &name,
            );
        }
        tracing::info!(view = name, "streaming materialized view dropped");
        return Ok(vec![DdlResult::Status {
            command: "DROP MATERIALIZED VIEW".to_string(),
            rows_affected: None,
        }]);
    }

    // Pre-check existence so `IF EXISTS` + missing is a no-op
    // that never touches raft.
    let exists_before = matches!(
        state
            .credentials
            .catalog()
            .get_materialized_view(tenant_id.as_u64(), &name),
        Ok(Some(_))
    );
    if !exists_before && !if_exists {
        return Err(err(
            "42P01",
            format!("materialized view '{name}' does not exist"),
        ));
    }
    if !exists_before {
        return Ok(vec![DdlResult::Status {
            command: "DROP MATERIALIZED VIEW".to_string(),
            rows_affected: None,
        }]);
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteMaterializedView {
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
    };
    let mut local_lifecycle = if state.metadata_raft.get().is_none() {
        Some(
            state
                .quiesce
                .try_acquire_lifecycle(0, tenant_id.as_u64(), &name)
                .ok_or_else(|| {
                    err(
                        "55006",
                        format!("materialized view '{name}' lifecycle is busy"),
                    )
                })?,
        )
    } else {
        None
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|error| err("XX000", format!("metadata propose: {error}")))?;
    if log_index == 0 {
        // No metadata Raft is active, so apply the same compound catalog
        // deletion locally and synchronously reclaim the implementation-owned
        // target collection. A reclaim failure after catalog deletion is
        // fatal: continuing would permit a same-name CREATE over stale rows.
        crate::control::catalog_entry::apply::apply_to(&entry, state.credentials.catalog());
        let purge_lsn = state.wal.next_lsn().as_u64();
        let purge_result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                crate::control::server::shared::ddl::neutral::collection::purge::hard_purge_collection(
                    state,
                    0,
                    tenant_id.as_u64(),
                    &name,
                    purge_lsn,
                    local_lifecycle.is_some(),
                )
                .await
            })
        });
        if let Err(failure) = purge_result {
            // Disarm only when a durable retry record owns the drain; a
            // no-retry failure releases the hold via the guard's unwind Drop.
            if failure.retry_queued
                && let Some(guard) = local_lifecycle.take()
            {
                guard.disarm();
            }
            panic!(
                "local materialized-view target reclaim failed: {}",
                failure.error
            );
        }
    }

    tracing::info!(view = name, "materialized view dropped");

    Ok(vec![DdlResult::Status {
        command: "DROP MATERIALIZED VIEW".to_string(),
        rows_affected: None,
    }])
}
