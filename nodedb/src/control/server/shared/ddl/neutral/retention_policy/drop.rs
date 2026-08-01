// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP RETENTION POLICY` DDL handler.
//!
//! Ported from the pgwire `ddl::retention_policy::drop` handler. The registry
//! existence check, the direct catalog delete (`delete_retention_policy`), the
//! CRDT tombstone delta emission, the best-effort continuous-aggregate
//! auto-wire unregistration (warn-and-continue on failure, preserved verbatim
//! because the pgwire handler treated it identically), the in-memory registry
//! unregister, and the audit record are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Syntax:
//! ```sql
//! DROP RETENTION POLICY <name> [ON <collection>]
//! ```

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::require_tenant_admin;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

pub async fn drop_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop retention policies")?;

    let name = name.to_string();
    let tenant_id = identity.tenant_id.as_u64();

    // Verify policy exists and capture definition for cleanup.
    let policy_def = state
        .retention_policy_registry
        .get(database_id.as_u64(), tenant_id, &name)
        .ok_or_else(|| err("42704", format!("retention policy '{name}' does not exist")))?;

    // Delete from catalog.
    let catalog = state.credentials.catalog();

    catalog
        .delete_retention_policy(database_id.as_u64(), tenant_id, &name)
        .map_err(|e| err("XX000", format!("catalog delete: {e}")))?;

    // Emit CRDT tombstone delta.
    {
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            database_id,
            collection: super::RETENTION_POLICIES_CRDT_COLLECTION.into(),
            document_id: name.clone(),
            payload: Vec::new(),
            op: crate::event::crdt_sync::types::DeltaOp::Delete,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(delta);
    }

    // Unregister auto-created continuous aggregates.
    if !policy_def.downsample_tiers().is_empty()
        && let Err(e) = crate::engine::timeseries::retention_policy::autowire::unregister_tiers(
            state,
            &policy_def,
        )
        .await
    {
        tracing::warn!(
            policy = name,
            error = %e,
            "failed to unregister some auto-wired aggregates (continuing drop)"
        );
    }

    let collection = policy_def.collection.clone();

    // Remove from in-memory registry.
    state
        .retention_policy_registry
        .unregister(database_id.as_u64(), tenant_id, &name);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP RETENTION POLICY {name}"),
    );

    tracing::info!(name, %collection, "retention policy dropped");

    Ok(vec![DdlResult::Status {
        command: "DROP RETENTION POLICY".to_string(),
        rows_affected: None,
    }])
}
