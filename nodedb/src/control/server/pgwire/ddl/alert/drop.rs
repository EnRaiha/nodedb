// SPDX-License-Identifier: BUSL-1.1

//! `DROP ALERT` DDL handler.
//!
//! Syntax: `DROP ALERT <name>`

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_tenant_admin, sqlstate_error};
use super::ALERT_RULES_CRDT_COLLECTION;

pub fn drop_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "drop alerts")?;

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP ALERT <name>"));
    }
    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    if state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, &name)
        .is_none()
    {
        return Err(sqlstate_error(
            "42704",
            &format!("alert '{name}' does not exist"),
        ));
    }

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .delete_alert_rule(database_id.as_u64(), tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;

    // Emit CRDT tombstone delta.
    {
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            collection: ALERT_RULES_CRDT_COLLECTION.into(),
            document_id: name.clone(),
            payload: Vec::new(),
            op: crate::event::crdt_sync::types::DeltaOp::Delete,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(tenant_id, delta);
    }

    // Clean up hysteresis state.
    state.alert_hysteresis.remove_alert(tenant_id, &name);

    // Remove from registry.
    state
        .alert_registry
        .unregister(database_id.as_u64(), tenant_id, &name);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP ALERT {name}"),
    );

    tracing::info!(name, "alert rule dropped");

    Ok(vec![Response::Execution(Tag::new("DROP ALERT"))])
}
