// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP ALERT` DDL handler.
//!
//! Ported from the pgwire `ddl::alert::drop` handler. The DIRECT
//! `catalog.delete_alert_rule` write, the `_alert_rules` CRDT-sync tombstone
//! delta, the hysteresis-state cleanup, the in-memory registry unregister, and
//! the `audit_record` call are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `DROP ALERT <name>`

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// CRDT collection name for alert rule sync between Origin and Lite.
const ALERT_RULES_CRDT_COLLECTION: &str = "_alert_rules";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Existence check used by the `DROP ALERT IF EXISTS` short-circuit in the
/// neutral router. Mirrors the pgwire `exists::alert_exists` helper.
pub fn alert_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state
        .alert_registry
        .get(database_id.as_u64(), tid, name)
        .is_some()
}

pub fn drop_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop alerts")?;

    if parts.len() < 3 {
        return Err(err("42601", "syntax: DROP ALERT <name>".to_string()));
    }
    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    if state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, &name)
        .is_none()
    {
        return Err(err("42704", format!("alert '{name}' does not exist")));
    }

    let catalog = state.credentials.catalog();

    catalog
        .delete_alert_rule(database_id.as_u64(), tenant_id, &name)
        .map_err(|e| err("XX000", format!("catalog delete: {e}")))?;

    // Emit CRDT tombstone delta.
    {
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            database_id,
            collection: ALERT_RULES_CRDT_COLLECTION.into(),
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

    Ok(status("DROP ALERT"))
}
