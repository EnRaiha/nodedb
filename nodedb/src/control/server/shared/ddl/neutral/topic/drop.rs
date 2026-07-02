// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP TOPIC` DDL handler.
//!
//! Ported from the pgwire `ddl::topic::drop` handler. The tenant-admin gate, the
//! direct `catalog.delete_ep_topic` path (NOT `propose_and_apply` — this family
//! writes the catalog directly), the not-existed error, the
//! `ep_topic_registry.unregister` + `cdc_router.remove_buffer` side effects, and
//! the `audit_record` call are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

pub fn drop_topic(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop topics")?;

    // parts: ["DROP", "TOPIC", "<name>"]
    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected DROP TOPIC <name>".to_string(),
        });
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system catalog not available".to_string(),
        })?;

    let existed = catalog
        .delete_ep_topic(tenant_id, &name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog delete: {e}"),
        })?;

    if !existed {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("topic '{name}' does not exist"),
        });
    }

    state.ep_topic_registry.unregister(tenant_id, &name);

    // Remove the buffer.
    let buffer_key = format!("topic:{name}");
    state.cdc_router.remove_buffer(tenant_id, &buffer_key);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP TOPIC {name}"),
    );

    Ok(status("DROP TOPIC"))
}
