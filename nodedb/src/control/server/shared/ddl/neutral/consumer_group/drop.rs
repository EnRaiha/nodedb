// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP CONSUMER GROUP` DDL handler.
//!
//! Ported from the pgwire `ddl::consumer_group::drop` handler. The token-based
//! syntax check, the direct `catalog.delete_consumer_group` path (NOT a
//! `propose_catalog_entry` proposal — this family writes the catalog directly),
//! the `group_registry.unregister`, the best-effort `offset_store.delete_group`
//! (warn-and-continue, preserved verbatim as the pre-existing behavior), and the
//! `audit_record` call are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `DROP CONSUMER GROUP <name> ON <stream>`

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Handle `DROP CONSUMER GROUP <name> ON <stream>`
pub fn drop_consumer_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop consumer groups")?;

    // parts: ["DROP", "CONSUMER", "GROUP", "<name>", "ON", "<stream>"]
    if parts.len() < 6 || !parts[4].eq_ignore_ascii_case("ON") {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected DROP CONSUMER GROUP <name> ON <stream>".to_string(),
        });
    }

    let group_name = parts[3].to_lowercase();
    let stream_name = parts[5].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state.credentials.catalog();

    let existed = catalog
        .delete_consumer_group(tenant_id, &stream_name, &group_name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog delete: {e}"),
        })?;

    if !existed {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!(
                "consumer group '{group_name}' does not exist on stream '{stream_name}'"
            ),
        });
    }

    state
        .group_registry
        .unregister(tenant_id, &stream_name, &group_name);

    // Delete committed offsets for this group.
    if let Err(e) = state
        .offset_store
        .delete_group(tenant_id, &stream_name, &group_name)
    {
        tracing::warn!(
            error = %e,
            "failed to delete offsets for consumer group {group_name}"
        );
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(status("DROP CONSUMER GROUP"))
}
