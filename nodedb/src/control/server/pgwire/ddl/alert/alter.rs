// SPDX-License-Identifier: BUSL-1.1

//! `ALTER ALERT` DDL handler.
//!
//! Syntax:
//! ```sql
//! ALTER ALERT <name> ENABLE
//! ALTER ALERT <name> DISABLE
//! ```

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_tenant_admin, sqlstate_error};

pub fn alter_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    action: &str,
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "alter alerts")?;

    let tenant_id = identity.tenant_id.as_u64();

    let mut def = state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, name)
        .ok_or_else(|| sqlstate_error("42704", &format!("alert '{name}' does not exist")))?;

    match action {
        "ENABLE" => def.enabled = true,
        "DISABLE" => def.enabled = false,
        _ => return Err(sqlstate_error("42601", "expected ENABLE or DISABLE")),
    }

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_alert_rule(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.alert_registry.update(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER ALERT {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER ALERT"))])
}
