// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER ALERT` DDL handler.
//!
//! Ported from the pgwire `ddl::alert::alter` handler. The registry lookup, the
//! DIRECT `catalog.put_alert_rule` write, the in-memory registry update, and the
//! `audit_record` call are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Syntax:
//! ```sql
//! ALTER ALERT <name> ENABLE
//! ALTER ALERT <name> DISABLE
//! ```

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

pub fn alter_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    action: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter alerts")?;

    let tenant_id = identity.tenant_id.as_u64();

    let mut def = state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, name)
        .ok_or_else(|| err("42704", format!("alert '{name}' does not exist")))?;

    match action {
        "ENABLE" => def.enabled = true,
        "DISABLE" => def.enabled = false,
        _ => return Err(err("42601", "expected ENABLE or DISABLE".to_string())),
    }

    let catalog = state.credentials.catalog();

    catalog
        .put_alert_rule(&def)
        .map_err(|e| err("XX000", format!("catalog write: {e}")))?;

    state.alert_registry.update(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER ALERT {name}"),
    );

    Ok(status("ALTER ALERT"))
}
