// SPDX-License-Identifier: BUSL-1.1

//! `ALTER RETENTION POLICY` DDL handler.
//!
//! Syntax:
//! ```sql
//! ALTER RETENTION POLICY <name> ON <collection> ENABLE | DISABLE
//! ALTER RETENTION POLICY <name> ON <collection> SET AUTO_TIER = TRUE | FALSE
//! ALTER RETENTION POLICY <name> ON <collection> SET EVAL_INTERVAL = '<duration>'
//! ```

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_tenant_admin, sqlstate_error};

/// Handle `ALTER RETENTION POLICY <name> ENABLE | DISABLE | SET <key> = <value>`.
///
/// `name`, `action`, `set_key`, and `set_value` come from the typed
/// [`NodedbStatement::AlterRetentionPolicy`] variant. `database_id` scopes
/// the in-memory registry lookup to the session's database.
pub fn alter_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    action: &str,
    set_key: Option<&str>,
    set_value: Option<&str>,
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "alter retention policies")?;

    let tenant_id = identity.tenant_id.as_u64();

    // Load existing policy.
    let mut def = state
        .retention_policy_registry
        .get(database_id.as_u64(), tenant_id, name)
        .ok_or_else(|| {
            sqlstate_error(
                "42704",
                &format!("retention policy '{name}' does not exist"),
            )
        })?;

    match action {
        "ENABLE" => def.enabled = true,
        "DISABLE" => def.enabled = false,
        "SET" => {
            let key = set_key.unwrap_or("");
            let val = set_value.unwrap_or("");
            match key {
                "AUTO_TIER" => {
                    def.auto_tier = val.eq_ignore_ascii_case("TRUE");
                }
                "EVAL_INTERVAL" => {
                    let ms = nodedb_types::kv_parsing::parse_interval_to_ms(val)
                        .map_err(|e| sqlstate_error("42601", &format!("invalid interval: {e}")))?;
                    def.eval_interval_ms = ms;
                }
                _ => {
                    return Err(sqlstate_error(
                        "42601",
                        "ALTER RETENTION POLICY SET supports: AUTO_TIER, EVAL_INTERVAL",
                    ));
                }
            }
        }
        _ => {
            return Err(sqlstate_error("42601", "expected ENABLE, DISABLE, or SET"));
        }
    }

    // Persist updated policy.
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_retention_policy(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    // Update in-memory registry.
    state.retention_policy_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER RETENTION POLICY {name}"),
    );

    Ok(vec![Response::Execution(Tag::new(
        "ALTER RETENTION POLICY",
    ))])
}
