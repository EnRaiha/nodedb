// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER RETENTION POLICY` DDL handler.
//!
//! ENABLE / DISABLE / SET mutate the loaded record and re-put it. The write is
//! a replicated `PutRetentionPolicy`, so the change reaches every node.
//!
//! Syntax:
//! ```sql
//! ALTER RETENTION POLICY <name> ON <collection> ENABLE | DISABLE
//! ALTER RETENTION POLICY <name> ON <collection> SET AUTO_TIER = TRUE | FALSE
//! ALTER RETENTION POLICY <name> ON <collection> SET EVAL_INTERVAL = '<duration>'
//! ```

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::require_tenant_admin;
use super::replicate::propose_put;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

fn parse_auto_tier(value: &str) -> Result<bool, DdlError> {
    if value.eq_ignore_ascii_case("TRUE") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("FALSE") {
        Ok(false)
    } else {
        Err(err("42601", "AUTO_TIER must be TRUE or FALSE".to_string()))
    }
}

/// Handle `ALTER RETENTION POLICY <name> ENABLE | DISABLE | SET <key> = <value>`.
///
/// `name`, `action`, `set_key`, and `set_value` come from the typed
/// [`PolicyStmt::AlterRetentionPolicy`] variant. `database_id` scopes
/// the in-memory registry lookup to the session's database.
pub fn alter_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    action: &str,
    set_key: Option<&str>,
    set_value: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter retention policies")?;

    let tenant_id = identity.tenant_id.as_u64();

    // Load existing policy.
    let mut def = state
        .retention_policy_registry
        .get(database_id.as_u64(), tenant_id, name)
        .ok_or_else(|| err("42704", format!("retention policy '{name}' does not exist")))?;

    match action {
        "ENABLE" => def.enabled = true,
        "DISABLE" => def.enabled = false,
        "SET" => {
            let key = set_key.unwrap_or("");
            let val = set_value.unwrap_or("");
            match key {
                "AUTO_TIER" => {
                    def.auto_tier = parse_auto_tier(val)?;
                }
                "EVAL_INTERVAL" => {
                    let ms = nodedb_types::kv_parsing::parse_interval_to_ms(val)
                        .map_err(|e| err("42601", format!("invalid interval: {e}")))?;
                    def.eval_interval_ms = ms;
                }
                _ => {
                    return Err(err(
                        "42601",
                        "ALTER RETENTION POLICY SET supports: AUTO_TIER, EVAL_INTERVAL".to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(err("42601", "expected ENABLE, DISABLE, or SET".to_string()));
        }
    }

    // Replicated: every node writes the row and refreshes its registry.
    propose_put(state, &def)?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER RETENTION POLICY {name}"),
    );

    Ok(vec![DdlResult::Status {
        command: "ALTER RETENTION POLICY".to_string(),
        rows_affected: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_tier_parser_accepts_only_boolean_keywords() {
        assert!(parse_auto_tier("TRUE").expect("true parses"));
        assert!(!parse_auto_tier("false").expect("false parses"));
        for value in ["", "1", "yes", "TRUE trailing", " false "] {
            let error = parse_auto_tier(value).expect_err("invalid boolean rejects");
            assert_eq!(error.sqlstate, "42601", "{value}");
        }
    }
}
