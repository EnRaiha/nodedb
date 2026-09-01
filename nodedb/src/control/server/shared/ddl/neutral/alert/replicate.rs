// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the alert DDL handlers.
//!
//! Every mutation of `_system.alert_rules` proposes a `CatalogEntry`, so each
//! node writes the row and installs the definition in its own `AlertRegistry`.
//! An alert created on one node evaluates on all.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::post_apply::alert_rule as post_apply;
use crate::control::state::SharedState;
use crate::event::alert::types::AlertDef;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Propose the full alert record. CREATE and ALTER both re-put the row.
///
/// The leader validates before proposing, so apply never rejects.
pub(super) fn propose_put(state: &SharedState, def: &AlertDef) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutAlertRule(Box::new(def.clone()));
    propose_and_apply(state, &entry, || {
        state
            .credentials
            .catalog()
            .put_alert_rule(def)
            .map_err(|e| err("XX000", format!("catalog write: {e}")))?;
        post_apply::put(def, state);
        Ok(())
    })
}

/// Propose removal of the alert row, the registry entry, and the hysteresis
/// state on every node.
pub(super) fn propose_delete(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteAlertRule {
        database_id,
        tenant_id,
        name: name.to_string(),
    };
    propose_and_apply(state, &entry, || {
        state
            .credentials
            .catalog()
            .delete_alert_rule(database_id, tenant_id, name)
            .map_err(|e| err("XX000", format!("catalog delete: {e}")))?;
        post_apply::delete(database_id, tenant_id, name, state);
        Ok(())
    })
}
