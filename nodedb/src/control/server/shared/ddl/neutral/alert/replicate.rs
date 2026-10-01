// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the alert DDL handlers.
//!
//! Every mutation of `_system.alert_rules` proposes a `CatalogEntry`, so each
//! node writes the row and installs the definition in its own `AlertRegistry`.
//! An alert created on one node evaluates on all.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;
use crate::event::alert::types::AlertDef;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply_async;

/// Propose the full alert record. CREATE and ALTER both re-put the row.
///
/// The leader validates before proposing, so apply never rejects.
pub(super) async fn propose_put(state: &SharedState, def: &AlertDef) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutAlertRule(Box::new(def.clone()));
    propose_and_apply_async(state, &entry).await
}

/// Propose removal of the alert row, the registry entry, and the hysteresis
/// state on every node.
pub(super) async fn propose_delete(
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
    propose_and_apply_async(state, &entry).await
}
