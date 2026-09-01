// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the retention policy DDL handlers.
//!
//! Every mutation of `_system.retention_policies` proposes a `CatalogEntry`,
//! so each node writes the row and installs the definition in its own
//! `RetentionPolicyRegistry`. A policy created on one node enforces on all.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::post_apply::retention_policy as post_apply;
use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Propose the full policy record. CREATE and ALTER both re-put the row.
///
/// The leader validates before proposing, so apply never rejects.
pub(super) fn propose_put(state: &SharedState, def: &RetentionPolicyDef) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutRetentionPolicy(Box::new(def.clone()));
    propose_and_apply(state, &entry, || {
        state
            .credentials
            .catalog()
            .put_retention_policy(def)
            .map_err(|e| err("XX000", format!("catalog write: {e}")))?;
        post_apply::put(def, state);
        Ok(())
    })
}

/// Propose removal of the policy row and the registry entry on every node.
pub(super) fn propose_delete(
    state: &SharedState,
    def: &RetentionPolicyDef,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteRetentionPolicy {
        database_id: def.database_id,
        tenant_id: def.tenant_id,
        name: def.name.clone(),
        collection: def.collection.clone(),
    };
    propose_and_apply(state, &entry, || {
        state
            .credentials
            .catalog()
            .delete_retention_policy(def.database_id, def.tenant_id, &def.name)
            .map_err(|e| err("XX000", format!("catalog delete: {e}")))?;
        post_apply::delete(def.database_id, def.tenant_id, &def.name, state);
        Ok(())
    })
}
