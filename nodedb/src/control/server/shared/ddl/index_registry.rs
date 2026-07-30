// SPDX-License-Identifier: BUSL-1.1

//! Proposer helpers for the index identity registry.
//!
//! Every `CREATE [<kind>] INDEX` registers its index here and every drop
//! removes it, through the metadata raft group so all nodes list and resolve
//! the same set. The single-node fallback writes the catalog row directly,
//! mirroring [`super::owner`].

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::{IndexKind, StoredIndexRecord};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::result::DdlError;

fn registry_err(message: String) -> DdlError {
    DdlError {
        sqlstate: "XX000".to_string(),
        message,
    }
}

/// The identity of one index, as its creating statement declared it.
pub struct IndexRegistration<'a> {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub name: &'a str,
    pub kind: IndexKind,
    pub collection: &'a str,
    pub fields: Vec<String>,
}

/// Register an index so it is listable and droppable by name.
pub fn propose_index_record(
    state: &SharedState,
    registration: &IndexRegistration<'_>,
) -> Result<(), DdlError> {
    let record = StoredIndexRecord {
        database_id: registration.database_id.as_u64(),
        tenant_id: registration.tenant_id.as_u64(),
        name: registration.name.to_string(),
        kind: registration.kind,
        collection: registration.collection.to_string(),
        fields: registration.fields.clone(),
        is_active: true,
    };
    let entry = CatalogEntry::PutIndexRecord(Box::new(record.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| registry_err(format!("metadata propose: {e}")))?;
    if log_index == 0 {
        state
            .credentials
            .catalog()
            .put_index_record(&record)
            .map_err(|e| registry_err(format!("catalog write: {e}")))?;
    }
    Ok(())
}

/// Remove an index's identity record.
pub fn propose_delete_index_record(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    name: &str,
    collection: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteIndexRecord {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: name.to_string(),
        collection: collection.to_string(),
    };
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| registry_err(format!("metadata propose: {e}")))?;
    if log_index == 0 {
        state
            .credentials
            .catalog()
            .delete_index_record(database_id.as_u64(), tenant_id.as_u64(), name)
            .map_err(|e| registry_err(format!("catalog write: {e}")))?;
    }
    Ok(())
}
