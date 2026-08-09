// SPDX-License-Identifier: BUSL-1.1

//! Post-create side effects for `build_and_persist`: vector-field
//! auto-config logging and `SERIAL` sequence auto-creation. Relocated
//! verbatim from the pgwire `pgwire::ddl::collection::create::build` module
//! (now deleted).

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::catalog::propose_and_apply;
use super::super::super::super::result::DdlError;

/// INFO-log every detected vector field so operators can see what
/// the engine auto-configured during a CREATE.
pub(super) fn log_vector_fields(collection_name: &str, fields: &[(String, String)]) {
    let vector_fields =
        crate::control::server::shared::ddl::schema_validation::extract_vector_fields(fields);
    for (field_name, _dim, metric) in &vector_fields {
        tracing::info!(
            name = %collection_name,
            field = %field_name,
            %metric,
            "auto-configuring vector field"
        );
    }
}

/// Materialise one `StoredSequence` per `SERIAL` column declared on
/// the new collection. Each sequence rides the same propose+apply
/// path as a standalone `CREATE SEQUENCE` so the OWNERS row lands
/// alongside it.
pub(super) fn create_serial_sequences(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection_name: &str,
    serial_fields: &[String],
    now: u64,
) -> Result<(), DdlError> {
    for field_name in serial_fields {
        let seq_name = format!("{collection_name}_{field_name}_seq");
        let mut seq_def = crate::control::security::catalog::sequence_types::StoredSequence::new(
            identity.tenant_id.as_u64(),
            seq_name.clone(),
            identity.username.clone(),
        );
        seq_def.created_at = now;
        // Route the auto-created sequence through the proposer +
        // local apply path so the OWNERS row lands alongside the
        // sequence row — the same architectural guarantee CREATE
        // SEQUENCE has, applied to SERIAL columns.
        let seq_entry =
            crate::control::catalog_entry::CatalogEntry::PutSequence(Box::new(seq_def.clone()));
        propose_and_apply(state, &seq_entry)?;
        let _ = state.sequence_registry.create(seq_def);
        tracing::info!(
            collection = %collection_name,
            field = %field_name,
            sequence = %seq_name,
            "auto-created SERIAL sequence"
        );
    }
    Ok(())
}
