// SPDX-License-Identifier: BUSL-1.1

//! Post-create side effects for `build_and_persist`: vector-field
//! auto-config logging and `SERIAL` sequence auto-creation. Relocated
//! verbatim from the pgwire `pgwire::ddl::collection::create::build` module
//! (now deleted).

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

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

/// Materialise one `StoredSequence` per `SERIAL` column, via the same
/// propose+apply path as `CREATE SEQUENCE`, gated the same way: shared-registry
/// install only on `needs_local_apply`, so a `Buffered` outcome cannot leak it.
pub(super) fn create_serial_sequences(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection_name: &str,
    serial_fields: &[String],
    now: u64,
) -> Result<(), DdlError> {
    for field_name in serial_fields {
        let seq_name = format!("{collection_name}_{field_name}_seq");
        let mut seq_def = crate::control::security::catalog::sequence_types::StoredSequence::new(
            database_id.as_u64(),
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
        let outcome = propose_and_apply(state, &seq_entry)?;
        if outcome.needs_local_apply() {
            let _ = state.sequence_registry.create(seq_def);
        }
        tracing::info!(
            collection = %collection_name,
            field = %field_name,
            sequence = %seq_name,
            "auto-created SERIAL sequence"
        );
    }
    Ok(())
}
