// SPDX-License-Identifier: BUSL-1.1

//! Pre-write work of one INSERT row after its BEFORE bodies: sequence
//! defaults, CHECK constraints and enum labels. Typeguards are enforced on
//! the Data Plane, on the body every write form stores.

use std::collections::HashMap;

use nodedb_types::{DatabaseId, Value};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

/// Finish one row: draw a sequence value for each sequence-backed field the
/// row leaves out, then check the row's CHECK constraints and enum labels.
/// Rows run in statement order, so each row draws its own sequence value.
pub(super) async fn prepare_insert_row(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    coll_name: &str,
    mut fields: HashMap<String, Value>,
) -> Result<HashMap<String, Value>, DdlError> {
    let tenant_id = identity.tenant_id;
    let catalog = state.credentials.catalog();
    let Ok(Some(coll_def)) = catalog.get_collection(database_id, tenant_id.as_u64(), coll_name)
    else {
        return Ok(fields);
    };

    for field_def in &coll_def.field_defs {
        let Some(ref seq_name) = field_def.sequence_name else {
            continue;
        };
        if fields.contains_key(&field_def.name) {
            continue;
        }
        let value = state
            .sequence_registry
            .nextval_formatted(
                database_id.as_u64(),
                tenant_id.as_u64(),
                seq_name,
                "",
                &HashMap::new(),
            )
            .map_err(|e| {
                DdlError::from_error(
                    &crate::control::sequence::error_map::sequence_error_to_error(seq_name, e),
                )
            })?;
        let typed = match value {
            crate::control::sequence::registry::SequenceValue::Int(i) => Value::Integer(i),
            crate::control::sequence::registry::SequenceValue::Formatted(s) => Value::String(s),
        };
        fields.insert(field_def.name.clone(), typed);
    }

    // General CHECK constraints (Control Plane enforcement, can have
    // subqueries), after the BEFORE bodies and the sequence values.
    if !coll_def.check_constraints.is_empty() {
        crate::control::server::shared::check_constraint::enforce_check_constraints(
            state,
            identity,
            database_id,
            &coll_def.check_constraints,
            &fields,
        )
        .await?;
    }

    // A user-defined enum type is stored as TEXT, so the Data Plane cannot
    // check its label. The Control Plane checks it here.
    for (field_name, type_name) in &coll_def.fields {
        let Some(Value::String(label)) = fields.get(field_name.as_str()) else {
            continue;
        };
        state
            .custom_type_registry
            .validate_enum_label(database_id.as_u64(), tenant_id.as_u64(), type_name, label)
            .map_err(|msg| DdlError::new("22P02", msg))?;
    }
    Ok(fields)
}
