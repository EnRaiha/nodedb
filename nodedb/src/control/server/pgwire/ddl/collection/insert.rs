// SPDX-License-Identifier: BUSL-1.1

//! INSERT INTO dispatch for schemaless, KV, and columnar collections.

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::VectorOp;

use super::insert_parse::{
    dispatch_plan, extract_vector_fields, fields_to_insert_sql, fire_before_triggers,
    fire_instead_triggers, fire_sync_after_triggers, parse_write_statement, plan_and_dispatch,
    returning_response,
};
use crate::control::server::pgwire::types::sqlstate_error;

/// INSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
pub async fn insert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let parsed = match parse_write_statement(state, identity, sql, "INSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

    let tenant_id = identity.tenant_id;

    // Fire INSTEAD OF INSERT triggers — if handled, skip normal dispatch.
    if let Some(result) = fire_instead_triggers(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        "INSERT",
    )
    .await
    {
        return Some(result);
    }

    // Fire BEFORE INSERT triggers — may reject via RAISE EXCEPTION, may mutate NEW fields.
    let fields = match fire_before_triggers(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    // Auto-generate sequence values for fields with sequence_name where the
    // INSERT didn't provide an explicit value.
    let mut fields = fields;
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll_def)) =
            catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        for field_def in &coll_def.field_defs {
            if let Some(ref seq_name) = field_def.sequence_name
                && !fields.contains_key(&field_def.name)
            {
                match state.sequence_registry.nextval_formatted(
                    tenant_id.as_u64(),
                    seq_name,
                    "",
                    &std::collections::HashMap::new(),
                ) {
                    Ok(val) => {
                        let typed_val = match val {
                            crate::control::sequence::registry::SequenceValue::Int(i) => {
                                nodedb_types::Value::Integer(i)
                            }
                            crate::control::sequence::registry::SequenceValue::Formatted(s) => {
                                nodedb_types::Value::String(s)
                            }
                        };
                        fields.insert(field_def.name.clone(), typed_val);
                    }
                    Err(e) => {
                        return Some(Err(sqlstate_error(
                            "XX000",
                            &format!("sequence '{seq_name}' error: {e}"),
                        )));
                    }
                }
            }
        }
    }

    // Enforce type guards and CHECK constraints (after BEFORE trigger + sequence injection).
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll_def)) =
            catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        // Inject DEFAULT/VALUE + validate type guards (combined).
        if !coll_def.type_guards.is_empty()
            && let Err(violation) =
                crate::data::executor::enforcement::typeguard::inject_and_validate(
                    &parsed.coll_name,
                    &coll_def.type_guards,
                    &mut fields,
                )
        {
            use crate::control::server::pgwire::types::error_code_to_sqlstate;
            let (severity, code, message) = error_code_to_sqlstate(&violation);
            return Some(Err(pgwire::error::PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(severity.to_owned(), code.to_owned(), message),
            ))));
        }

        // General CHECK constraints (Control Plane enforcement, may have subqueries).
        if !coll_def.check_constraints.is_empty()
            && let Err(e) = super::check_constraint::enforce_check_constraints(
                state,
                tenant_id,
                &coll_def.check_constraints,
                &fields,
            )
            .await
        {
            return Some(Err(e));
        }
    }

    // Validate enum-typed columns against the custom type registry.
    // Collections with user-defined enum types store them physically as TEXT;
    // label validation must happen here in the Control Plane since the Data
    // Plane sees only TEXT.
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll_def)) =
            catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        for (field_name, type_name) in &coll_def.fields {
            if let Some(value) = fields.get(field_name.as_str()) {
                let label = match value {
                    nodedb_types::Value::String(s) => s.as_str(),
                    _ => continue,
                };
                if let Err(msg) = state.custom_type_registry.validate_enum_label(
                    tenant_id.as_u64(),
                    type_name,
                    label,
                ) {
                    return Some(Err(sqlstate_error("22P02", &msg)));
                }
            }
        }
    }

    // Build SQL from fields and route through nodedb-sql → sql_plan_convert.
    // This ensures all engine-type routing goes through the shared EngineRules.
    let insert_sql = fields_to_insert_sql(&parsed.coll_name, &fields);
    if let Err(e) = plan_and_dispatch(state, identity, tenant_id, database_id, &insert_sql).await {
        return Some(Err(e));
    }

    // Track field names in catalog for schemaless collections.
    if parsed
        .collection_type
        .as_ref()
        .is_none_or(|ct| ct.is_schemaless())
        && let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(mut coll)) =
            catalog.get_collection(database_id, tenant_id.as_u64(), &parsed.coll_name)
    {
        let mut changed = false;
        for (name, val) in &fields {
            if name == "id" {
                continue;
            }
            if !coll.fields.iter().any(|(n, _)| n == name) {
                let type_str = match val {
                    nodedb_types::Value::Float(_) => "FLOAT",
                    nodedb_types::Value::Integer(_) => "INT",
                    nodedb_types::Value::Bool(_) => "BOOL",
                    _ => "TEXT",
                };
                coll.fields.push((name.clone(), type_str.to_string()));
                changed = true;
            }
        }
        if changed {
            let _ = catalog.put_collection(database_id, &coll);
        }
    }

    // Fire SYNC AFTER INSERT triggers.
    if let Some(err) =
        fire_sync_after_triggers(state, identity, tenant_id, &parsed.coll_name, &fields).await
    {
        return Some(err);
    }

    // Dispatch VectorInsert for vector fields.
    let vec_vshard =
        crate::types::VShardId::from_collection_in_database(database_id, &parsed.coll_name);
    for (field_name, vector) in extract_vector_fields(&fields) {
        let dim = vector.len();

        if let Some(catalog) = state.credentials.catalog() {
            let col = if field_name.is_empty() {
                "embedding"
            } else {
                field_name.as_str()
            };
            if let Ok(Some(entry)) =
                catalog.get_vector_model(tenant_id.as_u64(), &parsed.coll_name, col)
                && entry.metadata.strict_dimensions
                && entry.metadata.dimensions != dim
            {
                return Some(Err(sqlstate_error(
                    "23514",
                    &format!(
                        "strict_dimensions: vector has {} dimensions, model '{}' requires {}",
                        dim, entry.metadata.model, entry.metadata.dimensions
                    ),
                )));
            }
        }
        let surrogate = match state
            .surrogate_assigner
            .assign(&parsed.coll_name, parsed.doc_id.as_bytes())
        {
            Ok(s) => s,
            Err(e) => {
                return Some(Err(sqlstate_error(
                    "XX000",
                    &format!("surrogate assign: {e}"),
                )));
            }
        };
        let vec_plan = crate::bridge::envelope::PhysicalPlan::Vector(VectorOp::Insert {
            collection: parsed.coll_name.clone(),
            vector,
            dim,
            field_name: field_name.clone(),
            surrogate,
            provenance: None,
        });

        if let Some(err) = dispatch_plan(state, tenant_id, vec_vshard, vec_plan).await {
            return Some(err);
        }
    }

    if parsed.has_returning {
        return Some(returning_response(&parsed.doc_id, &fields));
    }

    Some(Ok(vec![Response::Execution(Tag::new("INSERT"))]))
}

#[cfg(test)]
mod tests {
    use super::super::insert_parse::extract_vector_fields;

    #[test]
    fn extract_vector_fields_keeps_named_numeric_arrays() {
        let fields = std::collections::HashMap::from([
            (
                "embedding".to_string(),
                nodedb_types::Value::Array(vec![
                    nodedb_types::Value::Float(1.0),
                    nodedb_types::Value::Integer(2),
                    nodedb_types::Value::Float(3.5),
                ]),
            ),
            (
                "tags".to_string(),
                nodedb_types::Value::Array(vec![nodedb_types::Value::String("rust".into())]),
            ),
        ]);

        let vectors = extract_vector_fields(&fields);

        assert_eq!(
            vectors,
            vec![("embedding".to_string(), vec![1.0, 2.0, 3.5])]
        );
    }
}
