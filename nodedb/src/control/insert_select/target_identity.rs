// SPDX-License-Identifier: BUSL-1.1

//! Derive a copied row's identity on the TARGET collection: resolve how the
//! target's primary key maps a row to a surrogate, then assign a fresh,
//! catalog-registered surrogate for it. Shared by the autocommit
//! `INSERT ... SELECT` orchestrator and the COMMIT-time expander.

use nodedb_types::columnar::DocumentMode;
use nodedb_types::{CollectionType, DatabaseId, Surrogate, TenantId, Value};

use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;

/// How the target collection's primary key drives surrogate assignment.
pub(crate) enum TargetPk {
    /// Auto-generated `_rowid` (no declared PK): every copied row gets a fresh,
    /// distinct surrogate — content-addressing an absent key would collapse all
    /// rows onto one surrogate.
    AutoRowId,
    /// A declared / built-in primary-key field: the fresh surrogate is
    /// content-addressed on this field's value, matching a plain `INSERT` so a
    /// later point-get / cross-engine resolve lands on the same identity.
    Field(String),
}

/// Assign a fresh, registered surrogate for one copied row on the TARGET's PK.
pub(crate) fn assign_target_surrogate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    target_collection: &str,
    target_pk: &TargetPk,
    value: &[u8],
) -> crate::Result<Surrogate> {
    match target_pk {
        TargetPk::AutoRowId => {
            state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection)
        }
        TargetPk::Field(field) => match extract_pk_value(value, field) {
            Some(pk) if !pk.is_empty() => state.surrogate_assigner.assign(
                database_id,
                tenant_id,
                target_collection,
                pk.as_bytes(),
            ),
            // No usable key value: mint a fresh unique surrogate rather than
            // collapsing every keyless row onto one binding.
            _ => state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection),
        },
    }
}

/// Resolve how the target collection's primary key maps a copied row to a
/// surrogate, mirroring the plain-`INSERT` identity path.
pub(crate) fn resolve_target_pk(target: &StoredCollection) -> crate::Result<TargetPk> {
    match &target.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            match schema.columns.iter().find(|c| c.primary_key) {
                Some(col) if col.name == "_rowid" => Ok(TargetPk::AutoRowId),
                Some(col) => Ok(TargetPk::Field(col.name.clone())),
                None => Ok(TargetPk::AutoRowId),
            }
        }
        CollectionType::Document(DocumentMode::Schemaless) => Ok(TargetPk::Field(
            target
                .declared_primary_key
                .clone()
                .unwrap_or_else(|| "id".to_string()),
        )),
        CollectionType::KeyValue(_) | CollectionType::Columnar(_) => Err(crate::Error::PlanError {
            detail: format!(
                "INSERT ... SELECT target '{}' must be a document collection",
                target.name
            ),
        }),
    }
}

/// Extract a stringified primary-key value from a stored MessagePack document.
fn extract_pk_value(value: &[u8], field: &str) -> Option<String> {
    let Value::Object(obj) = nodedb_types::value_from_msgpack(value).ok()? else {
        return None;
    };
    value_to_pk_string(obj.get(field)?)
}

/// Stringify a scalar value into its primary-key byte form (mirrors the
/// `sql_value_to_string` convention used by the plain-INSERT identity path).
fn value_to_pk_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Integer(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Decimal(d) => Some(d.to_string()),
        _ => None,
    }
}
