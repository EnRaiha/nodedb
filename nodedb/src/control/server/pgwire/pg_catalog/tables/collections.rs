// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for catalog table materializers: collection loading and
//! field-type → OID mapping.

use nodedb_types::DatabaseId;
use nodedb_types::columnar::ColumnType;

use crate::control::security::catalog::types::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Load the collections visible to `identity` (all active collections for a
/// superuser, tenant-scoped otherwise).
pub fn load_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Vec<StoredCollection> {
    let Some(catalog) = state.credentials.catalog() else {
        return Vec::new();
    };
    if identity.is_superuser {
        catalog
            .load_all_collections(DatabaseId::DEFAULT)
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.is_active)
            .collect()
    } else {
        catalog
            .load_collections_for_tenant(DatabaseId::DEFAULT, identity.tenant_id.as_u64())
            .unwrap_or_default()
    }
}

/// True if the collection has at least one secondary index (drives
/// `pg_class.relhasindex`, consistent with what `pg_index` reports).
pub fn has_secondary_index(coll: &StoredCollection) -> bool {
    !coll.indexes.is_empty()
}

pub fn field_type_to_oid(field_type: &str) -> i64 {
    if let Ok(ct) = field_type.parse::<ColumnType>() {
        return ct.to_pg_oid() as i64;
    }
    match field_type.to_lowercase().as_str() {
        "int" | "integer" | "int4" => 23,
        "smallint" | "int2" => 21,
        "bigint" | "int8" => 20,
        "float" | "float4" | "real" => 700,
        "double" | "float8" => 701,
        "bool" | "boolean" => 16,
        "varchar" => 1043,
        "date" => 1082,
        "timestamp" => 1114,
        "timestamptz" => 1184,
        "uuid" => 2950,
        "json" => 114,
        "jsonb" => 3802,
        _ => 25,
    }
}
