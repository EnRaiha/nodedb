// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral custom type DDL handlers.
//!
//! - `CREATE TYPE <name> AS ENUM ('label1', 'label2', ...)`
//! - `CREATE TYPE <name> AS (<field1> <type1>, ...)`
//! - `DROP TYPE [IF EXISTS] <name>`
//! - `ALTER TYPE <name> ADD VALUE 'label'`
//! - `SHOW TYPES`
//!
//! Ported from the pgwire `ddl::custom_type` handlers. All catalog logic
//! (existence pre-checks, OID allocation, drop-protection scan, catalog
//! propose + fallback apply, in-memory registry register/unregister) is
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].
//!
//! Custom types are tenant-scoped. DROP TYPE is blocked when any collection
//! schema references the type. Each type receives a stable u32 OID from the
//! high-numbered range (70000+) so pgwire clients see a recognisable type.

use nodedb_types::DatabaseId;

use serde_json::{Map, Value as JsonValue};

use crate::control::security::catalog::{CompositeField, CustomTypeDef, StoredCustomType};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Build a `DdlError` from a SQLSTATE code and message.
fn err(code: &str, msg: &str) -> DdlError {
    DdlError {
        sqlstate: code.to_string(),
        message: msg.to_string(),
    }
}

/// Build a single-tag status result.
fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Require that the identity is superuser or tenant_admin.
///
/// Folded in verbatim from the pgwire `require_tenant_admin` helper: it does
/// NOT emit an audit record on denial and returns SQLSTATE 42501 with the
/// identical message.
fn require_tenant_admin(identity: &AuthenticatedIdentity, action: &str) -> Result<(), DdlError> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        Ok(())
    } else {
        Err(err(
            "42501",
            &format!("permission denied: only superuser or tenant_admin can {action}"),
        ))
    }
}

/// Handle `CREATE TYPE <name> AS ENUM ('label1', ...)`.
pub fn create_enum_type(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    labels: &[String],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create custom types")?;
    let tenant_id = identity.tenant_id.as_u64();

    if state.custom_type_registry.exists(tenant_id, name) {
        return Err(err("42710", &format!("type '{name}' already exists")));
    }

    let oid = state.custom_type_registry.alloc_oid();
    let created_at = current_epoch_secs()?;
    let stored = StoredCustomType {
        tenant_id,
        name: name.to_string(),
        def: CustomTypeDef::Enum {
            labels: labels.to_vec(),
        },
        oid,
        created_at,
    };

    persist_and_register(state, stored)?;

    Ok(status("CREATE TYPE"))
}

/// Handle `CREATE TYPE <name> AS (<field1> <type1>, ...)`.
pub fn create_composite_type(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    fields: &[(String, String)],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create custom types")?;
    let tenant_id = identity.tenant_id.as_u64();

    if state.custom_type_registry.exists(tenant_id, name) {
        return Err(err("42710", &format!("type '{name}' already exists")));
    }

    let oid = state.custom_type_registry.alloc_oid();
    let created_at = current_epoch_secs()?;
    let composite_fields: Vec<CompositeField> = fields
        .iter()
        .map(|(n, t)| CompositeField {
            name: n.clone(),
            type_name: t.clone(),
        })
        .collect();
    let stored = StoredCustomType {
        tenant_id,
        name: name.to_string(),
        def: CustomTypeDef::Composite {
            fields: composite_fields,
        },
        oid,
        created_at,
    };

    persist_and_register(state, stored)?;

    Ok(status("CREATE TYPE"))
}

/// Handle `DROP TYPE [IF EXISTS] <name>`.
pub fn drop_type(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop custom types")?;
    let tenant_id = identity.tenant_id.as_u64();

    if !state.custom_type_registry.exists(tenant_id, name) {
        if if_exists {
            return Ok(status("DROP TYPE"));
        }
        return Err(err("42704", &format!("type '{name}' does not exist")));
    }

    // Drop-protection: reject if any collection schema references this type.
    let referencing = find_referencing_collections(state, tenant_id, name);
    if !referencing.is_empty() {
        let list = referencing.join(", ");
        return Err(err(
            "2BP01",
            &format!("cannot drop type '{name}': it is referenced by collections: {list}"),
        ));
    }

    let catalog = state.credentials.catalog();

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteCustomType {
        tenant_id,
        name: name.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .delete_custom_type(tenant_id, name)
            .map_err(|e| err("XX000", &format!("catalog delete: {e}")))?;
    }

    state.custom_type_registry.unregister(tenant_id, name);

    Ok(status("DROP TYPE"))
}

/// Handle `ALTER TYPE <name> ADD VALUE 'label'`.
pub fn alter_type_add_value(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    type_name: &str,
    label: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter custom types")?;
    let tenant_id = identity.tenant_id.as_u64();

    let mut stored = state
        .custom_type_registry
        .get(tenant_id, type_name)
        .ok_or_else(|| err("42704", &format!("type '{type_name}' does not exist")))?;

    let labels = match &mut stored.def {
        CustomTypeDef::Enum { labels } => labels,
        CustomTypeDef::Composite { .. } => {
            return Err(err(
                "42809",
                &format!("type '{type_name}' is not an enum type"),
            ));
        }
    };

    if labels.iter().any(|l| l == label) {
        return Err(err(
            "42710",
            &format!("enum label '{label}' already exists in type '{type_name}'"),
        ));
    }

    labels.push(label.to_string());

    persist_and_register(state, stored)?;

    Ok(status("ALTER TYPE"))
}

/// Handle `SHOW TYPES`.
pub fn show_types(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let types = state.custom_type_registry.list_for_tenant(tenant_id);

    let columns = vec![
        "name".to_string(),
        "kind".to_string(),
        "definition".to_string(),
        "oid".to_string(),
    ];

    let mut rows = Vec::with_capacity(types.len());
    for t in &types {
        let (kind, def_str) = type_summary(&t.def);
        let oid_str = t.oid.to_string();
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(t.name.clone()));
        row.insert("kind".to_string(), JsonValue::String(kind));
        row.insert("definition".to_string(), JsonValue::String(def_str));
        row.insert("oid".to_string(), JsonValue::String(oid_str));
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Persist the entry to catalog and register in the in-memory registry.
fn persist_and_register(state: &SharedState, stored: StoredCustomType) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();

    let entry =
        crate::control::catalog_entry::CatalogEntry::PutCustomType(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .put_custom_type(&stored)
            .map_err(|e| err("XX000", &format!("catalog write: {e}")))?;
    }

    state.custom_type_registry.register(stored);
    Ok(())
}

/// Scan all collection schemas for references to `type_name`.
///
/// Collections store field definitions as `(field_name, type_name)` pairs in `fields`.
/// A type is "referenced" when any field's type name matches `type_name`.
fn find_referencing_collections(
    state: &SharedState,
    tenant_id: u64,
    type_name: &str,
) -> Vec<String> {
    let catalog = state.credentials.catalog();
    let collections = match catalog.load_collections_for_tenant(DatabaseId::DEFAULT, tenant_id) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let mut referencing = Vec::new();
    for coll in &collections {
        if coll
            .fields
            .iter()
            .any(|(_field, ty)| ty.eq_ignore_ascii_case(type_name))
        {
            referencing.push(coll.name.clone());
        }
    }
    referencing
}

fn current_epoch_secs() -> Result<u64, DdlError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| err("XX000", "system clock error"))
}

fn type_summary(def: &CustomTypeDef) -> (String, String) {
    match def {
        CustomTypeDef::Enum { labels } => ("enum".to_string(), labels.join(", ")),
        CustomTypeDef::Composite { fields } => {
            let defs: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", f.name, f.type_name))
                .collect();
            ("composite".to_string(), defs.join(", "))
        }
    }
}
