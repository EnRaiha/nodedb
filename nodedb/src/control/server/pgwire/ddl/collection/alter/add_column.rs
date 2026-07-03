// SPDX-License-Identifier: BUSL-1.1

//! `ALTER {TABLE,COLLECTION} <name> ADD [COLUMN] <def>` — append a column
//! to a strict-document / columnar collection's schema.

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;
use super::super::helpers::parse_origin_column_def;

/// ALTER TABLE/COLLECTION <name> ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT ...]
///
/// All fields arrive pre-parsed:
/// - `table_name`: collection/table name.
/// - `col_def_str`: raw column definition string, e.g. `"email TEXT NOT NULL DEFAULT ''"`.
pub async fn alter_table_add_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    table_name: &str,
    col_def_str: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    let column = parse_origin_column_def(col_def_str)
        .map_err(|e| sqlstate_error("42601", &e.to_string()))?;
    let column_name = column.name.clone();

    // Validate: new column must be nullable or have a default.
    if !column.nullable && column.default.is_none() {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                column.name
            ),
        ));
    }

    let updated = if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), table_name) {
            Ok(Some(coll)) if coll.is_active => {
                if coll.collection_type.is_strict()
                    && let Some(config_json) = &coll.timeseries_config
                    && let Ok(mut schema) =
                        sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(config_json)
                {
                    if schema.columns.iter().any(|c| c.name == column.name) {
                        return Err(sqlstate_error(
                            "42P07",
                            &format!("column '{}' already exists", column.name),
                        ));
                    }
                    let new_version = schema.version.saturating_add(1);
                    let mut col = column;
                    col.added_at_version = new_version;
                    schema.columns.push(col);
                    schema.version = new_version;

                    let mut updated = coll;
                    updated.collection_type = nodedb_types::CollectionType::strict(schema.clone());
                    updated.timeseries_config = sonic_rs::to_string(&schema).ok();
                    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(
                        Box::new(updated.clone()),
                    );
                    super::super::super::catalog_propose::propose_and_apply(state, &entry)?;
                    Some(updated)
                } else {
                    None
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{table_name}' does not exist"),
                ));
            }
        }
    } else {
        None
    };

    if let Some(ref coll) = updated {
        crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored(
            state, coll,
        )
        .await
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER TABLE '{table_name}' ADD COLUMN '{column_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TABLE"))])
}
