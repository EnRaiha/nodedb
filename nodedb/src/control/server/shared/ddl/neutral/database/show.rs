// SPDX-License-Identifier: BUSL-1.1

//! Handler for `SHOW DATABASES`.
//!
//! Ported from the pgwire `ddl::database::show` handler. The tenant-admin gate,
//! catalog list, per-database collection count, status mapping, and parent
//! clone rendering are preserved verbatim; only the result construction changed
//! from pgwire `QueryResponse` to the protocol-neutral [`DdlResult`] over
//! `ShapedRows`. Every column is a `text_field` in the original, so all columns
//! stay `Text`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::catalog::database_types::DatabaseStatus;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_tenant_admin;
use super::support::{ddl_err, text_rows};

/// Handle `SHOW DATABASES`.
pub fn show_databases(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show databases")?;

    let catalog = state.credentials.catalog();

    let databases = catalog
        .list_databases()
        .map_err(|e| ddl_err("XX000", format!("catalog list failed: {e}")))?;

    let columns = vec![
        "name".to_string(),
        "status".to_string(),
        "created_at_lsn".to_string(),
        "quota_id".to_string(),
        "collection_count".to_string(),
        "tenant_count".to_string(),
        "parent_clone".to_string(),
    ];

    let mut rows: Vec<Map<String, JsonValue>> = Vec::with_capacity(databases.len());
    for db in &databases {
        let status_str = match db.status {
            DatabaseStatus::Active => "active",
            DatabaseStatus::Deactivated => "deactivated",
            DatabaseStatus::Cloning => "cloning",
            DatabaseStatus::Mirroring => "mirroring",
        };

        let collection_count = catalog
            .load_all_collections(db.id)
            .map(|c| c.len())
            .unwrap_or(0);

        let parent_clone = db
            .parent_clone
            .as_ref()
            .map(|p| format!("db:{}", p.source_db_id.as_u64()))
            .unwrap_or_default();

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(db.name.clone()));
        row.insert(
            "status".to_string(),
            JsonValue::String(status_str.to_string()),
        );
        row.insert(
            "created_at_lsn".to_string(),
            JsonValue::String(db.created_at_lsn.to_string()),
        );
        row.insert(
            "quota_id".to_string(),
            JsonValue::String(db.quota_ref.to_string()),
        );
        row.insert(
            "collection_count".to_string(),
            JsonValue::String(collection_count.to_string()),
        );
        // tenant_count: per-database tenant index not yet wired
        row.insert(
            "tenant_count".to_string(),
            JsonValue::String("0".to_string()),
        );
        row.insert("parent_clone".to_string(), JsonValue::String(parent_clone));
        rows.push(row);
    }

    Ok(text_rows(columns, rows))
}
