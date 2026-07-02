// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW MATERIALIZED VIEWS [FOR <source>]` handler.
//!
//! Ported from the pgwire `ddl::materialized_view::show` handler. The catalog
//! read, the optional `FOR <source>` filter, and the exact column set (all five
//! columns `text`) are preserved verbatim; only the result construction changed
//! from pgwire `Response` / `QueryResponse` to the protocol-neutral
//! [`DdlResult::Rows`] over [`ShapedRows`]. All columns are `text`, so
//! `ShapedRows::text_types(5)` reproduces the RowDescription byte-identically.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

pub fn show_materialized_views(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parts[4].to_lowercase())
    } else {
        None
    };

    let columns = vec![
        "name".to_string(),
        "source".to_string(),
        "refresh_mode".to_string(),
        "owner".to_string(),
        "query".to_string(),
    ];

    let views = if let Some(catalog) = state.credentials.catalog() {
        catalog
            .list_materialized_views(tenant_id.as_u64())
            .map_err(|e| err("XX000", format!("catalog read failed: {e}")))?
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    for view in &views {
        if let Some(ref filter) = source_filter
            && view.source != *filter
        {
            continue;
        }

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(view.name.clone()));
        row.insert("source".to_string(), JsonValue::String(view.source.clone()));
        row.insert(
            "refresh_mode".to_string(),
            JsonValue::String(view.refresh_mode.clone()),
        );
        row.insert("owner".to_string(), JsonValue::String(view.owner.clone()));
        row.insert(
            "query".to_string(),
            JsonValue::String(view.query_sql.clone()),
        );
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
