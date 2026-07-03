// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral SHOW INDEXES DDL.
//!
//! Ported from the pgwire `ddl::collection::index::show_indexes` handler. The
//! ownership-ledger reads and the optional `ON <collection>` filter are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` to the protocol-neutral `DdlResult` over
//! `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// SHOW INDEXES [ON <collection>]
///
/// Lists indexes for the current tenant (optionally filtered by collection).
pub fn show_indexes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    // Parse optional ON <collection> filter.
    let filter_collection = if parts.len() >= 4
        && parts[1].eq_ignore_ascii_case("INDEXES")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        Some(parts[3])
    } else {
        None
    };

    let columns = vec![
        "index_name".to_string(),
        "type".to_string(),
        "owner".to_string(),
    ];
    let column_types = vec![DdlColType::Text, DdlColType::Text, DdlColType::Text];

    // List all index types for this tenant.
    let index_types = [
        ("index", "btree"),
        ("vector_index", "vector"),
        ("fulltext_index", "fulltext"),
        ("spatial_index", "spatial"),
    ];

    let mut rows = Vec::new();

    for (owner_type, display_type) in &index_types {
        let indexes = state.permissions.list_owners(owner_type, tenant_id);
        for (index_name, owner) in &indexes {
            if let Some(coll) = filter_collection
                && !index_name.starts_with(coll)
            {
                continue;
            }

            let mut row = Map::new();
            row.insert(
                "index_name".to_string(),
                JsonValue::String(index_name.clone()),
            );
            row.insert(
                "type".to_string(),
                JsonValue::String((*display_type).to_string()),
            );
            row.insert("owner".to_string(), JsonValue::String(owner.clone()));
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}
