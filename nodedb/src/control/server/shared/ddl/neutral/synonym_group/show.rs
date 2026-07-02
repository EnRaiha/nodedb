// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW SYNONYM GROUPS` handler.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW SYNONYM GROUPS`.
pub fn show_synonym_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id_u64 = identity.tenant_id.as_u64();
    let groups = state.synonym_registry.list_for_tenant(tenant_id_u64);

    let columns = vec!["name".to_string(), "terms".to_string()];

    let mut rows = Vec::with_capacity(groups.len());
    for g in &groups {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(g.name.clone()));
        let terms_csv = g.terms.join(", ");
        row.insert("terms".to_string(), JsonValue::String(terms_csv));
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
