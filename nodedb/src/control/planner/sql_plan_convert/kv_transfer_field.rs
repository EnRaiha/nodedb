// SPDX-License-Identifier: BUSL-1.1

//! The declared type of the field a KV `TRANSFER` moves.

use std::sync::Arc;

use nodedb_sql::SqlCatalog;
use nodedb_sql::types_expr::SqlDataType;

use crate::control::planner::catalog_adapter::OriginCatalog;
use crate::control::planner::plan_error_map::map_plan_error;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Whether the catalog declares `field` of `collection` as `DECIMAL`, with
/// or without a typmod.
///
/// A raw collection, an undeclared field, and a collection the catalog does
/// not hold are not `DECIMAL`.
pub(crate) fn kv_transfer_field_is_decimal(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    field: &str,
) -> crate::Result<bool> {
    let catalog = OriginCatalog::new(
        Arc::clone(&state.credentials),
        state.array_catalog.clone(),
        tenant_id.as_u64(),
        database_id,
        Some(Arc::clone(&state.retention_policy_registry)),
    )
    .with_sequence_registry(Arc::clone(&state.sequence_registry));
    let info = catalog
        .get_collection(database_id, collection)
        .map_err(|e| map_plan_error(e.into(), tenant_id))?;
    Ok(info.is_some_and(|info| {
        info.columns.iter().any(|column| {
            column.name == field && matches!(column.data_type, SqlDataType::Decimal(_))
        })
    }))
}
