// SPDX-License-Identifier: BUSL-1.1

//! Data Plane dispatch and response shaping for the sorted-index family.
//!
//! Every plan here is hand-built and reaches the Data Plane through
//! `dispatch_utils::dispatch_to_data_plane`, which accepts a trusted internal
//! plan and runs neither the RBAC check nor RLS injection. Authorization
//! therefore happens before a plan gets this far — see [`super::gate`].

use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::super::super::result::{DdlError, DdlResult};
use super::parse::ddl_err;

/// The vShard a sorted index's state lives on.
///
/// A sorted index is keyed by its own name rather than by the collection it
/// covers, so registration, query, and teardown must all route by the same
/// name or they address different cores.
pub(super) fn sorted_index_vshard(index_name: &str) -> VShardId {
    VShardId::from_collection_in_database(DatabaseId::DEFAULT, index_name)
}

/// Remove a sorted index's Data Plane state.
///
/// Shared by `DROP SORTED INDEX` and by the generic `DROP INDEX` teardown, so
/// both remove the same state through the same route.
pub async fn drop_in_engine(
    state: &SharedState,
    tenant_id: TenantId,
    index_name: &str,
) -> Result<(), DdlError> {
    let plan = PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: index_name.to_string(),
    });
    crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        sorted_index_vshard(index_name),
        plan,
        TraceId::ZERO,
    )
    .await
    .map(|_| ())
    .map_err(|e| ddl_err("XX000", e.to_string()))
}

/// Dispatch plan and return a tag response (for DDL).
pub(super) async fn dispatch_and_respond_tag(
    state: &SharedState,
    tenant_id: TenantId,
    vshard: VShardId,
    plan: PhysicalPlan,
    tag: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(_) => Ok(vec![DdlResult::Status {
            command: tag.to_string(),
            rows_affected: None,
        }]),
        Err(e) => Err(ddl_err("XX000", e.to_string())),
    }
}

/// Dispatch plan and return a single-row JSON response.
pub(super) async fn dispatch_and_respond_json(
    state: &SharedState,
    tenant_id: TenantId,
    vshard: VShardId,
    plan: PhysicalPlan,
    col_name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(resp) => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let mut row = Map::new();
            row.insert(col_name.to_string(), JsonValue::String(payload_text));
            Ok(vec![DdlResult::Rows(ShapedRows {
                columns: vec![col_name.to_string()],
                column_types: ShapedRows::text_types(1),
                rows: vec![row],
                notice: None,
            })])
        }
        Err(e) => Err(ddl_err("XX000", e.to_string())),
    }
}

/// Dispatch plan and return multi-row response (for TOPK, RANGE).
pub(super) async fn dispatch_and_respond_rows(
    state: &SharedState,
    tenant_id: TenantId,
    vshard: VShardId,
    plan: PhysicalPlan,
) -> Result<Vec<DdlResult>, DdlError> {
    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(resp) => {
            let rows_json: Vec<serde_json::Value> =
                sonic_rs::from_slice(&resp.payload).unwrap_or_default();

            let mut rows = Vec::with_capacity(rows_json.len());
            for row_json in &rows_json {
                let rank = row_json
                    .get("rank")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0)
                    .to_string();
                let key = row_json
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let mut row = Map::new();
                row.insert("rank".to_string(), JsonValue::String(rank));
                row.insert("key".to_string(), JsonValue::String(key));
                rows.push(row);
            }

            Ok(vec![DdlResult::Rows(ShapedRows {
                columns: vec!["rank".to_string(), "key".to_string()],
                column_types: ShapedRows::text_types(2),
                rows,
                notice: None,
            })])
        }
        Err(e) => Err(ddl_err("XX000", e.to_string())),
    }
}
