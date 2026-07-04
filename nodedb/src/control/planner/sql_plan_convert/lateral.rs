// SPDX-License-Identifier: BUSL-1.1

//! Bridge lowering for `SqlPlan::LateralTopK` and `SqlPlan::LateralLoop`.
//!
//! Both variants embed the outer sub-plan as an `outer_plan: Box<PhysicalPlan>`
//! inside the `QueryOp` so the Data Plane executor can materialise outer rows
//! in-process before iterating over them.

use nodedb_sql::types::{Filter, Projection, SortKey, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{JoinProjection, QueryOp};

use super::convert::ConvertContext;
use super::filter::serialize_filters;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Lower `SqlPlan::LateralTopK` to a `QueryOp::LateralTopK` physical task.
#[allow(clippy::too_many_arguments)]
pub(super) fn convert_lateral_top_k(
    outer: &SqlPlan,
    outer_alias: Option<&str>,
    inner_collection: &str,
    inner_filters: &[Filter],
    inner_order_by: &[SortKey],
    inner_limit: usize,
    correlation_keys: &[(String, String)],
    lateral_alias: &str,
    projection: &[Projection],
    left_join: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let outer_tasks = super::convert::convert_one(outer, tenant_id, ctx)?;
    let outer_task = outer_tasks
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "LateralTopK: outer plan produced no physical tasks".into(),
        })?;
    let outer_vshard = outer_task.vshard_id;
    let outer_collection_name = collection_name_from_plan(outer).unwrap_or_default();
    let outer_alias_str = outer_alias.unwrap_or(&outer_collection_name).to_string();

    let inner_filter_bytes = serialize_filters(inner_filters)?;
    let order_by_spec = sort_keys_to_spec(inner_order_by);
    let join_projection = projection_to_join_projections(projection);
    let inner_coll_qualified = super::convert::db_qualified(ctx.database_id, inner_collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: outer_vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan: Box::new(outer_task.plan),
            outer_alias: outer_alias_str,
            inner_collection: inner_coll_qualified,
            inner_filters: inner_filter_bytes,
            inner_order_by: order_by_spec,
            inner_limit,
            correlation_keys: correlation_keys.to_vec(),
            lateral_alias: lateral_alias.to_string(),
            projection: join_projection,
            left_join,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Lower `SqlPlan::LateralLoop` to a `QueryOp::LateralLoop` physical task.
#[allow(clippy::too_many_arguments)]
pub(super) fn convert_lateral_loop(
    outer: &SqlPlan,
    outer_alias: Option<&str>,
    inner: &SqlPlan,
    correlation_predicates: &[(String, String)],
    lateral_alias: &str,
    projection: &[Projection],
    outer_row_cap: usize,
    left_join: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let outer_tasks = super::convert::convert_one(outer, tenant_id, ctx)?;
    let outer_task = outer_tasks
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "LateralLoop: outer plan produced no physical tasks".into(),
        })?;
    let outer_vshard = outer_task.vshard_id;
    let outer_collection_name = collection_name_from_plan(outer).unwrap_or_default();
    let outer_alias_str = outer_alias.unwrap_or(&outer_collection_name).to_string();

    let inner_collection = collection_name_from_plan(inner).unwrap_or_default();
    let inner_filter_bytes = inner_filters_from_plan(inner)?;
    let join_projection = projection_to_join_projections(projection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: outer_vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan: Box::new(outer_task.plan),
            outer_alias: outer_alias_str,
            inner_collection,
            inner_filters: inner_filter_bytes,
            correlation_predicates: correlation_predicates.to_vec(),
            lateral_alias: lateral_alias.to_string(),
            projection: join_projection,
            left_join,
            outer_row_cap,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Extract the collection name from a scan-like SqlPlan.
pub(super) fn collection_name_from_plan(plan: &SqlPlan) -> Option<String> {
    match plan {
        SqlPlan::Scan { collection, .. }
        | SqlPlan::DocumentIndexLookup { collection, .. }
        | SqlPlan::PointGet { collection, .. } => Some(collection.clone()),
        _ => None,
    }
}

/// Extract base filters from a scan-like SqlPlan.
fn inner_filters_from_plan(plan: &SqlPlan) -> crate::Result<Vec<u8>> {
    match plan {
        SqlPlan::Scan { filters, .. } | SqlPlan::DocumentIndexLookup { filters, .. } => {
            serialize_filters(filters)
        }
        _ => Ok(Vec::new()),
    }
}

/// Convert `SortKey` list to `(field, ascending)` pairs.
///
/// Only `SqlExpr::Column` keys are meaningful for a document scan; other
/// expression forms are skipped (they would be invalid in a plain scan).
fn sort_keys_to_spec(keys: &[SortKey]) -> Vec<(String, bool)> {
    keys.iter()
        .filter_map(|k| match &k.expr {
            SqlExpr::Column { name, .. } => Some((name.clone(), k.ascending)),
            _ => None,
        })
        .collect()
}

/// Convert `Projection` list to `JoinProjection` list.
fn projection_to_join_projections(projection: &[Projection]) -> Vec<JoinProjection> {
    projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(JoinProjection {
                source: name.clone(),
                output: name.clone(),
            }),
            Projection::Computed { alias, .. } => Some(JoinProjection {
                source: alias.clone(),
                output: alias.clone(),
            }),
            Projection::Star | Projection::QualifiedStar(_) => None,
        })
        .collect()
}
