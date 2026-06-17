// SPDX-License-Identifier: BUSL-1.1

//! Hash-join converter and the filter/condition merger and bitmap-hint plan
//! synthesis it depends on.

use nodedb_sql::planner::bitmap_emit::predicate::BitmapHint;
use nodedb_sql::types::{Filter, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::aggregate::{extract_join_projection_specs, extract_scan_alias};
use super::super::convert::convert_one;
use super::super::filter::{expr_filter_qualified, serialize_filters};
use super::super::scan_params::JoinPlanParams;
use super::super::value::sql_value_to_string;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Serialize WHERE filters + non-equi join condition into a single `Vec<u8>`.
///
/// The non-equi condition (from the ON clause) is appended as a
/// `FilterOp::Expr` ScanFilter so the join executor evaluates it on
/// merged rows alongside any post-join WHERE filters.
fn serialize_join_filters(
    filters: &[Filter],
    condition: &Option<nodedb_sql::types::SqlExpr>,
) -> crate::Result<Vec<u8>> {
    match condition {
        None => serialize_filters(filters),
        Some(cond) => {
            let mut scan_filters: Vec<nodedb_query::scan_filter::ScanFilter> =
                if !filters.is_empty() {
                    let base = serialize_filters(filters)?;
                    if base.is_empty() {
                        Vec::new()
                    } else {
                        zerompk::from_msgpack(&base).unwrap_or_default()
                    }
                } else {
                    Vec::new()
                };
            scan_filters.push(expr_filter_qualified(cond));
            zerompk::to_msgpack_vec(&scan_filters).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("join filter serialization: {e}"),
            })
        }
    }
}

/// Build a `PhysicalPlan` bitmap-producer sub-plan from a `BitmapHint`.
///
/// Returns `None` for hint shapes that cannot be represented as an
/// `IndexedFetch` (e.g. non-string primary values that have no reasonable
/// index-path encoding). The caller treats `None` as "no bitmap pushdown".
fn bitmap_hint_to_plan(hint: &BitmapHint, database_id: DatabaseId) -> Option<Box<PhysicalPlan>> {
    if !hint.extra_values.is_empty() {
        return None;
    }
    let collection = super::super::convert::db_qualified(database_id, &hint.collection);
    let value_str = sql_value_to_string(&hint.primary_value);
    Some(Box::new(PhysicalPlan::Document(DocumentOp::IndexedFetch {
        collection,
        path: hint.field.clone(),
        value: value_str,
        filters: Vec::new(),
        projection: Vec::new(),
        limit: 10_000,
        offset: 0,
    })))
}

pub(in crate::control::planner::sql_plan_convert) fn convert_join(
    p: JoinPlanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let JoinPlanParams {
        left,
        right,
        on,
        join_type,
        condition,
        limit,
        projection,
        filters,
        tenant_id,
        ctx,
    } = p;
    let mut left_collection =
        super::super::aggregate::join_side_collection(left, p.ctx.database_id);
    let mut right_collection =
        super::super::aggregate::join_side_collection(right, p.ctx.database_id);
    let mut left_alias = extract_scan_alias(left);
    let mut right_alias = extract_scan_alias(right);
    let join_projection = extract_join_projection_specs(projection);
    let filter_bytes = serialize_join_filters(filters, condition)?;

    // Check if the left side is a nested join (multi-way join).
    // If so, convert the inner join to a physical plan and pass it
    // as `left_input` so the executor runs it first. Sharded nested
    // joins are wrapped in Exchange{Broadcast} so the coordinator
    // gathers the nested join result and embeds it.
    let left_input = if matches!(left, SqlPlan::Join { .. }) {
        let inner_tasks = convert_one(left, tenant_id, ctx)?;
        inner_tasks.into_iter().next().map(|t| {
            let plan = t.plan;
            if plan.is_sharded_source() {
                Box::new(PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                    child: Box::new(plan),
                    mode: ExchangeMode::Broadcast,
                })))
            } else {
                Box::new(plan)
            }
        })
    } else {
        // Catalog left scans lower to an embedded `ProviderScan` (returns
        // `Some`); plain user-collection scans stay `None` and are scanned by
        // name via `left_collection`.
        super::super::aggregate::inline_join_side(left, tenant_id, ctx)?
    };
    let right_input = super::super::aggregate::inline_join_side(right, tenant_id, ctx)?;

    // RIGHT JOIN → swap sides and convert to LEFT JOIN.
    let mut on_keys = on.to_vec();
    let mut left_input = left_input;
    let mut right_input = right_input;
    let effective_join_type = if join_type.as_str() == "right" {
        std::mem::swap(&mut left_collection, &mut right_collection);
        std::mem::swap(&mut left_alias, &mut right_alias);
        std::mem::swap(&mut left_input, &mut right_input);
        on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
        "left".to_string()
    } else {
        join_type.as_str().to_string()
    };

    // Analyze join children for selective-predicate bitmap pushdown.
    // The analysis runs on the *original* (pre-swap) children since it inspects
    // SqlPlan shape. After the RIGHT→LEFT swap, we swap the resulting hints too.
    let bitmap_hints = nodedb_sql::planner::bitmap_emit::hashjoin::analyze_join_sides(left, right);
    let (mut raw_left_bm, mut raw_right_bm) = (bitmap_hints.left, bitmap_hints.right);
    if join_type.as_str() == "right" {
        std::mem::swap(&mut raw_left_bm, &mut raw_right_bm);
    }
    let db_id = p.ctx.database_id;
    let left_bitmap = raw_left_bm.and_then(|h| bitmap_hint_to_plan(&h, db_id));
    let right_bitmap = raw_right_bm.and_then(|h| bitmap_hint_to_plan(&h, db_id));

    let vshard = VShardId::from_collection_in_database(p.ctx.database_id, &left_collection);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: p.ctx.database_id,
        plan: PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on: on_keys,
            join_type: effective_join_type,
            // `QueryOp::HashJoin.limit` stays `usize`: `usize::MAX` is the
            // sentinel for "no SQL LIMIT". The handler distinguishes this from
            // an explicit limit and bounds a no-LIMIT join by the memory byte
            // budget (surfacing `ResourcesExhausted`) rather than truncating.
            limit: limit.unwrap_or(usize::MAX),
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: join_projection,
            post_filters: filter_bytes,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
        }),
        post_set_op: PostSetOp::None,
    }])
}
