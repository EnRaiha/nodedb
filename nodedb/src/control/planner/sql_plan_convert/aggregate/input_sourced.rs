// SPDX-License-Identifier: BUSL-1.1

//! Aggregate over an input-sourced body: a derived table, constant result, or
//! union that carries no routing collection. The body lowers to one physical
//! plan, a sharded body is gathered, and one coordinator-local `Aggregate`
//! task runs over the materialized rows.

use nodedb_sql::types::{AggregateExpr, Filter, SqlExpr, SqlPlan};

use crate::types::TenantId;
use nodedb_physical::physical_plan::*;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::body::convert_body_to_single_plan;
use super::super::convert::ConvertContext;
use super::super::filter::serialize_filters;
use super::spec::{InputFieldNames, InputSourcedTaskParams, build_input_sourced_aggregate_task};

pub(super) struct InputSourcedAggregateParams<'a> {
    pub input: &'a SqlPlan,
    pub group_by: &'a [SqlExpr],
    pub aggregates: &'a [AggregateExpr],
    pub having: &'a [Filter],
    pub limit: usize,
    pub grouping_sets: Option<&'a [Vec<usize>]>,
    /// Post-aggregate sort keys, already lowered to bridge specs.
    pub sort_keys: Vec<SortKeySpec>,
    /// SELECT-list output alias of each GROUP BY key, parallel to `group_by`.
    pub group_by_aliases: &'a [Option<String>],
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

/// Lower an aggregate whose input is a materialized relation.
///
/// The body lowers to ONE relation through `convert_body_to_single_plan`: a
/// set-operation body becomes a coordinator-resolved `SetOp`, and a sharded
/// body is wrapped in `Exchange{Gather}` so the coordinator resolves it to a
/// `ProviderScan` before the aggregate runs. The emitted task is
/// coordinator-local: an empty collection keeps it on the coordinator vshard
/// and `is_sharded_source` reports the `Some(input)` aggregate as
/// non-sharded, so it runs once and is never broadcast.
pub(super) fn convert_input_sourced_aggregate(
    p: InputSourcedAggregateParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let InputSourcedAggregateParams {
        input,
        group_by,
        aggregates,
        having,
        limit,
        grouping_sets,
        sort_keys,
        group_by_aliases,
        tenant_id,
        ctx,
    } = p;

    // The body is ONE relation, already gathered when sharded, so the
    // aggregate observes the full union exactly once. A body homed on one
    // core (a timeseries or KV scan) is gathered from its owning vShard when
    // the coordinator materializes the input.
    let child = convert_body_to_single_plan(input, tenant_id, ctx)?;

    let having_bytes = serialize_filters(having)?;
    let grouping_sets = grouping_sets
        .unwrap_or(&[])
        .iter()
        .map(|set| set.iter().map(|&i| i as u32).collect())
        .collect();

    Ok(vec![build_input_sourced_aggregate_task(
        InputSourcedTaskParams {
            tenant_id,
            ctx,
            raw_collection: String::new(),
            child,
            group_by,
            aggregates,
            having_bytes,
            limit,
            grouping_sets,
            sort_keys,
            group_by_aliases,
            names: input_field_names(input),
        },
    )?])
}

/// How the rows `input` produces key their fields.
///
/// A join with no projection emits its merged rows, which key every field
/// `qualifier.column`. Every other body emits rows keyed by output column
/// name.
fn input_field_names(input: &SqlPlan) -> InputFieldNames {
    match input {
        SqlPlan::Join { projection, .. } if projection.is_empty() => InputFieldNames::Qualified,
        _ => InputFieldNames::Bare,
    }
}
