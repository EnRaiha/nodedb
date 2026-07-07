// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for aggregate and lateral join variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_aggregate_lateral_arms_for_convert_visitor {
    () => {
        fn aggregate(
            &mut self,
            input: &nodedb_sql::types::SqlPlan,
            group_by: &[nodedb_sql::types_expr::SqlExpr],
            aggregates: &[nodedb_sql::types::query::AggregateExpr],
            having: &[nodedb_sql::types::filter::Filter],
            limit: usize,
            grouping_sets: Option<&[Vec<usize>]>,
            sort_keys: &[nodedb_sql::types::query::SortKey],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::aggregate::convert_aggregate(
                super::super::aggregate::ConvertAggregateParams {
                    input,
                    group_by,
                    aggregates,
                    having,
                    limit,
                    grouping_sets,
                    sort_keys,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                },
            )
        }

        fn lateral_top_k(
            &mut self,
            outer: &nodedb_sql::types::SqlPlan,
            outer_alias: Option<&str>,
            inner_collection: &str,
            inner_filters: &[nodedb_sql::types::filter::Filter],
            inner_order_by: &[nodedb_sql::types::query::SortKey],
            inner_limit: usize,
            correlation_keys: &[(String, String)],
            lateral_alias: &str,
            projection: &[nodedb_sql::types::query::Projection],
            left_join: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::lateral::convert_lateral_top_k(
                super::super::lateral::ConvertLateralTopKParams {
                    outer,
                    outer_alias,
                    inner_collection,
                    inner_filters,
                    inner_order_by,
                    inner_limit,
                    correlation_keys,
                    lateral_alias,
                    projection,
                    left_join,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                },
            )
        }

        fn lateral_loop(
            &mut self,
            outer: &nodedb_sql::types::SqlPlan,
            outer_alias: Option<&str>,
            inner: &nodedb_sql::types::SqlPlan,
            correlation_predicates: &[(String, String)],
            lateral_alias: &str,
            projection: &[nodedb_sql::types::query::Projection],
            outer_row_cap: usize,
            left_join: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::lateral::convert_lateral_loop(
                super::super::lateral::ConvertLateralLoopParams {
                    outer,
                    outer_alias,
                    inner,
                    correlation_predicates,
                    lateral_alias,
                    projection,
                    outer_row_cap,
                    left_join,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                },
            )
        }
    };
}

pub(super) use impl_aggregate_lateral_arms_for_convert_visitor;
