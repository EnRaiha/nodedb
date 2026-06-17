// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for scan/read/join/recursive variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_scan_read_arms_for_convert_visitor {
    () => {
        fn scan(
            &mut self,
            collection: &str,
            _alias: Option<&str>,
            engine: nodedb_sql::types::query::EngineType,
            filters: &[nodedb_sql::types::filter::Filter],
            projection: &[nodedb_sql::types::query::Projection],
            sort_keys: &[nodedb_sql::types::query::SortKey],
            limit: Option<usize>,
            offset: usize,
            distinct: bool,
            window_functions: &[nodedb_sql::types::query::WindowSpec],
            temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::scan::convert_scan(super::super::scan_params::ScanParams {
                collection,
                engine: &engine,
                filters,
                projection,
                sort_keys,
                limit: &limit,
                offset: &offset,
                distinct: &distinct,
                window_functions,
                tenant_id: self.tenant_id,
                temporal,
                database_id: self.ctx.database_id,
            })
        }

        fn point_get(
            &mut self,
            collection: &str,
            _alias: Option<&str>,
            engine: nodedb_sql::types::query::EngineType,
            key_column: &str,
            key_value: &nodedb_sql::types_expr::SqlValue,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::scan::convert_point_get(
                collection,
                &engine,
                key_column,
                key_value,
                self.tenant_id,
                self.ctx,
            )
        }

        fn document_index_lookup(
            &mut self,
            collection: &str,
            _alias: Option<&str>,
            _engine: nodedb_sql::types::query::EngineType,
            field: &str,
            value: &nodedb_sql::types_expr::SqlValue,
            filters: &[nodedb_sql::types::filter::Filter],
            projection: &[nodedb_sql::types::query::Projection],
            _sort_keys: &[nodedb_sql::types::query::SortKey],
            limit: Option<usize>,
            offset: usize,
            _distinct: bool,
            _window_functions: &[nodedb_sql::types::query::WindowSpec],
            _case_insensitive: bool,
            _temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::scan::convert_document_index_lookup(
                collection,
                field,
                value,
                filters,
                projection,
                limit,
                offset,
                self.tenant_id,
                self.ctx.database_id,
            )
        }

        fn join(
            &mut self,
            left: &nodedb_sql::types::SqlPlan,
            right: &nodedb_sql::types::SqlPlan,
            on: &[(String, String)],
            join_type: nodedb_sql::types::query::JoinType,
            condition: Option<&nodedb_sql::types_expr::SqlExpr>,
            limit: Option<usize>,
            projection: &[nodedb_sql::types::query::Projection],
            filters: &[nodedb_sql::types::filter::Filter],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let condition_owned: Option<nodedb_sql::types_expr::SqlExpr> = condition.cloned();
            super::super::scan::convert_join(super::super::scan_params::JoinPlanParams {
                left,
                right,
                on,
                join_type: &join_type,
                condition: &condition_owned,
                limit: &limit,
                projection,
                filters,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn recursive_scan(
            &mut self,
            collection: &str,
            base_filters: &[nodedb_sql::types::filter::Filter],
            recursive_filters: &[nodedb_sql::types::filter::Filter],
            join_link: Option<&(String, String)>,
            max_iterations: usize,
            distinct: bool,
            limit: usize,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let join_link_owned: Option<(String, String)> = join_link.cloned();
            super::super::scan::convert_recursive_scan(
                super::super::scan_params::RecursiveScanParams {
                    collection,
                    base_filters,
                    recursive_filters,
                    join_link: &join_link_owned,
                    max_iterations: &max_iterations,
                    distinct: &distinct,
                    limit: &limit,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }

        fn recursive_value(
            &mut self,
            cte_name: &str,
            columns: &[String],
            init_exprs: &[String],
            step_exprs: &[String],
            condition: Option<&str>,
            max_depth: usize,
            distinct: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let condition_owned: Option<String> = condition.map(str::to_owned);
            super::super::scan::convert_recursive_value(
                super::super::scan_params::RecursiveValueParams {
                    cte_name,
                    columns,
                    init_exprs,
                    step_exprs,
                    condition: &condition_owned,
                    max_depth: &max_depth,
                    distinct: &distinct,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }
    };
}

pub(super) use impl_scan_read_arms_for_convert_visitor;
