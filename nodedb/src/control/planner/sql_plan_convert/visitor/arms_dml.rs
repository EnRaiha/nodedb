// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for DML variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_dml_arms_for_convert_visitor {
    () => {
        fn insert(
            &mut self,
            collection: &str,
            engine: nodedb_sql::types::query::EngineType,
            rows: &[Vec<(String, nodedb_sql::types_expr::SqlValue)>],
            column_defaults: &[(String, String)],
            if_absent: bool,
            column_schema: &[(String, String)],
            primary_key: Option<&str>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_insert(
                collection,
                &engine,
                rows,
                column_defaults,
                column_schema,
                if_absent,
                primary_key,
                self.tenant_id,
                self.ctx,
            )
        }

        fn upsert(
            &mut self,
            collection: &str,
            engine: nodedb_sql::types::query::EngineType,
            rows: &[Vec<(String, nodedb_sql::types_expr::SqlValue)>],
            column_defaults: &[(String, String)],
            on_conflict_updates: &[(String, nodedb_sql::types_expr::SqlExpr)],
            column_schema: &[(String, String)],
            primary_key: Option<&str>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_upsert(
                collection,
                &engine,
                rows,
                column_defaults,
                column_schema,
                on_conflict_updates,
                primary_key,
                self.tenant_id,
                self.ctx,
            )
        }

        fn kv_insert(
            &mut self,
            collection: &str,
            entries: &[(
                nodedb_sql::types_expr::SqlValue,
                Vec<(String, nodedb_sql::types_expr::SqlValue)>,
            )],
            ttl_secs: u64,
            intent: nodedb_sql::types::plan::KvInsertIntent,
            on_conflict_updates: &[(String, nodedb_sql::types_expr::SqlExpr)],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_kv_insert(
                collection,
                entries,
                ttl_secs,
                intent,
                on_conflict_updates,
                self.tenant_id,
                self.ctx,
            )
        }

        fn update(
            &mut self,
            collection: &str,
            engine: nodedb_sql::types::query::EngineType,
            assignments: &[(String, nodedb_sql::types_expr::SqlExpr)],
            filters: &[nodedb_sql::types::filter::Filter],
            target_keys: &[nodedb_sql::types_expr::SqlValue],
            returning: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_update(super::super::dml::UpdateParams {
                collection,
                engine: &engine,
                assignments,
                filters,
                target_keys,
                returning,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn update_from(
            &mut self,
            collection: &str,
            _engine: nodedb_sql::types::query::EngineType,
            source: &nodedb_sql::types::SqlPlan,
            target_join_col: &str,
            source_join_col: &str,
            assignments: &[(String, nodedb_sql::types_expr::SqlExpr)],
            target_filters: &[nodedb_sql::types::filter::Filter],
            returning: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_update_from(super::super::dml::UpdateFromParams {
                collection,
                source,
                target_join_col,
                source_join_col,
                assignments,
                target_filters,
                returning,
                tenant_id: self.tenant_id,
                ctx: self.ctx,
            })
        }

        fn delete(
            &mut self,
            collection: &str,
            engine: nodedb_sql::types::query::EngineType,
            filters: &[nodedb_sql::types::filter::Filter],
            target_keys: &[nodedb_sql::types_expr::SqlValue],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_delete(
                collection,
                &engine,
                filters,
                target_keys,
                self.tenant_id,
                self.ctx,
            )
        }

        fn vector_primary_insert(
            &mut self,
            collection: &str,
            field: &str,
            quantization: &nodedb_types::VectorQuantization,
            storage_dtype: &nodedb_types::VectorStorageDtype,
            payload_indexes: &[(String, nodedb_types::PayloadIndexKind)],
            rows: &[nodedb_sql::types::plan::VectorPrimaryRow],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_vector_primary_insert(
                collection,
                &super::super::dml::VectorPrimaryInsertCfg {
                    field,
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes,
                },
                rows,
                self.tenant_id,
                self.ctx,
            )
        }

        fn merge(
            &mut self,
            target: &str,
            _engine: nodedb_sql::types::query::EngineType,
            source: &nodedb_sql::types::SqlPlan,
            target_join_col: &str,
            source_join_col: &str,
            source_alias: &str,
            clauses: &[nodedb_sql::types::plan::MergePlanClause],
            returning: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::dml::convert_merge(
                target,
                source,
                target_join_col,
                source_join_col,
                source_alias,
                clauses,
                returning,
                self.tenant_id,
                self.ctx,
            )
        }
    };
}

pub(super) use impl_dml_arms_for_convert_visitor;
