// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for Array DDL/DML/TVF variants on `ConvertVisitor`.
//! Defined as a macro and invoked once from `adapter.rs` inside the single impl block.

macro_rules! impl_array_arms_for_convert_visitor {
    () => {
        fn create_array(
            &mut self,
            args: nodedb_sql::CreateArrayVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::CreateArrayVisitArgs {
                name,
                dims,
                attrs,
                tile_extents,
                cell_order,
                tile_order,
                prefix_bits,
                audit_retain_ms,
                minimum_audit_retain_ms,
            } = args;
            super::super::array_convert::convert_create_array(
                super::super::array_convert::CreateArrayArgs {
                    name,
                    dims,
                    attrs,
                    tile_extents,
                    cell_order,
                    tile_order,
                    prefix_bits,
                    audit_retain_ms,
                    minimum_audit_retain_ms,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                },
            )
        }

        fn drop_array(
            &mut self,
            name: &str,
            if_exists: bool,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_convert::convert_drop_array(
                name,
                if_exists,
                self.tenant_id,
                self.ctx,
            )
        }

        fn alter_array(
            &mut self,
            name: &str,
            audit_retain_ms: Option<Option<i64>>,
            minimum_audit_retain_ms: Option<u64>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_alter_convert::convert_alter_array(
                name,
                audit_retain_ms,
                minimum_audit_retain_ms,
                self.tenant_id,
                self.ctx,
            )
        }

        fn insert_array(
            &mut self,
            name: &str,
            rows: &[nodedb_sql::types_array::ArrayInsertRow],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_convert::convert_insert_array(name, rows, self.tenant_id, self.ctx)
        }

        fn delete_array(
            &mut self,
            name: &str,
            coords: &[Vec<nodedb_sql::types_array::ArrayCoordLiteral>],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_convert::convert_delete_array(
                name,
                coords,
                self.tenant_id,
                self.ctx,
            )
        }

        fn array_slice(
            &mut self,
            name: &str,
            slice: &nodedb_sql::types_array::ArraySliceAst,
            attr_projection: &[String],
            limit: u32,
            temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_slice(
                name,
                slice,
                attr_projection,
                limit,
                *temporal,
                self.tenant_id,
                self.ctx,
            )
        }

        fn array_project(
            &mut self,
            name: &str,
            attr_projection: &[String],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_project(
                name,
                attr_projection,
                self.tenant_id,
                self.ctx,
            )
        }

        fn array_agg(
            &mut self,
            name: &str,
            attr: &str,
            reducer: &nodedb_sql::types_array::ArrayReducerAst,
            group_by_dim: Option<&str>,
            temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_agg(
                name,
                attr,
                *reducer,
                group_by_dim,
                *temporal,
                self.tenant_id,
                self.ctx,
            )
        }

        fn array_elementwise(
            &mut self,
            left: &str,
            right: &str,
            op: nodedb_sql::types_array::ArrayBinaryOpAst,
            attr: &str,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_elementwise(
                left,
                right,
                op,
                attr,
                self.tenant_id,
                self.ctx,
            )
        }

        fn array_flush(
            &mut self,
            name: &str,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_flush(name, self.tenant_id, self.ctx)
        }

        fn array_compact(
            &mut self,
            name: &str,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_compact(name, self.tenant_id, self.ctx)
        }
    };
}

pub(super) use impl_array_arms_for_convert_visitor;
