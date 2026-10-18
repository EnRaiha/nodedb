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
                georef,
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
                    georef,
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
            args: nodedb_sql::AlterArrayVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_alter_convert::convert_alter_array(args, self.tenant_id, self.ctx)
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
            args: nodedb_sql::ArraySliceVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_slice(args, self.tenant_id, self.ctx)
        }

        fn array_agg(
            &mut self,
            args: nodedb_sql::ArrayAggVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_agg(args, self.tenant_id, self.ctx)
        }

        fn array_elementwise(
            &mut self,
            left: &str,
            right: &str,
            op: nodedb_sql::types_array::ArrayBinaryOpAst,
            attr: &str,
            temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::array_fn_convert::convert_elementwise(
                super::super::array_fn_convert::elementwise::ElementwiseCall {
                    left_name: left,
                    right_name: right,
                    op,
                    attr,
                    temporal: *temporal,
                },
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
