// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_ELEMENTWISE → PhysicalPlan::Array(ArrayOp::Elementwise).

use nodedb_array::types::ArrayId;
use nodedb_sql::types_array::ArrayBinaryOpAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ArrayOp;

use super::super::convert::ConvertContext;
use super::helpers::{load_schema, map_binary_op};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_elementwise(
    left_name: &str,
    right_name: &str,
    op: ArrayBinaryOpAst,
    attr: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let lschema = load_schema(left_name, tenant_id, ctx)?;
    let rschema = load_schema(right_name, tenant_id, ctx)?;
    if lschema.dims.len() != rschema.dims.len() || lschema.attrs.len() != rschema.attrs.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "ARRAY_ELEMENTWISE: arrays '{left_name}' and '{right_name}' have different shapes"
            ),
        });
    }
    let attr_idx = lschema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_ELEMENTWISE: array '{left_name}' has no attr '{attr}'"),
        })? as u32;
    if !rschema.attrs.iter().any(|a| a.name == attr) {
        return Err(crate::Error::PlanError {
            detail: format!("ARRAY_ELEMENTWISE: array '{right_name}' has no attr '{attr}'"),
        });
    }
    let left = ArrayId::in_database(tenant_id, ctx.database_id, left_name);
    let right = ArrayId::in_database(tenant_id, ctx.database_id, right_name);
    let vshard = VShardId::from_collection_in_database(ctx.database_id, left_name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::Elementwise {
            left,
            right,
            op: map_binary_op(op),
            attr_idx,
            cell_filter: None,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
