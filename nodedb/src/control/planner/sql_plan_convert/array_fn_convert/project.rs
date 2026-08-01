// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_PROJECT → PhysicalPlan::Array(ArrayOp::Project).

use nodedb_array::types::ArrayId;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ArrayOp;

use super::super::convert::ConvertContext;
use super::helpers::{load_schema, resolve_attr_indices};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_project(
    name: &str,
    attr_projection: &[String],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let schema = load_schema(name, tenant_id, ctx)?;
    let attr_indices = resolve_attr_indices(name, attr_projection, &schema)?;
    if attr_indices.is_empty() {
        return Err(crate::Error::PlanError {
            detail: format!("ARRAY_PROJECT: array '{name}': attr list must not be empty"),
        });
    }
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = VShardId::from_collection_in_database(ctx.database_id, name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::Project {
            array_id: aid,
            attr_indices,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
