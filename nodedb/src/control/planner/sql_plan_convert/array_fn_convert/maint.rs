// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_FLUSH / ARRAY_COMPACT → PhysicalPlan::Array(ArrayOp::*).

use nodedb_array::types::ArrayId;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ArrayOp;

use super::super::convert::ConvertContext;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_flush(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let _ = super::helpers::load_entry(name, tenant_id, ctx)?;
    let wal = ctx.wal.as_ref().ok_or_else(|| crate::Error::PlanError {
        detail: "ARRAY_FLUSH: no WAL wired into convert context".into(),
    })?;
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = VShardId::from_collection_in_database(ctx.database_id, name);
    // A frontier read, not an allocation: `Flush` appends no WAL record, and
    // this LSN becomes the flushed segment's watermark — every cell it contains
    // was written by a record below the current frontier. The write funnel mints
    // nothing for `Flush` and so has no LSN to stamp here.
    let wal_lsn = wal.next_lsn().as_u64();
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::Flush {
            array_id: aid,
            wal_lsn,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(crate) fn convert_compact(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let entry = super::helpers::load_entry(name, tenant_id, ctx)?;
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = VShardId::from_collection_in_database(ctx.database_id, name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::Compact {
            array_id: aid,
            audit_retain_ms: entry.audit_retain_ms,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
