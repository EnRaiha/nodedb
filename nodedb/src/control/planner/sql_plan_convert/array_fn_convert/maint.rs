// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_FLUSH / ARRAY_COMPACT → PhysicalPlan::ClusterArray(Flush/Compact) or
//! Array(Flush/Compact).
//!
//! Each core holds the tiles of the vShards it owns. A cluster plan fans the
//! maintenance out to every primary vShard, so every core that holds tiles
//! of the array runs it.

use nodedb_array::types::ArrayId;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::super::convert::ConvertContext;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(crate) fn convert_flush(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let entry = super::helpers::load_entry(name, tenant_id, ctx)?;
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let plan = if ctx.cluster_enabled {
        // Each shard's executor stamps the flush with its own node's WAL
        // frontier.
        PhysicalPlan::ClusterArray(ClusterArrayOp::Flush {
            array_id: aid,
            prefix_bits: entry.prefix_bits,
        })
    } else {
        let wal = ctx.wal.as_ref().ok_or_else(|| crate::Error::PlanError {
            detail: "ARRAY_FLUSH: no WAL wired into convert context".into(),
        })?;
        // A frontier read, not an allocation: `Flush` appends no WAL record,
        // and this LSN becomes the flushed segment's watermark. Every cell it
        // contains was written by a record below the current frontier.
        PhysicalPlan::Array(ArrayOp::Flush {
            array_id: aid,
            wal_lsn: wal.next_lsn().as_u64(),
        })
    };
    Ok(vec![task(name, tenant_id, ctx, plan)])
}

pub(crate) fn convert_compact(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let entry = super::helpers::load_entry(name, tenant_id, ctx)?;
    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let plan = if ctx.cluster_enabled {
        PhysicalPlan::ClusterArray(ClusterArrayOp::Compact {
            array_id: aid,
            audit_retain_ms: entry.audit_retain_ms,
            prefix_bits: entry.prefix_bits,
        })
    } else {
        PhysicalPlan::Array(ArrayOp::Compact {
            array_id: aid,
            audit_retain_ms: entry.audit_retain_ms,
        })
    };
    Ok(vec![task(name, tenant_id, ctx, plan)])
}

fn task(name: &str, tenant_id: TenantId, ctx: &ConvertContext, plan: PhysicalPlan) -> PhysicalTask {
    PhysicalTask {
        tenant_id,
        vshard_id: ctx.collection_key(name).vshard(),
        database_id: ctx.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::array_catalog::{ArrayCatalog, ArrayCatalogEntry};
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn make_ctx(cluster_enabled: bool) -> ConvertContext {
        let schema = ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "x".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v".to_string(), AttrType::Float64, true))
            .tile_extents(vec![4])
            .build()
            .expect("schema build");
        let handle = ArrayCatalog::handle();
        handle
            .write()
            .expect("lock")
            .register(ArrayCatalogEntry {
                array_id: ArrayId::new(TenantId::new(1), "g"),
                name: "g".into(),
                schema_msgpack: zerompk::to_msgpack_vec(&schema).expect("encode"),
                schema_hash: 7,
                created_at_ms: 0,
                prefix_bits: 6,
                audit_retain_ms: Some(1_000),
                minimum_audit_retain_ms: None,
                modification_hlc: nodedb_types::Hlc::ZERO,
                incarnation: nodedb_types::Hlc::ZERO,
            })
            .expect("register");
        ConvertContext {
            purpose: super::super::super::convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: Some(handle),
            credentials: None,
            wal: None,
            surrogate_assigner:
                crate::control::planner::sql_plan_convert::test_support::test_assigner(),
            cluster_enabled,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            prefetched: Default::default(),
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    #[test]
    fn a_cluster_flush_fans_out_with_the_array_routing() {
        let ctx = make_ctx(true);
        let tasks = convert_flush("g", TenantId::new(1), &ctx).expect("convert");
        match &tasks[0].plan {
            PhysicalPlan::ClusterArray(ClusterArrayOp::Flush {
                array_id,
                prefix_bits,
            }) => {
                assert_eq!(array_id.name, "g");
                assert_eq!(*prefix_bits, 6);
            }
            other => panic!("expected ClusterArray Flush, got {other:?}"),
        }
    }

    #[test]
    fn a_cluster_compact_carries_the_retention_window() {
        let ctx = make_ctx(true);
        let tasks = convert_compact("g", TenantId::new(1), &ctx).expect("convert");
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::ClusterArray(ClusterArrayOp::Compact {
                audit_retain_ms: Some(1_000),
                prefix_bits: 6,
                ..
            })
        ));
    }

    #[test]
    fn a_single_node_compact_plans_a_local_op() {
        let ctx = make_ctx(false);
        let tasks = convert_compact("g", TenantId::new(1), &ctx).expect("convert");
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::Array(ArrayOp::Compact {
                audit_retain_ms: Some(1_000),
                ..
            })
        ));
    }

    #[test]
    fn a_single_node_flush_needs_a_wal() {
        let ctx = make_ctx(false);
        let err = convert_flush("g", TenantId::new(1), &ctx).expect_err("no WAL");
        assert!(err.to_string().contains("no WAL"), "{err}");
    }
}
