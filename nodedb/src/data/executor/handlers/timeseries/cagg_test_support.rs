// SPDX-License-Identifier: BUSL-1.1

//! Core-level fixtures for the continuous-aggregate tests: one ILP
//! collection `metrics` in the default database, tenant 1, and an aggregate
//! of `COUNT(*)` and `SUM(value)` per host.

use std::path::{Path, PathBuf};

use nodedb_physical::physical_plan::TimeseriesOp;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::continuous_agg::{
    AggFunction, AggregateExpr, CAGG_IMAGE_FILE, ColumnLayout, ContinuousAggregateDef,
    RefreshPolicy,
};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::{TimeseriesApplyMode, TimeseriesIngestExec};

pub const TENANT: u64 = 1;
pub const COLLECTION: &str = "metrics";

/// A task stamped `lsn` on the collection. The ingest reads only the
/// envelope.
pub fn task_at(lsn: u64) -> ExecutionTask {
    CoreLoop::replay_task(
        TenantId::new(TENANT),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            restart_identity: false,
        }),
        Some(Lsn::new(lsn)),
    )
}

/// An autocommit ILP ingest stamped `lsn`.
pub fn ingest(core: &mut CoreLoop, lines: &str, lsn: u64) -> Status {
    let task = task_at(lsn);
    core.execute_timeseries_ingest(TimeseriesIngestExec {
        task: &task,
        tid: TenantId::new(TENANT),
        collection: COLLECTION,
        payload: lines.as_bytes(),
        format: "ilp",
        wal_lsn: Some(lsn),
        provenance: None,
        mode: TimeseriesApplyMode::Immediate,
        rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: &[],
    })
    .status
}

/// Flush the collection's memtable to a partition.
pub fn flush(core: &mut CoreLoop) {
    core.flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
        .expect("flush");
}

/// `COUNT(*) AS c` and `SUM(value) AS s` per host, one-hour buckets.
pub fn host_def(name: &str, policy: RefreshPolicy) -> ContinuousAggregateDef {
    let expr = |function, column: &str, output: &str| AggregateExpr {
        function,
        source_column: column.into(),
        output_column: output.into(),
    };
    ContinuousAggregateDef {
        database_id: 0,
        name: name.into(),
        source: COLLECTION.into(),
        bucket_interval: "1h".into(),
        bucket_interval_ms: 3_600_000,
        group_by: vec!["host".into()],
        aggregates: vec![
            expr(AggFunction::Count, "*", "c"),
            expr(AggFunction::Sum, "value", "s"),
        ],
        refresh_policy: policy,
        retention_period_ms: 0,
        stale: false,
        incarnation: nodedb_types::Hlc::new(1, 0),
    }
}

/// Register `def` for the tenant on `core`.
pub fn register(core: &mut CoreLoop, def: ContinuousAggregateDef) {
    core.continuous_agg_mgr.register(TENANT, def);
}

/// Total `(COUNT, SUM)` across every bucket of `name`.
pub fn totals(core: &CoreLoop, name: &str) -> (i64, i64) {
    let mgr = &core.continuous_agg_mgr;
    let def = mgr.get_definition(0, TENANT, name).expect("registered");
    let layout = ColumnLayout::of(def);
    let mut count = 0;
    let mut sum = 0;
    for bucket in mgr.get_materialized(0, TENANT, name).expect("registered") {
        let int = |expr: &AggregateExpr| match bucket.finalize(expr, &layout) {
            Ok(nodedb_types::Value::Integer(i)) => i,
            Ok(nodedb_types::Value::Null) => 0,
            other => panic!("unexpected {other:?}"),
        };
        count += int(&def.aggregates[0]);
        sum += int(&def.aggregates[1]);
    }
    (count, sum)
}

/// Path of the collection's aggregate image under `data_dir`.
pub fn image_path(data_dir: &Path) -> PathBuf {
    super::paths::ts_collection_dir(data_dir, 0, TENANT, COLLECTION).join(CAGG_IMAGE_FILE)
}

/// Three rows over two hosts, at fixed nanosecond timestamps.
pub const ROWS_A: &str = "metrics,host=a value=1i 1700000000000000000\n\
                          metrics,host=b value=10i 1700000001000000000\n\
                          metrics,host=a value=100i 1700000002000000000\n";
/// Two more rows, in the same hour as `ROWS_A`.
pub const ROWS_B: &str = "metrics,host=b value=1000i 1700000003000000000\n\
                          metrics,host=a value=10000i 1700000004000000000\n";
