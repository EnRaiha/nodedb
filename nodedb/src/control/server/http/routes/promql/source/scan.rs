// SPDX-License-Identifier: BUSL-1.1

//! One raw timeseries scan of one metric collection.
//!
//! The scan is a `TimeseriesOp::Scan` with no aggregates, which the owning
//! core serves from its raw row source: the live memtable, the flushed
//! partitions in the time range, the caller's read policy and the
//! transaction overlay. It reaches storage only through the user-dispatch
//! door, which applies row-level security and redaction refusal, authorizes
//! Read on the collection for the caller's tenant and database, serves clone
//! reads, and routes the read to the node that owns the collection.

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use crate::control::promql::types::Series;
use crate::control::server::shared::ddl::user_dispatch::{
    DispatchRequest, RequestAdmission, dispatch_for_identity,
};

use super::budget::SampleBudget;
use super::discovery::MetricCollection;
use super::pushdown::{encode_filters, pushdown_filters};
use super::rows::{decode_rows, group_rows};
use super::scope::ReadScope;
use super::windows::SelectorRead;

/// Scan `metric` for the rows `read` selects and group them into series.
/// The rows count against `budget`.
pub(crate) async fn scan_metric(
    scope: &ReadScope<'_>,
    metric: &MetricCollection,
    read: &SelectorRead,
    budget: &mut SampleBudget,
) -> crate::Result<Vec<Series>> {
    let filters = encode_filters(&pushdown_filters(&read.matchers))?;
    let plan = PhysicalPlan::Timeseries(TimeseriesOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(scope.database_id, &metric.name),
        time_range: (read.min_ts, read.max_ts),
        projection: Vec::new(),
        limit: budget.fetch_limit(),
        filters,
        sort_keys: Vec::new(),
        bucket_interval_ms: 0,
        group_by: Vec::new(),
        aggregates: Vec::new(),
        gap_fill: String::new(),
        computed_columns: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        having: Vec::new(),
        tiering: metric.tiering.clone(),
        output_columns: Vec::new(),
    });
    let payload = dispatch_for_identity(DispatchRequest {
        state: scope.state,
        identity: scope.identity,
        database_id: scope.database_id,
        collection: &metric.name,
        plan,
        deadline: scope.deadline,
        // Every PromQL handler runs the admission gate at its own entry.
        admission: RequestAdmission::AlreadyAdmitted,
        // A read after a remote write must see that write.
        linearizable: true,
    })
    .await?;
    let rows = decode_rows(&metric.name, &payload)?;
    budget.charge(rows.len())?;
    group_rows(metric, rows, &read.matchers)
}
