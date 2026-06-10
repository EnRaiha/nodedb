// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for TimeseriesOp variants (scan, ingest).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::TimeseriesOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::TimeseriesScanParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_timeseries(
        &mut self,
        task: &ExecutionTask,
        op: &TimeseriesOp,
    ) -> Response {
        match op {
            TimeseriesOp::Scan {
                collection,
                time_range,
                limit,
                filters,
                bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
                computed_columns,
                system_time,
                valid_at_ms,
                ..
            } => self.execute_timeseries_scan(TimeseriesScanParams {
                task,
                tid: task.request.tenant_id,
                collection,
                time_range: *time_range,
                limit: *limit,
                filters,
                bucket_interval_ms: *bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
                computed_columns,
                system_time: *system_time,
                valid_at_ms: *valid_at_ms,
            }),

            TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn,
                surrogates: _,
            } => self.execute_timeseries_ingest(
                task,
                task.request.tenant_id,
                collection,
                payload,
                format,
                *wal_lsn,
            ),
        }
    }
}
