// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for SpatialOp variants (scan, insert, delete).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::SpatialOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::spatial_sync::SpatialInsertExec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_spatial(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &SpatialOp,
    ) -> Response {
        match op {
            SpatialOp::Insert {
                collection,
                field,
                surrogate,
                geometry,
                provenance,
            } => self.execute_spatial_insert(SpatialInsertExec {
                task,
                tid,
                collection,
                field,
                surrogate: *surrogate,
                geometry,
                provenance: provenance.as_ref(),
            }),

            SpatialOp::Delete {
                collection,
                field,
                surrogate,
                provenance,
            } => self.execute_spatial_delete(
                task,
                tid,
                collection,
                field,
                *surrogate,
                provenance.as_ref(),
            ),

            SpatialOp::Scan {
                collection,
                field,
                predicate,
                query_geometry,
                distance_meters,
                attribute_filters,
                limit,
                projection,
                rls_filters,
                prefilter,
            } => self.execute_spatial_scan(super::super::handlers::spatial::SpatialScanParams {
                task,
                tid,
                collection,
                field,
                predicate,
                query_geometry,
                distance_meters: *distance_meters,
                attribute_filters,
                limit: *limit,
                projection,
                rls_filters,
                prefilter: prefilter.as_ref(),
            }),
        }
    }
}
