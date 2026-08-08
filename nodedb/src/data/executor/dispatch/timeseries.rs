// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for TimeseriesOp variants (scan, ingest).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::TimeseriesOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::{TimeseriesIngestExec, TimeseriesScanParams};
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
                sort_keys,
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
                sort_keys,
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
                provenance,
                rls_write_check,
                returning,
                rls_filters,
            } => self.execute_timeseries_ingest(TimeseriesIngestExec {
                task,
                tid: task.request.tenant_id,
                collection,
                payload,
                format,
                // The write funnel mints the record's LSN and stamps it on the
                // REQUEST ENVELOPE; only the sync path also fills the plan's
                // copy, so the SQL and ILP planners both hand this arm a `None`
                // there. That LSN is what `flush_ts_collection` stamps onto the
                // partition it writes, and boot replay skips exactly the
                // records at or below the highest stamp it finds — so reading
                // the plan alone left every SQL/ILP ingest flushing partitions
                // stamped 0, a gate that never fires and a WAL tail that
                // replays on top of rows already on disk. The plan's copy stays
                // the fallback for callers that carry no envelope LSN.
                wal_lsn: task.wal_lsn().map(|lsn| lsn.as_u64()).or(*wal_lsn),
                provenance: provenance.as_ref(),
                mode: crate::data::executor::handlers::timeseries::TimeseriesApplyMode::Immediate,
                rls_write_check,
                returning: returning.as_ref(),
                rls_filters,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::TimeseriesOp;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{Admission, PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    const TENANT: u64 = 1;
    const COLLECTION: &str = "metrics";

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime; the
    /// test drives the handler directly and never ticks the event loop.
    struct Harness {
        core: CoreLoop,
        _req_tx: Producer<BridgeRequest>,
        _resp_rx: Consumer<BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        Harness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// An autocommit ILP ingest exactly as the SQL and ILP planners build it:
    /// the plan carries NO LSN, the request envelope carries the minted one.
    fn autocommit_ingest_task(envelope_lsn: Option<u64>) -> ExecutionTask {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: COLLECTION.to_string(),
            payload: format!("{COLLECTION},host=h0 value=1i\n").into_bytes(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TENANT),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: envelope_lsn.map(Lsn::new),
            resolved_now_ms: None,
            admission: Admission::Admitted,
        })
    }

    /// The invariant: the partition a flush writes must be stamped with the LSN
    /// of the last record it fully contains, because boot replay skips every
    /// record at or below the highest stamp it finds. A stamp of 0 makes that
    /// gate a no-op, and the acknowledged records replay on top of the very
    /// rows the partition already holds.
    ///
    /// The autocommit planners leave the plan's `wal_lsn` `None`, so this fails
    /// the moment the handler stops consulting the request envelope.
    #[test]
    fn autocommit_ingest_stamps_the_envelope_lsn_on_the_partition_it_flushes() {
        let mut h = make_core();
        let task = autocommit_ingest_task(Some(42));
        let PhysicalPlan::Timeseries(op) = task.request.plan.clone() else {
            panic!("timeseries plan");
        };

        let response = h.core.dispatch_timeseries(&task, &op);
        assert_eq!(response.status, Status::Ok, "ingest must succeed");

        h.core
            .flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        let registry = h.core.ts_registries.get(&key).expect("registry");
        let stamps: Vec<u64> = registry
            .iter()
            .map(|(_, entry)| entry.meta.last_flushed_wal_lsn)
            .collect();
        assert_eq!(
            stamps,
            vec![42],
            "the flushed partition must carry the record's WAL LSN, or replay's \
             dedup gate never fires for it"
        );
    }

    /// Nothing minted an LSN, so nothing may be claimed as flushed: a stamp
    /// invented here would gate away records that are genuinely un-flushed.
    #[test]
    fn an_ingest_with_no_lsn_anywhere_stamps_nothing() {
        let mut h = make_core();
        let task = autocommit_ingest_task(None);
        let PhysicalPlan::Timeseries(op) = task.request.plan.clone() else {
            panic!("timeseries plan");
        };

        let response = h.core.dispatch_timeseries(&task, &op);
        assert_eq!(response.status, Status::Ok);

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        assert_eq!(h.core.ts_max_ingested_lsn.get(&key), None);
    }
}
