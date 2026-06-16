// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` / `ExecuteStreamRequest` RPCs.
//!
//! When a remote node sends an `ExecuteRequest` to this node (because this
//! node is the leader for the target vShard), the [`LocalPlanExecutor`]
//! validates descriptor versions, decodes the `PhysicalPlan`, dispatches
//! it through the local SPSC bridge, and returns an `ExecuteResponse`. The
//! streaming sibling (`ExecuteStreamRequest`) shares the same validation +
//! dispatch prologue ([`LocalPlanExecutor::prepare_dispatch`]) but pushes each
//! result frame to a [`ChunkSink`] as it arrives.
//!
//! Unlike the retired SQL-string forwarding path, this path skips planning
//! entirely — the plan is already encoded by the sender.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tracing::{Instrument, info_span};

use nodedb_cluster::forward::{ChunkSink, PlanExecutor};
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, TypedClusterError};

use crate::bridge::envelope::{Priority, Request};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use crate::types::ReadConsistency;
use nodedb_physical::physical_plan::wire as plan_wire;

use super::support::{
    PLAN_DECODE_FAILED, SinkOutcome, plan_contains_exchange, stream_error_to_typed,
};

/// Executes pre-planned `PhysicalPlan` on the local Data Plane.
pub struct LocalPlanExecutor {
    state: Arc<SharedState>,
}

impl LocalPlanExecutor {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

impl PlanExecutor for LocalPlanExecutor {
    async fn execute_plan(&self, req: ExecuteRequest) -> ExecuteResponse {
        let trace_id = nodedb_types::TraceId(req.trace_id);
        let tenant_id = req.tenant_id;
        let exporter = Arc::clone(&self.state.trace_exporter);
        let start = SystemTime::now();
        let span = info_span!("executor.execute_plan", trace_id = %trace_id, tenant_id);
        let resp = self.execute_plan_inner(req).instrument(span).await;
        // Emit one OTLP executor span per leaseholder so the gateway's
        // upstream span joins the N leaseholder spans into a single
        // distributed trace via the shared `trace_id`.
        exporter.emit(
            "executor.execute_plan",
            trace_id,
            start,
            SystemTime::now(),
            tenant_id,
            0,
            resp.success,
        );
        resp
    }

    async fn execute_plan_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        let trace_id = nodedb_types::TraceId(req.trace_id);
        let tenant_id = req.tenant_id;
        let exporter = Arc::clone(&self.state.trace_exporter);
        let start = SystemTime::now();
        let span = info_span!("executor.execute_plan_streaming", trace_id = %trace_id, tenant_id);
        let outcome = self
            .execute_plan_streaming_inner(req, sink)
            .instrument(span)
            .await;
        exporter.emit(
            "executor.execute_plan_streaming",
            trace_id,
            start,
            SystemTime::now(),
            tenant_id,
            0,
            outcome.is_none(),
        );
        outcome
    }
}

/// Outcome of [`LocalPlanExecutor::prepare_dispatch`]: a registered tracker
/// receiver for a dispatched plan, plus the effective deadline and the
/// request id (needed to cancel the tracker on the streaming error paths).
struct PreparedDispatch {
    rx: tokio::sync::mpsc::Receiver<crate::bridge::envelope::Response>,
    deadline: Duration,
    request_id: crate::types::RequestId,
}

impl LocalPlanExecutor {
    /// Shared prologue for both the one-shot and streaming execution paths:
    /// validate the deadline + descriptor versions, decode the plan, reject
    /// unresolved Exchange nodes, then register a tracker and dispatch through
    /// the local SPSC bridge. Returns the registered receiver on success or a
    /// typed cluster error to surface to the caller.
    fn prepare_dispatch(
        &self,
        req: &ExecuteRequest,
    ) -> Result<PreparedDispatch, TypedClusterError> {
        // ── 1. Deadline check ─────────────────────────────────────────────────
        if req.deadline_remaining_ms == 0 {
            return Err(TypedClusterError::DeadlineExceeded { elapsed_ms: 0 });
        }

        let deadline = Duration::from_millis(req.deadline_remaining_ms).min(Duration::from_secs(
            self.state.tuning.network.default_deadline_secs,
        ));

        // ── 2. Descriptor version validation ──────────────────────────────────
        let catalog_ref = self.state.credentials.catalog();
        if let Some(catalog) = catalog_ref.as_ref() {
            for entry in &req.descriptor_versions {
                match catalog.get_collection(DatabaseId::DEFAULT, req.tenant_id, &entry.collection)
                {
                    Ok(Some(stored)) => {
                        let actual = if stored.descriptor_version == 0 {
                            1
                        } else {
                            stored.descriptor_version
                        };
                        if actual != entry.version {
                            return Err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: actual,
                            });
                        }
                    }
                    Ok(None) => {
                        if entry.version != 0 {
                            return Err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: 0,
                            });
                        }
                    }
                    Err(e) => {
                        return Err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: format!("catalog lookup failed: {e}"),
                        });
                    }
                }
            }
        }

        // ── 3. Decode the PhysicalPlan ────────────────────────────────────────
        let plan = match plan_wire::decode(&req.plan_bytes) {
            Ok(p) => p,
            Err(e) => {
                return Err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: format!("plan decode failed: {e}"),
                });
            }
        };

        // ── 3b. Reject unresolved Exchange nodes ──────────────────────────────
        if plan_contains_exchange(&plan) {
            return Err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "received plan with unresolved Exchange node; coordinator must resolve \
                          data movement before cross-node dispatch"
                    .into(),
            });
        }

        // ── 4. Dispatch through local SPSC bridge ─────────────────────────────
        let request_id = self.state.next_request_id();
        let tenant_id = crate::types::TenantId::new(req.tenant_id);
        let database_id = crate::types::DatabaseId::from(req.database_id);

        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id: crate::types::VShardId::new(0),
            plan,
            deadline: Instant::now() + deadline,
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId(req.trace_id),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
        };

        let rx = self.state.tracker.register(request_id);

        let dispatch_result = match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            return Err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: format!("dispatch failed: {e}"),
            });
        }

        Ok(PreparedDispatch {
            rx,
            deadline,
            request_id,
        })
    }

    /// Streaming execution: prepare + dispatch as usual, then push each result
    /// frame to `sink` as it arrives instead of collecting into one response.
    ///
    /// Returns `None` on a clean end, or `Some(err)` on a terminal failure
    /// (validation rejection, stream error, over-budget, or deadline). A
    /// `send_chunk` error means the coordinator is gone: cancel the tracker and
    /// return `None` (there is no peer to receive a terminal frame).
    async fn execute_plan_streaming_inner(
        &self,
        req: ExecuteRequest,
        mut sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        use futures::StreamExt;

        let PreparedDispatch {
            rx,
            deadline,
            request_id,
        } = match self.prepare_dispatch(&req) {
            Ok(p) => p,
            Err(e) => return Some(e),
        };

        let max_result_bytes = self.state.tuning.network.max_query_result_bytes as usize;
        // `tolerate_not_found: false` — a remote single-shard executor that
        // reports NotFound surfaces it as an error, matching the one-shot path
        // (which maps a `Status::Error` terminal frame to `Internal`). The
        // coordinator's `gather_all_cores_stream` is the place NotFound is
        // tolerated (per-core "no rows on this core"); a single remote shard
        // returning NotFound means the scan genuinely found nothing here.
        let mut stream = crate::control::server::result_stream::stream_response_channel(
            rx,
            max_result_bytes,
            false,
        );

        let stream_fut = async {
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(b) => {
                        if let Err(_e) =
                            sink.send_chunk(b.payload, b.watermark_lsn.as_u64()).await
                        {
                            // Coordinator gone — stop, cancel tracker, no terminal.
                            self.state.tracker.cancel(&request_id);
                            return SinkOutcome::CoordinatorGone;
                        }
                    }
                    Err(e) => {
                        self.state.tracker.cancel(&request_id);
                        return SinkOutcome::StreamError(stream_error_to_typed(e));
                    }
                }
            }
            SinkOutcome::CleanEnd
        };

        match tokio::time::timeout(deadline, stream_fut).await {
            Ok(SinkOutcome::CleanEnd) => None,
            Ok(SinkOutcome::CoordinatorGone) => None,
            Ok(SinkOutcome::StreamError(e)) => Some(e),
            Err(_) => {
                self.state.tracker.cancel(&request_id);
                Some(TypedClusterError::DeadlineExceeded {
                    elapsed_ms: deadline.as_millis() as u64,
                })
            }
        }
    }

    async fn execute_plan_inner(&self, req: ExecuteRequest) -> ExecuteResponse {
        // ── 1. Deadline check ─────────────────────────────────────────────────
        if req.deadline_remaining_ms == 0 {
            return ExecuteResponse::err(TypedClusterError::DeadlineExceeded { elapsed_ms: 0 });
        }

        let deadline = Duration::from_millis(req.deadline_remaining_ms).min(Duration::from_secs(
            self.state.tuning.network.default_deadline_secs,
        ));

        // ── 2. Descriptor version validation ──────────────────────────────────
        //
        // For each (collection, version) pair the caller sent, look up the local
        // descriptor version from SystemCatalog. If any version differs, the
        // caller's plan was built against a stale schema — reject with a typed
        // error so they re-plan against fresh leases.
        let catalog_ref = self.state.credentials.catalog();
        if let Some(catalog) = catalog_ref.as_ref() {
            for entry in &req.descriptor_versions {
                match catalog.get_collection(DatabaseId::DEFAULT, req.tenant_id, &entry.collection)
                {
                    Ok(Some(stored)) => {
                        // Version 0 is the pre-B.1 sentinel; treat as 1 (same
                        // floor the drain gate uses).
                        let actual = if stored.descriptor_version == 0 {
                            1
                        } else {
                            stored.descriptor_version
                        };
                        if actual != entry.version {
                            return ExecuteResponse::err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: actual,
                            });
                        }
                    }
                    Ok(None) => {
                        // Collection not found locally — could be a new collection
                        // the follower saw but we haven't applied yet, or a race.
                        // Treat as DescriptorMismatch so the caller re-plans.
                        if entry.version != 0 {
                            return ExecuteResponse::err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: 0,
                            });
                        }
                    }
                    Err(e) => {
                        return ExecuteResponse::err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: format!("catalog lookup failed: {e}"),
                        });
                    }
                }
            }
        }

        // ── 3. Decode the PhysicalPlan ────────────────────────────────────────
        let plan = match plan_wire::decode(&req.plan_bytes) {
            Ok(p) => p,
            Err(e) => {
                return ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: format!("plan decode failed: {e}"),
                });
            }
        };

        // ── 3b. Reject unresolved Exchange nodes ──────────────────────────────
        //
        // Exchange is data-movement and is always resolved by the coordinator on
        // the requesting node before a plan is shipped here. A local core cannot
        // perform cross-core/cross-node movement, so a plan that still contains an
        // Exchange node anywhere in its tree is a coordinator bug — reject it
        // deterministically instead of dispatching an unexecutable plan.
        if plan_contains_exchange(&plan) {
            return ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "received plan with unresolved Exchange node; coordinator must resolve \
                          data movement before cross-node dispatch"
                    .into(),
            });
        }

        // ── 4. Dispatch through local SPSC bridge ─────────────────────────────
        //
        // Build a Request, register a oneshot tracker, dispatch, and await the response.
        let request_id = self.state.next_request_id();
        let tenant_id = crate::types::TenantId::new(req.tenant_id);
        let database_id = crate::types::DatabaseId::from(req.database_id);

        let request = Request {
            request_id,
            tenant_id,
            database_id,
            // Use the first vshard_id from the plan — the sender already routed
            // this to the correct node. Use 0 as the default if the plan doesn't
            // embed vshard info directly; the Data Plane ignores it for local exec.
            vshard_id: crate::types::VShardId::new(0),
            plan,
            deadline: Instant::now() + deadline,
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId(req.trace_id),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
        };

        let mut rx = self.state.tracker.register(request_id);

        let dispatch_result = match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            return ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: format!("dispatch failed: {e}"),
            });
        }

        // ── 5. Collect response payloads ──────────────────────────────────────
        //
        // A remote scan can stream as several `Partial` chunks before its
        // terminal frame — the Data Plane chunks any result wider than
        // `stream_chunk_size`. Consuming only the first frame would silently
        // truncate the remote shard's result to one chunk and orphan the
        // request's tracker entry. Drain and concatenate every frame via the
        // same bounded collector the local dispatch path uses, so the combined
        // payload is held to the Control-Plane `max_query_result_bytes` ceiling.
        use crate::control::server::dispatch_utils::{
            DispatchCollectError, collect_bounded_response,
        };
        let max_result_bytes = self.state.tuning.network.max_query_result_bytes as usize;
        match tokio::time::timeout(deadline, collect_bounded_response(&mut rx, max_result_bytes))
            .await
        {
            Ok(Ok(resp)) => {
                if resp.status == crate::bridge::envelope::Status::Error {
                    let msg = resp
                        .error_code
                        .as_ref()
                        .map(|c| format!("{c:?}"))
                        .unwrap_or_else(|| "unknown error".into());
                    ExecuteResponse::err(TypedClusterError::Internal {
                        code: PLAN_DECODE_FAILED,
                        message: msg,
                    })
                } else {
                    ExecuteResponse::ok(vec![resp.payload.to_vec()])
                }
            }
            Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
                self.state.tracker.cancel(&request_id);
                ExecuteResponse::err(TypedClusterError::Internal {
                    code: 0,
                    message: format!(
                        "remote query result exceeded max_query_result_bytes \
                         ({bytes} > {max_result_bytes} bytes)"
                    ),
                })
            }
            Ok(Err(DispatchCollectError::ChannelClosed)) => {
                ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: "response channel closed".into(),
                })
            }
            Err(_) => ExecuteResponse::err(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline.as_millis() as u64,
            }),
        }
    }
}
