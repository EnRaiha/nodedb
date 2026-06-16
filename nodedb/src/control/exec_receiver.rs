// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` RPCs.
//!
//! When a remote node sends an `ExecuteRequest` to this node (because this
//! node is the leader for the target vShard), the [`LocalPlanExecutor`]
//! validates descriptor versions, decodes the `PhysicalPlan`, dispatches
//! it through the local SPSC bridge, and returns an `ExecuteResponse`.
//!
//! Unlike the retired SQL-string forwarding path, this path skips planning
//! entirely — the plan is already encoded by the sender.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tracing::{Instrument, info_span};

use nodedb_cluster::forward::PlanExecutor;
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, TypedClusterError};

use crate::bridge::envelope::{Priority, Request};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use crate::types::ReadConsistency;
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

/// Numeric code for `TypedClusterError::Internal` when plan bytes fail to decode.
const PLAN_DECODE_FAILED: u32 = nodedb_cluster::rpc_codec::PLAN_DECODE_FAILED;

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
}

impl LocalPlanExecutor {
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

/// Returns `true` if `plan` or any nested child plan still carries a
/// `QueryOp::Exchange` node.
///
/// Exchange is coordinator-resolved before cross-node dispatch, so a remote
/// executor must never receive one. This walks the realistic nesting points —
/// the `Exchange.child` box, the four `HashJoin` child/bitmap boxes, and the
/// `LateralTopK` / `LateralLoop` `outer_plan` box — and treats every other
/// variant as an Exchange-free leaf. Carriers are matched explicitly (no
/// catch-all) so a future plan variant that boxes a child plan fails to compile
/// here rather than silently bypassing the guard.
fn plan_contains_exchange(plan: &PhysicalPlan) -> bool {
    match plan {
        // Query operations may embed child plans that themselves carry Exchange.
        PhysicalPlan::Query(query_op) => match query_op {
            QueryOp::Exchange(op) => plan_contains_exchange(&op.child),
            QueryOp::HashJoin {
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
                ..
            } => [left_input, right_input, left_bitmap, right_bitmap]
                .into_iter()
                .flatten()
                .any(|child| plan_contains_exchange(child)),
            QueryOp::LateralTopK { outer_plan, .. } => plan_contains_exchange(outer_plan),
            QueryOp::LateralLoop { outer_plan, .. } => plan_contains_exchange(outer_plan),
            // Aggregate may carry a sub-plan input (catalog `ProviderScan`),
            // which could in principle nest an Exchange — recurse when present.
            QueryOp::Aggregate { input, .. } => {
                input.as_deref().is_some_and(plan_contains_exchange)
            }
            // Remaining query ops carry no nested PhysicalPlan child.
            QueryOp::ProviderScan { .. }
            | QueryOp::PartialAggregate { .. }
            | QueryOp::NestedLoopJoin { .. }
            | QueryOp::SortMergeJoin { .. }
            | QueryOp::FacetCounts { .. }
            | QueryOp::RecursiveScan { .. }
            | QueryOp::RecursiveValue { .. } => false,
        },

        // Leaf engine operations carry no nested PhysicalPlan child.
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_) => false,
    }
}
