// SPDX-License-Identifier: BUSL-1.1

//! The `DataPlaneArrayExecutor` type and its shared SPSC dispatch scaffolding.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_cluster::error::{ClusterError, Result};

use crate::bridge::envelope::{Priority, Request};
use crate::control::state::SharedState;
use crate::event::types::EventSource;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;

/// Timeout for a single shard-side array operation dispatched through the
/// local SPSC bridge. This bounds how long the cluster handler waits for the
/// Data Plane to respond before returning an error to the coordinator.
const LOCAL_DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// The vShard every shard-local array op on this node dispatches under.
///
/// The RPC layer has already routed this request to the owning NODE by tile;
/// what remains is picking the local Data Plane core, and `vshard % num_cores`
/// is that choice. Every op here — the reads AND the single-node write fallback
/// — must name the same vShard, because the array engine is per-core state: a
/// write dispatched under a different vShard would land in another core's
/// engine and be invisible to these reads. WAL replay routes each array record
/// by `record.vshard_id % num_cores` too, so the redo record minted for a write
/// under this vShard also replays back onto this same core.
pub(super) const LOCAL_DISPATCH_VSHARD: VShardId = VShardId::new(0);

/// Concrete implementation of `ArrayLocalExecutor` backed by the local Data Plane.
///
/// Holds a reference to `SharedState` so it can dispatch `PhysicalPlan::Array`
/// variants through the SPSC bridge and await their responses via the
/// `RequestTracker`.
pub struct DataPlaneArrayExecutor {
    pub(super) state: Arc<SharedState>,
}

impl DataPlaneArrayExecutor {
    /// Construct an executor backed by the given shared state.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Dispatch a `PhysicalPlan` through the local SPSC bridge and await the
    /// single (non-streaming) response.
    pub(super) async fn dispatch_and_await(
        &self,
        plan: PhysicalPlan,
    ) -> Result<crate::bridge::envelope::Response> {
        let request_id = self.state.next_request_id();

        let request = Request {
            request_id,
            tenant_id: TenantId::new(0),
            database_id: DatabaseId::DEFAULT,
            vshard_id: LOCAL_DISPATCH_VSHARD,
            plan,
            deadline: Instant::now() + LOCAL_DISPATCH_TIMEOUT,
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::AlreadyOrdered,
            ),
        };

        let mut rx = self.state.tracker.register(request_id);

        let dispatch_result = match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            return Err(ClusterError::Storage {
                detail: format!("array executor dispatch: {e}"),
            });
        }

        match tokio::time::timeout(LOCAL_DISPATCH_TIMEOUT, async { rx.recv().await.ok_or(()) })
            .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(ClusterError::Storage {
                detail: "array executor: response channel closed".into(),
            }),
            Err(_) => Err(ClusterError::Storage {
                detail: "array executor: local dispatch timed out".into(),
            }),
        }
    }
}
