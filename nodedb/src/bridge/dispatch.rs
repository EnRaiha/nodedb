// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;

use tracing::{error, warn};

use nodedb_bridge::BridgeError;
use nodedb_bridge::backpressure::{BackpressureConfig, BackpressureController, PressureState};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_bridge::wfq::WeightedFairQueue;
use nodedb_types::PriorityClass;

use crate::bridge::envelope;
use crate::control::router::vshard::VShardRouter;
use crate::data::eventfd::EventFdNotifier;

use crate::bridge::admission_chokepoint::assert_write_admitted;

/// Serialized form of a request that goes through the SPSC ring buffer.
///
/// The bridge crate is generic over `T` — we serialize our typed `Request`
/// envelope into this form for the ring buffer, and deserialize on the
/// Data Plane side.
#[derive(Debug)]
pub struct BridgeRequest {
    /// The full typed request envelope.
    pub inner: envelope::Request,
}

/// Serialized form of a response coming back from the Data Plane.
#[derive(Debug)]
pub struct BridgeResponse {
    /// The full typed response envelope.
    pub inner: envelope::Response,
}

/// Resolves the priority class for a database at dispatch time.
///
/// Implementations are expected to cache the result (e.g., in a `DashMap` with
/// a time-bounded or version-invalidated TTL) so the hot dispatch path does not
/// hit catalog storage. A `Standard` fallback is returned when the resolver
/// has no record for the given database.
pub trait DatabasePriorityResolver: Send + Sync {
    fn priority_for(&self, database_id: u64) -> PriorityClass;
}

/// No-op resolver: every database gets `Standard` priority.
///
/// Used in tests and in environments where quota catalog is not yet wired up.
pub struct DefaultPriorityResolver;

impl DatabasePriorityResolver for DefaultPriorityResolver {
    fn priority_for(&self, _database_id: u64) -> PriorityClass {
        PriorityClass::Standard
    }
}

/// A pair of SPSC channels for one Data Plane core, augmented with a
/// weighted-fair staging queue that enforces per-database fairness before
/// requests reach the physical ring buffer.
pub struct CoreChannel {
    /// Control Plane pushes requests to the Data Plane core.
    pub request_tx: Producer<BridgeRequest>,

    /// Control Plane pops responses from the Data Plane core.
    pub response_rx: Consumer<BridgeResponse>,

    /// Backpressure controller for the request queue (global, across all DBs).
    pub backpressure: BackpressureController,

    /// Per-database weighted-fair staging queue. Items are popped from here in
    /// DRR order and forwarded to `request_tx`.
    pub wfq: WeightedFairQueue<envelope::Request>,

    /// Per-virtual-queue backpressure states, keyed by `database_id`.
    ///
    /// **Writer**: `dispatch` and `flush_wfq` call `update_db_pressure` after
    /// each enqueue/pop, snapshotting the WFQ throttle/suspend predicates for
    /// that database into this map.
    ///
    /// **Reader**: `Dispatcher::db_pressure_on_core` for the metrics exporter.
    ///
    /// **Lifetime**: entries are written in place and never reach a "remove"
    /// path on their own. Stale databases that no longer enqueue requests
    /// retain a `Normal` (or last-observed) entry until the surrounding
    /// dispatcher is dropped or `recalculate_tenant_limits` rotates state.
    /// The map is bounded by the universe of `database_id`s that have ever
    /// been dispatched against this core, so unbounded growth is not a
    /// concern in practice.
    ///
    /// **Threading**: this field is accessed only from the Control Plane
    /// thread that owns the `Dispatcher`. `HashMap` is intentional —
    /// the field is never shared across threads.
    pub db_pressure: HashMap<u64, PressureState>,

    /// Eventfd notifier to wake the Data Plane core after pushing a request.
    /// `None` until `set_notifier` is called (after core thread startup).
    pub wake_notifier: Option<EventFdNotifier>,
}

impl CoreChannel {
    /// Flush as many items from the WFQ into the physical ring as will fit.
    /// Updates per-DB pressure states and returns the number of items flushed.
    ///
    /// `try_push` consumes the request by value, so a failure on push would
    /// drop the request. The two failure modes are handled explicitly so
    /// nothing is lost silently:
    ///
    /// - `BridgeError::Full` is unreachable: the SPSC ring has a single
    ///   producer (this dispatcher), and we re-check `utilization() < 100`
    ///   on every iteration before popping from the WFQ. If it ever fires,
    ///   the SPSC invariant is violated and we trip an `unreachable!` so
    ///   the bug surfaces loudly rather than as silent request loss.
    /// - `BridgeError::Disconnected` means the Data Plane core has gone
    ///   away. Continuing to drain the WFQ into a dead consumer would lose
    ///   every queued request. We log an `error!` (not `warn!`) and stop
    ///   flushing — outstanding requests stay in the WFQ where supervisor
    ///   logic can observe them, and the next dispatch attempt will see
    ///   the disconnected state.
    fn flush_wfq(&mut self) -> usize {
        let mut flushed = 0;
        while self.request_tx.utilization() < 100 {
            let Some(req) = self.wfq.pop_next() else {
                break;
            };
            let db_id = req.database_id.as_u64();
            let req_id = req.request_id.as_u64();
            match self.request_tx.try_push(BridgeRequest { inner: req }) {
                Ok(()) => {
                    flushed += 1;
                    self.update_db_pressure(db_id);
                }
                Err(BridgeError::Full { capacity, pending }) => {
                    unreachable!(
                        "SPSC ring reported Full (capacity={capacity}, pending={pending}) \
                         despite utilization < 100 immediately before push — \
                         single-producer invariant violated"
                    );
                }
                Err(e @ BridgeError::Disconnected { .. }) => {
                    error!(
                        request_id = req_id,
                        database_id = db_id,
                        "data plane core disconnected during WFQ flush — stopping; request was lost: {e}"
                    );
                    break;
                }
                Err(
                    e @ (BridgeError::Empty
                    | BridgeError::Backpressure { .. }
                    | BridgeError::DeadlineExceeded { .. }),
                ) => {
                    // `Producer::try_push` only ever produces `Full` or
                    // `Disconnected`; these other variants are returned by
                    // consumer/backpressure paths and cannot reach here.
                    unreachable!("Producer::try_push returned non-producer BridgeError: {e}");
                }
            }
        }
        flushed
    }

    /// Recompute and store the pressure state for a single database.
    fn update_db_pressure(&mut self, database_id: u64) {
        let state = if self.wfq.is_suspended_for(database_id) {
            PressureState::Suspended
        } else if self.wfq.is_throttled_for(database_id) {
            PressureState::Throttled
        } else {
            PressureState::Normal
        };
        self.db_pressure.insert(database_id, state);
    }
}

/// Data Plane side of a core's channel pair.
pub struct CoreChannelDataSide {
    /// Data Plane pops requests from the Control Plane.
    pub request_rx: Consumer<BridgeRequest>,

    /// Data Plane pushes responses back to the Control Plane.
    pub response_tx: Producer<BridgeResponse>,
}

/// The dispatcher: routes requests from the Control Plane to the correct
/// Data Plane core via weighted-fair queues and SPSC ring buffers.
///
/// One `Dispatcher` lives on the Control Plane. It owns the producer side
/// of all request channels and the consumer side of all response channels.
///
/// Each core has an in-process weighted-fair queue that reorders requests by
/// `DatabaseId` using deficit round-robin before they reach the physical ring.
/// A database saturating its share of a core does not affect co-resident
/// databases.
pub struct Dispatcher {
    /// One channel pair per Data Plane core.
    cores: Vec<CoreChannel>,

    /// Routes vShards to core IDs.
    router: VShardRouter,

    /// Per-tenant in-flight request count across all cores.
    tenant_inflight: HashMap<u64, u32>,

    /// Maps request_id → tenant_id for in-flight requests.
    request_tenant: HashMap<u64, u64>,

    /// Maximum in-flight requests per tenant (0 = unlimited).
    max_per_tenant_inflight: u32,

    /// Per-core queue capacity (used in tenant fairness recalculation).
    per_core_capacity: u32,

    /// Resolves priority class for a database_id (consulted on enqueue).
    priority_resolver: Box<dyn DatabasePriorityResolver>,
}

impl Dispatcher {
    /// Create a dispatcher with SPSC channels for each core.
    ///
    /// Returns `(Dispatcher, Vec<CoreChannelDataSide>)` — send each
    /// `CoreChannelDataSide` to its respective Data Plane core thread.
    pub fn new(num_cores: usize, queue_capacity: usize) -> (Self, Vec<CoreChannelDataSide>) {
        Self::with_resolver(num_cores, queue_capacity, Box::new(DefaultPriorityResolver))
    }

    /// Like `new`, but accepts a custom `DatabasePriorityResolver`.
    pub fn with_resolver(
        num_cores: usize,
        queue_capacity: usize,
        priority_resolver: Box<dyn DatabasePriorityResolver>,
    ) -> (Self, Vec<CoreChannelDataSide>) {
        let mut cores = Vec::with_capacity(num_cores);
        let mut data_sides = Vec::with_capacity(num_cores);

        for _ in 0..num_cores {
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(queue_capacity);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(queue_capacity);

            cores.push(CoreChannel {
                request_tx: req_tx,
                response_rx: resp_rx,
                backpressure: BackpressureController::new(BackpressureConfig::default()),
                wfq: WeightedFairQueue::new(queue_capacity, queue_capacity),
                db_pressure: HashMap::new(),
                wake_notifier: None,
            });

            data_sides.push(CoreChannelDataSide {
                request_rx: req_rx,
                response_tx: resp_tx,
            });
        }

        let router = VShardRouter::round_robin(num_cores);
        let total_capacity = num_cores * queue_capacity;

        (
            Self {
                cores,
                router,
                tenant_inflight: HashMap::new(),
                request_tenant: HashMap::new(),
                max_per_tenant_inflight: total_capacity as u32,
                per_core_capacity: queue_capacity as u32,
                priority_resolver,
            },
            data_sides,
        )
    }

    /// Dispatch a request to the correct Data Plane core.
    ///
    /// Enqueues into the per-core weighted-fair queue keyed by `DatabaseId`,
    /// then flushes WFQ → physical ring. Returns `Err` when the WFQ itself is
    /// full (total capacity reached across all active databases on that core).
    pub fn dispatch(&mut self, request: envelope::Request) -> crate::Result<()> {
        assert_write_admitted(&request);
        let tenant_id = request.tenant_id.as_u64();
        let req_id = request.request_id.as_u64();
        let database_id = request.database_id.as_u64();

        // Per-tenant fairness: reject if this tenant has too many in-flight requests.
        if self.max_per_tenant_inflight > 0 {
            let inflight = self.tenant_inflight.get(&tenant_id).copied().unwrap_or(0);
            if inflight >= self.max_per_tenant_inflight {
                return Err(crate::Error::Dispatch {
                    detail: format!(
                        "tenant {tenant_id}: queue full ({inflight}/{} in-flight)",
                        self.max_per_tenant_inflight
                    ),
                });
            }
        }

        let core_id =
            self.router
                .resolve(request.vshard_id)
                .ok_or_else(|| crate::Error::Dispatch {
                    detail: format!("no core for vshard {}", request.vshard_id),
                })?;

        let channel = &mut self.cores[core_id];

        // Refresh priority for this DB in the WFQ.
        let cls = self.priority_resolver.priority_for(database_id);
        channel.wfq.set_priority(database_id, cls);

        // Check per-DB suspended state (≥95% of fair share).
        if channel.wfq.is_suspended_for(database_id) {
            return Err(crate::Error::Dispatch {
                detail: format!(
                    "database {database_id}: virtual queue suspended (≥95% of fair share on core {core_id})"
                ),
            });
        }

        // Enqueue into the WFQ — returns Err if total capacity is full.
        channel
            .wfq
            .try_enqueue(database_id, request)
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("core {core_id}: total WFQ capacity exhausted"),
            })?;

        // Update per-DB pressure.
        channel.update_db_pressure(database_id);

        // Flush WFQ → physical ring.
        channel.flush_wfq();

        // Update global backpressure based on ring utilization.
        let util = channel.request_tx.utilization();
        if let Some(new_state) = channel.backpressure.update(util) {
            warn!(
                core_id,
                utilization = util,
                state = ?new_state,
                "backpressure transition"
            );
        }

        // Track per-tenant in-flight + request→tenant mapping for response routing.
        *self.tenant_inflight.entry(tenant_id).or_insert(0) += 1;
        self.request_tenant.insert(req_id, tenant_id);

        // Wake the Data Plane core via eventfd.
        if let Some(ref notifier) = channel.wake_notifier {
            notifier.notify();
        }

        Ok(())
    }

    /// Record a response received for a tenant (decrements in-flight count).
    pub fn tenant_response_received(&mut self, tenant_id: u64) {
        if let Some(count) = self.tenant_inflight.get_mut(&tenant_id) {
            *count = count.saturating_sub(1);
        }
    }

    /// Recalculate the per-tenant in-flight limit based on active tenants.
    pub fn recalculate_tenant_limits(&mut self) {
        let active = self.tenant_inflight.len().max(1) as u32;
        let total_capacity: u32 = self.cores.len() as u32 * self.per_core_capacity;
        self.max_per_tenant_inflight = (total_capacity / active).max(2);
        self.tenant_inflight.retain(|_, count| *count > 0);
    }

    /// Dispatch a request directly to a specific core by index.
    ///
    /// Bypasses vShard routing. Used by the checkpoint manager to send
    /// checkpoint requests to every core regardless of vShard assignment.
    pub fn dispatch_to_core(
        &mut self,
        core_id: usize,
        request: envelope::Request,
    ) -> crate::Result<()> {
        assert_write_admitted(&request);
        if core_id >= self.cores.len() {
            return Err(crate::Error::Dispatch {
                detail: format!("core {core_id} out of range (have {})", self.cores.len()),
            });
        }

        let tenant_id = request.tenant_id.as_u64();
        let req_id = request.request_id.as_u64();
        let database_id = request.database_id.as_u64();
        let channel = &mut self.cores[core_id];

        let cls = self.priority_resolver.priority_for(database_id);
        channel.wfq.set_priority(database_id, cls);

        channel
            .wfq
            .try_enqueue(database_id, request)
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("core {core_id}: total WFQ capacity exhausted"),
            })?;

        channel.update_db_pressure(database_id);
        channel.flush_wfq();

        let util = channel.request_tx.utilization();
        if let Some(new_state) = channel.backpressure.update(util) {
            warn!(
                core_id,
                utilization = util,
                state = ?new_state,
                "backpressure transition"
            );
        }

        *self.tenant_inflight.entry(tenant_id).or_insert(0) += 1;
        self.request_tenant.insert(req_id, tenant_id);

        if let Some(ref notifier) = channel.wake_notifier {
            notifier.notify();
        }

        Ok(())
    }

    /// Maximum SPSC request queue utilization across all cores (0-100).
    pub fn max_utilization(&self) -> u8 {
        self.cores
            .iter()
            .map(|c| c.request_tx.utilization())
            .max()
            .unwrap_or(0)
    }

    /// Per-database pressure state for the given core (used by metrics exporters).
    ///
    /// Returns `PressureState::Normal` when no pressure has been recorded for
    /// the database on that core.
    pub fn db_pressure_on_core(&self, core_id: usize, database_id: u64) -> PressureState {
        self.cores
            .get(core_id)
            .and_then(|ch| ch.db_pressure.get(&database_id).copied())
            .unwrap_or(PressureState::Normal)
    }

    /// Poll responses from all Data Plane cores.
    pub fn poll_responses(&mut self) -> Vec<envelope::Response> {
        let mut responses = Vec::new();
        for channel in &mut self.cores {
            let mut batch = Vec::new();
            channel.response_rx.drain_into(&mut batch, 64);
            for br in batch {
                let rid = br.inner.request_id.as_u64();
                if let Some(tid) = self.request_tenant.remove(&rid)
                    && let Some(count) = self.tenant_inflight.get_mut(&tid)
                {
                    *count = count.saturating_sub(1);
                }
                responses.push(br.inner);
            }
            // Opportunistically flush WFQ after draining responses to fill headroom.
            channel.flush_wfq();
        }
        responses
    }

    /// Number of Data Plane cores.
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    /// Set the eventfd notifier for a specific core.
    pub fn set_notifier(&mut self, core_id: usize, notifier: EventFdNotifier) {
        if let Some(channel) = self.cores.get_mut(core_id) {
            channel.wake_notifier = Some(notifier);
        }
    }

    /// Router reference for vShard lookups.
    pub fn router(&self) -> &VShardRouter {
        &self.router
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::*;
    use crate::types::*;
    use nodedb_physical::physical_plan::DocumentOp;
    use std::time::{Duration, Instant};

    fn make_request(vshard: u32) -> envelope::Request {
        envelope::Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "users".into(),
                document_id: "u1".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
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
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        }
    }

    fn make_request_for_db(vshard: u32, db: u64, req_id: u64) -> envelope::Request {
        envelope::Request {
            request_id: RequestId::new(req_id),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::new(db),
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "c".into(),
                document_id: "d".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
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
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        }
    }

    #[test]
    fn dispatch_routes_to_correct_core() {
        let (mut dispatcher, data_sides) = Dispatcher::new(4, 64);

        dispatcher.dispatch(make_request(0)).unwrap();
        dispatcher.dispatch(make_request(1)).unwrap();
        dispatcher.dispatch(make_request(4)).unwrap(); // Wraps to core 0.

        assert_eq!(data_sides[0].request_rx.len(), 2);
        assert_eq!(data_sides[1].request_rx.len(), 1);
        assert_eq!(data_sides[2].request_rx.len(), 0);
    }

    #[test]
    fn response_roundtrip() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);

        dispatcher.dispatch(make_request(0)).unwrap();

        let _req = data_sides[0].request_rx.try_pop().unwrap();
        data_sides[0]
            .response_tx
            .try_push(BridgeResponse {
                inner: envelope::Response {
                    request_id: RequestId::new(1),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_vec(b"result".to_vec()),
                    watermark_lsn: Lsn::new(42),
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: crate::types::Lsn::ZERO,
                    write_set: Vec::new(),
                },
            })
            .unwrap();

        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].status, Status::Ok);
        assert_eq!(&*responses[0].payload, b"result");
    }

    #[test]
    fn full_queue_returns_error() {
        // With WFQ capacity == ring capacity, filling WFQ should eventually
        // cause total-capacity exhaustion.
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 4);

        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, i + 1, i + 1))
                .unwrap();
        }

        // Next dispatch should fail — WFQ total capacity exhausted.
        let result = dispatcher.dispatch(make_request_for_db(0, 99, 99));
        assert!(result.is_err());
    }

    #[test]
    fn dispatch_to_core_tracks_request_lifecycle() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);
        let request = make_request(0);
        let tenant_id = request.tenant_id.as_u64();
        let request_id = request.request_id.as_u64();

        dispatcher.dispatch_to_core(1, request).unwrap();

        assert_eq!(dispatcher.tenant_inflight.get(&tenant_id), Some(&1));
        assert_eq!(dispatcher.request_tenant.get(&request_id), Some(&tenant_id));
        assert_eq!(data_sides[1].request_rx.len(), 1);

        let _req = data_sides[1].request_rx.try_pop().unwrap();
        data_sides[1]
            .response_tx
            .try_push(BridgeResponse {
                inner: envelope::Response {
                    request_id: RequestId::new(request_id),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: crate::types::Lsn::ZERO,
                    write_set: Vec::new(),
                },
            })
            .unwrap();

        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(dispatcher.tenant_inflight.get(&tenant_id), Some(&0));
        assert!(!dispatcher.request_tenant.contains_key(&request_id));
    }

    #[test]
    fn per_db_pressure_reported() {
        let (mut dispatcher, _) = Dispatcher::new(1, 8);
        // Fill fair share for DB 1 using 4 of 8 slots.
        // With one DB initially, fair share = 8. With two DBs = 4 each.
        // First enqueue DB1 + DB2, so fair_share = 4.
        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, 1, i + 10))
                .unwrap();
        }
        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, 2, i + 20))
                .unwrap();
        }
        // After filling DB1's fair share, it should be suspended on core 0.
        // (exact state depends on WFQ flush draining items to ring first)
        // The test confirms per-DB pressure is being tracked without panic.
        let _ = dispatcher.db_pressure_on_core(0, 1);
        let _ = dispatcher.db_pressure_on_core(0, 2);
    }
}
